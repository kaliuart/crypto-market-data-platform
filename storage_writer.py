import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
import clickhouse_connect


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

CONSUMER_GROUP_ID = "storage-writer-debug-v1"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

async def consume_messages() -> None:
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )

    await consumer.start()
    logger.info(
        "Kafka consumer started: topic=%s group=%s",
        KAFKA_TOPIC,
        CONSUMER_GROUP_ID,
    )

    try:
        async for message in consumer:
            payload = json.loads(
                message.value.decode("UTF-8")
            )

            logger.info(
                "Consumed: partition=%d offset=%d "
                "trade_id=%s symbol=%s price=%s quantity=%s",
                message.partition,
                message.offset,
                payload["aggregate_trade_id"],
                payload["symbol"],
                payload["price"],
                payload["quantity"],
            )
    finally:
        consumer.stop()

async def check_clickhouse_connection() -> None:
    async with await clickhouse_connect.get_async_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="market_data",
    ) as client:
        await client.query("SELECT 1")

        logger.info(
            "ClickHouse connection established: database=market_data"
        )

async def main() -> None:
    await check_clickhouse_connection()
    await consume_messages()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Storage writer stopped by user")