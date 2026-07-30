import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer
import clickhouse_connect
from mappers import map_to_clickhouse_row

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

CONSUMER_GROUP_ID = "storage-writer-debug-v1"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]

async def consume_messages(clickhouse_client: clickhouse_connect) -> None:

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
            data = json.loads(message.value.decode("UTF-8"))

            row = map_to_clickhouse_row(data, message.partition, message.offset)

            print(row)

    finally:
        await consumer.stop()
    

async def main() -> None:
    async with await clickhouse_connect.get_async_client(
        host="localhost",
        port=8123,
        username="default",
        password=CLICKHOUSE_PASSWORD,
        database="market_data",
    ) as clickhouse_client:

        await clickhouse_client.query("SELECT 1")

        await consume_messages(clickhouse_client)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Storage writer stopped by user")