import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer
import clickhouse_connect
from mappers import map_to_clickhouse_row
from decimal import InvalidOperation

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

CONSUMER_GROUP_ID = "storage-writer-debug-v1"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]


async def start_kafka_consumer() -> AIOKafkaConsumer:

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

def parse_and_validate_kafka_message(message: bytes) -> dict:
    try:
        data = json.loads(message.value.decode("utf-8"))

        row = map_to_clickhouse_row(data)

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        InvalidOperation,
    ) as error:
        logger.error(
            "Invalid Kafka message: partition=%s offset=%s error=%s",
            message.partition,
            message.offset,
            error,
        )
        return None
    return row
        

async def consume_messages(clickhouse_client: clickhouse_connect) -> None:
    consumer = start_kafka_consumer()

    try:
        while True:
            records_by_partition = await consumer.getmany(
                timeout_ms=1000,
                max_records=100,
            )
            batch = []
            for partition, messages in records_by_partition.items():
                for message in messages:

                    row = parse_and_validate_kafka_message(message)

                    if row is not None:
                        batch.append(row)

            await clickhouse_client.insert_batch()
                

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