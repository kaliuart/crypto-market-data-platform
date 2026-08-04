import asyncio
import logging
import os


from aiokafka import AIOKafkaConsumer
from exceptions import InvalidTradeMessage
import clickhouse_connect
from mappers import (map_to_clickhouse_row, row_to_clickhouse_values)
from validators import parse_and_validate_kafka_message

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

CONSUMER_GROUP_ID = "storage-writer-v1"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_TABLE_NAME = "agg_trades"
CLICKHOUSE_COLUMNS = [
    "aggregate_trade_id",
    "symbol",
    "price",
    "quantity",
    "event_time",
    "trade_time",
    "received_at",
    "first_trade_id",
    "last_trade_id",
    "buyer_is_market_maker",
    "kafka_partition",
    "kafka_offset",
]

async def start_kafka_consumer() -> AIOKafkaConsumer:

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    await consumer.start()

    logger.info(
        "Kafka consumer started: topic=%s group=%s",
        KAFKA_TOPIC,
        CONSUMER_GROUP_ID,
    )
    return consumer


async def insert_trade_batch(
        clickhouse_client,
        batch: list,
) -> None:

    data = [
        row_to_clickhouse_values(row) for row in batch
    ]

    await clickhouse_client.insert(
        table=CLICKHOUSE_TABLE_NAME,
        data=data,
        column_names=CLICKHOUSE_COLUMNS
    )

async def consume_messages(clickhouse_client) -> None:
    consumer = await start_kafka_consumer()

    try:
        while True:

            records_by_partition = await consumer.getmany(
                timeout_ms=1000,
                max_records=100,
            )

            if not records_by_partition:
                continue


            offsets_to_commit = {}
            batch = []

            for topic_partition, messages in records_by_partition.items():
                for message in messages:
                    try:
                        data = parse_and_validate_kafka_message(message)
                        row = map_to_clickhouse_row(
                            data=data,
                            partition=message.partition,
                            offset=message.offset
                        )
                        

                    except InvalidTradeMessage as error:
                            logger.warning(
                                "Skipping invalid Kafka message: partition=%s offset=%s error=%s",
                                message.partition,
                                message.offset,
                                error,
                            )
                            continue

                    batch.append(row)

                if messages:
                    offsets_to_commit[topic_partition] = (
                        messages[-1].offset + 1
                    )

            if batch:
                await insert_trade_batch(
                    clickhouse_client=clickhouse_client,
                    batch=batch
                )

            if offsets_to_commit:
                await consumer.commit(offsets_to_commit)

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
    except Exception:
        logger.exception("Storage writer stopped because of infrastructure failure")
        raise