import asyncio
import logging
import os
import time

from metrics.storage_writer_metrics import (
    BATCH_SIZE,
    CLICKHOUSE_INSERT_DURATION,
    COMMIT_FAILURES,
    INSERT_FAILURES,
    INVALID_MESSAGES,
    KAFKA_COMMIT_DURATION,
    CONSUMED_MESSAGES,
    METRICS_PORT,
    TRADES_INSERTED,
    start_storage_writer_metrics_server,
)

from aiokafka import AIOKafkaConsumer
from exceptions import InvalidTradeMessage
import clickhouse_connect
from mappers import (map_to_clickhouse_row, row_to_clickhouse_values)
from validators import parse_and_validate_kafka_message

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

CONSUMER_GROUP_ID = "storage-writer-v1"

MAX_BATCH_SIZE = 100
MAX_BATCH_WAIT_SECONDS = 2.0
KAFKA_POLL_TIMEOUT_MS = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

#CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"] 
CLICKHOUSE_PASSWORD="strongpassword"
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


async def commit_offsets(
    consumer: AIOKafkaConsumer,
    offsets_to_commit: dict,
) -> None:
    try:
        with KAFKA_COMMIT_DURATION.time():
            await consumer.commit(offsets_to_commit)
    except Exception:
        COMMIT_FAILURES.inc()
        raise


async def flush_batch(
    clickhouse_client,
    consumer: AIOKafkaConsumer,
    batch: list,
    offsets_to_commit: dict,
) -> None:
    BATCH_SIZE.observe(len(batch))

    try:
        with CLICKHOUSE_INSERT_DURATION.time():
            await insert_trade_batch(
                clickhouse_client=clickhouse_client,
                batch=batch,
            )
    except Exception:
        INSERT_FAILURES.inc()
        raise

    TRADES_INSERTED.inc(len(batch))

    await commit_offsets(
        consumer=consumer,
        offsets_to_commit=offsets_to_commit,
    )

def collect_records(
    records_by_partition,
    batch: list,
    offsets_to_commit: dict,
    batch_started_at: float | None,
) -> float | None:
    
    for topic_partition, messages in records_by_partition.items():
        for message in messages:
            CONSUMED_MESSAGES.inc()

            try:
                data = parse_and_validate_kafka_message(message)

                row = map_to_clickhouse_row(
                    data=data,
                    partition=message.partition,
                    offset=message.offset,
                )

            except InvalidTradeMessage as error:
                INVALID_MESSAGES.inc()

                logger.warning(
                    "Skipping invalid Kafka message: "
                    "partition=%s offset=%s error=%s",
                    message.partition,
                    message.offset,
                    error,
                )
                continue

            if batch_started_at is None:
                batch_started_at = time.monotonic()

            batch.append(row)

        offsets_to_commit[topic_partition] = (
            messages[-1].offset + 1
        )

    return batch_started_at


def should_flush_batch(
    batch: list,
    batch_started_at: float | None,
    now: float,
) -> bool:
    if not batch:
        return False

    if batch_started_at is None:
        raise RuntimeError(
            "Non-empty batch must have batch_started_at"
        )

    batch_is_full = len(batch) >= MAX_BATCH_SIZE

    batch_wait_expired = (
        now - batch_started_at >= MAX_BATCH_WAIT_SECONDS
    )

    return batch_is_full or batch_wait_expired


async def consume_messages(clickhouse_client) -> None:
    consumer = await start_kafka_consumer()

    batch = []
    offsets_to_commit = {}
    batch_started_at = None

    try:
        while True:

            remaining_capacity = MAX_BATCH_SIZE - len(batch)

            records_by_partition = await consumer.getmany(
                timeout_ms=KAFKA_POLL_TIMEOUT_MS,
                max_records=remaining_capacity,
            )

            batch_started_at = collect_records(
                batch=batch,
                offsets_to_commit=offsets_to_commit,
                batch_started_at=batch_started_at,
                records_by_partition=records_by_partition,
            )

            should_flush = should_flush_batch(
                batch_started_at=batch_started_at,
                batch=batch,
                now=time.monotonic(),
            )

            if should_flush:
                await flush_batch(
                    clickhouse_client=clickhouse_client,
                    consumer=consumer,
                    batch=batch,
                    offsets_to_commit=offsets_to_commit,
                )

                batch.clear()
                offsets_to_commit.clear()
                batch_started_at = None

            elif offsets_to_commit and not batch:

                await commit_offsets(
                    consumer=consumer,
                    offsets_to_commit=offsets_to_commit,
                )

                offsets_to_commit.clear()

    finally:
        await consumer.stop()
    

async def main() -> None:
    start_storage_writer_metrics_server()

    logger.info(
        "Storage writer metrics server started: port=%s",
        METRICS_PORT,
    )

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
        raise SystemExit(1)
