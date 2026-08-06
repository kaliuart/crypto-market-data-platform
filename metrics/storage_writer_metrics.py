from prometheus_client import (
    Counter,
    Histogram,
    start_http_server,
)

METRICS_PORT = 8001
METRICS_NAMESPACE = "crypto_storage_writer"

CONSUMED_MESSAGES = Counter(
    "messages_consumed",
    "Number of messages received from Kafka",
    namespace=METRICS_NAMESPACE,
)

INVALID_MESSAGES = Counter(
    "invalid_messages",
    "Number of invalid Kafka messages skipped",
    namespace=METRICS_NAMESPACE,

)

TRADES_INSERTED = Counter(
    "trades_inserted",
    "Number of trade rows successfully inserted into ClickHouse",
    namespace=METRICS_NAMESPACE,
)

INSERT_FAILURES = Counter(
    "insert_failures",
    "Number of failed ClickHouse inserts",
    namespace=METRICS_NAMESPACE,
)

COMMIT_FAILURES = Counter(
    "commit_failures",
    "Number of failed Kafka offset commits",
    namespace=METRICS_NAMESPACE,
)

BATCH_SIZE = Histogram(
    "batch_size",
    "Number of rows in a ClickHouse insert batch",
    namespace=METRICS_NAMESPACE,
    buckets=(1, 5, 10, 25, 50, 100),
)

CLICKHOUSE_INSERT_DURATION = Histogram(
    "clickhouse_insert_duration_seconds",
    "Duration of ClickHouse batch inserts",
    namespace=METRICS_NAMESPACE,
    buckets=(
        0.005,
        0.010,
        0.025,
        0.050,
        0.075,
        0.100,
        0.150,
        0.200,
        0.250,
        0.350,
        0.500,
        1.000,
    ),
)

KAFKA_COMMIT_DURATION = Histogram(
    "kafka_commit_duration_seconds",
    "Duration of Kafka offset commits",
    namespace=METRICS_NAMESPACE,
    buckets=(
        0.0005,
        0.001,
        0.002,
        0.003,
        0.004,
        0.005,
        0.0075,
        0.010,
        0.025,
        0.050,
        0.100,
    ),
)


def start_storage_writer_metrics_server() -> None:
    start_http_server(METRICS_PORT)
