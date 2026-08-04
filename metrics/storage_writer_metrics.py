from prometheus_client import Counter, Gauge, Histogram, start_http_server

METRICS_PORT = 8001
METRICS_NAMESPACE = "crypto_storage_writer"

"""messages_consumed_total
invalid_messages_total
trades_inserted_total
insert_failures_total
commit_failures_total
batch_size
clickhouse_insert_duration_seconds
kafka_commit_duration_seconds"""


CONSUMED_MESSAGES = Counter(
    "messages_consumed",
    "Number of messages received from Kafka",
    namespace=METRICS_NAMESPACE,
)

INVALID_MESSAGES = Counter(
    "invalid_messages",
    "Number of invalid Kafka messages skipped",
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
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

KAFKA_COMMIT_DURATION = Histogram(
    "kafka_commit_duration_seconds",
    "Duration of Kafka offset commits",
    namespace=METRICS_NAMESPACE,
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
)


def start_storage_writer_metrics_server() -> None:
    start_http_server(METRICS_PORT)
