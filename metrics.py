from prometheus_client import Counter, Gauge, Histogram, start_http_server
from dataclasses import dataclass
from models import AggregatedTradeEvent
from datetime import datetime

METRICS_PORT = 8000
METRICS_NAMESPACE = "crypto_collector"

@dataclass(frozen=True)
class TradeMetrics:
    trade_to_event_seconds: float
    event_to_collector_seconds: float
    exchange_to_collector_seconds: float
    collector_processing_seconds: float
    kafka_ack_latency_seconds: float
    exchange_to_kafka_seconds: float

def calculate_metrics(
    trade_event: AggregatedTradeEvent,
    *,
    processing_started: float,
    send_started: float,
    send_finished: float,
    kafka_acknowledged_at: datetime,
) -> TradeMetrics:
    return TradeMetrics(
        trade_to_event_seconds=(
            trade_event.event_time - trade_event.trade_time
        ).total_seconds(),
        event_to_collector_seconds=(
            trade_event.received_at - trade_event.event_time
        ).total_seconds(),
        exchange_to_collector_seconds=(
            trade_event.received_at - trade_event.trade_time
        ).total_seconds(),
        collector_processing_seconds=(
            send_started - processing_started
        ),
        kafka_ack_latency_seconds=(
            send_finished - send_started
        ),
        exchange_to_kafka_seconds=(
            kafka_acknowledged_at - trade_event.trade_time
        ).total_seconds()
    )

MESSAGES_RECEIVED = Counter(
    "messages_received",
    "Number of messages received from Binance WebSocket",
    namespace=METRICS_NAMESPACE,
)

TRADES_PUBLISHED = Counter(
    "trades_published",
    "Number of trades acknowledged by Kafka",
    namespace=METRICS_NAMESPACE,
)

QUEUE_SIZE = Gauge(
    "queue_size",
    "Current number of messages waiting in the collector queue",
    namespace=METRICS_NAMESPACE,
)

PIPELINE_STAGE_DURATION = Histogram(
    "pipeline_stage_duration_seconds",
    "Duration of collector pipeline stages",
    labelnames=("stage",),
    namespace=METRICS_NAMESPACE,
    buckets=(
        0.0001,
        0.00025,
        0.0005,
        0.001,
        0.0025,
        0.005,
        0.010,
        0.025,
        0.050,
        0.075,
        0.100,
        0.125,
        0.150,
        0.200,
        0.300,
        0.500,
        1.000,
    ),
)


def start_metrics_server() -> None:
    start_http_server(METRICS_PORT)

def observe_trade_metrics(metrics: TradeMetrics) -> None:
    stage_durations = {
        "trade_to_event": metrics.trade_to_event_seconds,
        "event_to_collector": metrics.event_to_collector_seconds,
        "exchange_to_collector": metrics.exchange_to_collector_seconds,
        "collector_processing": metrics.collector_processing_seconds,
        "kafka_ack": metrics.kafka_ack_latency_seconds,
        "exchange_to_kafka": metrics.exchange_to_kafka_seconds,
    }

    for stage, duration_seconds in stage_durations.items():
        PIPELINE_STAGE_DURATION.labels(
            stage=stage,
        ).observe(duration_seconds)