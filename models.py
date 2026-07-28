from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

@dataclass(frozen=True)
class AggregatedTradeEvent:
    aggregate_trade_id: int
    symbol: str
    price: Decimal
    quantity: Decimal
    event_time: datetime
    trade_time: datetime
    first_trade_id: int
    last_trade_id: int
    buyer_is_market_maker: bool
    received_at: datetime

@dataclass(frozen=True)
class TradeMetrics:
    trade_to_event_ms: float
    event_to_collector_ms: float
    exchange_to_collector_ms: float
    collector_processing_ms: float
    kafka_ack_latency_ms: float
    exchange_to_kafka_ms: float