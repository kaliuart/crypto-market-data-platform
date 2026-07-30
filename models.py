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
class ClickHouseTradeRow:
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
    kafka_partition: int
    kafka_offset: int