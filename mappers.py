from datetime import UTC, datetime
from decimal import Decimal
from models import AggregatedTradeEvent
from models import ClickHouseTradeRow
from typing import Any

def create_aggregated_trade_event(data: dict, received_at: datetime)-> AggregatedTradeEvent:
    return AggregatedTradeEvent(
        aggregate_trade_id=data["a"],
        symbol=data["s"],
        price=Decimal(data["p"]),
        quantity=Decimal(data["q"]),
        event_time=datetime.fromtimestamp(data["E"] / 1000, tz=UTC,),
        trade_time=datetime.fromtimestamp(data["T"] / 1000, tz=UTC,),
        first_trade_id=data["f"],
        last_trade_id=data["l"],
        buyer_is_market_maker=data["m"],
        received_at=received_at,

    )

def map_to_clickhouse_row(data: dict, partition:int, offset:int)-> ClickHouseTradeRow:
    return ClickHouseTradeRow(
        aggregate_trade_id=int(data["aggregate_trade_id"]),
        symbol=data["symbol"],
        price=Decimal(data["price"]),
        quantity=Decimal(data["quantity"]),
        event_time=datetime.fromisoformat(data["event_time"]),
        trade_time=datetime.fromisoformat(data["trade_time"]),
        first_trade_id=int(data["first_trade_id"]),
        last_trade_id=int(data["last_trade_id"]),
        buyer_is_market_maker=data["buyer_is_market_maker"],
        received_at=datetime.fromisoformat(data["received_at"]),
        kafka_partition=partition,
        kafka_offset=offset,
    )