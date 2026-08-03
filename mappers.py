from datetime import UTC, datetime
from models import AggregatedTradeEvent
from models import ClickHouseTradeRow
from exceptions import InvalidTradeMessage
from decimal import Decimal, InvalidOperation
from typing import Any



def create_aggregated_trade_event(
    data: dict[str, Any],
    received_at: datetime,
) -> AggregatedTradeEvent:


    try:
        price = Decimal(data["p"])
        quantity = Decimal(data["q"])

        event_time = datetime.fromtimestamp(
            data["E"] / 1000,
            tz=UTC,
        )
        trade_time = datetime.fromtimestamp(
            data["T"] / 1000,
            tz=UTC,
        )

    except (
        InvalidOperation,
        ValueError,
        OverflowError,
        OSError,
    ) as error:
        raise InvalidTradeMessage(
            f"Invalid trade values: {error}"
        ) from error

    if not price.is_finite() or price <= 0:
        raise InvalidTradeMessage(
            "price must be finite and positive"
        )

    if not quantity.is_finite() or quantity <= 0:
        raise InvalidTradeMessage(
            "quantity must be finite and positive"
        )

    if data["f"] > data["l"]:
        raise InvalidTradeMessage(
            "first_trade_id must not exceed last_trade_id"
        )

    return AggregatedTradeEvent(
        aggregate_trade_id=data["a"],
        symbol=data["s"],
        price=price,
        quantity=quantity,
        event_time=event_time,
        trade_time=trade_time,
        first_trade_id=data["f"],
        last_trade_id=data["l"],
        buyer_is_market_maker=data["m"],
        received_at=received_at,
    )

def map_to_clickhouse_row(data: dict, partition:int, offset:int)-> ClickHouseTradeRow:
    try:
        
        price = Decimal(data["price"])
        quantity = Decimal(data["quantity"])

        event_time =  datetime.fromisoformat(data["event_time"])
        trade_time =  datetime.fromisoformat(data["trade_time"])
        received_at = datetime.fromisoformat(data["received_at"])

        first_trade_id = int(data["first_trade_id"])
        last_trade_id = int(data["last_trade_id"])
        aggregated_trade_id = int(data["aggregate_trade_id"])

    except (
        InvalidOperation,
        ValueError,
        OverflowError,
        OSError,
    ) as error:
        raise InvalidTradeMessage(
            f"Invalid trade values: {error}"
        ) from error

    if not price.is_finite() or price <= 0:
        raise InvalidTradeMessage(
            "price must be finite and positive"
        )

    if not quantity.is_finite() or quantity <= 0:
        raise InvalidTradeMessage(
            "quantity must be finite and positive"
        )

    if first_trade_id > last_trade_id:
        raise InvalidTradeMessage(
            "first_trade_id must not exceed last_trade_id"
        )
    
        
    return ClickHouseTradeRow(
            aggregate_trade_id=aggregated_trade_id,
            symbol=data["symbol"],
            price=price,
            quantity=quantity,
            event_time=event_time,
            trade_time=trade_time,
            first_trade_id=first_trade_id,
            last_trade_id=last_trade_id,
            buyer_is_market_maker=data["buyer_is_market_maker"],
            received_at=received_at,
            kafka_partition=partition,
            kafka_offset=offset,
        )



def row_to_clickhouse_values(row) -> list:
    return [
        row.aggregate_trade_id,
        row.symbol,
        row.price,
        row.quantity,
        row.event_time,
        row.trade_time,
        row.received_at,
        row.first_trade_id,
        row.last_trade_id,
        row.buyer_is_market_maker,
        row.kafka_partition,
        row.kafka_offset,
    ]