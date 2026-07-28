from datetime import UTC, datetime
from decimal import Decimal
from models import AggregatedTradeEvent

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

    