import json

from models import AggregatedTradeEvent


def serialize_aggregated_trade_event(
    event: AggregatedTradeEvent,
) -> bytes:
    payload = {
        "aggregate_trade_id": event.aggregate_trade_id,
        "symbol": event.symbol,
        "price": str(event.price),
        "quantity": str(event.quantity),
        "event_time": event.event_time.isoformat(),
        "trade_time": event.trade_time.isoformat(),
        "first_trade_id": event.first_trade_id,
        "last_trade_id": event.last_trade_id,
        "buyer_is_market_maker": event.buyer_is_market_maker,
        "received_at": event.received_at.isoformat(),
    }

    json_message = json.dumps(payload)

    return json_message.encode("utf-8")