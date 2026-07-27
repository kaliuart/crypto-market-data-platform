import json
from datetime import UTC, datetime
from decimal import Decimal

from models import AggregatedTradeEvent
from serializers import serialize_aggregated_trade_event


def create_test_event() -> AggregatedTradeEvent:
    return AggregatedTradeEvent(
        aggregate_trade_id=4023160398,
        symbol="BTCUSDT",
        price=Decimal("65340.00000000"),
        quantity=Decimal("0.00765000"),
        first_trade_id=6536456692,
        last_trade_id=6536456693,
        event_time=datetime(
            2026, 7, 27, 14, 8, 48, 336000, tzinfo=UTC
        ),
        trade_time=datetime(
            2026, 7, 27, 14, 8, 48, 335000, tzinfo=UTC
        ),
        buyer_is_market_maker=True,
        received_at=datetime(
            2026, 7, 27, 14, 8, 49, tzinfo=UTC
        ),
    )


def test_serializer_returns_bytes():
    event = create_test_event()

    result = serialize_aggregated_trade_event(event)

    assert isinstance(result, bytes)


def test_serializer_preserves_event_fields():
    event = create_test_event()

    result = serialize_aggregated_trade_event(event)
    payload = json.loads(result.decode("utf-8"))

    assert payload["aggregate_trade_id"] == 4023160398
    assert payload["symbol"] == "BTCUSDT"
    assert payload["first_trade_id"] == 6536456692
    assert payload["last_trade_id"] == 6536456693
    assert payload["buyer_is_market_maker"] is True


def test_serializer_converts_decimal_and_datetime_to_strings():
    event = create_test_event()

    result = serialize_aggregated_trade_event(event)
    payload = json.loads(result.decode("utf-8"))

    assert payload["price"] == "65340.00000000"
    assert payload["quantity"] == "0.00765000"

    assert payload["event_time"] == (
        "2026-07-27T14:08:48.336000+00:00"
    )
    assert payload["trade_time"] == (
        "2026-07-27T14:08:48.335000+00:00"
    )
    assert payload["received_at"] == (
        "2026-07-27T14:08:49+00:00"
    )