import json
from types import SimpleNamespace

import pytest

from exceptions import InvalidTradeMessage
from validators import parse_and_validate_kafka_message


def valid_kafka_payload():
    return {
        "aggregate_trade_id": 4023160398,
        "symbol": "BTCUSDT",
        "price": "65340.00000000",
        "quantity": "0.00765000",
        "event_time": "2026-07-27T14:08:48.336000+00:00",
        "trade_time": "2026-07-27T14:08:48.335000+00:00",
        "first_trade_id": 6536456692,
        "last_trade_id": 6536456693,
        "buyer_is_market_maker": True,
        "received_at": "2026-07-27T14:08:49+00:00",
    }


def test_valid_kafka_message():
    message = SimpleNamespace(
        value=json.dumps(valid_kafka_payload()).encode("utf-8")
    )

    data = parse_and_validate_kafka_message(message)

    assert data["symbol"] == "BTCUSDT"
    assert data["aggregate_trade_id"] == 4023160398


def test_invalid_kafka_json():
    message = SimpleNamespace(value=b"not json")

    with pytest.raises(InvalidTradeMessage):
        parse_and_validate_kafka_message(message)


def test_kafka_message_without_symbol():
    payload = valid_kafka_payload()
    del payload["symbol"]

    message = SimpleNamespace(
        value=json.dumps(payload).encode("utf-8")
    )

    with pytest.raises(InvalidTradeMessage):
        parse_and_validate_kafka_message(message)


def test_kafka_message_with_none_value():
    message = SimpleNamespace(value=None)

    with pytest.raises(InvalidTradeMessage):
        parse_and_validate_kafka_message(message)