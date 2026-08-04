from datetime import UTC, datetime
from decimal import Decimal

import pytest

from exceptions import InvalidTradeMessage
from mappers import map_to_clickhouse_row


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


def test_map_to_clickhouse_row():
    row = map_to_clickhouse_row(
        data=valid_kafka_payload(),
        partition=2,
        offset=100,
    )

    assert row.aggregate_trade_id == 4023160398
    assert row.symbol == "BTCUSDT"
    assert row.price == Decimal("65340.00000000")
    assert row.quantity == Decimal("0.00765000")

    assert row.event_time == datetime(
        2026,
        7,
        27,
        14,
        8,
        48,
        336000,
        tzinfo=UTC,
    )

    assert row.buyer_is_market_maker is True
    assert row.kafka_partition == 2
    assert row.kafka_offset == 100


def test_invalid_price_raises_invalid_trade_message():
    payload = valid_kafka_payload()
    payload["price"] = "wrong price"

    with pytest.raises(InvalidTradeMessage):
        map_to_clickhouse_row(
            data=payload,
            partition=2,
            offset=100,
        )