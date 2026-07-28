from mappers import create_aggregated_trade_event
from datetime import UTC
from datetime import datetime
from decimal import Decimal

def test_maps_binance_fields_to_trade_event():
    data = {"e": "aggTrade", "E": 1785161328336, "s": "BTCUSDT", "a": 4023160398, "p": "65340.00000000", "q": "0.00765000", "f": 6536456692, "l": 6536456693, "T": 1785161328335, "m": True, "M": True}
    received_at = datetime.now(UTC)
    trade_event = create_aggregated_trade_event(data, received_at)

    assert trade_event.aggregate_trade_id == 4023160398 
    assert trade_event.first_trade_id == 6536456692
    assert trade_event.last_trade_id == 6536456693
    assert trade_event.buyer_is_market_maker is True
    assert trade_event.symbol == "BTCUSDT"
    assert trade_event.received_at == received_at

def test_converts_price_and_quantity_to_decimal():
    data = {"e": "aggTrade", "E": 1785161328336, "s": "BTCUSDT", "a": 4023160398, "p": "65340.00000000", "q": "0.00765000", "f": 6536456692, "l": 6536456693, "T": 1785161328335, "m": True, "M": True}
    trade_event = create_aggregated_trade_event(data, datetime.now(UTC))

    assert trade_event.price == Decimal("65340.00000000")
    assert trade_event.quantity == Decimal("0.00765000")

    assert isinstance(trade_event.price, Decimal)
    assert isinstance(trade_event.quantity, Decimal)

def test_converts_millisecond_timestamps_to_utc_datetime():
    data = {"e": "aggTrade", "E": 1785161328336, "s": "BTCUSDT", "a": 4023160398, "p": "65340.00000000", "q": "0.00765000", "f": 6536456692, "l": 6536456693, "T": 1785161328335, "m": True, "M": True}
    trade_event = create_aggregated_trade_event(data, datetime.now(UTC))

    assert isinstance(trade_event.event_time, datetime)
    assert isinstance(trade_event.trade_time, datetime)

    assert trade_event.event_time == datetime(
        2026, 7, 27, 14, 8, 48, 336000, tzinfo=UTC
    )

    assert trade_event.trade_time == datetime(
        2026, 7, 27, 14, 8, 48, 335000, tzinfo=UTC
    )   
    
    assert trade_event.event_time.tzinfo is UTC
    assert trade_event.trade_time.tzinfo is UTC
