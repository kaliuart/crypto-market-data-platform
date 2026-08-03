import pytest

from exceptions import InvalidTradeMessage
from validators import parse_and_validate_binance_message


def test_valid_aggregated_trade_returns_dictionary():
    message = (
        '{"E": 1785161328336, '
        '"s": "BTCUSDT", '
        '"a": 4023160398, '
        '"p": "65340.00000000", '
        '"q": "0.00765000", '
        '"f": 6536456692, '
        '"l": 6536456693, '
        '"T": 1785161328335, '
        '"m": true}'
    )

    data = parse_and_validate_binance_message(message)

    assert isinstance(data, dict)
    assert data["a"] == 4023160398
    assert data["s"] == "BTCUSDT"


def test_invalid_json_raises_invalid_trade_message():
    message = '{"E": 1785161328336,'

    with pytest.raises(InvalidTradeMessage):
        parse_and_validate_binance_message(message)


def test_missing_required_field_raises_invalid_trade_message():
    message = (
        '{"E": 1785161328336, '
        '"s": "BTCUSDT"}'
    )

    with pytest.raises(InvalidTradeMessage):
        parse_and_validate_binance_message(message)