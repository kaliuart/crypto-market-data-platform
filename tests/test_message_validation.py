from main import parse_and_validate_message

def test_valid_aggregated_trade_returns_dictionary():
    message = '{"e": "aggTrade", "E": 1785161328336, "s": "BTCUSDT", "a": 4023160398, "p": "65340.00000000", "q": "0.00765000", "f": 6536456692, "l": 6536456693, "T": 1785161328335, "m": true, "M": true}'
    data = parse_and_validate_message(message)

    assert isinstance(data, dict)
    assert data["e"] == "aggTrade"
    assert data["a"] == 4023160398
    assert data["s"] == "BTCUSDT"

def test_invalid_json_returns_none():
    message = '{"e": "aggTrade", "E": 1785161328336, '
    data = parse_and_validate_message(message)
    assert data is None

def test_message_without_required_field_returns_none():
    message = '{"e": "aggTrade", "E": 1785161328336, "s": "BTCUSDT"}'
    data = parse_and_validate_message(message)

    assert data is None

def test_wrong_event_type_returns_none():
    message = '{"e": "not a trade", "E": 1785161328336, "s": "strange", "a": 4023160398, "p": "65340.00000000", "q": "0.00765000", "f": 6536456692, "l": 6536456693, "T": 1785161328335, "m": true, "M": true}'
    data = parse_and_validate_message(message)

    assert data is None
