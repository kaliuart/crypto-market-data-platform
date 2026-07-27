from main import check_trade_sequence

def test_first_trade_is_accepted():
    previous_id = None
    current_id = 100
    result = check_trade_sequence(previous_id,current_id)

    assert result == (100, True)


def test_next_trade_is_accepted():
    previous_id = 100
    current_id = 101
    result = check_trade_sequence(previous_id,current_id)

    assert result == (101, True)


def test_duplicate_trade_is_rejected():
    previous_id = 100
    current_id = 100
    result = check_trade_sequence(previous_id,current_id)

    assert result == (100, False)


def test_trade_gap_is_detected_and_current_trade_is_accepted():
    previous_id = 100
    current_id = 105
    result = check_trade_sequence(previous_id,current_id)

    assert result == (105, True)


