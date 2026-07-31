
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
import json
from exceptions import InvalidTradeMessage
AGGREGATED_TRADE_SCHEMA = {
    "type": "object",
    "properties": {
        "e": {"const": "aggTrade"},
        "p": {"type": "string"},
        "q": {"type": "string"},
        "m": {"type": "boolean"},
        "s": {
            "type": "string",
            "minLength": 1,
        },
        "a": {
            "type": "integer",
            "minimum": 0,
        },
        "f": {
            "type": "integer",
            "minimum": 0,
        },
        "l": {
            "type": "integer",
            "minimum": 0,
        },
        "E": {
            "type": "integer",
            "minimum": 0,
        },
        "T": {
            "type": "integer",
            "minimum": 0,
        },
    },
    "required": [
        "e",
        "E",
        "s",
        "a",
        "p",
        "q",
        "f",
        "l",
        "T",
        "m",
    ],
}

AGGREGATED_TRADE_VALIDATOR  = Draft202012Validator(AGGREGATED_TRADE_SCHEMA)

def parse_and_validate_message(
    message: str,
) -> dict:

    try:
        data = json.loads(message)
        AGGREGATED_TRADE_VALIDATOR.validate(data)
        return data
    except (json.JSONDecodeError, ValidationError) as error:
        raise InvalidTradeMessage(f"Invalid Binance message: {error}") from error


