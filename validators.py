
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
import json
from exceptions import InvalidTradeMessage
AGGREGATED_TRADE_SCHEMA_BINANCE = {
    "type": "object",
    "properties": {
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
AGGREGATED_TRADE_SCHEMA_KAFKA = {
    "type": "object",
    "properties": {
        "price": {
            "type": "string",
            "minLength": 1,
        },
        "quantity": {
            "type": "string",
            "minLength": 1,
        },
        "buyer_is_market_maker" : {"type": "boolean"},
        "symbol": {
            "type": "string",
            "minLength": 1,
        },
        "aggregate_trade_id": {
            "type": "integer",
            "minimum": 0,
        },
        "first_trade_id": {
            "type": "integer",
            "minimum": 0,
        },
        "last_trade_id": {
            "type": "integer",
            "minimum": 0,
        },
        "event_time": {
            "type": "string",
            "minLength": 1,
        },
        "trade_time": {
            "type": "string",
            "minLength": 1,
        },
        "received_at": {
            "type": "string",
            "minLength": 1,
        }
    },
    "required": [
        "aggregate_trade_id",
        "price",
        "quantity",
        "symbol",
        "first_trade_id",
        "last_trade_id",
        "trade_time",
        "event_time",
        "buyer_is_market_maker",
        "received_at",
    ],
}

AGGREGATED_TRADE_VALIDATOR_BINANCE  = Draft202012Validator(AGGREGATED_TRADE_SCHEMA_BINANCE)
AGGREGATED_TRADE_VALIDATOR_KAFKA  = Draft202012Validator(AGGREGATED_TRADE_SCHEMA_KAFKA)

def parse_and_validate_binance_message(
    message: str,
) -> dict:

    try:
        data = json.loads(message)
        AGGREGATED_TRADE_VALIDATOR_BINANCE.validate(data)
        return data
    except (json.JSONDecodeError, ValidationError) as error:
        raise InvalidTradeMessage(f"Invalid Binance message: {error}") from error



def parse_and_validate_kafka_message(message) -> dict:

    if message.value is None:
        raise InvalidTradeMessage(
            "Kafka message value is null"
        )

    try:
        data = json.loads(message.value.decode("utf-8"))
        AGGREGATED_TRADE_VALIDATOR_KAFKA.validate(data)
        return data
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValidationError
    ) as error:
        raise InvalidTradeMessage(f"Invalid Kafka message: {error}") from error
