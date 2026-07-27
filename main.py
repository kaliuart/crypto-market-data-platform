import asyncio
import json
import logging

from datetime import datetime
from datetime import UTC
from mappers import create_aggregated_trade_events
from serializers import serialize_aggregated_trade_event
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

AGGREGATED_TRADE_SCHEMA = {
    "type": "object",
    "properties": {
        "e": {"const": "aggTrade"},
        "a": {"type": "integer"},
        "s": {"type": "string"},
        "p": {"type": "string"},
        "q": {"type": "string"},
    },
    "required": ["e", "a", "s", "p", "q"],
}

logger = logging.getLogger(__name__)

AGGREGATED_TRADE_VALIDATOR  = Draft202012Validator(AGGREGATED_TRADE_SCHEMA)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"


def parse_and_validate_message(
    message: str,
) -> dict | None:
    try:
        data = json.loads(message)
        AGGREGATED_TRADE_VALIDATOR.validate(data)
    except json.JSONDecodeError as error:
        logger.warning("Invalid JSON %s", error)
        return None
    except ValidationError as error:
        logger.warning("Invalid message structure: %s", error.message)
        return None

    return data


def check_trade_sequence(
    previous_id: int | None,
    current_id: int,
) -> tuple[int, bool]:

    if previous_id is None or current_id == previous_id + 1:
        return current_id, True

    if current_id == previous_id:
        logger.warning(
            "Duplicate trade detected: trade_id=%d",
            current_id,
        )
        return previous_id, False

    if current_id < previous_id:
        logger.warning(
            "Out-of-order trade detected: current_id=%d, last_id=%d",
            current_id,
            previous_id,
        )
        return previous_id, False

    logger.warning(
        "Trade ID gap detected: missing_from=%d, missing_to=%d",
        previous_id + 1,
        current_id - 1,
    )
    return current_id, True


def process_message(message: dict) -> None:
    print(message.decode("utf-8"))


async def main() -> None:
    previous_id = None
    async for websocket in connect(BINANCE_WS_URL):
        logger.info("Connected to Binance WebSocket")

        try:
            async for message in websocket:
                data = parse_and_validate_message(message)

                if data is None:
                    continue

                received_at = datetime.now(UTC)
                trade_event = create_aggregated_trade_events(data, received_at)
                message_bytes = serialize_aggregated_trade_event(trade_event)

                current_id = trade_event.aggregate_trade_id

                previous_id, should_process = check_trade_sequence(
                    previous_id,
                    current_id,
                )

                if should_process:
                    process_message(message_bytes)     

        except ConnectionClosed as error:
            logger.warning("WebSocket connection lost: %s", error)            
            continue


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
