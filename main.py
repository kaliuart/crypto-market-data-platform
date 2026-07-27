import asyncio
import json
import logging

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

AGG_TRADE_SCHEMA = {
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

AGG_TRADE_VALIDATOR  = Draft202012Validator(AGG_TRADE_SCHEMA)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"


def parse_and_validate_message(
    message: str,
) -> dict | None:
    try:
        data = json.loads(message)
        AGG_TRADE_VALIDATOR.validate(data)
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

    should_process = True

    if previous_id is None:
        previous_id = current_id

    elif current_id == previous_id + 1:
        previous_id = current_id

    elif current_id < previous_id:
        logger.warning(
            "Out-of-order trade detected: current_id=%d, last_id=%d",
            current_id,
            previous_id,
        )
        should_process = False

    elif current_id == previous_id:
        logger.warning(
            "Duplicate trade detected: trade_id=%d",
            current_id,
        )
        should_process = False

    elif current_id > previous_id + 1:
        logger.warning(
            "Trade ID gap detected: missing_from=%d, missing_to=%d",
            previous_id + 1,
            current_id - 1,
        )
        previous_id = current_id

    return previous_id, should_process


def process_message(data: dict) -> None:
    print(data)


async def main() -> None:
    previous_id = None
    async for websocket in connect(BINANCE_WS_URL):
        logger.info("Connected to Binance WebSocket")

        try:
            async for message in websocket:
                data = parse_and_validate_message(message)

                if data is None:
                    continue

                current_id = data["a"]
                previous_id, should_process = check_trade_sequence(
                    previous_id,
                    current_id,
                )

                if should_process:
                    process_message(data)     

        except ConnectionClosed as error:
            logger.warning("WebSocket connection lost: %s", error)            
            continue


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
