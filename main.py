import asyncio
import json
import logging

from datetime import datetime
from datetime import UTC
from aiokafka import AIOKafkaProducer
from mappers import create_aggregated_trade_event
from serializers import serialize_aggregated_trade_event

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
from time import perf_counter

from metrics import (
    MESSAGES_RECEIVED,
    QUEUE_SIZE,
    TRADES_PUBLISHED,
    calculate_metrics,
    observe_trade_metrics,
    start_metrics_server,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

AGGREGATED_TRADE_SCHEMA = {
    "type": "object",
    "properties": {
        "e": {"const": "aggTrade"},
        "E": {"type": "integer"},
        "s": {"type": "string"},
        "a": {"type": "integer"},
        "p": {"type": "string"},
        "q": {"type": "string"},
        "f": {"type": "integer"},
        "l": {"type": "integer"},
        "T": {"type": "integer"},
        "m": {"type": "boolean"},
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

logger = logging.getLogger(__name__)

AGGREGATED_TRADE_VALIDATOR  = Draft202012Validator(AGGREGATED_TRADE_SCHEMA)

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

QUEUE_MAX_SIZE = 1000
QueueItem = tuple[str, datetime]

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


async def handle_message(
    message: str,
    producer: AIOKafkaProducer,
    previous_id: int | None,
    received_at: datetime
) -> int | None:    

    processing_started = perf_counter()
    
    data = parse_and_validate_message(message)
    
    if data is None:
        return previous_id
    
                
    trade_event = create_aggregated_trade_event(data, received_at)

    current_id = trade_event.aggregate_trade_id
    
    next_previous_id, should_process = check_trade_sequence(previous_id, current_id,)
                
    
    if not should_process:
        return previous_id
    
    message_bytes = serialize_aggregated_trade_event(trade_event)
    
    send_started = perf_counter()

    await producer.send_and_wait(
        topic=KAFKA_TOPIC,
        key=trade_event.symbol.encode("utf-8"),
        value=message_bytes,
    )

    send_finished = perf_counter()
    kafka_acknowledged_at = datetime.now(UTC)

    metrics = calculate_metrics(
        trade_event, 
        processing_started=processing_started, 
        send_started=send_started, 
        send_finished=send_finished, 
        kafka_acknowledged_at=kafka_acknowledged_at
        )
    
    TRADES_PUBLISHED.inc()
    observe_trade_metrics(metrics)

    return next_previous_id

async def receive_messages(
        queue: asyncio.Queue[QueueItem]
) -> None:
    async for websocket in connect(BINANCE_WS_URL):
        logger.info("Connected to Binance WebSocket")

        try:
            async for message in websocket:
                received_at = datetime.now(UTC)

                MESSAGES_RECEIVED.inc()

                await queue.put((message, received_at))

        except ConnectionClosed as error:
            logger.warning("WebSocket connection lost: %s", error)            
            continue


async def publish_messages(
        queue: asyncio.Queue[QueueItem],
        producer: AIOKafkaProducer,
) -> None:
    previous_id: int | None = None

    while True:
        message, received_at = await queue.get()
        try:
            previous_id = await handle_message(message,producer, previous_id, received_at)
        finally:
            queue.task_done()


async def main() -> None:

    queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    QUEUE_SIZE.set_function(queue.qsize)

    start_metrics_server()

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,)

    await producer.start()
    
    try:
        async with asyncio.TaskGroup() as task_group:

            task_group.create_task(
                receive_messages(queue),
                name="binance-receiver",
            )

            task_group.create_task(
                publish_messages(queue, producer),
                name="kafka-publisher",
            )

    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
