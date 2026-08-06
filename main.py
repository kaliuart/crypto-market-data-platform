import asyncio
import logging

from datetime import datetime
from datetime import UTC
from aiokafka import AIOKafkaProducer
from mappers import create_aggregated_trade_event
from serializers import serialize_aggregated_trade_event
from validators import parse_and_validate_binance_message
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
from time import perf_counter
from exceptions import InvalidTradeMessage

from metrics.collector_metrics import (
    MESSAGES_RECEIVED,
    QUEUE_SIZE,
    TRADES_PUBLISHED,
    calculate_metrics,
    observe_trade_metrics,
    start_metrics_server,
    METRICS_PORT
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


logger = logging.getLogger(__name__)

BINANCE_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
)
BINANCE_STREAMS = "/".join(
    f"{symbol.lower()}@aggTrade"
    for symbol in BINANCE_SYMBOLS
)

BINANCE_WS_URL = (
    "wss://stream.binance.com:9443/stream"
    f"?streams={BINANCE_STREAMS}"
)

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "binance.aggregated-trades.v1"

QUEUE_MAX_SIZE = 1000
QueueItem = tuple[str, datetime, float]


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


async def process_and_publish_message(
    message: str,
    producer: AIOKafkaProducer,
    previous_ids: dict[str, int],
    received_at: datetime,
    queued_at: float
) -> None:    

    processing_started = perf_counter()

    try:
        data = parse_and_validate_binance_message(message)
        trade_event = create_aggregated_trade_event(data, received_at)
        print(data)
    except InvalidTradeMessage as error:
        logger.warning(
        "Skipping invalid Binance message: %s",
        error,
        )
        return 
     
    symbol = trade_event.symbol
    previous_id = previous_ids.get(symbol)
    current_id = trade_event.aggregate_trade_id
    
    next_previous_id, should_process = check_trade_sequence(
        previous_id, 
        current_id,
    )
                
    if not should_process:
        return 

    message_bytes = serialize_aggregated_trade_event(trade_event)
    
    send_started = perf_counter()

    await producer.send_and_wait(
        topic=KAFKA_TOPIC,
        key=symbol.encode("utf-8"),
        value=message_bytes,
        )


    send_finished = perf_counter()
    kafka_acknowledged_at = datetime.now(UTC)

    metrics = calculate_metrics(
        trade_event, 
        processing_started=processing_started, 
        send_started=send_started, 
        send_finished=send_finished, 
        kafka_acknowledged_at=kafka_acknowledged_at,
        queued_at=queued_at,
        )
    
    TRADES_PUBLISHED.inc()
    observe_trade_metrics(metrics)

    previous_ids[symbol] = next_previous_id

async def receive_messages(
        queue: asyncio.Queue[QueueItem]
) -> None:
    async for websocket in connect(BINANCE_WS_URL):
        logger.info(
            "Connected to Binance WebSocket: symbols=%s",
            ",".join(BINANCE_SYMBOLS),
        )

        try:
            async for message in websocket:
                received_at = datetime.now(UTC)

                MESSAGES_RECEIVED.inc()

                queued_at = perf_counter()
                await queue.put((message, received_at, queued_at))

        except ConnectionClosed as error:
            logger.warning("WebSocket connection lost: %s", error)            
            continue


async def publish_messages(
        queue: asyncio.Queue[QueueItem],
        producer: AIOKafkaProducer,
) -> None:
    previous_ids: dict[str, int] = {}

    while True:
        message, received_at, queued_at = await queue.get()

        await process_and_publish_message(
            message=message,
            producer=producer,
            previous_ids=previous_ids,
            received_at=received_at,
            queued_at=queued_at,
        )



async def main() -> None:

    queue: asyncio.Queue[QueueItem] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    QUEUE_SIZE.set_function(queue.qsize)

    start_metrics_server()

    logger.info(
        "Collector metrics server started: port=%s",
        METRICS_PORT,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        enable_idempotence=True,
        )
    
    try:
        await producer.start()

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
    except Exception:
        logger.exception(
            "Collector stopped because of an unexpected failure"
        )
        raise SystemExit(1)
    