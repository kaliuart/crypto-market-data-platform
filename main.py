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
from models import PreparedMessage

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

KAFKA_TRANSACTIONAL_ID = "binance-collector-v1"

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

KAFKA_TOPIC = "binance.aggregated-trades.v1"
KAFKA_STATE_TOPIC = "collector.sequence-state.v1"

QUEUE_MAX_SIZE = 1000
QueueItem = tuple[str, datetime, float]

MAX_BATCH_SIZE = 100
MAX_BATCH_WAIT_SECONDS = 0.1


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

async def collect_batch(
    queue: asyncio.Queue[QueueItem],
) -> list[QueueItem]:
    batch = [await queue.get()]
    deadline = asyncio.get_running_loop().time() + MAX_BATCH_WAIT_SECONDS

    while len(batch) < MAX_BATCH_SIZE:
        remaining_time = deadline - asyncio.get_running_loop().time()

        if remaining_time <= 0:
            break

        try:
            item = await asyncio.wait_for(
                queue.get(),
                timeout=remaining_time,
            )
        except TimeoutError:
            break

        batch.append(item)

    return batch


def prepare_batch(
    batch: list[QueueItem],
    previous_ids: dict[str, int],
) -> tuple[list[PreparedMessage], dict[str, int]]:

    candidate_ids: dict[str, int] = {}
    prepared_messages: list[PreparedMessage] = []

    for message, received_at, queued_at in batch:
        processing_started = perf_counter()

        try:
            data = parse_and_validate_binance_message(message)
            trade_event = create_aggregated_trade_event(
                data=data,
                received_at=received_at
            )
            print(data)

        except InvalidTradeMessage as error:
            logger.warning(
                "Skipping invalid Binance message: %s",
                error,
            )
            continue

        symbol = trade_event.symbol

        if symbol in candidate_ids:
            previous_id = candidate_ids.get(symbol)
        else:
            previous_id = previous_ids.get(symbol)

        current_id = trade_event.aggregate_trade_id

        next_previous_id, should_process = check_trade_sequence(
            previous_id=previous_id,
            current_id=current_id,
        )

        if not should_process:
            continue

        message_bytes = serialize_aggregated_trade_event(trade_event)

        prepared_message = PreparedMessage(
            message_bytes=message_bytes,
            trade_event=trade_event,
            queued_at=queued_at,
            processing_started=processing_started,
        )

        prepared_messages.append(prepared_message)

        candidate_ids[symbol] = next_previous_id

    return prepared_messages, candidate_ids


async def process_and_publish_message(
    producer: AIOKafkaProducer,
    previous_ids: dict[str, int],
    batch: list,
) -> None:    

    prepared_messages, candidate_ids = prepare_batch(
        batch=batch,
        previous_ids=previous_ids,
    )
    if not prepared_messages:
        return
    
    send_started = perf_counter()

    async with producer.transaction():

        delivery_futures = []

        for prepared_message in prepared_messages:
            
            future = await producer.send(
                topic=KAFKA_TOPIC,
                key=prepared_message.trade_event.symbol.encode("utf-8"),
                value=prepared_message.message_bytes,
            )
            delivery_futures.append(future)

        for symbol, last_id in candidate_ids.items():

            future = await producer.send(
                topic=KAFKA_STATE_TOPIC,
                key=symbol.encode("utf-8"),
                value=str(last_id).encode("utf-8"),
            )

            delivery_futures.append(future)

        await asyncio.gather(*delivery_futures)

    previous_ids.update(candidate_ids)

    send_finished = perf_counter()
    kafka_acknowledged_at = datetime.now(UTC)


"""
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
"""
    

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
        batch = await collect_batch(
            queue=queue,
        )
        await process_and_publish_message(
            producer=producer,
            previous_ids=previous_ids,
            batch=batch
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
        transactional_id=KAFKA_TRANSACTIONAL_ID,
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
    