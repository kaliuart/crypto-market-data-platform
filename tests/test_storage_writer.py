import asyncio
from types import SimpleNamespace

import pytest

import storage_writer
from exceptions import InvalidTradeMessage


def test_should_flush_batch_rules() -> None:
    assert storage_writer.should_flush_batch(
        batch=[],
        batch_started_at=None,
        now=100.0,
    ) is False

    assert storage_writer.should_flush_batch(
        batch=[object()],
        batch_started_at=99.0,
        now=100.0,
    ) is False

    assert storage_writer.should_flush_batch(
        batch=[object()] * storage_writer.MAX_BATCH_SIZE,
        batch_started_at=100.0,
        now=100.0,
    ) is True

    assert storage_writer.should_flush_batch(
        batch=[object()],
        batch_started_at=98.0,
        now=100.0,
    ) is True


def test_should_flush_batch_rejects_missing_start_time() -> None:
    with pytest.raises(
        RuntimeError,
        match="Non-empty batch must have batch_started_at",
    ):
        storage_writer.should_flush_batch(
            batch=[object()],
            batch_started_at=None,
            now=100.0,
        )


def test_flush_batch_inserts_before_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operations: list[str] = []

    async def fake_insert_trade_batch(
        *,
        clickhouse_client,
        batch,
    ) -> None:
        operations.append("insert")

    async def fake_commit_offsets(
        *,
        consumer,
        offsets_to_commit,
    ) -> None:
        operations.append("commit")

    monkeypatch.setattr(
        storage_writer,
        "insert_trade_batch",
        fake_insert_trade_batch,
    )
    monkeypatch.setattr(
        storage_writer,
        "commit_offsets",
        fake_commit_offsets,
    )

    asyncio.run(
        storage_writer.flush_batch(
            clickhouse_client=object(),
            consumer=object(),
            batch=[object()],
            offsets_to_commit={object(): 10},
        )
    )

    assert operations == ["insert", "commit"]


def test_flush_batch_does_not_commit_when_insert_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_insert_trade_batch(
        *,
        clickhouse_client,
        batch,
    ) -> None:
        raise RuntimeError("ClickHouse unavailable")

    async def unexpected_commit_offsets(**kwargs):
        raise AssertionError("Commit must not be called")

    monkeypatch.setattr(
        storage_writer,
        "insert_trade_batch",
        failing_insert_trade_batch,
    )
    monkeypatch.setattr(
        storage_writer,
        "commit_offsets",
        unexpected_commit_offsets,
    )

    with pytest.raises(
        RuntimeError,
        match="ClickHouse unavailable",
    ):
        asyncio.run(
            storage_writer.flush_batch(
                clickhouse_client=object(),
                consumer=object(),
                batch=[object()],
                offsets_to_commit={object(): 10},
            )
        )


def test_collect_records_adds_valid_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic_partition = object()
    message = SimpleNamespace(
        partition=2,
        offset=41,
    )
    expected_row = object()

    monkeypatch.setattr(
        storage_writer,
        "parse_and_validate_kafka_message",
        lambda received_message: {"valid": True},
    )
    monkeypatch.setattr(
        storage_writer,
        "map_to_clickhouse_row",
        lambda *, data, partition, offset: expected_row,
    )
    monkeypatch.setattr(
        storage_writer.time,
        "monotonic",
        lambda: 123.0,
    )

    batch: list = []
    offsets_to_commit: dict = {}

    batch_started_at = storage_writer.collect_records(
        records_by_partition={
            topic_partition: [message],
        },
        batch=batch,
        offsets_to_commit=offsets_to_commit,
        batch_started_at=None,
    )

    assert batch == [expected_row]
    assert offsets_to_commit == {
        topic_partition: 42,
    }
    assert batch_started_at == 123.0


def test_collect_records_skips_invalid_message_but_saves_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topic_partition = object()
    message = SimpleNamespace(
        partition=2,
        offset=41,
    )

    def invalid_parser(received_message):
        raise InvalidTradeMessage("invalid message")

    def unexpected_mapper(**kwargs):
        raise AssertionError(
            "Mapper must not be called for invalid messages"
        )

    monkeypatch.setattr(
        storage_writer,
        "parse_and_validate_kafka_message",
        invalid_parser,
    )
    monkeypatch.setattr(
        storage_writer,
        "map_to_clickhouse_row",
        unexpected_mapper,
    )

    batch: list = []
    offsets_to_commit: dict = {}

    batch_started_at = storage_writer.collect_records(
        records_by_partition={
            topic_partition: [message],
        },
        batch=batch,
        offsets_to_commit=offsets_to_commit,
        batch_started_at=None,
    )

    assert batch == []
    assert offsets_to_commit == {
        topic_partition: 42,
    }
    assert batch_started_at is None