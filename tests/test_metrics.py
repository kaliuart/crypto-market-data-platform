from datetime import UTC, datetime
from decimal import Decimal

import pytest
from prometheus_client import REGISTRY

from metrics.collector_metrics import TradeMetrics, calculate_metrics, observe_trade_metrics
from models import AggregatedTradeEvent


def create_test_event() -> AggregatedTradeEvent:
    return AggregatedTradeEvent(
        aggregate_trade_id=1,
        symbol="BTCUSDT",
        price=Decimal("65000.00"),
        quantity=Decimal("0.01"),
        event_time=datetime(
            2026, 7, 29, 10, 0, 0, 1_000, tzinfo=UTC
        ),
        trade_time=datetime(
            2026, 7, 29, 10, 0, 0, tzinfo=UTC
        ),
        first_trade_id=10,
        last_trade_id=10,
        buyer_is_market_maker=True,
        received_at=datetime(
            2026, 7, 29, 10, 0, 0, 121_000, tzinfo=UTC
        ),
    )


def test_calculate_metrics_returns_durations_in_seconds() -> None:
    metrics = calculate_metrics(
        create_test_event(),
        processing_started=10.0,
        send_started=10.0004,
        send_finished=10.0034,
        kafka_acknowledged_at=datetime(
            2026, 7, 29, 10, 0, 0, 132_000, tzinfo=UTC
        ),
        queued_at=9.998,
    )

    assert metrics.trade_to_event_seconds == pytest.approx(0.001)
    assert metrics.event_to_collector_seconds == pytest.approx(0.120)
    assert metrics.exchange_to_collector_seconds == pytest.approx(0.121)
    assert metrics.collector_processing_seconds == pytest.approx(0.0004)
    assert metrics.kafka_ack_latency_seconds == pytest.approx(0.003)
    assert metrics.exchange_to_kafka_seconds == pytest.approx(0.132)
    assert metrics.queue_wait_seconds == pytest.approx(0.002)

def test_calculate_metrics_returns_float_values() -> None:
    metrics = calculate_metrics(
        create_test_event(),
        processing_started=10.0,
        send_started=10.0004,
        send_finished=10.0034,
        kafka_acknowledged_at=datetime(
            2026, 7, 29, 10, 0, 0, 132_000, tzinfo=UTC
        ),
        queued_at=9.998,
    )

    assert all(
        isinstance(value, float)
        for value in (
            metrics.trade_to_event_seconds,
            metrics.event_to_collector_seconds,
            metrics.exchange_to_collector_seconds,
            metrics.collector_processing_seconds,
            metrics.kafka_ack_latency_seconds,
            metrics.exchange_to_kafka_seconds,
            metrics.queue_wait_seconds,
        )
    )


def test_observe_trade_metrics_records_each_pipeline_stage() -> None:
    metrics = TradeMetrics(
        trade_to_event_seconds=0.001,
        event_to_collector_seconds=0.120,
        exchange_to_collector_seconds=0.121,
        collector_processing_seconds=0.0004,
        kafka_ack_latency_seconds=0.003,
        exchange_to_kafka_seconds=0.132,
        queue_wait_seconds=0.002,
    )

    expected_durations = {
        "trade_to_event": 0.001,
        "event_to_collector": 0.120,
        "exchange_to_collector": 0.121,
        "collector_processing": 0.0004,
        "kafka_ack": 0.003,
        "exchange_to_kafka": 0.132,
        "queue_wait": 0.002,
    }

    before = {
        stage: (
            REGISTRY.get_sample_value(
                "crypto_collector_pipeline_stage_duration_seconds_count",
                {"stage": stage},
            )
            or 0.0,
            REGISTRY.get_sample_value(
                "crypto_collector_pipeline_stage_duration_seconds_sum",
                {"stage": stage},
            )
            or 0.0,
        )
        for stage in expected_durations
    }

    observe_trade_metrics(metrics)

    for stage, expected_duration in expected_durations.items():
        count_before, sum_before = before[stage]

        count_after = REGISTRY.get_sample_value(
            "crypto_collector_pipeline_stage_duration_seconds_count",
            {"stage": stage},
        )
        sum_after = REGISTRY.get_sample_value(
            "crypto_collector_pipeline_stage_duration_seconds_sum",
            {"stage": stage},
        )

        assert count_after == pytest.approx(count_before + 1)
        assert sum_after == pytest.approx(
            sum_before + expected_duration
        )