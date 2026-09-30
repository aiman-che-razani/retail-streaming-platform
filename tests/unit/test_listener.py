"""StreamingQueryListener -> Prometheus mapping, with fake progress objects (no JVM)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

pytest.importorskip("pyspark")

from retail_platform.processing.listener import (
    PrometheusStreamingListener,
    StreamingMetrics,
)

pytestmark = pytest.mark.unit


class FakeRow:
    def __init__(self, **values: Any) -> None:
        self._values = values

    def asDict(self) -> dict[str, Any]:  # noqa: N802 - mirrors pyspark.sql.Row
        return self._values


def progress_event(**overrides: Any) -> SimpleNamespace:
    progress = SimpleNamespace(
        name="ingest_pos_transactions",
        id="q-1",
        batchId=4,
        batchDuration=1500,
        numInputRows=120,
        inputRowsPerSecond=4.0,
        processedRowsPerSecond=80.0,
        sources=[
            SimpleNamespace(
                metrics={"maxOffsetsBehindLatest": "12", "avgOffsetsBehindLatest": "3.5"}
            )
        ],
        stateOperators=[SimpleNamespace(numRowsTotal=10, numRowsDroppedByWatermark=2)],
        observedMetrics={
            "ingest": FakeRow(valid=115, rejected=5, warned=1, max_lag_s=9.5, avg_lag_s=2.0)
        },
    )
    for key, value in overrides.items():
        setattr(progress, key, value)
    return SimpleNamespace(progress=progress)


def value(registry: CollectorRegistry, name: str, **labels: str) -> float | None:
    return registry.get_sample_value(name, labels)


def test_progress_is_exported_as_prometheus_metrics() -> None:
    registry = CollectorRegistry()
    listener = PrometheusStreamingListener(StreamingMetrics(registry), "ingest")
    listener.onQueryProgress(progress_event())
    q = {"query": "ingest_pos_transactions"}
    assert value(registry, "retail_spark_batch_duration_seconds", **q) == 1.5
    assert value(registry, "retail_spark_input_rows_total", **q) == 120
    assert value(registry, "retail_spark_records_total", outcome="valid", **q) == 115
    assert value(registry, "retail_spark_records_total", outcome="rejected", **q) == 5
    assert value(registry, "retail_spark_kafka_offsets_behind_latest", stat="max", **q) == 12
    assert value(registry, "retail_spark_event_lag_seconds", stat="max", **q) == 9.5
    assert value(registry, "retail_spark_late_rows_dropped_total", **q) == 2
    assert value(registry, "retail_spark_state_rows", **q) == 10


def test_counters_accumulate_across_batches() -> None:
    registry = CollectorRegistry()
    listener = PrometheusStreamingListener(StreamingMetrics(registry), "ingest")
    listener.onQueryProgress(progress_event())
    listener.onQueryProgress(progress_event())
    assert value(registry, "retail_spark_input_rows_total", query="ingest_pos_transactions") == 240


def test_missing_observed_metrics_and_sources_are_tolerated() -> None:
    registry = CollectorRegistry()
    listener = PrometheusStreamingListener(StreamingMetrics(registry), "ingest")
    listener.onQueryProgress(progress_event(observedMetrics={}, sources=[], stateOperators=[]))
    assert value(registry, "retail_spark_input_rows_total", query="ingest_pos_transactions") == 120


def test_failed_query_is_counted() -> None:
    registry = CollectorRegistry()
    listener = PrometheusStreamingListener(StreamingMetrics(registry), "ingest")
    listener.onQueryTerminated(SimpleNamespace(id="q-9", exception="boom"))
    assert value(registry, "retail_spark_query_failures_total", query="q-9") == 1
