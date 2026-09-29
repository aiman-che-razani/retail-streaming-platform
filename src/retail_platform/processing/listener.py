"""StreamingQueryListener -> Prometheus metrics + one structured log line per batch (ADR-009).

Consumer lag note: Spark's Kafka source tracks offsets in its checkpoint and does NOT commit
them to a Kafka consumer group, so `kafka-consumer-groups.sh` / kafka-exporter show nothing
for Spark. The Kafka source reports its own lag per batch (`avgOffsetsBehindLatest`,
`maxOffsetsBehindLatest`), which we export as `retail_spark_kafka_offsets_behind_latest`.
"""

from __future__ import annotations

import time
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge
from pyspark.sql.streaming.listener import StreamingQueryListener

from retail_platform.observability.logging import get_logger

log = get_logger(__name__)


class StreamingMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        q = ["query"]
        self.batch_duration = Gauge(
            "retail_spark_batch_duration_seconds", "Last micro-batch duration", q, registry=registry
        )
        self.input_rows = Counter(
            "retail_spark_input_rows_total", "Rows read from Kafka", q, registry=registry
        )
        self.input_rate = Gauge(
            "retail_spark_input_rows_per_second", "Input rate", q, registry=registry
        )
        self.processed_rate = Gauge(
            "retail_spark_processed_rows_per_second", "Processing rate", q, registry=registry
        )
        self.records = Counter(
            "retail_spark_records_total",
            "Validated records by outcome",
            ["query", "outcome"],
            registry=registry,
        )
        self.offsets_behind = Gauge(
            "retail_spark_kafka_offsets_behind_latest",
            "Kafka offsets not yet processed (Spark consumer lag)",
            ["query", "stat"],
            registry=registry,
        )
        self.event_lag = Gauge(
            "retail_spark_event_lag_seconds",
            "Processing time minus event time in the last batch",
            ["query", "stat"],
            registry=registry,
        )
        self.state_rows = Gauge(
            "retail_spark_state_rows", "Rows held in state stores", q, registry=registry
        )
        self.late_dropped = Counter(
            "retail_spark_late_rows_dropped_total",
            "Rows dropped by the watermark (too late)",
            q,
            registry=registry,
        )
        self.last_progress = Gauge(
            "retail_spark_last_progress_timestamp_seconds",
            "Unix time of the last progress event",
            q,
            registry=registry,
        )
        self.query_failures = Counter(
            "retail_spark_query_failures_total", "Queries terminated with an error", q,
            registry=registry,
        )  # fmt: skip


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class PrometheusStreamingListener(StreamingQueryListener):
    def __init__(self, metrics: StreamingMetrics, observed_name: str) -> None:
        self._m = metrics
        self._observed = observed_name

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802 - Spark API name
        log.info("query_started", query=event.name, query_id=str(event.id), run_id=str(event.runId))

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        p = event.progress
        name = p.name or str(p.id)
        m = self._m
        m.batch_duration.labels(name).set(_num(p.batchDuration) / 1000)
        m.input_rows.labels(name).inc(_num(p.numInputRows))
        m.input_rate.labels(name).set(_num(p.inputRowsPerSecond))
        m.processed_rate.labels(name).set(_num(p.processedRowsPerSecond))
        m.last_progress.labels(name).set(time.time())

        for source in p.sources:
            metrics = source.metrics or {}
            if "maxOffsetsBehindLatest" in metrics:
                m.offsets_behind.labels(name, "max").set(_num(metrics["maxOffsetsBehindLatest"]))
                m.offsets_behind.labels(name, "avg").set(_num(metrics["avgOffsetsBehindLatest"]))

        state_rows = sum(_num(op.numRowsTotal) for op in p.stateOperators)
        dropped = sum(_num(op.numRowsDroppedByWatermark) for op in p.stateOperators)
        m.state_rows.labels(name).set(state_rows)
        if dropped:
            m.late_dropped.labels(name).inc(dropped)

        observed = (p.observedMetrics or {}).get(self._observed)
        summary: dict[str, Any] = {}
        if observed is not None:
            row = observed.asDict()
            for outcome in ("valid", "rejected", "warned"):
                count = _num(row.get(outcome))
                if count:
                    m.records.labels(name, outcome).inc(count)
                summary[outcome] = int(count)
            if row.get("max_lag_s") is not None:
                m.event_lag.labels(name, "max").set(_num(row["max_lag_s"]))
                m.event_lag.labels(name, "avg").set(_num(row["avg_lag_s"]))

        log.info(
            "batch_completed",
            query=name,
            batch_id=p.batchId,
            input_rows=p.numInputRows,
            processing_time_ms=p.batchDuration,
            state_rows=int(state_rows),
            late_rows_dropped=int(dropped),
            status="ok",
            **summary,
        )

    def onQueryIdle(self, event: Any) -> None:  # noqa: N802
        return None

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        if event.exception:
            self._m.query_failures.labels(str(event.id)).inc()
            log.error("query_failed", query_id=str(event.id), error=str(event.exception)[:2000])
        else:
            log.info("query_stopped", query_id=str(event.id))
