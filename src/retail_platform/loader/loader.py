"""The Snowflake loader loop (ADR-010, FM-11, FM-12).

One cycle:
    1. discover completed landing batches (`_SUCCESS` present);
    2. PUT their files to @RAW.LANDING_STAGE/<dataset>/query_id=../batch_id=../;
    3. one COPY INTO per dataset (loads every staged, not-yet-loaded file; PURGE on success);
    4. archive the local batch directories.

Idempotency: if the process dies after COPY but before archiving, the next cycle PUTs the
same files again and COPY skips them via load metadata — no duplicate rows. If Snowflake is
unavailable, nothing is archived and the batches stay in landing for the next cycle, while
Spark keeps running (the landing zone decouples the two failure domains).

Cost: a cycle with no new batches never opens a connection, so an idle pipeline never
resumes the warehouse.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from retail_platform.config.settings import LoaderSettings
from retail_platform.errors import LoaderError, LoadVerificationError, WarehouseUnavailableError
from retail_platform.loader.copy_specs import COPY_SPECS, LOAD_ORDER, STAGE, STORES_SPEC
from retail_platform.loader.landing import LandingBatch, archive, backlog, discover, purge_archive
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)

# COPY INTO result columns we rely on (by name; Snowflake returns them per file).
_STATUS_OK = {"LOADED"}


class Cursor(Protocol):
    description: Any

    def execute(self, command: str, params: Any = ...) -> Any: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...

    def close(self) -> Any: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def close(self) -> Any: ...


class LoaderMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self.files = Counter(
            "retail_loader_files_loaded_total",
            "Files processed by COPY INTO",
            ["dataset", "status"],
            registry=registry,
        )
        self.rows = Counter(
            "retail_loader_rows_loaded_total",
            "Rows loaded into RAW",
            ["dataset"],
            registry=registry,
        )
        self.cycle_duration = Histogram(
            "retail_loader_cycle_duration_seconds",
            "Duration of load cycles that touched Snowflake",
            buckets=(1, 2.5, 5, 10, 30, 60, 120, 300, 600),
            registry=registry,
        )
        self.backlog = Gauge(
            "retail_loader_landing_backlog_batches",
            "Completed landing batches waiting to be loaded",
            ["dataset"],
            registry=registry,
        )
        self.last_success = Gauge(
            "retail_loader_last_success_timestamp_seconds",
            "Unix time the dataset was last fully loaded (idle cycles count: nothing waiting)",
            ["dataset"],
            registry=registry,
        )
        self.failures = Counter(
            "retail_loader_cycle_failures_total",
            "Failed load cycles by kind",
            ["kind"],
            registry=registry,
        )


@dataclass
class CycleReport:
    batches: int = 0
    files_loaded: int = 0
    rows_loaded: dict[str, int] = field(default_factory=dict)
    skipped_warehouse: bool = False


def _rows(cursor: Cursor) -> list[dict[str, Any]]:
    names = [d[0].lower() for d in (cursor.description or [])]
    return [dict(zip(names, row, strict=False)) for row in cursor.fetchall()]


class SnowflakeLoader:
    def __init__(
        self,
        *,
        connect: Callable[[], Connection],
        settings: LoaderSettings,
        metrics: LoaderMetrics,
        stores_csv: Path,
        classify_error: Callable[[BaseException, str], LoaderError],
        driver_error: type[Exception],
    ) -> None:
        self._connect = connect
        self._settings = settings
        self._metrics = metrics
        self._stores_csv = stores_csv
        self._classify = classify_error
        self._driver_error = driver_error
        self._loaded_reference_checksum: str | None = None

    # ------------------------------------------------------------------ cycle
    def run_cycle(self) -> CycleReport:
        s = self._settings
        for dataset, count in backlog(s.landing_dir, LOAD_ORDER).items():
            self._metrics.backlog.labels(dataset).set(count)
        batches = discover(s.landing_dir, LOAD_ORDER, s.max_batches_per_cycle)
        reference_checksum = hashlib.sha256(self._stores_csv.read_bytes()).hexdigest()
        reference_due = reference_checksum != self._loaded_reference_checksum
        report = CycleReport(batches=len(batches))
        if not batches and not reference_due:
            report.skipped_warehouse = True  # no connection -> no warehouse resume -> no cost
            self._mark_fresh(LOAD_ORDER)  # nothing waiting = up to date (no false stale alert)
            log.info("load_cycle_idle")
            return report

        started = time.monotonic()
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                if reference_due:
                    self._load_reference(cursor)
                    self._loaded_reference_checksum = reference_checksum
                for dataset in LOAD_ORDER:
                    dataset_batches = [b for b in batches if b.dataset == dataset]
                    if not dataset_batches:
                        continue
                    self._put(cursor, dataset_batches)
                    self._copy(cursor, dataset, report)
                    # Archive as soon as THIS dataset is loaded: if a later dataset fails, these
                    # files are never re-uploaded (a re-upload could get a new checksum and be
                    # loaded twice into RAW).
                    for batch in dataset_batches:
                        archive(batch, s.landing_dir, s.archive_dir)
                    self._mark_fresh([dataset])
            except self._driver_error as exc:
                raise self._classify(exc, "load cycle") from exc
            finally:
                cursor.close()
        finally:
            connection.close()

        self._mark_fresh([d for d in LOAD_ORDER if not any(b.dataset == d for b in batches)])
        purge_archive(s.archive_dir, s.archive_retention_days)
        self._metrics.cycle_duration.observe(time.monotonic() - started)
        log.info(
            "load_cycle_complete",
            batches=report.batches,
            files_loaded=report.files_loaded,
            rows_loaded=report.rows_loaded,
            processing_time_ms=int((time.monotonic() - started) * 1000),
            status="ok",
        )
        return report

    def _mark_fresh(self, datasets: list[str]) -> None:
        """Dataset has nothing left to load as of now (loaded, or nothing was waiting)."""
        now = time.time()
        for dataset in datasets:
            self._metrics.last_success.labels(dataset).set(now)

    # ------------------------------------------------------------------ steps
    def _put(self, cursor: Cursor, batches: list[LandingBatch]) -> None:
        for batch in batches:
            for path in batch.files:
                # OVERWRITE=TRUE: a re-PUT after a crash replaces the identical staged file;
                # COPY's load metadata (not the stage) is what prevents double loading.
                cursor.execute(
                    f"PUT 'file://{path.resolve().as_posix()}' @{STAGE}/{batch.relative}/ "
                    "AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 4"
                )

    def _copy(self, cursor: Cursor, dataset: str, report: CycleReport) -> None:
        spec = COPY_SPECS[dataset]
        cursor.execute(spec.statement())
        results = _rows(cursor)
        loaded_rows = 0
        for result in results:
            status = str(result.get("status", "")).upper()
            # "Copy executed with 0 files processed." comes back as a single status row.
            if "file" not in result:
                continue
            self._metrics.files.labels(dataset, status.lower()).inc()
            if status not in _STATUS_OK:
                raise LoadVerificationError(
                    f"{dataset}: file {result.get('file')} status={status} "
                    f"first_error={result.get('first_error')}"
                )
            if int(result.get("errors_seen") or 0) != 0:
                raise LoadVerificationError(f"{dataset}: {result.get('file')} reported errors")
            loaded_rows += int(result.get("rows_loaded") or 0)
            report.files_loaded += 1
        report.rows_loaded[dataset] = report.rows_loaded.get(dataset, 0) + loaded_rows
        self._metrics.rows.labels(dataset).inc(loaded_rows)

    def _load_reference(self, cursor: Cursor) -> None:
        cursor.execute(
            f"PUT 'file://{self._stores_csv.resolve().as_posix()}' @{STAGE}/{STORES_SPEC.dataset}/ "
            "AUTO_COMPRESS = TRUE OVERWRITE = TRUE"
        )
        cursor.execute(STORES_SPEC.statement())
        log.info("reference_data_loaded", table=STORES_SPEC.table, results=len(_rows(cursor)))

    # ------------------------------------------------------------------ loop
    def run_forever(self, stop: Any) -> None:
        """Cycle every `interval_seconds` until `stop.requested`; back off on outages."""
        s = self._settings
        backoff = s.retry_initial_backoff_seconds
        while not stop.requested:
            try:
                self.run_cycle()
                backoff = s.retry_initial_backoff_seconds
                wait = float(s.interval_seconds)
            except WarehouseUnavailableError as exc:
                # FM-11: Snowflake down. Keep files in landing and retry with backoff.
                self._metrics.failures.labels("unavailable").inc()
                log.warning("snowflake_unavailable", error=str(exc)[:500], retry_in_s=backoff)
                wait = backoff
                backoff = min(backoff * 2, s.retry_max_backoff_seconds)
            if stop.wait(wait):
                break
