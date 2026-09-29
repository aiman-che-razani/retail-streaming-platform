from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from retail_platform.config.settings import LoaderSettings
from retail_platform.errors import LoaderError, LoadVerificationError, WarehouseUnavailableError
from retail_platform.loader.copy_specs import COPY_SPECS, LOAD_ORDER, STORES_SPEC
from retail_platform.loader.landing import discover, purge_archive
from retail_platform.loader.loader import LoaderMetrics, SnowflakeLoader

pytestmark = pytest.mark.unit

QUERY_ID = "5f0c1d2e-aaaa-bbbb-cccc-0123456789ab"
COPY_COLUMNS = ["file", "status", "rows_parsed", "rows_loaded", "errors_seen", "first_error"]


class DriverError(Exception):
    pass


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, command: str, params: Any = None) -> None:
        self._conn.commands.append(command)
        if self._conn.fail_on and self._conn.fail_on in command:
            raise DriverError("boom")
        if command.startswith("COPY INTO"):
            table = command.split()[2]
            status = self._conn.copy_status.get(table, "LOADED")
            self.description = [(c,) for c in COPY_COLUMNS]
            if status == "NONE":
                self.description = [("status",)]
                self._rows = [("Copy executed with 0 files processed.",)]
            else:
                self._rows = [("stage/f1.parquet", status, 10, 10, 0, None)]
        else:
            self.description, self._rows = [("status",)], [("UPLOADED",)]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.fail_on: str | None = None
        self.copy_status: dict[str, str] = {}
        self.opened = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        return None


def make_batch(landing: Path, dataset: str, batch_id: int, *, complete: bool = True) -> Path:
    path = landing / dataset / f"query_id={QUERY_ID}" / f"batch_id={batch_id:012d}"
    path.mkdir(parents=True)
    (path / "part-00000.parquet").write_bytes(b"PAR1")
    if complete:
        (path / "_SUCCESS").write_bytes(b"")
    return path


@pytest.fixture
def env(tmp_path: Path) -> dict[str, Any]:
    stores = tmp_path / "stores.csv"
    stores.write_text("store_id\nKLCC-01\n")
    conn = FakeConnection()

    def connect() -> FakeConnection:
        conn.opened += 1
        return conn

    def classify(exc: BaseException, what: str) -> LoaderError:
        return WarehouseUnavailableError(what) if "boom" in str(exc) else LoaderError(what)

    settings = LoaderSettings(landing_dir=tmp_path / "landing", archive_dir=tmp_path / "archive")
    loader = SnowflakeLoader(
        connect=connect,
        settings=settings,
        metrics=LoaderMetrics(CollectorRegistry()),
        stores_csv=stores,
        classify_error=classify,
        driver_error=DriverError,
    )
    return {
        "loader": loader,
        "conn": conn,
        "landing": settings.landing_dir,
        "archive": settings.archive_dir,
    }


def test_idle_cycle_never_opens_a_connection(env: dict[str, Any]) -> None:
    env["loader"].run_cycle()  # first cycle loads reference data
    opened = env["conn"].opened
    report = env["loader"].run_cycle()
    assert report.skipped_warehouse
    assert env["conn"].opened == opened  # idle = no warehouse resume = no credits


def test_cycle_puts_copies_then_archives(env: dict[str, Any]) -> None:
    make_batch(env["landing"], "pos_transactions", 1)
    make_batch(env["landing"], "ingest_audit", 1)
    report = env["loader"].run_cycle()
    commands = env["conn"].commands
    puts = [c for c in commands if c.startswith("PUT") and "parquet" in c]
    copies = [c.split()[2] for c in commands if c.startswith("COPY INTO")]
    assert len(puts) == 2
    assert all("AUTO_COMPRESS = FALSE" in p for p in puts)
    assert copies == ["RAW.STORE_REFERENCE", "RAW.INGEST_BATCH_AUDIT", "RAW.POS_TRANSACTIONS"]
    assert report.files_loaded == 2
    assert not list(env["landing"].rglob("_SUCCESS"))
    assert len(list(env["archive"].rglob("_SUCCESS"))) == 2


def test_incomplete_batches_are_ignored(env: dict[str, Any]) -> None:
    make_batch(env["landing"], "pos_transactions", 1, complete=False)
    assert discover(env["landing"], LOAD_ORDER, 100) == []


def test_failed_file_aborts_and_keeps_batches(env: dict[str, Any]) -> None:
    make_batch(env["landing"], "pos_transactions", 1)
    env["conn"].copy_status["RAW.POS_TRANSACTIONS"] = "LOAD_FAILED"
    with pytest.raises(LoadVerificationError):
        env["loader"].run_cycle()
    assert list(env["landing"].rglob("_SUCCESS")), "nothing may be archived after a failure"


def test_snowflake_outage_is_transient_and_keeps_batches(env: dict[str, Any]) -> None:
    make_batch(env["landing"], "pos_transactions", 1)
    env["conn"].fail_on = "COPY INTO RAW.POS_TRANSACTIONS"
    with pytest.raises(WarehouseUnavailableError):
        env["loader"].run_cycle()
    assert list(env["landing"].rglob("_SUCCESS"))


def test_replay_after_crash_is_idempotent(env: dict[str, Any]) -> None:
    """FM-12: files already loaded come back as '0 files processed' and are just archived."""
    make_batch(env["landing"], "pos_transactions", 1)
    env["conn"].copy_status["RAW.POS_TRANSACTIONS"] = "NONE"
    report = env["loader"].run_cycle()
    assert report.rows_loaded["pos_transactions"] == 0
    assert not list(env["landing"].rglob("_SUCCESS"))


def test_run_forever_backs_off_on_outage(env: dict[str, Any]) -> None:
    make_batch(env["landing"], "pos_transactions", 1)
    env["conn"].fail_on = "COPY INTO"
    waits: list[float] = []

    class Stop:
        requested = False

        def wait(self, seconds: float) -> bool:
            waits.append(seconds)
            return len(waits) >= 3

    env["loader"].run_forever(Stop())
    assert waits == [2.0, 4.0, 8.0]  # exponential backoff, no crash


def test_copy_statements_are_safe_and_complete() -> None:
    for spec in [*COPY_SPECS.values(), STORES_SPEC]:
        sql = spec.statement()
        assert "ON_ERROR = ABORT_STATEMENT" in sql
        assert "PURGE = TRUE" in sql
        assert len(spec.columns) == len(spec.expressions)
    event_sql = COPY_SPECS["pos_transactions"].statement()
    assert "PARSE_JSON($1:event_json::VARCHAR)" in event_sql
    assert "METADATA$FILENAME" in event_sql


def test_archive_retention(tmp_path: Path) -> None:
    old = make_batch(tmp_path, "pos_transactions", 1)
    make_batch(tmp_path, "pos_transactions", 2)
    ten_days_ago = time.time() - 10 * 86400
    os.utime(old / "_SUCCESS", (ten_days_ago, ten_days_ago))
    assert purge_archive(tmp_path, retention_days=3) == 1
    assert not old.exists()
