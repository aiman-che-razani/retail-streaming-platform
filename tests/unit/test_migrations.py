from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from retail_platform.errors import MigrationError
from retail_platform.warehouse.migrations import Kind, MigrationRunner, discover, render

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        if sql.startswith("SELECT SCRIPT_KEY"):
            self._rows = list(self._conn.history.items())
        elif sql.startswith("INSERT INTO"):
            self._conn.history[params[0]] = params[4]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self) -> None:
        self.history: dict[str, str] = {}
        self.executed: list[str] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def execute_string(self, sql_text: str, remove_comments: bool = False) -> None:
        self.executed.append(sql_text)


def project(tmp_path: Path) -> Path:
    (tmp_path / "migrations").mkdir()
    (tmp_path / "transformations").mkdir()
    (tmp_path / "migrations" / "V001__first.sql").write_text(
        "CREATE TABLE {{DATABASE}}.RAW.A (X INT);"
    )
    (tmp_path / "migrations" / "V002__second.sql").write_text("CREATE TABLE B (X INT);")
    (tmp_path / "transformations" / "R__views.sql").write_text(
        "CREATE OR REPLACE VIEW V AS SELECT 1;"
    )
    return tmp_path


def test_render_substitutes_and_rejects_unknown_placeholders() -> None:
    assert render("USE {{DATABASE}};", {"DATABASE": "RETAIL_DEV"}) == "USE RETAIL_DEV;"
    with pytest.raises(MigrationError, match="TYPO"):
        render("USE {{TYPO}};", {"DATABASE": "X"})


def test_discover_orders_versioned_before_repeatable(tmp_path: Path) -> None:
    scripts = discover(project(tmp_path))
    assert [(s.kind, s.version) for s in scripts] == [
        (Kind.VERSIONED, "0001"),
        (Kind.VERSIONED, "0002"),
        (Kind.REPEATABLE, "views"),
    ]


def test_apply_runs_pending_once_and_records_history(tmp_path: Path) -> None:
    conn = FakeConnection()
    runner = MigrationRunner(conn, project(tmp_path), {"DATABASE": "RETAIL_DEV"})
    assert len(runner.apply()) == 3
    assert "CREATE TABLE RETAIL_DEV.RAW.A" in conn.executed[0]
    assert runner.apply() == []  # idempotent


def test_changed_repeatable_is_reapplied(tmp_path: Path) -> None:
    conn = FakeConnection()
    root = project(tmp_path)
    runner = MigrationRunner(conn, root, {"DATABASE": "D"})
    runner.apply()
    (root / "transformations" / "R__views.sql").write_text("CREATE OR REPLACE VIEW V AS SELECT 2;")
    assert [s.version for s in runner.apply()] == ["views"]


def test_editing_an_applied_versioned_migration_is_refused(tmp_path: Path) -> None:
    conn = FakeConnection()
    root = project(tmp_path)
    runner = MigrationRunner(conn, root, {"DATABASE": "D"})
    runner.apply()
    (root / "migrations" / "V001__first.sql").write_text("DROP TABLE A;")
    with pytest.raises(MigrationError, match="immutable"):
        runner.pending()


def test_repository_sql_uses_only_known_placeholders() -> None:
    known = {"DATABASE", "LOADER_PUBLIC_KEY", "ADMIN_USER", "MONTHLY_CREDIT_QUOTA"}
    for path in (REPO / "snowflake").rglob("*.sql"):
        render(path.read_text(encoding="utf-8"), dict.fromkeys(known, "X"))


def test_post_deploy_grants_rerun_after_any_change(tmp_path: Path) -> None:
    """Replacing a procedure drops its grants, so the R__9xx grants script must re-run."""
    conn = FakeConnection()
    root = project(tmp_path)
    (root / "transformations" / "R__900_grants.sql").write_text(
        "GRANT USAGE ON SCHEMA X TO ROLE Y;"
    )
    runner = MigrationRunner(conn, root, {"DATABASE": "D"})
    runner.apply()
    (root / "transformations" / "R__views.sql").write_text("CREATE OR REPLACE VIEW V AS SELECT 3;")
    applied = [s.path.name for s in runner.apply()]
    assert applied == ["R__views.sql", "R__900_grants.sql"]
    assert runner.apply() == []  # nothing changed -> nothing re-applied
