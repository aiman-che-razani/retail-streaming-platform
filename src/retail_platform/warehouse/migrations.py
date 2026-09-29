"""A small, explicit schema-migration runner (Flyway-style conventions, no extra tool).

* `snowflake/migrations/V###__description.sql` — versioned; applied once, in order, and never
  edited afterwards (a changed checksum of an applied migration is an error).
* `snowflake/transformations/R__description.sql` — repeatable (procedures, views, tasks,
  grants); re-applied whenever their checksum changes, after all versioned migrations.
* History lives in `OPS.SCHEMA_MIGRATIONS`.
* `{{PLACEHOLDER}}` tokens are rendered from explicit variables; an unknown token is an error
  (so a typo can never reach Snowflake as literal text).
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from retail_platform.errors import MigrationError
from retail_platform.observability.logging import get_logger
from retail_platform.warehouse.connection import connector_error

log = get_logger(__name__)

_PLACEHOLDER = re.compile(r"\{\{\s*([A-Z_]+)\s*\}\}")
_VERSIONED = re.compile(r"^V(\d+)__(.+)\.sql$")
_REPEATABLE = re.compile(r"^R__(.+)\.sql$")


class Kind(StrEnum):
    VERSIONED = "V"
    REPEATABLE = "R"


@dataclass(frozen=True, slots=True)
class Script:
    kind: Kind
    version: str  # zero-padded number for V, name for R
    description: str
    path: Path
    checksum: str

    @property
    def key(self) -> str:
        return f"{self.kind.value}{self.version}"


class Connection(Protocol):
    def cursor(self) -> Any: ...

    def execute_string(self, sql_text: str, remove_comments: bool = ...) -> Any: ...


def render(text: str, variables: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            raise MigrationError(f"unknown placeholder {{{{{name}}}}}")
        return variables[name]

    return _PLACEHOLDER.sub(replace, text)


def _checksum(path: Path) -> str:
    # Normalise line endings so Windows checkouts produce the same checksum as Linux.
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def discover(snowflake_dir: Path) -> list[Script]:
    scripts: list[Script] = []
    for path in sorted((snowflake_dir / "migrations").glob("V*.sql")):
        match = _VERSIONED.match(path.name)
        if not match:
            raise MigrationError(f"bad migration file name: {path.name}")
        scripts.append(
            Script(
                Kind.VERSIONED, f"{int(match.group(1)):04d}", match.group(2), path, _checksum(path)
            )
        )
    versions = [s.version for s in scripts]
    if len(versions) != len(set(versions)):
        raise MigrationError("duplicate migration version numbers")
    for path in sorted((snowflake_dir / "transformations").glob("R__*.sql")):
        match = _REPEATABLE.match(path.name)
        if not match:
            raise MigrationError(f"bad repeatable file name: {path.name}")
        scripts.append(
            Script(Kind.REPEATABLE, match.group(1), match.group(1), path, _checksum(path))
        )
    return scripts


HISTORY_DDL = """
CREATE TABLE IF NOT EXISTS {database}.OPS.SCHEMA_MIGRATIONS (
    SCRIPT_KEY    VARCHAR       NOT NULL,
    KIND          VARCHAR(1)    NOT NULL,
    DESCRIPTION   VARCHAR,
    SCRIPT        VARCHAR       NOT NULL,
    CHECKSUM      VARCHAR(64)   NOT NULL,
    APPLIED_AT    TIMESTAMP_NTZ NOT NULL DEFAULT SYSDATE(),
    APPLIED_BY    VARCHAR       NOT NULL DEFAULT CURRENT_USER(),
    EXECUTION_MS  NUMBER
) COMMENT = 'Applied migrations and repeatable scripts (latest row per key wins)'
"""


class MigrationRunner:
    def __init__(
        self, connection: Connection, snowflake_dir: Path, variables: dict[str, str]
    ) -> None:
        self._conn = connection
        self._dir = snowflake_dir
        self._vars = variables
        self._database = variables["DATABASE"]

    def _query(self, sql_text: str) -> list[tuple[Any, ...]]:
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql_text)
            return list(cursor.fetchall())
        finally:
            cursor.close()

    def _applied(self) -> dict[str, str]:
        self._query(HISTORY_DDL.format(database=self._database))
        rows = self._query(
            f"SELECT SCRIPT_KEY, CHECKSUM FROM {self._database}.OPS.SCHEMA_MIGRATIONS "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY SCRIPT_KEY ORDER BY APPLIED_AT DESC) = 1"
        )
        return {str(key): str(checksum) for key, checksum in rows}

    def pending(self) -> list[Script]:
        applied = self._applied()
        pending: list[Script] = []
        for script in discover(self._dir):
            previous = applied.get(script.key)
            if script.kind is Kind.VERSIONED:
                if previous is not None and previous != script.checksum:
                    raise MigrationError(
                        f"{script.path.name} was modified after being applied. Versioned "
                        "migrations are immutable - add a new V### migration instead."
                    )
                if previous is None:
                    pending.append(script)
            elif previous != script.checksum:
                pending.append(script)
        return pending

    def apply(self, *, dry_run: bool = False) -> list[Script]:
        pending = self.pending()
        for script in pending:
            sql_text = render(script.path.read_text(encoding="utf-8"), self._vars)
            if dry_run:
                log.info("migration_pending", script=script.path.name)
                continue
            started = time.monotonic()
            try:
                self._conn.execute_string(sql_text, remove_comments=True)
            except connector_error() as exc:
                raise MigrationError(f"{script.path.name} failed: {exc}") from exc
            elapsed_ms = int((time.monotonic() - started) * 1000)
            cursor = self._conn.cursor()
            try:
                cursor.execute(
                    f"INSERT INTO {self._database}.OPS.SCHEMA_MIGRATIONS "
                    "(SCRIPT_KEY, KIND, DESCRIPTION, SCRIPT, CHECKSUM, EXECUTION_MS) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        script.key,
                        script.kind.value,
                        script.description,
                        script.path.name,
                        script.checksum,
                        elapsed_ms,
                    ),
                )
            finally:
                cursor.close()
            log.info("migration_applied", script=script.path.name, execution_ms=elapsed_ms)
        return pending


def run_bootstrap(connection: Connection, snowflake_dir: Path, variables: dict[str, str]) -> None:
    for path in sorted((snowflake_dir / "ddl").glob("*.sql")):
        log.info("bootstrap_script", script=path.name)
        try:
            connection.execute_string(render(path.read_text(encoding="utf-8"), variables))
        except connector_error() as exc:
            raise MigrationError(f"{path.name} failed: {exc}") from exc
