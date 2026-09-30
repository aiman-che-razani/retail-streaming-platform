"""Static checks of the Snowflake SQL (no Snowflake account needed).

These cannot prove runtime behaviour, but they catch syntax errors in queries, views and the
DML inside stored procedures, and drift between the landing contract, the COPY statements and
the RAW table DDL. Live verification is the `snowflake` marked tests.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot.errors import ParseError

from retail_platform.loader.copy_specs import COPY_SPECS, EVENT_TABLES, STORES_SPEC
from retail_platform.warehouse.migrations import render

pytestmark = pytest.mark.contract

SNOWFLAKE = Path(__file__).resolve().parents[2] / "snowflake"
VARIABLES = {
    "DATABASE": "RETAIL_DEV",
    "LOADER_PUBLIC_KEY": "KEY",
    "ADMIN_USER": "ADMIN",
    "MONTHLY_CREDIT_QUOTA": "20",
}
DML = re.compile(r"^\s*(SELECT|WITH|INSERT|MERGE|UPDATE|DELETE)\b", re.IGNORECASE)
# Snowflake Scripting variables referenced as :name inside procedure SQL.
SCRIPT_VARS = re.compile(r"(?<=[\s(,=<>])\:(d|dk|end_of_day_utc)\b")


def _strip_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _statements(sql: str) -> list[str]:
    return [s.strip() for s in _strip_comments(sql).split(";") if s.strip()]


def _parse(statement: str, origin: str) -> None:
    try:
        sqlglot.parse_one(SCRIPT_VARS.sub("NULL", statement), read="snowflake")
    except ParseError as exc:
        pytest.fail(f"{origin}: {exc}\n---\n{statement[:600]}")


@pytest.mark.parametrize(
    "path", sorted((SNOWFLAKE / "analytics").glob("*.sql")), ids=lambda p: p.name
)
def test_analytics_queries_parse(path: Path) -> None:
    for statement in _statements(path.read_text(encoding="utf-8")):
        _parse(statement, path.name)


def _procedure_bodies(sql: str) -> list[str]:
    return re.findall(r"AS\s*\$\$(.*?)\$\$", sql, flags=re.DOTALL)


@pytest.mark.parametrize(
    "path",
    sorted((SNOWFLAKE / "transformations").glob("R__*.sql")),
    ids=lambda p: p.name,
)
def test_procedure_and_view_sql_parses(path: Path) -> None:
    sql = render(path.read_text(encoding="utf-8"), VARIABLES)
    checked = 0
    for body in _procedure_bodies(sql):
        for statement in _statements(body):
            if DML.match(statement):
                _parse(statement, path.name)
                checked += 1
    for statement in _statements(re.sub(r"\$\$.*?\$\$", "", sql, flags=re.DOTALL)):
        if statement.upper().startswith("CREATE OR REPLACE VIEW"):
            _parse(statement, path.name)
            checked += 1
    if "procedures" in path.name or "views" in path.name:
        assert checked > 0, f"no SQL statements checked in {path.name}"


def _table_columns(ddl: str, table: str) -> list[str]:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\)", ddl, flags=re.DOTALL | re.IGNORECASE
    )
    assert match, f"table {table} not found in RAW DDL"
    columns = []
    for raw_line in match.group(1).splitlines():
        line = raw_line.split("--", 1)[0].strip()
        if line and not line.upper().startswith("PRIMARY KEY"):
            columns.append(line.split()[0])
    return columns


def test_copy_statements_match_raw_table_ddl() -> None:
    ddl = (SNOWFLAKE / "migrations" / "V001__raw_layer.sql").read_text(encoding="utf-8")
    event_columns = set(_table_columns(ddl, "POS_TRANSACTIONS"))
    for dataset, table in EVENT_TABLES.items():
        spec = COPY_SPECS[dataset.value]
        assert spec.table == table
        # every COPY target column exists; the only column COPY omits is the defaulted load time
        assert set(spec.columns) == event_columns - {"_LOADED_AT"}
    for dataset, table in (
        ("dead_letters", "DEAD_LETTERS"),
        ("ingest_audit", "INGEST_BATCH_AUDIT"),
    ):
        assert set(COPY_SPECS[dataset].columns) == set(_table_columns(ddl, table)) - {"_LOADED_AT"}
    assert set(STORES_SPEC.columns) == set(_table_columns(ddl, "STORE_REFERENCE")) - {"_LOADED_AT"}


def test_copy_statements_read_every_landing_column() -> None:
    """The landing contract (overview.md §6) and the COPY expressions must agree."""
    landing = {
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_key",
        "schema_id", "event_id", "event_type", "schema_version", "event_timestamp", "produced_at",
        "producer", "correlation_id", "causation_id", "event_json", "dq_warnings", "ingested_at",
        "spark_query_id", "spark_batch_id",
    }  # fmt: skip
    read = set(re.findall(r"\$1:(\w+)", " ".join(COPY_SPECS["pos_transactions"].expressions)))
    assert read == landing


def test_fact_tables_document_their_grain() -> None:
    ddl = (SNOWFLAKE / "migrations" / "V004__analytics_layer.sql").read_text(encoding="utf-8")
    for fact in ("FACT_SALES", "FACT_INVENTORY", "FACT_INVENTORY_SNAPSHOT"):
        block = ddl.split(f"CREATE TABLE IF NOT EXISTS {fact} (", 1)[1].split(";", 1)[0]
        assert "COMMENT = 'GRAIN:" in block, f"{fact} must state its grain"


def test_every_task_called_procedure_is_defined() -> None:
    tasks = (SNOWFLAKE / "transformations" / "R__800_tasks.sql").read_text(encoding="utf-8")
    called = set(re.findall(r"CALL \{\{DATABASE\}\}\.(\w+\.\w+)\(", tasks))
    defined_sql = " ".join(
        p.read_text(encoding="utf-8") for p in (SNOWFLAKE / "transformations").glob("R__*.sql")
    )
    defined = set(re.findall(r"PROCEDURE \{\{DATABASE\}\}\.(\w+\.\w+)\(", defined_sql))
    assert called <= defined, f"tasks call undefined procedures: {called - defined}"
