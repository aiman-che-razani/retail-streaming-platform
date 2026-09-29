"""COPY INTO statements per landing dataset.

The landing Parquet columns (overview.md §6) are read as `$1:<column>` and cast explicitly;
timestamps arrive as ISO-8601 UTC strings (`...Z`) and are converted with an explicit format
so no session-timezone setting can shift them.
"""

from __future__ import annotations

from dataclasses import dataclass

from retail_platform.contracts.catalog import Dataset

STAGE = "RAW.LANDING_STAGE"
ISO_FORMAT = '\'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"\''
DEAD_LETTERS = "dead_letters"
INGEST_AUDIT = "ingest_audit"
REFERENCE_STORES = "reference/stores"


def _ts(column: str) -> str:
    return f"TO_TIMESTAMP_NTZ($1:{column}::VARCHAR, {ISO_FORMAT})"


@dataclass(frozen=True, slots=True)
class CopySpec:
    dataset: str
    table: str
    columns: tuple[str, ...]
    expressions: tuple[str, ...]
    file_format: str = "RAW.FF_PARQUET"

    def statement(self) -> str:
        if len(self.columns) != len(self.expressions):
            raise ValueError(f"column/expression mismatch for {self.table}")
        return (
            f"COPY INTO {self.table} ({', '.join(self.columns)})\n"
            f"FROM (SELECT {', '.join(self.expressions)}\n"
            f"      FROM @{STAGE}/{self.dataset}/)\n"
            f"FILE_FORMAT = (FORMAT_NAME = {self.file_format})\n"
            # A file that cannot be loaded is a bug, never something to silently skip.
            "ON_ERROR = ABORT_STATEMENT\n"
            # Loaded files are removed from the stage (storage cost); COPY load metadata still
            # prevents re-loading the same file for 64 days (idempotency, ADR-007).
            "PURGE = TRUE"
        )


_EVENT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("KAFKA_TOPIC", "$1:kafka_topic::VARCHAR"),
    ("KAFKA_PARTITION", "$1:kafka_partition::NUMBER"),
    ("KAFKA_OFFSET", "$1:kafka_offset::NUMBER"),
    ("KAFKA_TIMESTAMP", _ts("kafka_timestamp")),
    ("KAFKA_KEY", "$1:kafka_key::VARCHAR"),
    ("SCHEMA_ID", "$1:schema_id::NUMBER"),
    ("EVENT_ID", "$1:event_id::VARCHAR"),
    ("EVENT_TYPE", "$1:event_type::VARCHAR"),
    ("SCHEMA_VERSION", "$1:schema_version::VARCHAR"),
    ("EVENT_TIMESTAMP", _ts("event_timestamp")),
    ("PRODUCED_AT", _ts("produced_at")),
    ("PRODUCER", "$1:producer::VARCHAR"),
    ("CORRELATION_ID", "$1:correlation_id::VARCHAR"),
    ("CAUSATION_ID", "$1:causation_id::VARCHAR"),
    ("EVENT", "PARSE_JSON($1:event_json::VARCHAR)"),
    ("DQ_WARNINGS", "PARSE_JSON($1:dq_warnings::VARCHAR)::ARRAY"),
    ("INGESTED_AT", _ts("ingested_at")),
    ("SPARK_QUERY_ID", "$1:spark_query_id::VARCHAR"),
    ("SPARK_BATCH_ID", "$1:spark_batch_id::NUMBER"),
    ("_FILE_NAME", "METADATA$FILENAME"),
    ("_FILE_ROW_NUMBER", "METADATA$FILE_ROW_NUMBER"),
)

_DEAD_LETTER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("SOURCE_TOPIC", "$1:source_topic::VARCHAR"),
    ("SOURCE_PARTITION", "$1:source_partition::NUMBER"),
    ("SOURCE_OFFSET", "$1:source_offset::NUMBER"),
    ("KAFKA_TIMESTAMP", _ts("kafka_timestamp")),
    ("KAFKA_KEY", "$1:kafka_key::VARCHAR"),
    ("ERROR_CODE", "$1:error_code::VARCHAR"),
    ("ERROR_STAGE", "$1:error_stage::VARCHAR"),
    ("ERROR_MESSAGE", "$1:error_message::VARCHAR"),
    ("EVENT_ID", "$1:event_id::VARCHAR"),
    ("RAW_VALUE_BASE64", "$1:raw_value_base64::VARCHAR"),
    ("RAW_VALUE_PREVIEW", "$1:raw_value_preview::VARCHAR"),
    ("FAILED_AT", _ts("failed_at")),
    ("SPARK_QUERY_ID", "$1:spark_query_id::VARCHAR"),
    ("SPARK_BATCH_ID", "$1:spark_batch_id::NUMBER"),
    ("_FILE_NAME", "METADATA$FILENAME"),
    ("_FILE_ROW_NUMBER", "METADATA$FILE_ROW_NUMBER"),
)

_AUDIT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("DATASET", "$1:dataset::VARCHAR"),
    ("KAFKA_TOPIC", "$1:kafka_topic::VARCHAR"),
    ("KAFKA_PARTITION", "$1:kafka_partition::NUMBER"),
    ("MIN_OFFSET", "$1:min_offset::NUMBER"),
    ("MAX_OFFSET", "$1:max_offset::NUMBER"),
    ("RECORDS_READ", "$1:records_read::NUMBER"),
    ("RECORDS_VALID", "$1:records_valid::NUMBER"),
    ("RECORDS_REJECTED", "$1:records_rejected::NUMBER"),
    ("RECORDS_WARNED", "$1:records_warned::NUMBER"),
    ("MIN_EVENT_TIMESTAMP", _ts("min_event_timestamp")),
    ("MAX_EVENT_TIMESTAMP", _ts("max_event_timestamp")),
    ("SPARK_QUERY_ID", "$1:spark_query_id::VARCHAR"),
    ("SPARK_BATCH_ID", "$1:spark_batch_id::NUMBER"),
    ("PROCESSED_AT", _ts("processed_at")),
    ("_FILE_NAME", "METADATA$FILENAME"),
)

_STORE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("STORE_ID", "$1"),
    ("STORE_NAME", "$2"),
    ("CITY", "$3"),
    ("STATE", "$4"),
    ("REGION", "$5"),
    ("STORE_FORMAT", "$6"),
    ("SIZE_SQM", "$7"),
    ("OPENED_DATE", "$8"),
    ("TIMEZONE", "$9"),
    ("TRAFFIC_WEIGHT", "$10"),
    ("_FILE_NAME", "METADATA$FILENAME"),
)


def _spec(dataset: str, table: str, pairs: tuple[tuple[str, str], ...], **kw: str) -> CopySpec:
    return CopySpec(dataset, table, tuple(c for c, _ in pairs), tuple(e for _, e in pairs), **kw)


EVENT_TABLES = {
    Dataset.POS_TRANSACTIONS: "RAW.POS_TRANSACTIONS",
    Dataset.INVENTORY_MOVEMENTS: "RAW.INVENTORY_MOVEMENTS",
    Dataset.CUSTOMER_EVENTS: "RAW.CUSTOMER_EVENTS",
    Dataset.PRODUCT_EVENTS: "RAW.PRODUCT_EVENTS",
}

COPY_SPECS: dict[str, CopySpec] = {
    **{ds.value: _spec(ds.value, table, _EVENT_COLUMNS) for ds, table in EVENT_TABLES.items()},
    DEAD_LETTERS: _spec(DEAD_LETTERS, "RAW.DEAD_LETTERS", _DEAD_LETTER_COLUMNS),
    INGEST_AUDIT: _spec(INGEST_AUDIT, "RAW.INGEST_BATCH_AUDIT", _AUDIT_COLUMNS),
}

STORES_SPEC = _spec(
    REFERENCE_STORES, "RAW.STORE_REFERENCE", _STORE_COLUMNS, file_format="RAW.FF_STORES_CSV"
)

# Load order within a cycle: audit first, so reconciliation never sees events without audit.
LOAD_ORDER: list[str] = [
    INGEST_AUDIT,
    DEAD_LETTERS,
    *(ds.value for ds in EVENT_TABLES),
]
