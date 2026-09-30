"""End-to-end reconciliation: can every Kafka record be accounted for? (F9, N4)

For each primary topic the report follows records through every hop:

    Kafka records (high - low watermark, within retention)
      -> read by Spark                 (RAW.INGEST_BATCH_AUDIT.records_read)
         = valid + rejected            (Spark's own accounting must balance)
      -> valid records loaded to RAW   (distinct offsets in RAW)
      -> business keys in STAGING / rows in facts

Every gap gets an explanation category instead of a bare "mismatch":

    not_yet_processed    Kafka offsets beyond what Spark has landed (consumer lag)
    rejected_to_dlq      failed validation; in <topic>.dlq and RAW.DEAD_LETTERS
    not_yet_loaded       landed by Spark but not yet COPY'd into RAW (loader interval)
    duplicate            same business key delivered again (removed by STAGING)
    unexplained          anything else -> investigate (reported as a failure)

The pure `reconcile()` function holds the arithmetic and is unit-tested; the I/O functions
gather its inputs from Kafka and Snowflake.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class PartitionOffsets:
    low: int
    high: int  # next offset to be written


@dataclass(frozen=True, slots=True)
class TopicInputs:
    """Everything known about one primary topic (+ its retry topic) across the layers."""

    topic: str
    kafka: dict[int, PartitionOffsets]
    spark_max_offset: dict[int, int]  # highest offset Spark has landed, per partition
    spark_read: int
    spark_valid: int
    spark_rejected: int
    raw_distinct_offsets: int
    staged_keys: int | None = None  # STAGING business keys (POS/inventory only)
    raw_distinct_keys: int | None = None


@dataclass
class TopicReport:
    topic: str
    kafka_records_in_retention: int
    not_yet_processed: int
    spark_read: int
    spark_valid: int
    rejected_to_dlq: int
    loaded_to_raw: int
    not_yet_loaded: int
    duplicates_removed_in_staging: int | None
    spark_accounting_balanced: bool
    unexplained: int
    status: str
    notes: list[str] = field(default_factory=list)


def reconcile(inputs: TopicInputs) -> TopicReport:
    in_retention = sum(max(0, p.high - p.low) for p in inputs.kafka.values())
    not_yet_processed = 0
    for partition, offsets in inputs.kafka.items():
        processed_up_to = inputs.spark_max_offset.get(partition, offsets.low - 1) + 1
        not_yet_processed += max(0, offsets.high - max(processed_up_to, offsets.low))

    balanced = inputs.spark_read == inputs.spark_valid + inputs.spark_rejected
    not_yet_loaded = max(0, inputs.spark_valid - inputs.raw_distinct_offsets)
    # RAW holding MORE distinct offsets than Spark reported valid can't be explained by lag.
    unexplained = max(0, inputs.raw_distinct_offsets - inputs.spark_valid)

    duplicates = None
    notes: list[str] = []
    if inputs.staged_keys is not None and inputs.raw_distinct_keys is not None:
        duplicates = max(0, inputs.raw_distinct_offsets - inputs.raw_distinct_keys)
        missing_in_staging = max(0, inputs.raw_distinct_keys - inputs.staged_keys)
        if missing_in_staging:
            notes.append(
                f"{missing_in_staging} business keys in RAW not yet in STAGING "
                "(task graph runs every 15 min; persistent values fail SF-006)"
            )
    if not balanced:
        notes.append("Spark read != valid + rejected: audit rows missing or duplicated")
    if inputs.spark_read and in_retention and inputs.spark_read > in_retention:
        notes.append("Spark read more than Kafka retains: older records already expired (normal)")

    ok = balanced and unexplained == 0
    return TopicReport(
        topic=inputs.topic,
        kafka_records_in_retention=in_retention,
        not_yet_processed=not_yet_processed,
        spark_read=inputs.spark_read,
        spark_valid=inputs.spark_valid,
        rejected_to_dlq=inputs.spark_rejected,
        loaded_to_raw=inputs.raw_distinct_offsets,
        not_yet_loaded=not_yet_loaded,
        duplicates_removed_in_staging=duplicates,
        spark_accounting_balanced=balanced,
        unexplained=unexplained,
        status="OK" if ok else "INVESTIGATE",
        notes=notes,
    )


def as_dict(report: TopicReport) -> dict[str, Any]:
    return asdict(report)
