from __future__ import annotations

import pytest

from retail_platform.quality.reconciliation import PartitionOffsets, TopicInputs, reconcile

pytestmark = pytest.mark.unit


def inputs(**overrides: object) -> TopicInputs:
    base: dict[str, object] = {
        "topic": "pos.transactions",
        "kafka": {0: PartitionOffsets(0, 100), 1: PartitionOffsets(0, 50)},
        "spark_max_offset": {0: 99, 1: 49},
        "spark_read": 150,
        "spark_valid": 145,
        "spark_rejected": 5,
        "raw_distinct_offsets": 145,
        "staged_keys": 140,
        "raw_distinct_keys": 140,
    }
    base.update(overrides)
    return TopicInputs(**base)  # type: ignore[arg-type]


def test_fully_reconciled_topic_is_ok() -> None:
    report = reconcile(inputs())
    assert report.status == "OK"
    assert report.kafka_records_in_retention == 150
    assert report.not_yet_processed == 0
    assert report.rejected_to_dlq == 5
    assert report.not_yet_loaded == 0
    assert report.duplicates_removed_in_staging == 5  # 145 offsets carried 140 distinct keys


def test_consumer_lag_is_explained_not_a_failure() -> None:
    report = reconcile(inputs(spark_max_offset={0: 79, 1: 49}))
    assert report.not_yet_processed == 20
    assert report.status == "OK"


def test_loader_lag_is_explained_not_a_failure() -> None:
    report = reconcile(inputs(raw_distinct_offsets=120))
    assert report.not_yet_loaded == 25
    assert report.status == "OK"


def test_more_rows_in_raw_than_spark_validated_is_unexplained() -> None:
    report = reconcile(inputs(raw_distinct_offsets=150))
    assert report.unexplained == 5
    assert report.status == "INVESTIGATE"


def test_unbalanced_spark_accounting_needs_investigation() -> None:
    report = reconcile(inputs(spark_read=151))
    assert not report.spark_accounting_balanced
    assert report.status == "INVESTIGATE"


def test_partition_never_processed_counts_everything_as_lag() -> None:
    report = reconcile(inputs(spark_max_offset={0: 99}))
    assert report.not_yet_processed == 50


def test_retention_expiry_is_noted() -> None:
    report = reconcile(inputs(kafka={0: PartitionOffsets(90, 100), 1: PartitionOffsets(40, 50)}))
    assert report.kafka_records_in_retention == 20
    assert any("expired" in note for note in report.notes)
