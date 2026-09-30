"""Decode/parse/validate transformations against the contract examples."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import SparkSession

from retail_platform.contracts.catalog import Dataset, TopicCatalog
from retail_platform.processing.transformations.envelope import decode_and_parse
from retail_platform.processing.transformations.validation import apply_rules, rules_for

from .conftest import EXAMPLES, example, framed, fresh, kafka_df

pytestmark = pytest.mark.spark

TOPICS = {
    "pos-transactions": ("pos.transactions", Dataset.POS_TRANSACTIONS),
    "inventory-events": ("inventory.events", Dataset.INVENTORY_MOVEMENTS),
    "customer-events": ("customer.events", Dataset.CUSTOMER_EVENTS),
    "product-updates": ("product.updates", Dataset.PRODUCT_EVENTS),
}


def validate(
    spark: SparkSession, catalog: TopicCatalog, values: list[bytes], schema_stem: str
) -> list[dict[str, Any]]:
    topic, dataset = TOPICS[schema_stem]
    df = decode_and_parse(kafka_df(spark, values, topic=topic), dataset)
    rules = rules_for(dataset, catalog.allowed_event_types(topic), future_tolerance_minutes=5)
    rows = apply_rules(df, rules).select("offset", "is_valid", "rejection", "dq_warnings").collect()
    # collect() is fine here: a handful of rows in a unit test, never in pipeline code.
    return [r.asDict(recursive=True) for r in sorted(rows, key=lambda r: r.offset)]


@pytest.mark.parametrize("path", sorted((EXAMPLES / "valid").glob("*.json")), ids=lambda p: p.name)
def test_valid_examples_pass(spark: SparkSession, catalog: TopicCatalog, path) -> None:  # type: ignore[no-untyped-def]
    stem = path.name.split(".")[0]
    (row,) = validate(spark, catalog, [framed(fresh(json.loads(path.read_text())))], stem)
    assert row["is_valid"], row["rejection"]
    assert row["dq_warnings"] == []


@pytest.mark.parametrize(
    "path", sorted((EXAMPLES / "invalid").glob("*.json")), ids=lambda p: p.name
)
def test_invalid_examples_are_rejected_with_their_rule(
    spark: SparkSession, catalog: TopicCatalog, path
) -> None:  # type: ignore[no-untyped-def]
    stem, rule_id = path.name.split(".")[:2]
    event = json.loads(path.read_text())
    if "metadata" in event and "event_timestamp" in event["metadata"]:
        event = {
            **event,
            "metadata": {
                **event["metadata"],
                "event_timestamp": fresh(event)["metadata"]["event_timestamp"],
            },
        }
    (row,) = validate(spark, catalog, [framed(event)], stem)
    assert not row["is_valid"]
    assert row["rejection"]["rule_id"] == rule_id


def test_malformed_json_is_env_002(spark: SparkSession, catalog: TopicCatalog) -> None:
    text = (EXAMPLES / "invalid" / "pos-transactions.ENV-002.malformed-json.txt").read_text()
    (row,) = validate(spark, catalog, [framed(text)], "pos-transactions")
    assert row["rejection"] == {
        "rule_id": "ENV-002",
        "stage": "parse",
        "message": row["rejection"]["message"],
    }


def test_unframed_value_is_env_001(spark: SparkSession, catalog: TopicCatalog) -> None:
    raw = json.dumps(fresh(example("pos-transactions.basket-two-lines.json"))).encode()
    (row,) = validate(spark, catalog, [raw], "pos-transactions")
    assert row["rejection"]["rule_id"] == "ENV-001"
    assert row["rejection"]["stage"] == "decode"


def test_future_timestamp_is_env_006(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = example("pos-transactions.basket-two-lines.json")
    future = datetime.now(UTC) + timedelta(hours=2)
    event["metadata"]["event_timestamp"] = future.strftime("%Y-%m-%dT%H:%M:%SZ")
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["rejection"]["rule_id"] == "ENV-006"


def test_old_events_are_valid_on_the_ingest_path(
    spark: SparkSession, catalog: TopicCatalog
) -> None:
    """Lateness is NOT a validation failure: the ingest path must keep late sales (ADR-004)."""
    event = example("pos-transactions.basket-two-lines.json")  # dated in the past
    event["metadata"]["event_timestamp"] = "2025-01-01T08:00:00Z"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["is_valid"]


def test_wrong_event_type_is_env_004(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    event["metadata"]["event_type"] = "product.updated"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["rejection"]["rule_id"] == "ENV-004"


def test_major_version_2_is_env_005(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    event["metadata"]["schema_version"] = "2.0"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["rejection"]["rule_id"] == "ENV-005"


def test_wrong_type_field_is_rejected_not_crashing(
    spark: SparkSession, catalog: TopicCatalog
) -> None:
    """A string where a number is expected must become a DLQ record, never an exception."""
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    event["payload"]["line_items"][0]["quantity"] = "two"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert not row["is_valid"]


def test_three_decimal_places_is_pos_007(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    line = event["payload"]["line_items"][0]
    line["unit_price"] = 18.905
    event["payload"]["total_amount"] = round(2 * 18.905 + 5.0, 3)
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["rejection"]["rule_id"] == "POS-007"


def test_unknown_payment_method_is_a_warning(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    event["payload"]["payment_method"] = "CRYPTO"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["is_valid"]
    assert row["dq_warnings"] == ["POS-008"]


def test_negative_stock_is_a_warning(spark: SparkSession, catalog: TopicCatalog) -> None:
    event = fresh(example("inventory-events.sale-depletion.json"))
    event["payload"]["quantity_on_hand_after"] = -3
    (row,) = validate(spark, catalog, [framed(event)], "inventory-events")
    assert row["is_valid"]
    assert row["dq_warnings"] == ["INV-003"]


def test_unknown_extra_fields_are_tolerated(spark: SparkSession, catalog: TopicCatalog) -> None:
    """Forward compatibility: a v1.1 producer adding an optional field must not break v1."""
    event = fresh(example("pos-transactions.basket-two-lines.json"))
    event["metadata"]["schema_version"] = "1.1"
    event["payload"]["channel"] = "CLICK_AND_COLLECT"
    (row,) = validate(spark, catalog, [framed(event)], "pos-transactions")
    assert row["is_valid"]


def test_product_without_brand_is_rejected(spark: SparkSession, catalog: TopicCatalog) -> None:
    """DIM_PRODUCT.BRAND is NOT NULL: one such event would otherwise stall the task graph."""
    event = fresh(example("product-updates.price-change.json"))
    del event["payload"]["brand"]
    (row,) = validate(spark, catalog, [framed(event)], "product-updates")
    assert row["rejection"]["rule_id"] == "PRD-001"
