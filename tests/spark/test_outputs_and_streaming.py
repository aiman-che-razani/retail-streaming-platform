"""Output projections, aggregations, watermark behaviour and idempotent landing."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from retail_platform.contracts import dlq
from retail_platform.contracts.catalog import Dataset, TopicCatalog
from retail_platform.processing.sinks import IngestBatchWriter, batch_dir
from retail_platform.processing.transformations.aggregations import (
    product_units_1h,
    store_revenue_5m,
    valid_pos_sales,
)
from retail_platform.processing.transformations.envelope import decode_and_parse
from retail_platform.processing.transformations.outputs import (
    dlq_records,
    ingest_audit,
    landing_events,
)
from retail_platform.processing.transformations.validation import apply_rules, rules_for

from .conftest import KAFKA_SCHEMA, example, framed, fresh, kafka_df

pytestmark = pytest.mark.spark

LANDING_EVENT_COLUMNS = [
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "kafka_key",
    "schema_id", "event_id", "event_type", "schema_version", "event_timestamp", "produced_at",
    "producer", "correlation_id", "causation_id", "event_json", "dq_warnings", "ingested_at",
    "spark_query_id", "spark_batch_id",
]  # fmt: skip


def pos_event_at(ts: datetime, event_id: str, store: str = "KLCC-01") -> bytes:
    event = example("pos-transactions.basket-two-lines.json")
    event["metadata"]["event_id"] = event_id
    event["metadata"]["event_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    event["payload"]["store_id"] = store
    return framed(event)


def validated(spark: SparkSession, catalog: TopicCatalog, values: list[bytes]):  # type: ignore[no-untyped-def]
    df = decode_and_parse(kafka_df(spark, values), Dataset.POS_TRANSACTIONS)
    return apply_rules(
        df, rules_for(Dataset.POS_TRANSACTIONS, catalog.allowed_event_types("pos.transactions"), 5)
    )


def test_landing_columns_match_the_contract(spark: SparkSession, catalog: TopicCatalog) -> None:
    df = validated(
        spark, catalog, [framed(fresh(example("pos-transactions.basket-two-lines.json")))]
    )
    out = landing_events(df, "q-1", 7)
    assert out.columns == LANDING_EVENT_COLUMNS
    row = out.collect()[0]
    assert row.event_timestamp.endswith("Z")
    assert json.loads(row.event_json)["payload"]["store_id"] == "KLCC-01"
    assert row.spark_batch_id == 7


def test_dlq_records_keep_original_bytes_and_add_headers(
    spark: SparkSession, catalog: TopicCatalog
) -> None:
    bad = framed(example("pos-transactions.POS-006.total-mismatch.json"))
    df = validated(spark, catalog, [bad])
    (row,) = dlq_records(df, "pos.transactions.dlq", "spark-ingest/test").collect()
    assert bytes(row.value) == bad
    assert row.topic == "pos.transactions.dlq"
    headers = {h.key: bytes(h.value).decode() for h in row.headers}
    assert headers[dlq.ERROR_CODE] in {"POS-006", "ENV-006"}  # example date may be in the past only
    assert headers[dlq.SOURCE_TOPIC] == "pos.transactions"
    assert headers[dlq.SOURCE_OFFSET] == "0"


def test_audit_accounts_for_every_record(spark: SparkSession, catalog: TopicCatalog) -> None:
    good = framed(fresh(example("pos-transactions.basket-two-lines.json")))
    df = validated(spark, catalog, [good, b"garbage", good])
    (row,) = ingest_audit(df, "pos_transactions", "q", 1).collect()
    assert row.records_read == 3
    assert row.records_valid + row.records_rejected == row.records_read
    assert row.records_rejected == 1


def test_windowed_aggregations(spark: SparkSession, catalog: TopicCatalog) -> None:
    base = datetime(2026, 9, 29, 4, 0, tzinfo=UTC)
    values = [
        pos_event_at(base + timedelta(minutes=1), "00000000-0000-4000-8000-000000000001"),
        pos_event_at(base + timedelta(minutes=4), "00000000-0000-4000-8000-000000000002"),
        pos_event_at(base + timedelta(minutes=6), "00000000-0000-4000-8000-000000000003"),
        pos_event_at(base + timedelta(minutes=6), "00000000-0000-4000-8000-000000000003"),  # dup
    ]
    sales = valid_pos_sales(validated(spark, catalog, values), watermark_minutes=10)
    windows = {(r.store_id, r.window_start.minute): r for r in store_revenue_5m(sales).collect()}
    assert windows[("KLCC-01", 0)].transactions == 2
    assert windows[("KLCC-01", 0)].revenue == Decimal("85.60")
    assert windows[("KLCC-01", 5)].transactions == 1  # duplicate event_id removed
    products = {r.product_id: r for r in product_units_1h(sales).collect()}
    assert products["SKU-10293"].units == 6  # 2 units x 3 distinct sales


def test_watermark_drops_late_rows_and_counts_them(
    spark: SparkSession, catalog: TopicCatalog, tmp_path: Path
) -> None:
    """Streaming: after max(event_time) reaches 12:30, a 12:05 event (> 10 min late) is dropped."""
    source_dir = tmp_path / "source"
    base = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)

    def write_batch(name: str, values: list[bytes]) -> None:
        kafka_df(spark, values).write.mode("append").parquet(str(source_dir / name))

    source_dir.mkdir(parents=True)  # a streaming file source needs an existing directory
    stream = (
        spark.readStream.schema(KAFKA_SCHEMA)
        .option("recursiveFileLookup", "true")
        .parquet(str(source_dir))
    )
    rules = rules_for(
        Dataset.POS_TRANSACTIONS, catalog.allowed_event_types("pos.transactions"), 10**6
    )
    sales = valid_pos_sales(
        apply_rules(decode_and_parse(stream, Dataset.POS_TRANSACTIONS), rules), 10
    )

    def run_once() -> dict:  # type: ignore[type-arg]
        query = (
            store_revenue_5m(sales).writeStream.format("noop").queryName("wm_test")
            .outputMode("update").option("checkpointLocation", str(tmp_path / "ckpt"))
            .trigger(availableNow=True).start()
        )  # fmt: skip
        query.awaitTermination()
        return query.lastProgress  # type: ignore[no-any-return]

    write_batch(
        "b1",
        [
            pos_event_at(base + timedelta(minutes=m), f"00000000-0000-4000-8000-{m:012d}")
            for m in (1, 12, 30)
        ],
    )
    run_once()
    write_batch(
        "b2", [pos_event_at(base + timedelta(minutes=5), "00000000-0000-4000-8000-000000000099")]
    )
    progress = run_once()
    dropped = sum(op["numRowsDroppedByWatermark"] for op in progress["stateOperators"])
    assert dropped >= 1


def test_ingest_writer_is_idempotent_per_batch(
    spark: SparkSession, catalog: TopicCatalog, tmp_path: Path
) -> None:
    checkpoint = tmp_path / "ckpt"
    checkpoint.mkdir()
    (checkpoint / "metadata").write_text(json.dumps({"id": "query-abc"}))
    from retail_platform.config.settings import KafkaSettings

    writer = IngestBatchWriter(
        dataset=Dataset.POS_TRANSACTIONS,
        dlq_topic="pos.transactions.dlq",
        landing_dir=tmp_path / "landing",
        checkpoint_location=checkpoint,
        kafka_settings=KafkaSettings(bootstrap_servers="unused:9092"),
    )
    batch = validated(
        spark, catalog, [framed(fresh(example("pos-transactions.basket-two-lines.json")))]
    )
    writer(batch, 3)
    events_dir = batch_dir(tmp_path / "landing", "pos_transactions", "query-abc", 3)
    assert (events_dir / "_SUCCESS").exists()
    assert (batch_dir(tmp_path / "landing", "ingest_audit", "query-abc", 3) / "_SUCCESS").exists()
    first_files = sorted(p.name for p in events_dir.iterdir())

    writer(batch, 3)  # replay of the same batch id: must be skipped, files untouched
    assert sorted(p.name for p in events_dir.iterdir()) == first_files
    assert spark.read.parquet(str(events_dir)).count() == 1
