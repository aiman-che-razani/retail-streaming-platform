"""Kafka -> Spark -> landing/DLQ and Spark -> PostgreSQL integration tests.

Run inside the Spark image on the compose network (`make test-spark-integration`), so the
JVM, the Kafka connector jars and the services are all available. Each test uses its own
topics and checkpoint, so tests never interfere with the running pipeline or each other.
"""

from __future__ import annotations

import os
import socket
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_platform.config.settings import KafkaSettings, PostgresSettings
from retail_platform.contracts import dlq
from retail_platform.contracts.catalog import Dataset, TopicCatalog
from retail_platform.processing.jobs import kafka_source, observe_batch
from retail_platform.processing.sinks import IngestBatchWriter, PostgresUpsertWriter
from retail_platform.processing.transformations.envelope import decode_and_parse
from retail_platform.processing.transformations.validation import apply_rules, rules_for

from .conftest import example, framed

pytestmark = [pytest.mark.spark, pytest.mark.integration]

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def _reachable(host_port: str) -> bool:
    host, port = host_port.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def kafka() -> KafkaSettings:
    if not _reachable(BOOTSTRAP):
        pytest.skip(f"Kafka not reachable at {BOOTSTRAP} - run via `make test-spark-integration`")
    return KafkaSettings(bootstrap_servers=BOOTSTRAP)


@pytest.fixture
def topics(kafka: KafkaSettings) -> Iterator[dict[str, str]]:
    suffix = uuid.uuid4().hex[:8]
    names = {"source": f"it.pos.{suffix}", "dlq": f"it.pos.{suffix}.dlq"}
    admin = AdminClient({"bootstrap.servers": kafka.bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(n, num_partitions=2, replication_factor=1) for n in names.values()]
    )
    for future in futures.values():
        future.result()
    yield names
    admin.delete_topics(list(names.values()))


def pos_event(minutes_ago: float = 0.0, event_id: str | None = None) -> dict[str, Any]:
    event = example("pos-transactions.basket-two-lines.json")
    ts = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    event["metadata"]["event_id"] = event_id or str(uuid.uuid4())
    event["metadata"]["event_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    event["payload"]["transaction_id"] = f"TXN-IT-{uuid.uuid4().hex[:12].upper()}"
    return event


def produce(kafka: KafkaSettings, topic: str, values: list[bytes]) -> None:
    producer = Producer({"bootstrap.servers": kafka.bootstrap_servers})
    for value in values:
        producer.produce(topic, key=b"KLCC-01", value=value)
    assert producer.flush(15) == 0


def run_ingest_once(
    spark: SparkSession,
    kafka: KafkaSettings,
    catalog: TopicCatalog,
    topics: dict[str, str],
    tmp: Path,
) -> None:
    checkpoint = tmp / "checkpoint"
    source = kafka_source(
        spark,
        [topics["source"]],
        kafka,
        max_offsets_per_trigger=10_000,
        starting_offsets="earliest",
    )
    rules = rules_for(Dataset.POS_TRANSACTIONS, catalog.allowed_event_types("pos.transactions"), 5)
    validated = observe_batch(
        apply_rules(decode_and_parse(source, Dataset.POS_TRANSACTIONS), rules), "it"
    )
    writer = IngestBatchWriter(
        dataset=Dataset.POS_TRANSACTIONS,
        dlq_topic=topics["dlq"],
        landing_dir=tmp / "landing",
        checkpoint_location=checkpoint,
        kafka_settings=kafka,
    )
    query = (
        validated.writeStream.foreachBatch(writer)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(120)
    assert query.exception() is None


def dlq_records(kafka: KafkaSettings, topic: str) -> list[Any]:
    consumer = Consumer({"bootstrap.servers": kafka.bootstrap_servers, "group.id": uuid.uuid4().hex,
                         "auto.offset.reset": "earliest"})  # fmt: skip
    consumer.assign([TopicPartition(topic, p, 0) for p in (0, 1)])
    records, deadline = [], time.monotonic() + 20
    while time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            records.append(msg)
        elif records:
            break
    consumer.close()
    return records


def test_valid_events_land_and_invalid_events_go_to_dlq(
    spark: SparkSession,
    kafka: KafkaSettings,
    catalog: TopicCatalog,
    topics: dict[str, str],
    tmp_path: Path,
) -> None:
    valid = [framed(pos_event()) for _ in range(5)]
    bad_total = pos_event()
    bad_total["payload"]["total_amount"] = 1.0
    invalid = [framed(bad_total), b"not even framed"]
    produce(kafka, topics["source"], valid + invalid)

    run_ingest_once(spark, kafka, catalog, topics, tmp_path)

    landed = spark.read.option("recursiveFileLookup", "true").parquet(
        str(tmp_path / "landing" / "pos_transactions")
    )
    assert landed.count() == 5
    audit = spark.read.option("recursiveFileLookup", "true").parquet(
        str(tmp_path / "landing" / "ingest_audit")
    )
    totals = audit.agg(
        F.sum("records_read"), F.sum("records_valid"), F.sum("records_rejected")
    ).first()
    assert tuple(totals) == (7, 5, 2)

    codes = sorted(
        dict(m.headers())[dlq.ERROR_CODE].decode() for m in dlq_records(kafka, topics["dlq"])
    )
    assert codes == ["ENV-001", "POS-006"]


def test_restart_from_checkpoint_processes_only_new_records(
    spark: SparkSession,
    kafka: KafkaSettings,
    catalog: TopicCatalog,
    topics: dict[str, str],
    tmp_path: Path,
) -> None:
    """FM-03/FM-17: a restarted query resumes from its checkpoint - no loss, no duplicates."""
    produce(kafka, topics["source"], [framed(pos_event()) for _ in range(3)])
    run_ingest_once(spark, kafka, catalog, topics, tmp_path)
    produce(kafka, topics["source"], [framed(pos_event()) for _ in range(4)])
    run_ingest_once(spark, kafka, catalog, topics, tmp_path)  # "restart": same checkpoint

    landed = spark.read.option("recursiveFileLookup", "true").parquet(
        str(tmp_path / "landing" / "pos_transactions")
    )
    assert landed.count() == 7
    assert landed.select("kafka_partition", "kafka_offset").distinct().count() == 7


@pytest.fixture
def postgres() -> PostgresSettings:
    settings = PostgresSettings()
    if not _reachable(f"{settings.host}:{settings.port}"):
        pytest.skip("PostgreSQL not reachable - run via `make test-spark-integration`")
    return settings


def test_postgres_upsert_is_idempotent_on_replay(
    spark: SparkSession, postgres: PostgresSettings
) -> None:
    import psycopg

    product = f"SKU-9{uuid.uuid4().int % 10000:04d}"
    window = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    batch = spark.createDataFrame(
        [(product, window, window + timedelta(hours=1), 7, Decimal("42.50"))],
        "product_id string, window_start timestamp, window_end timestamp, "
        "units long, revenue decimal(14,2)",
    )
    writer = PostgresUpsertWriter(
        table="product_units_1h",
        keys=("product_id", "window_start"),
        values=("window_end", "units", "revenue"),
        timestamp_columns=("window_start", "window_end"),
        settings=postgres,
    )
    try:
        writer(batch, 1)
        writer(batch, 1)  # replay of the same batch after a crash
        with psycopg.connect(postgres.conninfo()) as conn:
            rows = conn.execute(
                "SELECT units, revenue FROM realtime.product_units_1h WHERE product_id = %s",
                (product,),
            ).fetchall()
        assert rows == [(7, Decimal("42.50"))]
    finally:
        with psycopg.connect(postgres.conninfo()) as conn:
            conn.execute("DELETE FROM realtime.product_units_1h WHERE product_id = %s", (product,))
