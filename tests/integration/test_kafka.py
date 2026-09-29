"""Python -> Kafka integration tests (needs `make up`)."""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from prometheus_client import CollectorRegistry

from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts import dlq
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.contracts.models import PosTransaction
from retail_platform.messaging import wire
from retail_platform.messaging.dlq import Redriver, consumer_config
from retail_platform.messaging.producer import KafkaEventSink, ProducerMetrics, producer_config
from retail_platform.messaging.records import OutgoingEvent
from tests.factories import T0, make_engine

from .conftest import counter_value, tail_consumer

pytestmark = pytest.mark.integration


def _poll_until(consumer: Consumer, predicate, timeout: float = 20.0):  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is not None and msg.error() is None and predicate(msg):
            return msg
    pytest.fail("expected record not consumed in time")


def test_topics_exist_with_declared_partitions_and_configs(
    kafka_settings: KafkaSettings, catalog: TopicCatalog
) -> None:
    admin = AdminClient({"bootstrap.servers": kafka_settings.bootstrap_servers})
    topics = admin.list_topics(timeout=10).topics
    for spec in catalog.topics:
        assert spec.name in topics, f"{spec.name} missing - run `make topics`"
        assert len(topics[spec.name].partitions) == spec.partitions
    resource = ConfigResource(ResourceType.TOPIC, "pos.transactions")
    config = admin.describe_configs([resource])[resource].result()
    assert config["cleanup.policy"].value == "delete"
    assert config["retention.ms"].value == "604800000"


def test_event_round_trip_with_key_headers_and_wire_format(
    kafka_settings: KafkaSettings, sink: KafkaEventSink, sr_client: SchemaRegistryClient
) -> None:
    event = make_engine(seed=int(time.time())).sale(datetime.now(UTC))[0]
    consumer = tail_consumer(kafka_settings, "pos.transactions")
    try:
        sink.send(event)
        assert sink.flush(10) == 0
        msg = _poll_until(consumer, lambda m: event.event_id.encode() in (m.value() or b""))
    finally:
        consumer.close()

    assert event.envelope is not None and isinstance(event.envelope.payload, PosTransaction)
    assert msg.key() == event.envelope.payload.store_id.encode()
    framed = wire.decode(msg.value())
    registered = sr_client.get_latest_version("pos.transactions-value")
    assert framed.schema_id == registered.schema_id
    body = json.loads(framed.payload)
    assert body["metadata"]["event_id"] == event.event_id
    assert dict(msg.headers())["event_type"] == b"pos.transaction.completed"


def test_producer_refuses_schema_violations(
    sink: KafkaEventSink, producer_metrics: ProducerMetrics
) -> None:
    good = make_engine().sale(T0)[0]
    assert good.envelope is not None
    bad_payload = good.envelope.payload.model_copy(update={"store_id": "not a store"})
    bad = OutgoingEvent(
        good.topic, good.key, envelope=good.envelope.model_copy(update={"payload": bad_payload})
    )
    sink.send(bad)
    assert counter_value(producer_metrics.validation_failures, "pos.transactions") == 1
    assert counter_value(producer_metrics.messages, "pos.transactions", "acked") == 0


@pytest.mark.parametrize(
    ("change", "compatible"),
    [("add_optional_field", True), ("remove_field", False), ("change_type", False)],
)
def test_registry_enforces_backward_transitive_rules(
    sr_client: SchemaRegistryClient, catalog: TopicCatalog, change: str, compatible: bool
) -> None:
    """The evolution table in event-contracts.md §6, verified against the real registry."""
    subject = f"it-evolution-{uuid.uuid4().hex[:8]}-value"
    base = catalog.schema_for("pos.transactions")
    sr_client.register_schema(subject, Schema(json.dumps(base), "JSON"))
    sr_client.set_compatibility(subject_name=subject, level="BACKWARD_TRANSITIVE")

    evolved = json.loads(json.dumps(base))
    payload = evolved["definitions"]["payload"]
    if change == "add_optional_field":
        payload["properties"]["channel"] = {
            "type": "string",
            "enum": ["IN_STORE", "CLICK_AND_COLLECT"],
        }
    elif change == "remove_field":
        del payload["properties"]["register_id"]
        payload["required"].remove("register_id")
    else:
        payload["properties"]["total_amount"] = {"type": "string"}
    try:
        assert (
            sr_client.test_compatibility(subject, Schema(json.dumps(evolved), "JSON")) is compatible
        )
    finally:
        sr_client.delete_subject(subject, permanent=False)
        sr_client.delete_subject(subject, permanent=True)


def test_redrive_republishes_original_bytes_to_retry_topic(kafka_settings: KafkaSettings) -> None:
    code = f"IT-{uuid.uuid4().hex[:8]}"
    original = b'\x00\x00\x00\x00\x01{"broken": true}'
    producer = Producer(producer_config(kafka_settings, "it-dlq-writer"))
    for redrive_count in (0, dlq.MAX_REDRIVES):  # second one must hit the loop guard
        producer.produce(
            "pos.transactions.dlq",
            key=b"KLCC-01",
            value=original,
            headers=[
                (dlq.ERROR_CODE, code.encode()),
                (dlq.ERROR_STAGE, b"validate"),
                (dlq.REDRIVE_COUNT, str(redrive_count).encode()),
            ],
        )
    assert producer.flush(10) == 0

    retry_consumer = tail_consumer(kafka_settings, "pos.transactions.retry")
    consumer = Consumer(consumer_config(kafka_settings, f"it-redrive-{uuid.uuid4().hex}"))
    try:
        report = Redriver(
            consumer, Producer(producer_config(kafka_settings, "it-redrive"))
        ).redrive(
            dlq_topic="pos.transactions.dlq",
            retry_topic="pos.transactions.retry",
            predicate=lambda r: r.error_code == code,
            max_records=None,
            dry_run=False,
            force=False,
            from_beginning=True,
        )
        assert report.redriven == 1
        assert report.skipped_loop_guard == 1
        msg = _poll_until(
            retry_consumer, lambda m: dict(m.headers() or []).get(dlq.ERROR_CODE) == code.encode()
        )
    finally:
        consumer.close()
        retry_consumer.close()
    assert msg.value() == original  # byte-for-byte
    assert msg.key() == b"KLCC-01"
    assert dict(msg.headers())[dlq.REDRIVE_COUNT] == b"1"


def test_unavailable_broker_is_reported_not_silently_dropped(
    catalog: TopicCatalog, sr_client: SchemaRegistryClient
) -> None:
    """FM-01: when Kafka is unreachable, delivery fails within the timeout and is counted."""
    metrics = ProducerMetrics(CollectorRegistry())
    settings = KafkaSettings(bootstrap_servers="localhost:1", delivery_timeout_ms=2000)
    sink = KafkaEventSink(
        settings=settings,
        catalog=catalog,
        registry_client=sr_client,
        metrics=metrics,
        client_id="it-unavailable",
    )
    sink.send(make_engine().sale(T0)[0])
    remaining = sink.flush(10)
    assert remaining == 0  # nothing stuck: the record failed definitively
    assert counter_value(metrics.messages, "pos.transactions", "failed") == 1
