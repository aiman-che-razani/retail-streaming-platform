"""Integration fixtures: talk to the docker compose services from the host.

Tests skip (not fail) when a service is unreachable, with a message saying what to start.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from prometheus_client import CollectorRegistry

from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.messaging.producer import KafkaEventSink, ProducerMetrics

KAFKA = ("localhost", 9092)
SCHEMA_REGISTRY = ("localhost", 8081)


def _reachable(host_port: tuple[str, int]) -> bool:
    try:
        with socket.create_connection(host_port, timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def kafka_settings() -> KafkaSettings:
    if not (_reachable(KAFKA) and _reachable(SCHEMA_REGISTRY)):
        pytest.skip("Kafka/Schema Registry not reachable - run `make up`")
    return KafkaSettings(
        bootstrap_servers=f"{KAFKA[0]}:{KAFKA[1]}",
        schema_registry_url=f"http://{SCHEMA_REGISTRY[0]}:{SCHEMA_REGISTRY[1]}",
    )


@pytest.fixture(scope="session")
def sr_client(kafka_settings: KafkaSettings) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": kafka_settings.schema_registry_url})


@pytest.fixture
def producer_metrics() -> ProducerMetrics:
    return ProducerMetrics(CollectorRegistry())


@pytest.fixture
def sink(
    kafka_settings: KafkaSettings,
    catalog: TopicCatalog,
    sr_client: SchemaRegistryClient,
    producer_metrics: ProducerMetrics,
) -> Iterator[KafkaEventSink]:
    with KafkaEventSink(
        settings=kafka_settings,
        catalog=catalog,
        registry_client=sr_client,
        metrics=producer_metrics,
        client_id="it-producer",
    ) as s:
        yield s


def tail_consumer(kafka_settings: KafkaSettings, topic: str) -> Consumer:
    """A consumer positioned at the current END of every partition (sees only new records)."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_settings.bootstrap_servers,
            "group.id": f"it-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    metadata = consumer.list_topics(topic, timeout=10)
    partitions = []
    for p in metadata.topics[topic].partitions:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        partitions.append(TopicPartition(topic, p, high))
    consumer.assign(partitions)
    return consumer


def counter_value(metric: object, *labels: str) -> float:
    value: float = metric.labels(*labels)._value.get()  # type: ignore[attr-defined]
    return value
