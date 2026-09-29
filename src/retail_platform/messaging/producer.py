"""Idempotent Kafka producer with JSON Schema serialisation (kafka-topology.md §3).

Why each non-default setting exists is documented in kafka-topology.md; the short version:

* `acks=all` + `enable.idempotence=true` — no loss on leader failover, no duplicates from
  internal retries (exactly-once per partition per producer session);
* `partitioner=murmur2_random` — same key -> partition mapping as Java clients;
* `linger.ms`/`batch.size`/`compression.type=zstd` — throughput and smaller JSON on the wire;
* `delivery.timeout.ms` — bounded "delivered or failed" time, reported via callbacks.

`produce()` only enqueues. Success/failure is known in the delivery callback, which updates
metrics and logs failures with the event id. `close()` flushes and reports anything that
could not be delivered — a non-zero number there is potential data loss and is logged as an
error, never silently ignored.
"""

from __future__ import annotations

import socket
import time
from types import TracebackType
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError as _SRError
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError
from prometheus_client import CollectorRegistry, Counter, Histogram

from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.errors import SchemaRegistryError
from retail_platform.messaging import wire
from retail_platform.messaging.records import OutgoingEvent
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)

Headers = list[tuple[str, str | bytes | None]]


class ProducerMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self.messages = Counter(
            "retail_producer_messages_total",
            "Records whose delivery completed, by outcome",
            ["topic", "status"],
            registry=registry,
        )
        self.delivery_latency = Histogram(
            "retail_producer_delivery_latency_seconds",
            "Time from produce() to broker acknowledgement",
            ["topic"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 120),
            registry=registry,
        )
        self.validation_failures = Counter(
            "retail_producer_validation_failures_total",
            "Events refused by producer-side schema validation",
            ["topic"],
            registry=registry,
        )
        self.queue_full = Counter(
            "retail_producer_queue_full_total",
            "Times produce() blocked because the local queue was full (back-pressure)",
            registry=registry,
        )


def producer_config(settings: KafkaSettings, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": settings.delivery_timeout_ms,
        "linger.ms": settings.linger_ms,
        "batch.size": settings.batch_size_bytes,
        "compression.type": settings.compression_type,
        "partitioner": "murmur2_random",
        **settings.client_security_config(),
    }


def default_client_id(service: str) -> str:
    return f"{service}-{socket.gethostname()}"


class KafkaEventSink:
    """`EventSink` for the simulator; also used by the DLQ redrive tool for raw records."""

    def __init__(
        self,
        *,
        settings: KafkaSettings,
        catalog: TopicCatalog,
        registry_client: SchemaRegistryClient,
        metrics: ProducerMetrics,
        client_id: str,
        producer: Producer | None = None,
    ) -> None:
        self._metrics = metrics
        self._producer = producer or Producer(producer_config(settings, client_id))
        self._serializers: dict[str, JSONSerializer] = {}
        self._schema_ids: dict[str, int] = {}
        for topic in catalog.primaries():
            self._serializers[topic.name] = JSONSerializer(
                catalog.schema_text_for(topic.name),
                registry_client,
                # Only CI/bootstrap registers schemas: an unregistered or incompatible schema
                # must fail here instead of silently creating a new version.
                conf={"auto.register.schemas": False, "use.latest.version": False},
            )
            try:
                latest = registry_client.get_latest_version(topic.subject)
            except (_SRError, OSError) as exc:
                raise SchemaRegistryError(
                    f"subject {topic.subject} not registered - run `make schemas`"
                ) from exc
            if latest.schema_id is None:
                raise SchemaRegistryError(f"subject {topic.subject} has no schema id")
            self._schema_ids[topic.name] = int(latest.schema_id)

    # ------------------------------------------------------------------ EventSink
    def send(self, event: OutgoingEvent) -> None:
        primary = event.topic
        headers: Headers = []
        if event.envelope is not None:
            meta = event.envelope.metadata
            headers = [
                ("event_type", meta.event_type.value.encode()),
                ("schema_version", meta.schema_version.encode()),
                ("correlation_id", str(meta.correlation_id).encode()),
            ]
            try:
                value = self._serializers[primary](
                    event.envelope.to_wire_dict(),
                    SerializationContext(primary, MessageField.VALUE),
                )
            except SerializationError as exc:
                # The producer refuses to publish contract violations (event-contracts.md §1.6).
                self._metrics.validation_failures.labels(primary).inc()
                log.warning(
                    "event_rejected_by_schema",
                    topic=primary,
                    event_id=event.event_id,
                    error=str(exc)[:300],
                )
                return
        else:
            raw = event.raw_body or b""
            value = wire.encode(self._schema_ids[primary], raw) if event.framed else raw
        if value is None:
            raise SerializationError(f"serializer returned no bytes for {primary}")
        self.produce_raw(
            primary, event.key.encode(), value, headers, event_id=event.event_id, fault=event.fault
        )

    def poll(self) -> None:
        self._producer.poll(0)

    # ------------------------------------------------------------------ raw produce
    def produce_raw(
        self,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: Headers,
        *,
        event_id: str | None = None,
        fault: str | None = None,
    ) -> None:
        started = time.monotonic()

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                self._metrics.messages.labels(topic, "failed").inc()
                log.error(
                    "delivery_failed",
                    topic=topic,
                    event_id=event_id,
                    error=err.str(),
                    retriable=err.retriable(),
                    status="failed",
                )
                return
            self._metrics.messages.labels(topic, "acked").inc()
            self._metrics.delivery_latency.labels(topic).observe(time.monotonic() - started)
            if fault:
                log.info(
                    "fault_injected_event_delivered",
                    topic=topic,
                    partition=msg.partition(),
                    offset=msg.offset(),
                    event_id=event_id,
                    fault=fault,
                )

        while True:
            try:
                self._producer.produce(
                    topic, key=key, value=value, headers=headers, on_delivery=on_delivery
                )
            except BufferError:
                # Local queue full (broker slow/unavailable): block here, serving callbacks,
                # instead of buffering unboundedly in memory (FM-01).
                self._metrics.queue_full.inc()
                self._producer.poll(0.5)
            else:
                return

    # ------------------------------------------------------------------ lifecycle
    def flush(self, timeout_seconds: float = 30.0) -> int:
        """Wait for outstanding deliveries. Returns the number still undelivered."""
        return int(self._producer.flush(timeout_seconds))

    def close(self, timeout_seconds: float = 30.0) -> int:
        remaining = self.flush(timeout_seconds)
        if remaining:
            log.error("producer_closed_with_undelivered_records", undelivered=remaining)
        else:
            log.info("producer_closed", undelivered=0)
        return remaining

    def __enter__(self) -> KafkaEventSink:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
