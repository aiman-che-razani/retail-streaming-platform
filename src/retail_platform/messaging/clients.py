"""Factories for Kafka/Schema Registry clients plus readiness waiting.

`depends_on: service_healthy` in Docker Compose orders startup, but a dependency can still be
restarting or briefly unavailable, so every client waits for readiness itself with bounded
exponential backoff and then fails with a clear error (overview.md §7).
"""

from __future__ import annotations

import time
from collections.abc import Callable

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError as _SRError

from retail_platform.config.settings import KafkaSettings
from retail_platform.errors import KafkaUnavailableError, SchemaRegistryError
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)


def admin_client(settings: KafkaSettings) -> AdminClient:
    return AdminClient(
        {"bootstrap.servers": settings.bootstrap_servers, **settings.client_security_config()}
    )


def schema_registry_client(settings: KafkaSettings) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": settings.schema_registry_url})


def retry_until[T](
    probe: Callable[[], T],
    *,
    what: str,
    timeout_seconds: float,
    retry_on: tuple[type[BaseException], ...],
    initial_backoff: float = 0.5,
    max_backoff: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> T:
    """Call `probe` until it succeeds or `timeout_seconds` elapse (exponential backoff)."""
    deadline = clock() + timeout_seconds
    backoff = initial_backoff
    attempt = 0
    while True:
        attempt += 1
        try:
            return probe()
        except retry_on as exc:
            if clock() + backoff > deadline:
                raise TimeoutError(f"{what} not ready after {timeout_seconds:.0f}s: {exc}") from exc
            log.info("dependency_not_ready", dependency=what, attempt=attempt, retry_in_s=backoff)
            sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


def wait_for_kafka(admin: AdminClient, timeout_seconds: float) -> None:
    def probe() -> None:
        admin.list_topics(timeout=5)

    try:
        retry_until(
            probe, what="kafka", timeout_seconds=timeout_seconds, retry_on=(KafkaException,)
        )
    except TimeoutError as exc:
        raise KafkaUnavailableError(str(exc)) from exc


def wait_for_schema_registry(client: SchemaRegistryClient, timeout_seconds: float) -> None:
    def probe() -> None:
        client.get_subjects()

    try:
        retry_until(
            probe,
            what="schema-registry",
            timeout_seconds=timeout_seconds,
            retry_on=(_SRError, OSError, ConnectionError),
        )
    except TimeoutError as exc:
        raise SchemaRegistryError(str(exc)) from exc
