"""Schema Registry management: compatibility level, compatibility checks and registration.

Producers run with `auto.register.schemas=False`: only this bootstrap/CI step may register
schemas (event-contracts.md §6). That turns an incompatible change into a deploy-time failure
instead of a runtime surprise.
"""

from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError as _SRError

from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.errors import IncompatibleSchemaError, SchemaRegistryError
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)

COMPATIBILITY_LEVEL = "BACKWARD_TRANSITIVE"
_SUBJECT_NOT_FOUND = 40401


@dataclass(frozen=True, slots=True)
class CompatibilityResult:
    subject: str
    compatible: bool
    is_new_subject: bool


class SchemaManager:
    def __init__(self, client: SchemaRegistryClient, catalog: TopicCatalog) -> None:
        self._client = client
        self._catalog = catalog

    def _schema(self, topic: str) -> Schema:
        return Schema(self._catalog.schema_text_for(topic), schema_type="JSON")

    def _subject_exists(self, subject: str) -> bool:
        try:
            return subject in self._client.get_subjects()
        except _SRError as exc:
            raise SchemaRegistryError(f"cannot list subjects: {exc}") from exc

    def check_all(self) -> list[CompatibilityResult]:
        """Test every declared schema against the registry without registering it."""
        results = []
        for topic in self._catalog.primaries():
            subject = topic.subject
            if not self._subject_exists(subject):
                results.append(CompatibilityResult(subject, compatible=True, is_new_subject=True))
                continue
            try:
                ok = bool(self._client.test_compatibility(subject, self._schema(topic.name)))
            except _SRError as exc:
                raise SchemaRegistryError(
                    f"compatibility check failed for {subject}: {exc}"
                ) from exc
            results.append(CompatibilityResult(subject, compatible=ok, is_new_subject=False))
            log.info("schema_compatibility_checked", subject=subject, compatible=ok)
        return results

    def register_all(self) -> dict[str, int]:
        """Set compatibility, verify, then register. Idempotent: re-registering an identical
        schema returns the existing id."""
        incompatible = [r.subject for r in self.check_all() if not r.compatible]
        if incompatible:
            raise IncompatibleSchemaError(
                f"schemas incompatible with {COMPATIBILITY_LEVEL}: {incompatible}"
            )
        ids: dict[str, int] = {}
        for topic in self._catalog.primaries():
            subject = topic.subject
            try:
                self._client.set_compatibility(subject_name=subject, level=COMPATIBILITY_LEVEL)
                schema_id = self._client.register_schema(subject, self._schema(topic.name))
            except _SRError as exc:
                raise SchemaRegistryError(f"registration failed for {subject}: {exc}") from exc
            ids[subject] = int(schema_id)
            log.info("schema_registered", subject=subject, schema_id=schema_id)
        return ids
