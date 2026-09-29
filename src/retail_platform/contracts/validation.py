"""Producer-side validation helpers built on the JSON Schemas.

The confluent-kafka JSONSerializer also validates on serialize; this module lets code and
tests validate *without* a Schema Registry (e.g. the simulator's dry-run mode, unit tests).
"""

from __future__ import annotations

from functools import cache
from typing import Any

from jsonschema import Draft7Validator

from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.errors import EventValidationError


class SchemaValidator:
    def __init__(self, catalog: TopicCatalog) -> None:
        self._catalog = catalog

    @cache  # noqa: B019 - bounded by the number of topics; validator objects are immutable
    def _validator(self, topic: str) -> Draft7Validator:
        return Draft7Validator(self._catalog.schema_for(topic))

    def errors(self, topic: str, event: dict[str, Any]) -> list[str]:
        return [
            f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
            for e in self._validator(topic).iter_errors(event)
        ]

    def validate(self, topic: str, event: dict[str, Any]) -> None:
        errors = self.errors(topic, event)
        if errors:
            raise EventValidationError("; ".join(errors[:5]))
