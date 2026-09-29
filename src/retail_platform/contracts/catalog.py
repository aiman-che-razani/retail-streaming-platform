"""Topic catalog loaded from `kafka/config/topics.yaml` — the single source of truth for topics.

No module hard-codes topic names, keys or partition counts; they ask the catalog.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

from retail_platform.errors import ContractError


class TopicRole(StrEnum):
    PRIMARY = "primary"
    RETRY = "retry"
    DLQ = "dlq"


class Dataset(StrEnum):
    """Logical datasets = landing-zone directories = RAW tables (overview.md §6)."""

    POS_TRANSACTIONS = "pos_transactions"
    INVENTORY_MOVEMENTS = "inventory_movements"
    CUSTOMER_EVENTS = "customer_events"
    PRODUCT_EVENTS = "product_events"


# Primary topic -> dataset. Declared once here; the rest of the code derives from it.
_PRIMARY_DATASETS: dict[str, Dataset] = {
    "pos.transactions": Dataset.POS_TRANSACTIONS,
    "inventory.events": Dataset.INVENTORY_MOVEMENTS,
    "customer.events": Dataset.CUSTOMER_EVENTS,
    "product.updates": Dataset.PRODUCT_EVENTS,
}


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    role: TopicRole
    owner: str
    key: str
    partitions: int
    value_schema: str | None
    config: dict[str, str] = field(default_factory=dict)

    @property
    def subject(self) -> str:
        """Schema Registry subject under TopicNameStrategy."""
        return f"{self.name}-value"


@dataclass(frozen=True, slots=True)
class DurabilityProfile:
    replication_factor: int
    min_insync_replicas: int


@dataclass(frozen=True, slots=True)
class TopicCatalog:
    topics: tuple[TopicSpec, ...]
    profiles: dict[str, DurabilityProfile]
    schemas_dir: Path

    @classmethod
    def load(cls, topics_file: Path, schemas_dir: Path) -> TopicCatalog:
        if not topics_file.is_file():
            raise ContractError(f"topic declarations not found: {topics_file.resolve()}")
        raw = yaml.safe_load(topics_file.read_text(encoding="utf-8"))
        topics = tuple(
            TopicSpec(
                name=t["name"],
                role=TopicRole(t["role"]),
                owner=t["owner"],
                key=t["key"],
                partitions=int(t["partitions"]),
                value_schema=t["value_schema"],
                config={k: str(v) for k, v in t.get("config", {}).items()},
            )
            for t in raw["topics"]
        )
        profiles = {
            name: DurabilityProfile(int(p["replication_factor"]), int(p["min_insync_replicas"]))
            for name, p in raw["profiles"].items()
        }
        catalog = cls(topics=topics, profiles=profiles, schemas_dir=schemas_dir)
        catalog._verify()
        return catalog

    def _verify(self) -> None:
        names = [t.name for t in self.topics]
        if len(names) != len(set(names)):
            raise ContractError("duplicate topic names in topics.yaml")
        for primary in self.primaries():
            if primary.name not in _PRIMARY_DATASETS:
                raise ContractError(f"primary topic {primary.name} has no dataset mapping")
            self.retry_for(primary.name)
            self.dlq_for(primary.name)

    def get(self, name: str) -> TopicSpec:
        for topic in self.topics:
            if topic.name == name:
                return topic
        raise ContractError(f"unknown topic: {name}")

    def primaries(self) -> list[TopicSpec]:
        return [t for t in self.topics if t.role is TopicRole.PRIMARY]

    def retry_for(self, primary: str) -> TopicSpec:
        return self.get(f"{primary}.retry")

    def dlq_for(self, primary: str) -> TopicSpec:
        return self.get(f"{primary}.dlq")

    @staticmethod
    def dataset_for(primary: str) -> Dataset:
        try:
            return _PRIMARY_DATASETS[primary]
        except KeyError as exc:
            raise ContractError(f"{primary} is not a primary topic") from exc

    @staticmethod
    def primary_for_dataset(dataset: Dataset) -> str:
        return next(topic for topic, ds in _PRIMARY_DATASETS.items() if ds is dataset)

    def schema_for(self, topic: str) -> dict[str, Any]:
        spec = self.get(topic)
        if spec.value_schema is None:
            raise ContractError(f"topic {topic} carries raw bytes and has no schema")
        path = self.schemas_dir / spec.value_schema
        if not path.is_file():
            raise ContractError(f"schema file not found: {path}")
        schema: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return schema

    def schema_text_for(self, topic: str) -> str:
        spec = self.get(topic)
        if spec.value_schema is None:
            raise ContractError(f"topic {topic} carries raw bytes and has no schema")
        return (self.schemas_dir / spec.value_schema).read_text(encoding="utf-8")

    def allowed_event_types(self, topic: str) -> list[str]:
        schema = self.schema_for(topic)
        event_type_rule = schema["properties"]["metadata"]["allOf"][1]
        types: list[str] = event_type_rule["properties"]["event_type"]["enum"]
        return types
