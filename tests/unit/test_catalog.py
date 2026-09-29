from __future__ import annotations

from pathlib import Path

import pytest

from retail_platform.contracts.catalog import Dataset, TopicCatalog, TopicRole
from retail_platform.errors import ContractError

pytestmark = pytest.mark.unit


def test_catalog_exposes_primaries_with_companions(catalog: TopicCatalog) -> None:
    primaries = {t.name for t in catalog.primaries()}
    assert primaries == {
        "pos.transactions",
        "inventory.events",
        "customer.events",
        "product.updates",
    }
    for name in primaries:
        assert catalog.retry_for(name).role is TopicRole.RETRY
        assert catalog.dlq_for(name).role is TopicRole.DLQ


def test_dataset_mapping_round_trips(catalog: TopicCatalog) -> None:
    for primary in catalog.primaries():
        dataset = catalog.dataset_for(primary.name)
        assert catalog.primary_for_dataset(dataset) == primary.name
    assert catalog.dataset_for("pos.transactions") is Dataset.POS_TRANSACTIONS


def test_subject_uses_topic_name_strategy(catalog: TopicCatalog) -> None:
    assert catalog.get("pos.transactions").subject == "pos.transactions-value"


def test_allowed_event_types_come_from_schema(catalog: TopicCatalog) -> None:
    assert catalog.allowed_event_types("customer.events") == [
        "customer.registered",
        "customer.profile_updated",
    ]


def test_dlq_topics_have_no_schema(catalog: TopicCatalog) -> None:
    with pytest.raises(ContractError):
        catalog.schema_for("pos.transactions.dlq")


def test_unknown_topic_raises(catalog: TopicCatalog) -> None:
    with pytest.raises(ContractError):
        catalog.get("does.not.exist")


def test_missing_topics_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ContractError, match="not found"):
        TopicCatalog.load(tmp_path / "topics.yaml", tmp_path)


def test_local_profile_is_single_broker(catalog: TopicCatalog) -> None:
    assert catalog.profiles["local"].replication_factor == 1
    assert catalog.profiles["production"].min_insync_replicas == 2
