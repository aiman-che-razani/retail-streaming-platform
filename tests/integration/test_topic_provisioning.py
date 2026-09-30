"""Declarative topic provisioning and schema management against the live stack."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
from confluent_kafka.schema_registry import SchemaRegistryClient

from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts.catalog import TopicCatalog, TopicRole, TopicSpec
from retail_platform.errors import TopicProvisioningError
from retail_platform.messaging.schemas import SchemaManager
from retail_platform.messaging.topics import TopicProvisioner

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]


def throwaway_catalog(base: TopicCatalog, name: str, partitions: int = 2) -> TopicCatalog:
    """A catalog with one uniquely named topic (constructed directly: no dataset mapping needed)."""
    spec = TopicSpec(
        name=name,
        role=TopicRole.PRIMARY,
        owner="integration-test",
        key="store_id",
        partitions=partitions,
        value_schema=None,
        config={"cleanup.policy": "delete", "retention.ms": "3600000"},
    )
    return TopicCatalog(topics=(spec,), profiles=base.profiles, schemas_dir=base.schemas_dir)


@pytest.fixture
def admin(kafka_settings: KafkaSettings) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_settings.bootstrap_servers})


@pytest.fixture
def topic_name(admin: AdminClient) -> Iterator[str]:
    name = f"it.provision.{uuid.uuid4().hex[:8]}"
    yield name
    admin.delete_topics([name])


def test_provisioning_is_declarative_and_idempotent(
    admin: AdminClient, catalog: TopicCatalog, topic_name: str
) -> None:
    test_catalog = throwaway_catalog(catalog, topic_name)
    provisioner = TopicProvisioner(admin, test_catalog, catalog.profiles["local"])

    first = provisioner.ensure_topics()
    assert first.created == [topic_name]

    second = provisioner.ensure_topics()
    assert second.created == [] and second.unchanged == [topic_name]


def test_config_drift_is_corrected(
    admin: AdminClient, catalog: TopicCatalog, topic_name: str
) -> None:
    test_catalog = throwaway_catalog(catalog, topic_name)
    provisioner = TopicProvisioner(admin, test_catalog, catalog.profiles["local"])
    provisioner.ensure_topics()

    # Someone changes retention by hand...
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry

    resource = ConfigResource(
        ResourceType.TOPIC,
        topic_name,
        incremental_configs=[
            ConfigEntry("retention.ms", "1000", incremental_operation=AlterConfigOpType.SET)
        ],
    )
    admin.incremental_alter_configs([resource])[resource].result()

    # ...and the next provisioning run restores the declared value.
    report = provisioner.ensure_topics()
    assert report.reconfigured[topic_name]["retention.ms"] == "3600000"
    described = admin.describe_configs([ConfigResource(ResourceType.TOPIC, topic_name)])
    config = next(iter(described.values())).result()
    assert config["retention.ms"].value == "3600000"


def test_partition_count_change_is_refused(
    admin: AdminClient, catalog: TopicCatalog, topic_name: str
) -> None:
    """ADR-003: adding partitions remaps keys and breaks ordering - never done in place."""
    TopicProvisioner(
        admin, throwaway_catalog(catalog, topic_name, 2), catalog.profiles["local"]
    ).ensure_topics()
    with pytest.raises(TopicProvisioningError, match="partitions"):
        TopicProvisioner(
            admin, throwaway_catalog(catalog, topic_name, 4), catalog.profiles["local"]
        ).ensure_topics()


def test_registered_schemas_match_the_repository(
    sr_client: SchemaRegistryClient, catalog: TopicCatalog
) -> None:
    """CI gate `make schemas-check`: the repo schemas are compatible with what is registered."""
    results = SchemaManager(sr_client, catalog).check_all()
    assert {r.subject for r in results} == {t.subject for t in catalog.primaries()}
    assert all(r.compatible for r in results)
