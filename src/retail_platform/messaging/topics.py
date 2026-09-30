"""Declarative topic provisioning from topics.yaml.

* Missing topics are created with the declared partitions/configs and the environment's
  durability profile.
* Existing topics are verified: a partition-count mismatch is an ERROR (adding partitions
  remaps keys and breaks per-key ordering — ADR-003), config drift is corrected with
  incremental alter configs so the declared state always wins.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from retail_platform.contracts.catalog import DurabilityProfile, TopicCatalog, TopicSpec
from retail_platform.errors import TopicProvisioningError
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)

_TIMEOUT_S = 30.0


@dataclass
class ProvisionReport:
    created: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    reconfigured: dict[str, dict[str, str]] = field(default_factory=dict)


class TopicProvisioner:
    def __init__(self, admin: AdminClient, catalog: TopicCatalog, profile: DurabilityProfile):
        self._admin = admin
        self._catalog = catalog
        self._profile = profile

    def _desired_config(self, spec: TopicSpec) -> dict[str, str]:
        return {**spec.config, "min.insync.replicas": str(self._profile.min_insync_replicas)}

    def ensure_topics(self) -> ProvisionReport:
        report = ProvisionReport()
        existing = self._admin.list_topics(timeout=_TIMEOUT_S).topics
        missing = [t for t in self._catalog.topics if t.name not in existing]
        if missing:
            report.created = self._create(missing)
        for spec in self._catalog.topics:
            if spec.name in report.created:
                continue
            actual_partitions = self._partition_count(spec.name)
            if actual_partitions != spec.partitions:
                raise TopicProvisioningError(
                    f"{spec.name} has {actual_partitions} partitions, declared {spec.partitions}."
                    " Partition counts are never changed in place (ADR-003)."
                )
            drift = self._config_drift(spec)
            if drift:
                self._apply_config(spec.name, drift)
                report.reconfigured[spec.name] = drift
            else:
                report.unchanged.append(spec.name)
        log.info(
            "topics_provisioned",
            created=report.created,
            reconfigured=sorted(report.reconfigured),
            unchanged=len(report.unchanged),
        )
        return report

    def _partition_count(self, topic: str) -> int:
        """Partition count, waiting briefly for metadata to include a just-created topic."""
        for _ in range(10):
            metadata = self._admin.list_topics(topic=topic, timeout=_TIMEOUT_S).topics.get(topic)
            if metadata is not None and metadata.error is None and metadata.partitions:
                return len(metadata.partitions)
            time.sleep(0.5)
        raise TopicProvisioningError(f"topic {topic} not visible in cluster metadata")

    def _create(self, specs: list[TopicSpec]) -> list[str]:
        """Create topics; returns those actually created by this call.

        Metadata is eventually consistent: a topic created moments ago (e.g. by a previous
        run or a concurrent init job) may not be listed yet. TOPIC_ALREADY_EXISTS is therefore
        success, and the topic is verified like any other existing topic.
        """
        created: list[str] = []
        new_topics = [
            NewTopic(
                spec.name,
                num_partitions=spec.partitions,
                replication_factor=self._profile.replication_factor,
                config=self._desired_config(spec),
            )
            for spec in specs
        ]
        futures = self._admin.create_topics(new_topics, request_timeout=_TIMEOUT_S)
        for name, future in futures.items():
            try:
                future.result()
            except KafkaException as exc:
                if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    log.info("topic_already_exists", topic=name)
                    continue
                raise TopicProvisioningError(f"failed to create {name}: {exc}") from exc
            created.append(name)
            log.info("topic_created", topic=name)
        return created

    def _config_drift(self, spec: TopicSpec) -> dict[str, str]:
        resource = ConfigResource(ResourceType.TOPIC, spec.name)
        described = self._admin.describe_configs([resource], request_timeout=_TIMEOUT_S)
        try:
            actual = described[resource].result()
        except KafkaException as exc:
            raise TopicProvisioningError(f"cannot describe {spec.name}: {exc}") from exc
        desired = self._desired_config(spec)
        return {k: v for k, v in desired.items() if k not in actual or actual[k].value != v}

    def _apply_config(self, topic: str, changes: dict[str, str]) -> None:
        resource = ConfigResource(
            ResourceType.TOPIC,
            topic,
            incremental_configs=[
                ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
                for k, v in changes.items()
            ],
        )
        futures = self._admin.incremental_alter_configs([resource])
        try:
            futures[resource].result()
        except KafkaException as exc:
            raise TopicProvisioningError(f"cannot alter {topic}: {exc}") from exc
        log.warning("topic_config_drift_corrected", topic=topic, changes=changes)
