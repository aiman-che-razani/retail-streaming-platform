"""Exception hierarchy.

Callers catch the narrowest class that lets them act differently. The split between
*transient* and *permanent* failures drives retry decisions (see ADR-008): transient errors
are retried at the unit-of-work level; permanent ones are surfaced or dead-lettered.
"""

from __future__ import annotations


class RetailPlatformError(Exception):
    """Base class for all platform errors."""


class TransientError(RetailPlatformError):
    """A failure that may succeed if retried later (dependency unavailable, timeout)."""


class ConfigurationError(RetailPlatformError):
    """Invalid or missing configuration. Never retried."""


# ------------------------------------------------------------------ contracts
class ContractError(RetailPlatformError):
    """Contract files are missing or inconsistent."""


class EventValidationError(RetailPlatformError):
    """An event does not satisfy its contract."""

    def __init__(self, message: str, *, rule_id: str | None = None) -> None:
        super().__init__(message)
        self.rule_id = rule_id


# ------------------------------------------------------------------ messaging
class MessagingError(RetailPlatformError):
    """Kafka or Schema Registry failure."""


class KafkaUnavailableError(MessagingError, TransientError):
    """The Kafka cluster cannot be reached."""


class DeliveryError(MessagingError):
    """A record could not be delivered within the delivery timeout."""


class SchemaRegistryError(MessagingError):
    """Schema registration or compatibility failure."""


class IncompatibleSchemaError(SchemaRegistryError):
    """A schema change violates the subject's compatibility level."""


class TopicProvisioningError(MessagingError):
    """Topic creation or verification failed."""


# ------------------------------------------------------------------ processing / loading
class ProcessingError(RetailPlatformError):
    """Stream processing failure."""


class LoaderError(RetailPlatformError):
    """Warehouse loading failure."""


class WarehouseUnavailableError(LoaderError, TransientError):
    """Snowflake cannot be reached or rejected the connection transiently."""


class LoadVerificationError(LoaderError):
    """Rows loaded do not match what the landing batch declared."""


class MigrationError(RetailPlatformError):
    """A warehouse migration failed or history is inconsistent."""


class DataQualityError(RetailPlatformError):
    """A data-quality check with ERROR severity failed."""
