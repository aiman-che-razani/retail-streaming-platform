"""Typed settings loaded from environment variables (and an optional `.env` file).

Each component receives only the settings group it needs (dependency injection); nothing
reads `os.environ` directly. Every variable is documented in `.env.example`.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from retail_platform.errors import ConfigurationError


def _config(prefix: str) -> SettingsConfigDict:
    return SettingsConfigDict(
        env_prefix=prefix,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )


Probability = Annotated[float, Field(ge=0.0, le=1.0)]


class LoggingSettings(BaseSettings):
    model_config = _config("LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


class KafkaSettings(BaseSettings):
    """Kafka + Schema Registry connectivity and producer tuning (see kafka-topology.md §3)."""

    model_config = _config("KAFKA_")

    bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None
    config_dir: Path = Path("kafka")
    environment_profile: Literal["local", "production"] = "local"
    delivery_timeout_ms: int = Field(default=120_000, ge=1_000)
    linger_ms: int = Field(default=20, ge=0)
    batch_size_bytes: int = Field(default=65_536, ge=1_024)
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "zstd"
    connect_timeout_seconds: float = Field(default=60.0, gt=0)

    @property
    def topics_file(self) -> Path:
        return self.config_dir / "config" / "topics.yaml"

    @property
    def schemas_dir(self) -> Path:
        return self.config_dir / "schemas"

    def client_security_config(self) -> dict[str, str]:
        """librdkafka security settings; empty for local PLAINTEXT."""
        config: dict[str, str] = {"security.protocol": self.security_protocol}
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password:
            config["sasl.password"] = self.sasl_password.get_secret_value()
        return config


class SimulatorMode(StrEnum):
    REALTIME = "realtime"
    BACKFILL = "backfill"


class SimulatorSettings(BaseSettings):
    """Business-volume and fault-injection knobs for the retail simulator."""

    model_config = _config("SIMULATOR_")

    seed: int = 42
    mode: SimulatorMode = SimulatorMode.REALTIME
    transactions_per_second: float = Field(default=20.0, gt=0, le=5_000)
    backfill_days: int = Field(default=7, ge=1, le=365)
    backfill_transactions_per_store_per_day: int = Field(default=300, ge=1, le=100_000)
    bootstrap_master_data: bool = Field(
        default=True, description="emit product/customer master data + opening stock at start"
    )
    product_count: int = Field(default=500, ge=10, le=10_000)
    customer_count: int = Field(default=20_000, ge=10, le=1_000_000)
    guest_checkout_ratio: Probability = 0.4
    max_events: int = Field(default=0, ge=0, description="0 = run until stopped")
    product_change_rate_per_hour: float = Field(default=6.0, ge=0)
    customer_change_rate_per_hour: float = Field(default=30.0, ge=0)
    # Fault injection (probabilities per generated event)
    malformed_rate: Probability = 0.0
    missing_field_rate: Probability = 0.0
    invalid_value_rate: Probability = 0.0
    duplicate_rate: Probability = 0.0
    late_event_rate: Probability = 0.0
    late_event_max_minutes: int = Field(default=120, ge=1)
    unknown_product_rate: Probability = 0.0
    metrics_port: int = 8000


class SparkSettings(BaseSettings):
    model_config = _config("SPARK_")

    master: str = "local[*]"
    app_name_prefix: str = "retail"
    checkpoint_dir: Path = Path("data/checkpoints")
    landing_dir: Path = Path("data/landing")
    ingest_trigger_seconds: int = Field(default=30, ge=1)
    realtime_trigger_seconds: int = Field(default=10, ge=1)
    watermark_minutes: int = Field(default=10, ge=1)
    ingest_max_offsets_per_trigger: int = Field(default=50_000, ge=100)
    realtime_max_offsets_per_trigger: int = Field(default=20_000, ge=100)
    shuffle_partitions: int = Field(default=6, ge=1)
    starting_offsets: Literal["earliest", "latest"] = "earliest"
    future_tolerance_minutes: int = Field(default=5, ge=0)
    metrics_port: int = 8002


class PostgresSettings(BaseSettings):
    model_config = _config("POSTGRES_")

    host: str = "localhost"
    port: int = 5434
    db: str = "retail_realtime"
    user: str = "retail"
    password: SecretStr = SecretStr("")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.db} "
            f"user={self.user} password={self.password.get_secret_value()}"
        )


class _SnowflakeIdentity(BaseSettings):
    """Who connects, and how. Prefer key-pair auth (private_key_path) over passwords."""

    user: str = ""
    password: SecretStr | None = None
    private_key_path: Path | None = None
    private_key_passphrase: SecretStr | None = None
    authenticator: str | None = None
    role: str = ""
    warehouse: str = "RETAIL_PIPELINE_WH"

    @property
    def has_credentials(self) -> bool:
        return bool(self.user and (self.password or self.private_key_path or self.authenticator))

    @model_validator(mode="after")
    def _check_auth(self) -> _SnowflakeIdentity:
        if self.user and not self.has_credentials:
            prefix = self.model_config.get("env_prefix", "")
            raise ConfigurationError(
                f"Snowflake user {self.user!r} needs {prefix}PRIVATE_KEY_PATH (preferred), "
                f"{prefix}PASSWORD or {prefix}AUTHENTICATOR"
            )
        return self


class SnowflakeSettings(_SnowflakeIdentity):
    """Account + the LOADER service identity (least privilege: INSERT into RAW only)."""

    model_config = _config("SNOWFLAKE_")

    account: str = ""
    role: str = "RETAIL_LOADER"
    database: str = "RETAIL_DEV"
    schema_: str = Field(default="RAW", alias="SNOWFLAKE_SCHEMA")
    login_timeout_seconds: int = 30
    network_timeout_seconds: int = 120

    @property
    def is_configured(self) -> bool:
        return bool(self.account and self.has_credentials)


class SnowflakeAdminSettings(_SnowflakeIdentity):
    """Human/CI identity for bootstrap and migrations (RETAIL_ADMIN, or ACCOUNTADMIN once)."""

    model_config = _config("SNOWFLAKE_ADMIN_")

    role: str = "RETAIL_ADMIN"


class LoaderSettings(BaseSettings):
    model_config = _config("LOADER_")

    interval_seconds: int = Field(default=900, ge=10)
    landing_dir: Path = Path("data/landing")
    archive_dir: Path = Path("data/archive")
    archive_retention_days: int = Field(default=3, ge=0)
    max_batches_per_cycle: int = Field(default=500, ge=1)
    retry_attempts: int = Field(default=5, ge=1)
    retry_initial_backoff_seconds: float = Field(default=2.0, gt=0)
    retry_max_backoff_seconds: float = Field(default=120.0, gt=0)
    metrics_port: int = 8001
