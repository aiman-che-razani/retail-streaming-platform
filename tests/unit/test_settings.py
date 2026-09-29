from __future__ import annotations

from pathlib import Path

import pytest

from retail_platform.config.settings import KafkaSettings, SimulatorSettings, SnowflakeSettings
from retail_platform.errors import ConfigurationError

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Never read the developer's real .env in unit tests.
    monkeypatch.chdir(tmp_path)
    for var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "KAFKA_SASL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)


def test_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:1234")
    monkeypatch.setenv("SIMULATOR_DUPLICATE_RATE", "0.25")
    assert KafkaSettings().bootstrap_servers == "broker:1234"
    assert SimulatorSettings().duplicate_rate == 0.25


def test_probabilities_are_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIMULATOR_MALFORMED_RATE", "1.5")
    with pytest.raises(ValueError, match="less than or equal to 1"):
        SimulatorSettings()


def test_security_config_includes_sasl_only_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    assert KafkaSettings().client_security_config() == {"security.protocol": "PLAINTEXT"}
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "secret")
    config = KafkaSettings().client_security_config()
    assert config["sasl.password"] == "secret"
    assert config["security.protocol"] == "SASL_SSL"


def test_secrets_are_not_rendered_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "supersecret")
    assert "supersecret" not in repr(KafkaSettings())


def test_snowflake_unconfigured_is_allowed() -> None:
    assert not SnowflakeSettings().is_configured


def test_snowflake_requires_an_auth_method(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "org-acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "svc")
    with pytest.raises(ConfigurationError, match="PRIVATE_KEY_PATH"):
        SnowflakeSettings()
