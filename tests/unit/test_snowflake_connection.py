from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("snowflake.connector")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from snowflake.connector import errors

from retail_platform.config.settings import SnowflakeSettings
from retail_platform.errors import (
    ConfigurationError,
    LoaderError,
    WarehouseUnavailableError,
)
from retail_platform.warehouse.connection import (
    classify,
    connection_params,
    load_private_key,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_env_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)


def encrypted_key(tmp_path: Path, passphrase: str) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / "key.p8"
    path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(passphrase.encode()),
        )
    )
    return path


def test_key_pair_auth_passes_der_bytes_not_password(tmp_path: Path) -> None:
    path = encrypted_key(tmp_path, "s3cret")
    settings = SnowflakeSettings(
        account="org-acct",
        user="RETAIL_LOADER_SVC",
        private_key_path=path,
        private_key_passphrase="s3cret",  # type: ignore[arg-type]
    )
    params = connection_params(settings, settings, component="loader")
    assert isinstance(params["private_key"], bytes)
    assert "password" not in params
    assert params["session_parameters"]["QUERY_TAG"] == "retail-platform:loader"
    assert params["session_parameters"]["TIMEZONE"] == "UTC"
    assert params["role"] == "RETAIL_LOADER"


def test_wrong_passphrase_is_a_configuration_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="private key"):
        load_private_key(encrypted_key(tmp_path, "right"), "wrong")


def test_missing_account_is_rejected() -> None:
    settings = SnowflakeSettings(user="svc", password="x")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="SNOWFLAKE_ACCOUNT"):
        connection_params(settings, settings, component="loader")


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (errors.OperationalError("network down"), True),
        (errors.DatabaseError("warehouse suspended", errno=608), True),
        (errors.ProgrammingError("SQL compilation error", errno=1003), False),
    ],
)
def test_error_classification(exc: Exception, transient: bool) -> None:
    classified = classify(exc, "copy")
    assert isinstance(classified, WarehouseUnavailableError) is transient
    assert isinstance(classified, LoaderError)
