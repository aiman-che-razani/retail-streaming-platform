"""Snowflake connections: key-pair authentication, query tags, error classification.

* Key-pair auth: the encrypted PKCS#8 private key is decrypted in memory and passed to the
  connector as DER bytes; neither the key nor its passphrase is ever logged.
* QUERY_TAG labels every query with the component, so `QUERY_HISTORY` / warehouse metering
  can attribute credits to "loader" vs "migrations" (cost visibility, ADR-005).
* Network/availability failures become `WarehouseUnavailableError` (transient, retried at the
  unit-of-work level); everything else (bad SQL, permissions) is permanent and surfaces.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from retail_platform.config.settings import SnowflakeSettings, _SnowflakeIdentity
from retail_platform.errors import ConfigurationError, LoaderError, WarehouseUnavailableError

# Connector error numbers that indicate the service or network, not our SQL, is at fault.
_TRANSIENT_ERRNOS = {
    250001,  # could not connect
    250003,  # failed to get a response (retry exhausted)
    250005,  # connection timeout
    251005,  # login request timeout
    252005,  # connection pool / HTTP error
    390114,  # auth token expired
    604,  # query cancelled (e.g. warehouse suspended by a resource monitor)
    608,  # warehouse suspended
}


def load_private_key(path: Path, passphrase: str | None) -> bytes:
    from cryptography.hazmat.primitives import serialization

    try:
        key = serialization.load_pem_private_key(
            path.read_bytes(), password=passphrase.encode() if passphrase else None
        )
    except (OSError, ValueError, TypeError) as exc:
        raise ConfigurationError(f"cannot load Snowflake private key {path}: {exc}") from exc
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connection_params(
    account: SnowflakeSettings, identity: _SnowflakeIdentity, *, component: str
) -> dict[str, Any]:
    if not account.account:
        raise ConfigurationError("SNOWFLAKE_ACCOUNT is not set")
    if not identity.has_credentials:
        raise ConfigurationError(f"no Snowflake credentials configured for role {identity.role}")
    params: dict[str, Any] = {
        "account": account.account,
        "user": identity.user,
        "role": identity.role,
        "warehouse": identity.warehouse,
        "database": account.database,
        "schema": account.schema_,
        "login_timeout": account.login_timeout_seconds,
        "network_timeout": account.network_timeout_seconds,
        "application": "retail-platform",
        "session_parameters": {
            "QUERY_TAG": f"retail-platform:{component}",
            "TIMEZONE": "UTC",
        },
    }
    if identity.private_key_path:
        passphrase = identity.private_key_passphrase
        params["private_key"] = load_private_key(
            identity.private_key_path, passphrase.get_secret_value() if passphrase else None
        )
    elif identity.authenticator:
        params["authenticator"] = identity.authenticator
    if identity.password and not identity.private_key_path:
        params["password"] = identity.password.get_secret_value()
    return params


def connector_error() -> type[Exception]:
    """Base class of all Snowflake connector errors (imported lazily: optional dependency)."""
    from snowflake.connector import errors

    base: type[Exception] = errors.Error
    return base


def is_transient(exc: BaseException) -> bool:
    from snowflake.connector import errors

    if isinstance(exc, errors.OperationalError | errors.InterfaceError):
        return True
    errno = getattr(exc, "errno", None)
    return isinstance(exc, errors.Error) and errno in _TRANSIENT_ERRNOS


def classify(exc: BaseException, what: str) -> LoaderError:
    if is_transient(exc):
        return WarehouseUnavailableError(f"{what}: Snowflake unavailable: {exc}")
    return LoaderError(f"{what}: {exc}")


def connect(params: dict[str, Any]) -> Any:
    """Open a connection. Returns a snowflake.connector.SnowflakeConnection."""
    import snowflake.connector

    try:
        return snowflake.connector.connect(**params)
    except snowflake.connector.errors.Error as exc:
        raise classify(exc, "connect") from exc
