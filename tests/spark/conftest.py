"""Spark test fixtures. Requires a JVM (run via `make test-spark` in the Spark image)."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")
if shutil.which("java") is None:
    pytest.skip("no JVM on PATH - run `make test-spark`", allow_module_level=True)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retail_platform.config.settings import SparkSettings  # noqa: E402
from retail_platform.messaging import wire  # noqa: E402
from retail_platform.processing.session import build_session  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
EXAMPLES = REPO / "kafka" / "schemas" / "examples"

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
        StructField(
            "headers",
            ArrayType(
                StructType([StructField("key", StringType()), StructField("value", BinaryType())])
            ),
        ),
    ]
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    settings = SparkSettings(master="local[2]", shuffle_partitions=2)
    session = build_session(settings, "tests")
    session.conf.set("spark.sql.streaming.checkpointLocation", str(tmp_path_factory.mktemp("ckpt")))
    yield session
    session.stop()


def framed(event: dict[str, Any] | str | bytes, schema_id: int = 1) -> bytes:
    if isinstance(event, dict):
        event = json.dumps(event)
    body = event.encode() if isinstance(event, str) else event
    return wire.encode(schema_id, body)


def kafka_df(
    spark: SparkSession,
    values: list[bytes],
    topic: str = "pos.transactions",
    key: bytes = b"KLCC-01",
) -> DataFrame:
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [(key, v, topic, 0, i, now, 0, []) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def example(name: str) -> dict[str, Any]:
    for folder in ("valid", "invalid"):
        path = EXAMPLES / folder / name
        if path.exists():
            data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
            return data
    raise FileNotFoundError(name)


def fresh(event: dict[str, Any]) -> dict[str, Any]:
    """Examples are dated 2026-09-29; make them 'now' so ENV-006 (future) never interferes."""
    copy = json.loads(json.dumps(event))
    copy["metadata"]["event_timestamp"] = (
        datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )
    return copy
