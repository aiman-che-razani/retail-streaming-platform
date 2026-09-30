"""End-to-end: host producer -> Kafka -> Spark (ingest + realtime) -> PostgreSQL and DLQ.

Needs the running pipeline (`make up-pipeline`). Uses a random, otherwise unused SKU so the
assertions are exact even while the simulator produces traffic. The same events also flow
to the warehouse path, where the unknown SKU becomes an inferred product member - exactly
the documented late-arriving-dimension behaviour (FM-10).
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition

from retail_platform.config.settings import PostgresSettings
from retail_platform.contracts import dlq
from retail_platform.messaging import wire

pytestmark = pytest.mark.e2e

EXAMPLE = (
    Path(__file__).resolve().parents[2]
    / "kafka/schemas/examples/valid/pos-transactions.basket-two-lines.json"
)
BOOTSTRAP = "localhost:9092"


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def services() -> PostgresSettings:
    settings = PostgresSettings()
    if not (_reachable("localhost", 9092) and _reachable(settings.host, settings.port)):
        pytest.skip("pipeline not running - `make up-pipeline`")
    if not _reachable("localhost", 8003):
        pytest.skip("spark-realtime not running - `make up-pipeline`")
    _wait_for_realtime_to_catch_up()
    return settings


def _wait_for_realtime_to_catch_up(max_lag: float = 2000, timeout_s: float = 300) -> None:
    """Right after (re)start the realtime app replays a backlog (e.g. the simulator's master-data
    bootstrap). Measure its own consumer lag from /metrics and wait, so timings below are fair."""
    import re
    import urllib.request

    deadline = time.monotonic() + timeout_s
    lag = float("inf")
    while time.monotonic() < deadline:
        with urllib.request.urlopen("http://127.0.0.1:8003/metrics", timeout=5) as response:
            text = response.read().decode()
        values = [
            float(v)
            for v in re.findall(
                r'retail_spark_kafka_offsets_behind_latest\{[^}]*stat="max"[^}]*\} (\S+)', text
            )
        ]
        lag = max(values) if values else float("inf")
        if lag <= max_lag:
            return
        time.sleep(5)
    pytest.fail(f"spark-realtime still {lag} offsets behind after {timeout_s}s")


def _event(sku: str, minutes_ago: float = 0, event_id: str | None = None) -> dict[str, Any]:
    event = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    ts = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    event["metadata"]["event_id"] = event_id or str(uuid.uuid4())
    event["metadata"]["event_timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    event["payload"]["transaction_id"] = f"TXN-E2E-{uuid.uuid4().hex[:10].upper()}"
    event["payload"]["line_items"] = [
        {
            "line_number": 1,
            "product_id": sku,
            "quantity": 2,
            "unit_price": 10.0,
            "discount_amount": 0.0,
        }
    ]
    event["payload"]["total_amount"] = 20.0
    return event


def _schema_id() -> int:
    from confluent_kafka.schema_registry import SchemaRegistryClient

    registered = SchemaRegistryClient({"url": "http://localhost:8081"}).get_latest_version(
        "pos.transactions-value"
    )
    return int(registered.schema_id or 0)


def _units(settings: PostgresSettings, sku: str) -> tuple[int, Decimal]:
    with psycopg.connect(settings.conninfo()) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(units), 0), COALESCE(SUM(revenue), 0) "
            "FROM realtime.product_units_1h WHERE product_id = %s",
            (sku,),
        ).fetchone()
    return (int(row[0]), Decimal(row[1])) if row else (0, Decimal(0))


def test_sales_flow_to_realtime_store_with_dedupe_late_drop_and_dlq(
    services: PostgresSettings,
) -> None:
    schema_id = _schema_id()
    sku = f"SKU-9{uuid.uuid4().int % 10000:04d}"
    marker_key = f"E2E-{uuid.uuid4().hex[:8]}".encode()
    valid = [_event(sku) for _ in range(5)]
    malformed = b"\0\0\0\0{truncated"  # -> ENV-002

    def framed(event: dict[str, Any]) -> bytes:
        return wire.encode(schema_id, json.dumps(event).encode())

    dlq_consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": uuid.uuid4().hex})
    parts = [TopicPartition("pos.transactions.dlq", p) for p in range(3)]
    dlq_consumer.assign(
        [
            TopicPartition(tp.topic, tp.partition, dlq_consumer.get_watermark_offsets(tp)[1])
            for tp in parts
        ]
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True, "acks": "all"})
    try:
        # 1. Five sales + a duplicate of the first (same event_id) + a malformed record.
        for event in [*valid, valid[0]]:
            producer.produce("pos.transactions", key=b"KLCC-01", value=framed(event))
        producer.produce("pos.transactions", key=marker_key, value=malformed)
        assert producer.flush(15) == 0

        deadline = time.monotonic() + 180
        units, revenue = 0, Decimal(0)
        while time.monotonic() < deadline and units < 10:
            time.sleep(5)
            units, revenue = _units(services, sku)
        assert (units, revenue) == (10, Decimal("100.00")), "duplicate must be counted once"

        # 2. The watermark is now >= (now - 10 min): a 45-minute-old sale is too late for the
        #    realtime view (it still reaches the warehouse via the stateless ingest path).
        producer.produce(
            "pos.transactions", key=b"KLCC-01", value=framed(_event(sku, minutes_ago=45))
        )
        assert producer.flush(15) == 0
        time.sleep(35)  # > 2 realtime triggers
        assert _units(services, sku)[0] == 10, "late event must be dropped by the watermark"

        # 3. The malformed record is in the DLQ, byte-for-byte, with its rule id.
        dead = None
        end = time.monotonic() + 90
        while time.monotonic() < end and dead is None:
            msg = dlq_consumer.poll(1.0)
            if msg is not None and msg.error() is None and msg.key() == marker_key:
                dead = msg
        assert dead is not None, "malformed record did not reach the DLQ"
        assert dict(dead.headers())[dlq.ERROR_CODE] == b"ENV-002"
        assert dead.value() == malformed
    finally:
        dlq_consumer.close()
        with psycopg.connect(services.conninfo()) as conn:
            conn.execute("DELETE FROM realtime.product_units_1h WHERE product_id = %s", (sku,))
