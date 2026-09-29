"""DLQ inspection and redrive (ADR-008).

* `inspect` reads a DLQ without committing anything and summarises it by rule id / stage.
* `redrive` republishes selected dead letters to `<primary>.retry` so they flow through the
  same Spark validation path again. It is a classic consumer-group consumer with manual
  commits: offsets are committed only after every republished record is acknowledged, which
  makes it at-least-once (duplicates are harmless: downstream dedupes by event_id).
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Message,
    Producer,
    TopicPartition,
)

from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts import dlq
from retail_platform.errors import DeliveryError
from retail_platform.observability.logging import get_logger

log = get_logger(__name__)


def _as_bytes(value: str | bytes | None) -> bytes:
    if value is None:
        return b""
    return value.encode() if isinstance(value, str) else value


@dataclass(frozen=True, slots=True)
class DeadLetter:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: dict[str, bytes]
    timestamp_ms: int

    @classmethod
    def from_message(cls, msg: Message) -> DeadLetter:
        raw_headers = msg.headers() or []
        pairs = raw_headers.items() if isinstance(raw_headers, dict) else raw_headers
        headers = {str(k): _as_bytes(v) for k, v in pairs}
        _, ts = msg.timestamp()
        key = msg.key()
        return cls(
            topic=msg.topic() or "",
            partition=msg.partition() or 0,
            offset=msg.offset() or 0,
            key=None if key is None else _as_bytes(key),
            value=_as_bytes(msg.value()),
            headers=headers,
            timestamp_ms=ts,
        )

    def header(self, name: str) -> str | None:
        raw = self.headers.get(name)
        return raw.decode("utf-8", errors="replace") if raw is not None else None

    @property
    def error_code(self) -> str:
        return self.header(dlq.ERROR_CODE) or "UNKNOWN"

    @property
    def redrive_count(self) -> int:
        try:
            return int(self.header(dlq.REDRIVE_COUNT) or 0)
        except ValueError:
            return 0

    def redrive_headers(self) -> list[tuple[str, str | bytes | None]]:
        """Original headers with an incremented redrive counter."""
        headers: list[tuple[str, str | bytes | None]] = [
            (k, v) for k, v in self.headers.items() if k != dlq.REDRIVE_COUNT
        ]
        headers.append((dlq.REDRIVE_COUNT, str(self.redrive_count + 1).encode()))
        return headers


def consumer_config(settings: KafkaSettings, group_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,  # commit only after the republish is acknowledged
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        **settings.client_security_config(),
    }


def iterate(
    consumer: Consumer, *, idle_timeout_s: float = 5.0, max_records: int | None = None
) -> Iterator[DeadLetter]:
    """Yield records until none arrive for `idle_timeout_s` or `max_records` are read."""
    seen = 0
    last_record = time.monotonic()
    while max_records is None or seen < max_records:
        msg = consumer.poll(1.0)
        if msg is None:
            if time.monotonic() - last_record > idle_timeout_s:
                return
            continue
        error = msg.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            raise DeliveryError(f"consumer error: {error}")
        last_record = time.monotonic()
        seen += 1
        yield DeadLetter.from_message(msg)


def seek_to_beginning(consumer: Consumer, topic: str) -> None:
    """Subscribe and rewind every partition to the earliest offset on assignment."""

    def on_assign(c: Consumer, partitions: list[TopicPartition]) -> None:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([topic], on_assign=on_assign)


@dataclass
class DlqSummary:
    topic: str
    total: int = 0
    by_error_code: Counter[str] = field(default_factory=Counter)
    by_stage: Counter[str] = field(default_factory=Counter)
    oldest: str | None = None
    newest: str | None = None
    samples: list[dict[str, Any]] = field(default_factory=list)

    def add(self, record: DeadLetter, sample_limit: int) -> None:
        self.total += 1
        self.by_error_code[record.error_code] += 1
        self.by_stage[record.header(dlq.ERROR_STAGE) or "unknown"] += 1
        ts = datetime.fromtimestamp(record.timestamp_ms / 1000, tz=UTC).isoformat()
        self.oldest = min(self.oldest or ts, ts)
        self.newest = max(self.newest or ts, ts)
        if len(self.samples) < sample_limit:
            self.samples.append(
                {
                    "partition": record.partition,
                    "offset": record.offset,
                    "error_code": record.error_code,
                    "error_message": record.header(dlq.ERROR_MESSAGE),
                    "source": f"{record.header(dlq.SOURCE_TOPIC)}"
                    f"[{record.header(dlq.SOURCE_PARTITION)}]@{record.header(dlq.SOURCE_OFFSET)}",
                    "value_preview": record.value[:160].decode("utf-8", errors="replace"),
                }
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "total": self.total,
            "by_error_code": dict(self.by_error_code.most_common()),
            "by_stage": dict(self.by_stage),
            "oldest": self.oldest,
            "newest": self.newest,
            "samples": self.samples,
        }


def inspect(consumer: Consumer, dlq_topic: str, sample_limit: int = 5) -> DlqSummary:
    seek_to_beginning(consumer, dlq_topic)
    summary = DlqSummary(topic=dlq_topic)
    for record in iterate(consumer):
        summary.add(record, sample_limit)
    return summary


@dataclass
class RedriveReport:
    redriven: int = 0
    skipped_filter: int = 0
    skipped_loop_guard: int = 0


class Redriver:
    def __init__(self, consumer: Consumer, producer: Producer) -> None:
        self._consumer = consumer
        self._producer = producer

    def redrive(
        self,
        *,
        dlq_topic: str,
        retry_topic: str,
        predicate: Callable[[DeadLetter], bool],
        max_records: int | None,
        dry_run: bool,
        force: bool,
        from_beginning: bool,
    ) -> RedriveReport:
        if from_beginning:
            seek_to_beginning(self._consumer, dlq_topic)
        else:
            self._consumer.subscribe([dlq_topic])
        report = RedriveReport()
        failures: list[str] = []

        def on_delivery(err: KafkaError | None, _msg: Message) -> None:
            if err is not None:
                failures.append(err.str())

        for record in iterate(self._consumer, max_records=max_records):
            if not predicate(record):
                report.skipped_filter += 1
                continue
            if record.redrive_count >= dlq.MAX_REDRIVES and not force:
                report.skipped_loop_guard += 1
                log.warning(
                    "redrive_refused_loop_guard",
                    offset=record.offset,
                    redrive_count=record.redrive_count,
                )
                continue
            report.redriven += 1
            if dry_run:
                continue
            self._producer.produce(
                retry_topic,
                key=record.key,
                value=record.value,
                headers=record.redrive_headers(),
                on_delivery=on_delivery,
            )
            self._producer.poll(0)

        if dry_run:
            log.info("redrive_dry_run", **report.__dict__)
            return report
        if report.redriven + report.skipped_filter + report.skipped_loop_guard == 0:
            log.info("redrive_nothing_to_do", dlq_topic=dlq_topic)
            return report
        remaining = self._producer.flush(30)
        if remaining or failures:
            # Do NOT commit: the next run re-reads these records (at-least-once).
            raise DeliveryError(
                f"redrive incomplete: {remaining} undelivered, {len(failures)} failed "
                f"({failures[:3]}); offsets not committed"
            )
        self._consumer.commit(asynchronous=False)
        log.info(
            "redrive_complete", dlq_topic=dlq_topic, retry_topic=retry_topic, **report.__dict__
        )
        return report
