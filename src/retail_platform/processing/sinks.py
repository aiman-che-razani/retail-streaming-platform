"""`foreachBatch` sinks, each idempotent per micro-batch (ADR-007).

Why idempotency matters: Spark writes a batch's offset range to the checkpoint *before*
running it and marks it committed *after* the sink returns. If the process dies in between,
the same batch id is re-run over the same offsets. `foreachBatch` is therefore at-least-once
by itself; exactly-once *effects* come from these sinks tolerating a replay.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame

from retail_platform import __version__
from retail_platform.config.settings import KafkaSettings, PostgresSettings
from retail_platform.contracts.catalog import Dataset
from retail_platform.errors import ProcessingError
from retail_platform.observability.logging import get_logger
from retail_platform.processing.transformations.outputs import (
    dlq_records,
    ingest_audit,
    landing_dead_letters,
    landing_events,
)

log = get_logger(__name__)

DEAD_LETTERS = "dead_letters"
INGEST_AUDIT = "ingest_audit"
SUCCESS_MARKER = "_SUCCESS"
PURGE_EVERY_N_BATCHES = 360  # ~1 hour at the default 10 s realtime trigger


def batch_dir(landing_dir: Path, dataset: str, query_id: str, batch_id: int) -> Path:
    return landing_dir / dataset / f"query_id={query_id}" / f"batch_id={batch_id:012d}"


def checkpoint_query_id(checkpoint_location: Path) -> str:
    """The streaming query id persisted in the checkpoint's `metadata` file.

    Stable across restarts of the same checkpoint and new for a new checkpoint, so landing
    paths never collide after a checkpoint reset (ADR-007).
    """
    metadata = checkpoint_location / "metadata"
    try:
        query_id: str = json.loads(metadata.read_text(encoding="utf-8"))["id"]
    except (OSError, ValueError, KeyError) as exc:
        raise ProcessingError(f"cannot read query id from {metadata}") from exc
    return query_id


def kafka_writer_options(settings: KafkaSettings) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": settings.bootstrap_servers,
        "kafka.acks": "all",
        "kafka.enable.idempotence": "true",
        "kafka.compression.type": settings.compression_type,
    }
    options.update({f"kafka.{k}": v for k, v in settings.client_security_config().items()})
    return options


class IngestBatchWriter:
    """Writes one validated micro-batch to: DLQ topic, dead_letters, ingest_audit, events.

    Write order matters: the events dataset is written LAST and its `_SUCCESS` marker is the
    batch's commit record. On replay, a batch whose events marker exists is skipped entirely;
    otherwise every output is rewritten (overwrite), so partial earlier attempts vanish.
    """

    def __init__(
        self,
        *,
        dataset: Dataset,
        dlq_topic: str,
        landing_dir: Path,
        checkpoint_location: Path,
        kafka_settings: KafkaSettings,
    ) -> None:
        self._dataset = dataset
        self._dlq_topic = dlq_topic
        self._landing_dir = landing_dir
        self._checkpoint_location = checkpoint_location
        self._kafka_options = kafka_writer_options(kafka_settings)
        self._app = f"spark-ingest/{__version__}"
        self._query_id: str | None = None

    def _qid(self) -> str:
        if self._query_id is None:
            self._query_id = checkpoint_query_id(self._checkpoint_location)
        return self._query_id

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        query_id = self._qid()
        events_dir = batch_dir(self._landing_dir, self._dataset.value, query_id, batch_id)
        bound = log.bind(dataset=self._dataset.value, batch_id=batch_id, query_id=query_id)
        if (events_dir / SUCCESS_MARKER).exists():
            bound.warning("batch_already_landed_skipping", status="replay_skipped")
            return
        batch.persist()
        try:
            if batch.isEmpty():
                return
            rejected = dlq_records(batch, self._dlq_topic, self._app)
            if not rejected.isEmpty():
                rejected.write.format("kafka").options(**self._kafka_options).save()
                landing_dead_letters(batch, query_id, batch_id).coalesce(1).write.mode(
                    "overwrite"
                ).parquet(str(batch_dir(self._landing_dir, DEAD_LETTERS, query_id, batch_id)))
            ingest_audit(batch, self._dataset.value, query_id, batch_id).coalesce(1).write.mode(
                "overwrite"
            ).parquet(str(batch_dir(self._landing_dir, INGEST_AUDIT, query_id, batch_id)))
            landing_events(batch, query_id, batch_id).coalesce(1).write.mode("overwrite").parquet(
                str(events_dir)
            )
            bound.info("batch_landed", status="landed", path=str(events_dir))
        finally:
            batch.unpersist()


class PostgresUpsertWriter:
    """Upserts windowed aggregates with ABSOLUTE values (replay-safe, ADR-007/011).

    Rows are bulk-written by Spark's JDBC writer into a per-batch staging table, then merged
    with one `INSERT ... ON CONFLICT DO UPDATE` and the staging table dropped — all inside a
    single PostgreSQL transaction. No per-row Python.
    """

    def __init__(
        self,
        *,
        table: str,
        keys: tuple[str, ...],
        values: tuple[str, ...],
        timestamp_columns: tuple[str, ...],
        settings: PostgresSettings,
        connect: Any = psycopg.connect,
    ) -> None:
        self._table = table
        self._keys = keys
        self._values = values
        self._timestamps = set(timestamp_columns)
        self._settings = settings
        self._connect = connect

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        stage = f"_stg_{self._table}_{batch_id}"
        batch.write.mode("overwrite").jdbc(
            self._settings.jdbc_url,
            f"realtime.{stage}",
            properties={
                "user": self._settings.user,
                "password": self._settings.password.get_secret_value(),
                "driver": "org.postgresql.Driver",
            },
        )
        columns = [*self._keys, *self._values]

        def source(column: str) -> sql.Composable:
            ident = sql.Identifier(column)
            if column in self._timestamps:  # staging column is `timestamp` holding UTC
                return sql.SQL("{} AT TIME ZONE 'UTC'").format(ident)
            return ident

        statement = sql.SQL(
            "INSERT INTO realtime.{table} ({cols}, updated_at) "
            "SELECT {src}, now() FROM realtime.{stage} "
            "ON CONFLICT ({keys}) DO UPDATE SET {updates}, updated_at = now()"
        ).format(
            table=sql.Identifier(self._table),
            cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
            src=sql.SQL(", ").join(source(c) for c in columns),
            stage=sql.Identifier(stage),
            keys=sql.SQL(", ").join(map(sql.Identifier, self._keys)),
            updates=sql.SQL(", ").join(
                sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in self._values
            ),
        )
        with self._connect(self._settings.conninfo()) as conn, conn.transaction():
            conn.execute(statement)
            conn.execute(sql.SQL("DROP TABLE realtime.{}").format(sql.Identifier(stage)))
            if batch_id % PURGE_EVERY_N_BATCHES == 0:
                # Operational store keeps 7 days; the warehouse keeps history (ADR-011).
                conn.execute("SELECT realtime.purge_old_windows()")
        log.info("aggregates_upserted", table=self._table, batch_id=batch_id)
