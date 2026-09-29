# ADR-007: Delivery semantics and idempotency

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, kafka-engineer, spark-engineer, snowflake-engineer

## Context

Distributed systems can't cheaply guarantee that a message is *delivered* exactly once. Networks time out after a write succeeded, processes crash after doing work but before recording it, and retries resend. The three possible semantics:

- **At-most-once:** never retry. Loss is possible, duplicates are not.
- **At-least-once:** retry until acknowledged. No loss, but duplicates are possible.
- **Exactly-once:** each record affects the result once. In practice this is achieved as **at-least-once delivery + idempotent (or transactional) processing**, and it must hold for *every* hop, since one non-idempotent hop breaks the chain.

Revenue must never be double-counted or lost (N4, N5).

## Decision

**At-least-once delivery everywhere, with idempotency at every sink, keyed on stable identifiers.**

| Hop | Mechanism | Identity used |
|---|---|---|
| Simulator → Kafka | `enable.idempotence=true`, `acks=all`. Application resends reuse the original `event_id` | producer id + sequence; `event_id` |
| Kafka → Spark ingest | offset write-ahead log + commit log in the checkpoint; replay of uncommitted batches over identical offsets | (`query_id`, `batch_id`) |
| Spark ingest → landing | `foreachBatch` writes to `…/query_id=<id>/batch_id=<n>/`. If `_SUCCESS` exists → skip; otherwise overwrite the directory, then write `_SUCCESS` last | (`query_id`, `batch_id`) |
| Spark ingest → DLQ | Kafka sink, at-least-once; duplicate DLQ records are identifiable | `dlq.source.topic/partition/offset` |
| Landing → RAW | PUT (skips unchanged files) + COPY INTO (load metadata skips files loaded in the last 64 days); archive after success | stage file path |
| RAW → STAGING | `MERGE … WHEN NOT MATCHED` on `event_id` (and on business key for business duplicates) | `event_id`, `transaction_id`/`movement_id` |
| STAGING → ANALYTICS | `MERGE` on deterministic keys | `sales_line_key`, `inventory_movement_key`, dimension keys |
| Spark realtime → PostgreSQL | upsert `ON CONFLICT (key, window_start) DO UPDATE SET value = EXCLUDED.value` — **absolute values, never increments** | (dimension key, window start) |

**Why `query_id` in the landing path.** `batch_id` restarts at 0 when a checkpoint is deleted. Without a namespace, a new run's batch 0 would find the old run's `_SUCCESS` and skip, losing data. The streaming query id is persisted in the checkpoint's `metadata` file, so it is stable across restarts of the same checkpoint and new for a new checkpoint.

**Why absolute upserts matter.** When a batch is re-run after a crash, the state store is restored to the previous version and the batch recomputes the *same* window totals. Writing `revenue = EXCLUDED.revenue` is idempotent. `revenue = revenue + EXCLUDED.delta` would double-count.

**Why no Kafka transactions.** Kafka exactly-once (transactions + `read_committed`) makes *Kafka-to-Kafka* read-process-write atomic. Our sinks are files, Snowflake and PostgreSQL, which Kafka transactions can't cover. Idempotent sinks are simpler and cover all of them.

## Alternatives

| Option | Why not chosen |
|---|---|
| Dedupe in Spark ingest with `dropDuplicates` + watermark | Bounded memory means bounded dedupe horizon, and it drops late events (ADR-004). Snowflake MERGE has unbounded history at negligible cost. |
| Rely on Spark's file sink (`writeStream.parquet`) exactly-once log | Also exactly-once, but only for readers that consult `_spark_metadata`; the loader would have to parse Spark's internal log. `foreachBatch` + `_SUCCESS` gives an explicit, documented contract. |
| Track "last loaded offset" in the warehouse | Reinvents checkpoints and Streams; fragile under replays. |
| Two-phase commit across sinks | Not supported by the sinks and operationally fragile. |

## Consequences

**Positive**
- Any component can be killed at any moment and restarted without corrupting results. Tests demonstrate this.
- Replay (from Kafka within retention, or from RAW forever) is always safe.

**Negative / accepted trade-offs**
- RAW can contain duplicates by design. Anyone querying RAW directly must dedupe; ANALYTICS never has duplicates.
- DLQ can contain duplicates after a crash.
- Correctness depends on every new sink following this ADR. The code-reviewer checks for it.
