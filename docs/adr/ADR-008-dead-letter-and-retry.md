# ADR-008: Dead-letter handling and retry (redrive) topics

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, kafka-engineer, spark-engineer, data-quality-engineer

## Context

Some records can't be processed: malformed bytes, schema violations, business-rule violations. The pipeline must not crash on them (one bad record would halt everything), must not load them (corruption), and must not silently drop them (invisible loss). There are two kinds of failure, and they need different treatment:

- **Deterministic (poison) failures.** The record itself is bad. Retrying produces the same failure forever.
- **Transient failures.** The record is fine but a dependency (Snowflake, PostgreSQL, Kafka) is temporarily down. Retrying later succeeds.

The popular "retry topic chain" pattern (`topic.retry.1m → topic.retry.10m → dlq`) exists for consumers that call a flaky dependency *per record*. Our Spark jobs make no per-record external calls. Their only transient failures are sink outages, which affect a whole batch.

## Decision

1. **Deterministic failures → DLQ immediately, no retry.** Spark evaluates the validation rules (`event-contracts.md` §5) with native expressions. Rejected rows are written to `<primary>.dlq` in the same `foreachBatch` as the valid rows:
   - **key:** original key; **value:** original bytes, unchanged (so they can be replayed exactly);
   - **headers:** original headers plus `dlq.error.code` (rule id), `dlq.error.message`, `dlq.error.stage` (`decode`|`parse`|`validate`), `dlq.source.topic`, `dlq.source.partition`, `dlq.source.offset`, `dlq.failed_at`, `dlq.app` (`spark-ingest/<version>`), `dlq.redrive_count`.
   - A copy lands in `RAW.DEAD_LETTERS` for SQL-based DQ reporting.
2. **Transient failures → retry the unit of work, never the DLQ.** If a sink fails, the batch fails, the query fails fast, and the container restarts and re-runs the batch from the checkpoint. The loader retries COPY with exponential backoff. Sending a record to a DLQ because Snowflake was down would be data loss by misclassification.
3. **Retry topics are used for redrive only.** `<primary>.retry` topics receive records that a human (or a script) has decided to reprocess after fixing the cause: a consumer bug fixed, a schema registered, or a record corrected. The `retail-dlq redrive` CLI reads the DLQ with filters (error code, time range, source offsets), optionally applies a correction, increments `dlq.redrive_count`, and produces to `<primary>.retry`. Spark subscribes to primary + retry, so redriven records take **the identical validation path**, and `event_id` dedupe makes a redrive of an already-loaded record harmless.
4. **Loop protection:** records with `dlq.redrive_count >= 3` are refused by the redrive CLI unless `--force`.
5. **Ownership:** only Spark writes DLQs; only the redrive CLI writes retry topics; only the source writes primaries.

## Alternatives

| Option | Why not chosen |
|---|---|
| Timed retry-topic chain (1 m / 10 m / 1 h) | Solves per-record transient dependency failures, which we don't have. Retrying deterministic failures only adds latency and noise before the DLQ. |
| Redrive directly into the primary topic | Breaks topic ownership (only the source writes primaries), mixes redriven records with live traffic in metrics, and hides redrive activity. |
| Single shared DLQ for all topics | One retention and partitioning policy for different domains; redrive must demultiplex. Per-topic DLQs make the target of a redrive obvious. |
| Fail the whole pipeline on any bad record | Maximum strictness, but one malformed event from one till stops all 30 stores. |
| Wrap DLQ value in a JSON error envelope | Easier to read, but the original bytes must be base64-encoded and unwrapped for redrive. Headers keep the original bytes intact. |

## Consequences

**Positive**
- Bad records never block good ones and are never lost.
- Every DLQ record is self-explaining (rule id plus source coordinates) and replayable.
- DLQ volume is a first-class metric and alert (`kafka-exporter` offset rate on `*.dlq`).

**Negative / accepted trade-offs**
- Someone must actually look at the DLQ. Alerts and a runbook make that explicit.
- Redriven POS events are usually older than the realtime watermark, so they appear in Snowflake but not in realtime aggregates (documented, consistent with FM-09).
- Headers are dropped by some tools; the `RAW.DEAD_LETTERS` copy mitigates this for analysis.
