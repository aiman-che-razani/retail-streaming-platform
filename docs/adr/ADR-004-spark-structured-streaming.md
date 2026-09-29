# ADR-004: Spark Structured Streaming — stateless ingest app and stateful realtime app

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, spark-engineer

## Context

We need a stream processor that validates and routes every event, lands valid data for the warehouse, and computes event-time windowed aggregates with bounded lateness. It must recover from crashes without loss or duplication. Requirements that pull in different directions:

- **Completeness:** every valid event must reach the warehouse, however late.
- **Bounded state:** windowed aggregation and streaming deduplication must eventually *forget* old windows and IDs, which requires a watermark, and the watermark by definition drops data older than itself.

These can't both hold in one query. A watermark in the ingest path would silently discard late-but-valid sales.

## Decision

Use **PySpark 4.2 Structured Streaming** (micro-batch engine), packaged in a project Docker image (Python 3.12 + JDK 21), running in **local mode** (`local[*]`) inside containers. There are two separate applications with separate checkpoints:

1. **`ingest` (stateless):** one query per source topic group (`<primary>,<primary>.retry`). Decode → parse → validate (native expressions) → `foreachBatch`: valid rows → Parquet landing batch; rejected rows → Kafka DLQ; per-partition audit rows → landing. No watermark, no dedupe, so nothing is ever dropped for lateness. Trigger: 30 s. `maxOffsetsPerTrigger` for back-pressure.
2. **`realtime` (stateful):** POS only. `withWatermark("event_ts", "10 minutes")` → `dropDuplicatesWithinWatermark(["event_id"])` → explode lines → tumbling windows (store revenue per 5 min; product units per 1 h) → `update` output mode → `foreachBatch` upsert into PostgreSQL. Trigger: 10 s. RocksDB state store provider.

Rules for both apps: native Spark SQL functions only (no Python UDFs); no `collect()`/`toPandas()`; `failOnDataLoss=true`; transformations are pure DataFrame-to-DataFrame functions, unit-tested with a local SparkSession; I/O lives in job modules. Per-batch counts come from `DataFrame.observe()` and are exported by a `StreamingQueryListener`.

### Key concepts this relies on

- **Lazy evaluation.** Transformations (`select`, `filter`, `join`) only build a logical plan. Nothing runs until an action (`write`, `count`) or, in streaming, until the query is started. The optimizer (Catalyst) sees the whole plan, pushes filters down and prunes columns. This is why a Python UDF hurts: it is a black box to Catalyst, and it forces rows to be serialized to a Python worker and back.
- **Micro-batches.** Each trigger, Spark (1) asks Kafka for the latest offsets, (2) writes the planned offset range to the checkpoint **offset log** (write-ahead), (3) runs the batch as a normal Spark job, (4) writes the **commit log** once the sink finishes. A restart re-runs any planned-but-uncommitted batch over the identical offset range.
- **Checkpoint.** Directory holding the offset log, commit log, query metadata (its stable `id`) and state store files. It is tied to the query plan: changing aggregation keys or stateful operators generally requires a new checkpoint. It must be on durable storage (production: object storage, not a pod's local disk).
- **Watermark.** `max(event_time seen) − delay`. It tells stateful operators when a window can be finalized and when state can be evicted. Records older than the watermark are dropped by stateful operators and counted in `numRowsDroppedByWatermark`. With a 10-min delay, a window closes 10 minutes after the latest event time passes its end.
- **Output modes.** `update` emits only windows that changed in this batch, which suits upserts. `append` emits a window once, after it is final (watermark passed), so results are delayed by the watermark. `complete` re-emits everything and is unbounded.
- **Exactly-once limits.** Spark guarantees exactly-once *processing* of the offset range into the sink **only if the sink is idempotent or transactional**. `foreachBatch` gives at-least-once by itself: a batch can run twice. We make each sink idempotent (ADR-007).

## Alternatives

| Option | Why not chosen |
|---|---|
| One app doing everything | A watermark would drop late sales from the warehouse path, and a failure in the PostgreSQL sink would stall warehouse ingestion. |
| Watermark + dedupe in ingest | Silently drops late events, violating F7 and N4. Duplicates are cheaper to remove in Snowflake with unbounded history. |
| Apache Flink | Stronger true streaming semantics (per-record, event-time timers, native two-phase-commit sinks). Heavier to run and operate locally, a weaker Python API, and the brief specifies Spark. |
| Kafka Streams / Faust / plain Python consumers | JVM-only (Kafka Streams) or limited ecosystem (Faust). Plain consumers would need windowing, state and checkpointing reinvented. |
| Spark standalone cluster (master + workers) | More realistic topology but ~2 GB more RAM for no functional difference; the same code runs via `spark-submit` against a cluster. |
| Continuous processing mode | Experimental; doesn't support aggregations or `foreachBatch`. |
| Real-time mode (Spark 4.1+) | Newer low-latency mode; second-level latency is enough here, and micro-batch is the mature, well-documented path for learning checkpoints. |

## Consequences

**Positive**
- The warehouse path is complete regardless of lateness; the realtime path has bounded memory.
- Crash recovery is automatic and demonstrable (restart tests).
- Both apps are independently deployable, scalable and restartable.

**Negative / accepted trade-offs**
- Realtime aggregates exclude events more than 10 minutes late. They are an operational view, documented as such; Snowflake is the financial truth.
- Two Kafka reads of `pos.transactions` (ingest and realtime) double the read load on that topic. Negligible, and it's what Kafka is designed for.
- Local mode has one JVM per app, so there's no real distributed shuffle. Partition-level parallelism still applies.
- 30 s ingest triggers create many small Parquet files. Acceptable locally; production would use longer triggers or compaction, and larger files for COPY (ADR-010).
