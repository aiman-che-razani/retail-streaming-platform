# ADR-010: Snowflake loading — landing zone + PUT/COPY, then Streams and Tasks

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, snowflake-engineer, spark-engineer

## Context

Spark produces a continuous flow of validated events. Snowflake compute is billed per second with a 60 s minimum per warehouse resume (ADR-005). Writing to Snowflake from every 30-second micro-batch would keep a warehouse running permanently (~24 credits/day). We also want Spark to keep working when Snowflake is unavailable, loads that are idempotent, and in-warehouse incremental transformation.

## Decision

1. **Landing zone.** Spark ingest writes each micro-batch as Parquet to `data/landing/<dataset>/query_id=<id>/batch_id=<n>/` with a `_SUCCESS` marker (layout in `overview.md`). Locally this is a bind-mounted directory; in production it would be object storage (S3/GCS/ADLS).
2. **Loader service** (Python, every `LOADER_INTERVAL_SECONDS`, default 900):
   1. find completed batch directories (`_SUCCESS` present) not yet archived;
   2. `PUT` their files to the internal named stage `@RAW.LANDING_STAGE/<dataset>/query_id=<id>/batch_id=<n>/` (`AUTO_COMPRESS=FALSE` for Parquet; `OVERWRITE=TRUE`, because COPY load metadata, not the stage, prevents double loading);
   3. `COPY INTO RAW.<table> … FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE`, plus load metadata columns (`METADATA$FILENAME`, `METADATA$FILE_ROW_NUMBER`, load timestamp) and `PARSE_JSON(event_json)` into the `EVENT` VARIANT. `ON_ERROR = ABORT_STATEMENT`: a file that doesn't load is a bug, not something to skip;
   4. verify every file's COPY status (`LOADED`, no errors); `PURGE = TRUE` removes loaded files from the stage; on success move the local directory to `data/archive/` (deleted after `LOADER_ARCHIVE_RETENTION_DAYS`). Row-level completeness is reconciled against `RAW.INGEST_BATCH_AUDIT` (DQ rule SF-005).
   - Store reference CSV → `RAW.STORE_REFERENCE` (same mechanism, loaded when the file checksum changes).
3. **In-warehouse transformation with Streams + Tasks** (see `data-model.md` §5). Append-only streams on RAW tables; a task graph scheduled every 15 min with `WHEN SYSTEM$STREAM_HAS_DATA(...)`; stored procedures for multi-statement transactional units (stream consumption + SCD2 + re-keying).
4. **Deployment of Snowflake objects** by a small Python migration runner: `ddl/` (bootstrap, ACCOUNTADMIN, idempotent), `migrations/V###__*.sql` (applied once, checksummed, recorded in `OPS.SCHEMA_MIGRATIONS`), `transformations/R__*.sql` (re-applied when the checksum changes).

**Why Streams + Tasks are genuinely needed here (not decoration):** the loader only moves files. Incremental "what's new since last time" logic would otherwise need hand-rolled watermarks in a table, which are fragile under replays and concurrent runs. Streams give transactional change offsets for free. Tasks keep transformation scheduling inside Snowflake, so the same design works unchanged when the loader is replaced by Snowpipe in production (no client to trigger transforms). **Stored procedures** are used only where a stream must be consumed by several statements atomically.

## Alternatives

| Option | Why not chosen |
|---|---|
| Spark → Snowflake connector in `foreachBatch` | A warehouse resume every micro-batch, so effectively always on (cost). Couples Spark availability to Snowflake: an outage stalls or crashes streaming. Adds JDBC/connector jars. |
| Snowpipe (auto-ingest) | The production answer with cloud storage: serverless, no warehouse, event-driven. Needs an external stage on S3/GCS/Azure plus notifications, which isn't possible with local files. The landing-zone contract is designed so swapping the loader for Snowpipe changes nothing downstream. |
| Snowpipe Streaming / Kafka connector | Lowest latency, row-based, cheap. Bypasses Spark (the brief requires Spark in the path) and needs Kafka Connect. Documented alternative. |
| Loader triggers transformations directly (`CALL` after COPY) | Simpler and aligns warehouse resumes perfectly, but ties transformations to this loader and teaches nothing about in-warehouse orchestration. We keep the option open via `EXECUTE TASK` for demos. |
| External orchestrator (Airflow) | A major new component for two schedules. Rejected per the engineering rules; noted as a future enhancement. |

## Consequences

**Positive**
- Streaming and warehousing fail independently. Landing absorbs Snowflake outages (FM-11).
- Snowflake compute is spent only in short, predictable windows, and zero when idle.
- Idempotent at every step (file-level COPY metadata, `event_id` MERGE).

**Negative / accepted trade-offs**
- Freshness is bounded by loader and task intervals (≈ 15–35 min by default).
- Many small files (one per batch per dataset). COPY handles them, but per-file overhead grows. Production would use longer triggers or compaction to reach ~100–250 MB files.
- The loader is custom code to maintain, and is well-tested because of that.
- COPY load metadata expires after 64 days. Re-loading a file older than that would duplicate RAW rows (still removed by MERGE downstream). `PURGE = TRUE` removes files from the stage as soon as they load, and local copies are archived, so this doesn't happen in practice.
