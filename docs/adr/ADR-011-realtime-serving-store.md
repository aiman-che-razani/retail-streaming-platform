# ADR-011: PostgreSQL as the real-time serving store

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, spark-engineer, observability-engineer

## Context

Store managers and operations want near-real-time views ("revenue per store in the last few 5-minute windows"). The warehouse path is deliberately minutes-behind to save credits (ADR-005/010). Serving second-level data from Snowflake would mean a permanently running warehouse. The brief lists PostgreSQL as an optional local component.

## Decision

The Spark **realtime** app upserts windowed aggregates into **PostgreSQL 17** (`realtime` schema):

- `realtime.store_revenue_5m (store_id, window_start, window_end, revenue, transactions, units, updated_at)`, PK (`store_id`, `window_start`);
- `realtime.product_units_1h (product_id, window_start, window_end, units, revenue, updated_at)`, PK (`product_id`, `window_start`).

Writes happen in `foreachBatch`. Each batch's updated rows are written with the Spark JDBC writer into an unlogged staging table (`realtime._stg_<table>_<batch>`). A single `INSERT … SELECT … ON CONFLICT DO UPDATE` statement then merges them with absolute values and drops the staging table, all in one transaction (ADR-007). No per-row Python. Grafana reads PostgreSQL directly for the *Real-time Sales* dashboard. A retention job deletes windows older than 7 days.

PostgreSQL also serves as the integration-test sink for the realtime app, so its correctness can be tested in CI without Snowflake.

## Alternatives

| Option | Why not chosen |
|---|---|
| Snowflake for real-time too | Continuous warehouse cost; Snowflake is optimised for analytical scans, not high-frequency small upserts. |
| Redis | Fast, but awkward for SQL/Grafana time-series queries, and no durable relational constraints for idempotent upsert keys. |
| Druid / Pinot / ClickHouse | Purpose-built real-time OLAP, excellent at scale, but a heavy additional system. Overkill at 30 stores; future enhancement. |
| Write aggregates to Kafka (compacted topic) | Good for downstream services, but Grafana can't query it directly; it would need yet another consumer. |
| No real-time path | Fails the "real-time analytics" goal and removes the natural home for watermark/window concepts. |

## Consequences

**Positive**
- Second-level dashboards at zero cloud cost.
- A clean example of stateful streaming with idempotent upserts.
- Integration-testable in CI.

**Negative / accepted trade-offs**
- Two stores of "revenue" that can differ (late events, the realtime watermark). Documented: PostgreSQL = operational, approximate; Snowflake = financial truth. The reconciliation report quantifies the difference.
- One more component to monitor (health check, restart policy).
