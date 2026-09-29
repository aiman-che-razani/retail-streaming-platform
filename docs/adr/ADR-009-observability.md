# ADR-009: Observability — structured logs, Prometheus metrics, Grafana dashboards

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, observability-engineer

## Context

A streaming pipeline fails in quiet ways: a consumer falls behind, a DLQ fills, a warehouse load stops, data goes stale. Operators need to answer four questions quickly: **Is data flowing? Is it fresh? Is it correct? Where is it stuck?** The stack must run locally for free.

One Spark-specific fact shapes the design: **Spark's Kafka source doesn't commit offsets to a Kafka consumer group.** Standard lag tools (`kafka-consumer-groups.sh`, kafka-exporter's `kafka_consumergroup_lag`, Burrow) therefore show nothing for Spark.

## Decision

- **Logs:** `structlog` rendering one JSON object per line to stdout, with standard fields (`timestamp`, `level`, `service`, `event`, and where relevant `event_id`, `transaction_id`, `topic`, `partition`, `offset`, `correlation_id`, `processing_time_ms`, `status`). Context is *bound* (`log.bind(topic=…)`), never string-formatted. Docker collects stdout. Spark JVM logs use log4j2 at WARN to cut noise.
- **Metrics:** `prometheus_client` HTTP endpoints in every Python process (simulator, loader, and the Spark drivers). The metric catalog in `overview.md` §8 is a contract.
- **Spark metrics** come from a `StreamingQueryListener` on the driver: batch duration, input/processed rows per second, state rows, rows dropped by watermark, and Kafka `avgOffsetsBehindLatest`/`maxOffsetsBehindLatest` (**this is Spark's consumer lag**). Valid/rejected/warned counts are computed inside the batch with `DataFrame.observe()` (no extra Spark job) and delivered to the listener.
- **Kafka broker/topic metrics:** `kafka-exporter` for topic offsets (throughput and DLQ volume) and classic consumer-group lag (the redrive CLI).
- **Pipeline latency:** `retail_spark_event_lag_seconds` (processing time − event time) in Spark; warehouse freshness from `OPS` SQL (`_LOADED_AT − EVENT_TIMESTAMP`, `max(event_ts)` in facts) via the reconciliation CLI.
- **Prometheus** scrapes every 15 s. **Grafana** is provisioned from files (datasources and dashboards in git): *Pipeline Overview*, *Kafka*, *Spark Streaming*, *Loader & Warehouse*, *Real-time Sales* (PostgreSQL datasource).
- **Alert rules** (Prometheus): producer failures > 0, Spark lag growing for 5 min, no Spark progress for 2 min, DLQ rate > threshold, loader last success > 45 min, landing backlog growing. Each rule links to a runbook anchor.
- **Cardinality rule:** labels only from small, bounded sets (topic, query, dataset, outcome, status). Never IDs.

## Alternatives

| Option | Why not chosen |
|---|---|
| Spark's built-in `PrometheusServlet` sink | Exposes generic Spark metrics but not our semantic counters (valid/rejected) or the Kafka-lag values in the format we want. We can enable it additionally for JVM/executor metrics. |
| Commit Spark offsets to a consumer group just for lag tooling | Possible via the listener, but it creates a misleading "consumer group" that doesn't control anything. Exporting Spark's own metric is honest. |
| OpenTelemetry (traces + metrics + logs) | The right long-term standard; `correlation_id` is already trace-ready. A collector plus tracing backend adds containers and learning load; documented as a future enhancement. |
| ELK / Loki for logs | Useful for log search across services. `docker compose logs` + JSON + `jq` is sufficient locally. Loki is a documented optional add-on. |
| JMX exporter on the broker | Rich broker internals (request latency, ISR), but heavy config. kafka-exporter covers what the pipeline dashboards need. |

## Consequences

**Positive**
- The four operational questions are answerable from one Grafana home dashboard.
- Metrics are cheap and local; logs are machine-parseable and correlatable by `correlation_id`.

**Negative / accepted trade-offs**
- No distributed tracing; correlation relies on IDs in logs.
- Prometheus is scraped per process. The Spark driver metrics port must be reachable; executor metrics aren't needed in local mode.
- Warehouse-side metrics (freshness, reconciliation) are pulled by a CLI, not natively exported by Snowflake.
