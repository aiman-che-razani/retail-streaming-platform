# Real-Time Retail Data Platform

A production-oriented, locally runnable streaming data platform for a fictional 30-store Malaysian retail chain:

```
Retail Simulator → Kafka (+ Schema Registry) → Spark Structured Streaming → landing zone → Snowflake (RAW → STAGING → ANALYTICS) → SQL analytics
                                                          └──────────────→ PostgreSQL (real-time aggregates) → Grafana
```

> **Status:** Phase 1 (Architecture) complete. Implementation is in progress; this README will be completed in Phase 13.

## Start here

| Document | What it answers |
|---|---|
| [Architecture overview](docs/architecture/overview.md) | Requirements, components, boundaries, data flow, deployment, observability, security |
| [Event contracts](docs/architecture/event-contracts.md) | Envelope, event catalog, validation rules, schema evolution |
| [Kafka topology](docs/architecture/kafka-topology.md) | Topics, keys, partitions, configs, delivery semantics |
| [Data model](docs/architecture/data-model.md) | Snowflake layers, star schema, grains, SCD strategy |
| [Failure modes](docs/architecture/failure-modes.md) | What happens when things break, and how it recovers |
| [ADRs](docs/adr/README.md) | Why each major decision was made, and what was rejected |

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Architecture, contracts, ADRs | ✅ |
| 2 | Local infrastructure (Docker Compose, Kafka, Schema Registry, PostgreSQL, monitoring) | ⏳ |
| 3 | Data simulator | ⏳ |
| 4 | Kafka producers, topics, schemas, DLQ tooling | ⏳ |
| 5 | Spark Structured Streaming | ⏳ |
| 6 | Snowflake infrastructure and loading | ⏳ |
| 7 | Dimensional model and analytics | ⏳ |
| 8 | Data quality and reconciliation | ⏳ |
| 9 | Test suite | ⏳ |
| 10 | Observability | ⏳ |
| 11 | CI/CD | ⏳ |
| 12 | Production review | ⏳ |
| 13 | Documentation | ⏳ |
