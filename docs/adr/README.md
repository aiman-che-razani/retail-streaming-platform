# Architecture Decision Records

An ADR records *one* significant decision: the forces at play (Context), what we chose (Decision), what else we considered and why we rejected it (Alternatives), and what we accept as a result (Consequences). ADRs are immutable once accepted; a changed decision gets a new ADR that supersedes the old one.

| ADR | Title | Status |
|---|---|---|
| [ADR-001](ADR-001-kafka-streaming-backbone.md) | Kafka as the streaming backbone | Accepted |
| [ADR-002](ADR-002-event-serialization.md) | Event serialization: JSON Schema + Schema Registry, envelope/payload | Accepted |
| [ADR-003](ADR-003-kafka-partitioning.md) | Kafka partitioning and keying strategy | Accepted |
| [ADR-004](ADR-004-spark-structured-streaming.md) | Spark Structured Streaming: two apps, stateless ingest vs stateful realtime | Accepted |
| [ADR-005](ADR-005-snowflake-architecture.md) | Snowflake architecture: layers, warehouses, RBAC, cost controls | Accepted |
| [ADR-006](ADR-006-dimensional-modelling.md) | Dimensional modelling: star schema, deterministic keys, hybrid SCD | Accepted |
| [ADR-007](ADR-007-delivery-and-idempotency.md) | Delivery semantics and idempotency | Accepted |
| [ADR-008](ADR-008-dead-letter-and-retry.md) | Dead-letter handling and retry (redrive) topics | Accepted |
| [ADR-009](ADR-009-observability.md) | Observability: structured logs, Prometheus metrics, Grafana | Accepted |
| [ADR-010](ADR-010-snowflake-loading.md) | Snowflake loading: landing zone + PUT/COPY, Streams and Tasks | Accepted |
| [ADR-011](ADR-011-realtime-serving-store.md) | PostgreSQL as the real-time serving store | Accepted |
| [ADR-012](ADR-012-repository-and-toolchain.md) | Repository layout and Python toolchain | Accepted |

Template: [template.md](template.md)
