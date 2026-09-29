# CLAUDE.md — Retail Streaming Platform

Real-time retail data platform: **Simulator → Kafka → Spark Structured Streaming → landing zone → Snowflake (RAW → STAGING → ANALYTICS)**, with a PostgreSQL real-time serving store and Prometheus/Grafana monitoring.

## Sources of truth (read before changing anything)

| Concern | Source of truth | Consumers that MUST conform |
|---|---|---|
| Event contracts | `kafka/schemas/*.schema.json` + `docs/architecture/event-contracts.md` | Python models, Spark `StructType`s, Snowflake STAGING SQL |
| Topics, keys, partitions, retention | `kafka/config/topics.yaml` + `docs/architecture/kafka-topology.md` | topic bootstrap script, producers, Spark readers |
| System boundaries & data flow | `docs/architecture/overview.md` | everyone |
| Warehouse layers & dimensional model | `docs/architecture/data-model.md` | Snowflake DDL/transformations, analytics SQL |
| Failure behaviour | `docs/architecture/failure-modes.md` | everyone |
| Decisions | `docs/adr/ADR-*.md` | everyone |

**Never invent a field, topic, key, or table that is not in these documents.** If a change is required, update the contract/doc first (architect), then the implementations, then the contract tests. A contract change without a matching contract-test update is incomplete.

## Repository layout

- `src/retail_platform/` — the single installable Python package (src layout)
  - `config/` settings (pydantic-settings, env-driven) · `observability/` logging + metrics · `contracts/` envelope/payload models + schema loading
  - `simulator/` domain generators + fault injection · `messaging/` Kafka producer/admin/serialization · `processing/` Spark jobs + transformations
  - `loader/` landing zone → Snowflake stage → COPY · `quality/` reconciliation + DQ runners · `cli/` entry points
- `kafka/schemas/` JSON Schema contracts (+ `examples/`) · `kafka/config/topics.yaml`
- `spark/config/` Spark and log4j config · `docker/` Dockerfiles
- `snowflake/ddl/` account bootstrap (ACCOUNTADMIN, run once) · `snowflake/migrations/` versioned `V###__*.sql` · `snowflake/transformations/` repeatable `R__*.sql` (procedures, views, tasks) · `snowflake/analytics/` business queries · `snowflake/quality/` DQ SQL
- `postgres/init/` real-time serving schema · `monitoring/` Prometheus + Grafana provisioning
- `tests/unit` (no external deps) · `tests/integration` (Docker services) · `tests/e2e` · `tests/contract` (schemas vs code)

## Engineering rules

- Python ≥ 3.12, full type hints, `mypy --strict` on `src/`, `ruff` for lint/format.
- Config only from environment (`pydantic-settings`); no hard-coded hosts, ports, or credentials. Add every new setting to `.env.example`.
- Logging: `structlog` JSON to stdout via `retail_platform.observability.logging`. Never `print` in application code.
- Errors: raise from the `retail_platform.errors` hierarchy. No bare `except:`; `except Exception` only at process boundaries (CLI main, Spark `foreachBatch` wrapper) and it must log + re-raise or route to DLQ with a reason.
- No global mutable state; pass dependencies (settings, producer, clock, RNG) explicitly. Randomness is seedable for tests.
- Spark: native functions only unless impossible (no Python UDFs); never `collect()`/`toPandas()` on unbounded data; every streaming query has its own checkpoint location and explicit trigger.
- Snowflake: every object created by versioned or repeatable scripts — never by hand. Warehouses XSMALL, `AUTO_SUSPEND = 60`. Least-privilege roles.
- Timestamps: UTC on the wire (RFC 3339 `Z`); local business dates are derived using the store's timezone in STAGING.
- Money: JSON numbers with ≤ 2 decimal places, parsed as DECIMAL(12,2) — never binary float in Spark/Snowflake.

## Commands (see Makefile)

`make up` · `make down` · `make topics` · `make schemas` · `make simulate` · `make spark-ingest` · `make spark-realtime` · `make load` · `make test-unit` · `make test-integration` · `make lint` · `make typecheck`

## Agents

Specialist agents live in `.claude/agents/`. The **architect** owns contracts and ADRs; specialists implement within them; the **code-reviewer** reviews and does not implement.
