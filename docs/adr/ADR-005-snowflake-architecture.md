# ADR-005: Snowflake architecture — layers, warehouses, RBAC and cost controls

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, snowflake-engineer

## Context

Snowflake is the analytical system of record. It runs on a **trial account with limited credits**, so compute must be spent only when there is work. Requirements: layered data (RAW/STAGING/ANALYTICS), least-privilege access, reproducible deployment, and protection against runaway cost.

How Snowflake bills (the forces that shape this ADR):

- **Storage and compute are separate.** Data sits in cloud storage as micro-partitions. A **virtual warehouse** is a compute cluster started on demand. Warehouses are billed per second while running, **with a 60-second minimum each time they resume**. An XSMALL warehouse costs 1 credit/hour, and each size up doubles it.
- **Cloud services** (metadata, query compilation, `SYSTEM$STREAM_HAS_DATA` checks) are free up to 10% of daily warehouse usage.
- A warehouse left running idle burns credits, so `AUTO_SUSPEND` is the most important cost setting.

## Decision

**Objects** (all created by scripts, see ADR-010/012):

- Database `RETAIL_<ENV>`, with schemas `RAW`, `STAGING`, `ANALYTICS`, `OPS`. The environment is a prefix, so dev and prod could share an account without sharing objects.
- Warehouses, all `XSMALL`, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, `INITIALLY_SUSPENDED = TRUE`, `STATEMENT_TIMEOUT_IN_SECONDS = 900`:
  - `RETAIL_PIPELINE_WH` — COPY loads and task transformations (one workload, so one resume serves both when schedules align);
  - `RETAIL_ANALYTICS_WH` — human and BI queries, isolated so an expensive analyst query can't delay the pipeline, and so cost is attributable.
- **Resource monitor** `RETAIL_MONTHLY_MONITOR`: 20 credits/month (configurable); notify at 50% and 80%, **suspend** at 100%, suspend immediately at 110%.
- **Roles** (all rolled up to `SYSADMIN`, so administrators keep visibility):

| Role | Grants | Used by |
|---|---|---|
| `RETAIL_ADMIN` | owns database, schemas and objects; runs migrations | developer / CI deploy |
| `RETAIL_LOADER` | USAGE on `RETAIL_PIPELINE_WH`, db, `RAW`; INSERT/SELECT on RAW tables; READ/WRITE on `RAW.LANDING_STAGE` | loader service user |
| `RETAIL_TRANSFORMER` | owns tasks and procedures; SELECT on RAW + streams; DML on STAGING/ANALYTICS/OPS; `EXECUTE TASK` | tasks (run as owner) |
| `RETAIL_ANALYST` | USAGE on `RETAIL_ANALYTICS_WH`; SELECT on ANALYTICS (including future tables/views) | analysts, BI |

- **Authentication:** the loader runs as `RETAIL_LOADER_SVC` (`TYPE = SERVICE`) with **key-pair auth** (RSA 2048, encrypted private key, path and passphrase from env). Password auth is supported only for an interactive developer user in `.env`, and is documented as dev-only. Snowflake is phasing out single-factor password sign-in, and long-lived passwords in env files are the weakest option.
- **Schedules:** loader every 15 min; task graph every 15 min with `WHEN SYSTEM$STREAM_HAS_DATA`. Tasks are created suspended.

### Expected cost (defaults)

Each load cycle ≈ a few seconds of COPY plus 60 s idle before suspend, so about 70 s billed. The task graph adds another ~70 s when schedules don't overlap. Per hour: 4 × ~140 s ≈ 9.5 min of XSMALL ≈ **0.16 credits/hour while the pipeline runs**, and **0 when stopped or idle** (tasks skip, warehouses suspend). An 8-hour dev day ≈ 1.3 credits. Setting both intervals to 60 min cuts this ~4×.

## Alternatives

| Option | Why not chosen |
|---|---|
| One warehouse for everything | Cheapest in theory, but analyst queries compete with loads and cost can't be attributed. The two-warehouse split costs nothing extra when idle. |
| Serverless tasks | No 60 s minimum, and billing is per actual compute (with a multiplier). Attractive for tiny workloads. We keep a user-managed warehouse so cost is visible in one place (`WAREHOUSE_METERING_HISTORY`) and behaviour is predictable. Switching is a one-line change per task, documented as an optimisation. |
| Snowflake-managed dynamic tables | Declarative incremental pipelines, a strong modern option. But SCD2 with out-of-order handling and inferred members needs procedural control, and the target-lag refresh model makes cost less explicit. Streams and Tasks teach the underlying mechanics. Future enhancement for simple aggregates. |
| Larger warehouse "to be fast" | Data volume is tiny; SMALL would double cost for no benefit. |
| Password for service user | Weakest option and being phased out by Snowflake. Rejected. |
| Terraform for account objects | Excellent in production (drift detection, reviewable plans), but adds a major tool. Idempotent SQL bootstrap scripts suffice; Terraform/Snowflake provider is a documented future enhancement. |

## Consequences

**Positive**
- Idle cost is zero; active cost is bounded by the resource monitor.
- A compromised loader credential can only insert into RAW; it can't read analytics or drop objects.
- All objects are reproducible from scripts.

**Negative / accepted trade-offs**
- Warehouse freshness is minutes, not seconds (N3). This is deliberate: second-level freshness comes from PostgreSQL (ADR-011).
- Account bootstrap (roles, warehouses, resource monitor, service user) needs `ACCOUNTADMIN` once, run by a human.
- Production would split roles further into *access roles* (per schema, read/write) granted to *functional roles*, add network policies, masking policies for PII, and separate accounts or databases per environment.
