# ADR-012: Repository layout and Python toolchain

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, python-engineer, devops-engineer

## Context

The code base contains a simulator, Kafka tooling, two Spark apps, a Snowflake loader, a migration runner and DQ tooling. They share configuration, logging, error types and event contracts. The suggested layout put `simulator/`, `kafka/producers/` and `spark/jobs/` at the top level beside `src/`, which would create several import roots and duplicated shared code. The developer machine is Windows with no working Python ≥ 3.12 on PATH and only JDK 25, which Spark doesn't support. CI runs on Linux.

## Decision

- **One installable package**, `retail_platform`, in **src layout** (`src/retail_platform/…`), with subpackages per component (`config`, `observability`, `contracts`, `simulator`, `messaging`, `processing`, `loader`, `quality`, `cli`). Console entry points: `retail-simulator`, `retail-topics`, `retail-schemas`, `retail-spark-ingest`, `retail-spark-realtime`, `retail-loader`, `retail-snowflake-migrate`, `retail-dlq`, `retail-reconcile`.
- **Language-neutral assets stay top level:** `kafka/schemas`, `kafka/config`, `spark/config`, `snowflake/*`, `postgres/`, `monitoring/`.
- **Toolchain:** Python **3.12** (containers pin `python:3.12-slim`); **uv** for dependency resolution, locking (`uv.lock`) and virtualenvs; **ruff** (lint + format); **mypy --strict** on `src/`; **pytest** with markers `unit`, `contract`, `spark`, `integration`, `e2e`. Optional dependency groups: `spark` (pyspark), `snowflake` (connector), `dev`.
- **Runtime:** everything that needs Java (Spark) or specific versions runs in Docker. On the host, `uv` installs a managed Python 3.12, so no system Python change is needed. Spark unit tests run in the Spark container (`make test-spark`) or anywhere with JDK 17/21.
- **Task runner:** a `Makefile` (works with GNU make on Linux/macOS and on Windows via Git Bash + GNU make). Targets call scripts, so logic isn't buried in make syntax.

## Alternatives

| Option | Why not chosen |
|---|---|
| Top-level `simulator/`, `spark/jobs/` etc. as separate packages | Several `pyproject`s or `sys.path` hacks, duplicated config/logging code, harder type checking. |
| Poetry / pip-tools | Both fine. uv is faster, manages Python versions itself (which solves the missing Python 3.12 on this machine), and has a cross-platform lockfile. |
| flake8 + black + isort | ruff replaces all three with one fast tool and one config. |
| Monorepo with separate images per component and separate dependency sets | Leaner images; unnecessary at this size. Two images (app, Spark) cover all components. |
| Invoke/nox/just instead of make | Extra tool to install; `make` is the most universally recognised entry point. |

## Consequences

**Positive**
- One import root, and one dependency lock shared by containers and CI.
- The host needs only Docker, git, make and uv.

**Negative / accepted trade-offs**
- The Spark image carries the whole package (small overhead).
- Windows users need GNU make (Git Bash or `choco install make`). All targets are also documented as plain commands in the README.
