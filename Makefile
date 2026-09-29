# Developer entry points. Every target is a thin wrapper around a documented command.
# Windows: run from Git Bash with GNU make (choco install make).

UV ?= uv
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help env install build up up-pipeline up-all down clean ps logs topics schemas schemas-check simulate backfill simulate-faults spark-ingest spark-realtime landing load snowflake-migrate snowflake-tasks-resume snowflake-tasks-suspend reconcile dlq lint format typecheck test-unit test-integration test-spark test-e2e audit check

help: ## Show this help
	@grep -E "^[a-zA-Z0-9_-]+:.*## " $(MAKEFILE_LIST) | awk -F ":.*## " '{printf "  %-24s %s\n", $$1, $$2}'

env: ## Create .env from the template if missing
	@test -f .env || (cp .env.example .env && echo "Created .env - edit the CHANGE_ME values")

install: ## Create the host virtualenv (uv) with dev tools
	$(UV) sync --python 3.12 --extra snowflake --extra spark

build: ## Build the application and Spark images
	$(COMPOSE) --profile pipeline --profile snowflake build

up: ## Start core infrastructure (Kafka, registry, UI, Postgres, monitoring)
	$(COMPOSE) up -d --wait
	$(COMPOSE) ps

up-pipeline: ## Start core + simulator + Spark ingest/realtime
	$(COMPOSE) --profile pipeline up -d --build
	$(COMPOSE) --profile pipeline ps

up-all: ## Start everything including the Snowflake loader
	$(COMPOSE) --profile pipeline --profile snowflake up -d --build

down: ## Stop all containers (keeps data volumes)
	$(COMPOSE) --profile pipeline --profile snowflake down

clean: ## Stop all containers AND delete volumes (Kafka data, checkpoints, landing)
	$(COMPOSE) --profile pipeline --profile snowflake down -v

ps: ## Show container status
	$(COMPOSE) --profile pipeline --profile snowflake ps

logs: ## Follow logs (make logs s=spark-ingest)
	$(COMPOSE) --profile pipeline --profile snowflake logs -f --tail=100 $(s)

topics: ## Create/verify topics from kafka/config/topics.yaml
	$(COMPOSE) run --rm kafka-init retail-topics ensure

schemas: ## Register schemas in Schema Registry
	$(COMPOSE) run --rm kafka-init retail-schemas register

schemas-check: ## Check schema compatibility against the running registry (CI gate)
	$(COMPOSE) run --rm kafka-init retail-schemas check

simulate: ## Run the simulator in the foreground (host); Ctrl+C to stop
	$(UV) run retail-simulator run

backfill: ## Generate SIMULATOR_BACKFILL_DAYS of history into Kafka (containerised)
	$(COMPOSE) run --rm -e SIMULATOR_MODE=backfill simulator retail-simulator run

simulate-faults: ## Run the simulator with 2% of every fault type (containerised)
	$(COMPOSE) run --rm -e SIMULATOR_MALFORMED_RATE=0.02 -e SIMULATOR_MISSING_FIELD_RATE=0.02 -e SIMULATOR_INVALID_VALUE_RATE=0.02 -e SIMULATOR_DUPLICATE_RATE=0.02 -e SIMULATOR_LATE_EVENT_RATE=0.02 -e SIMULATOR_UNKNOWN_PRODUCT_RATE=0.02 -e SIMULATOR_MAX_EVENTS=5000 simulator retail-simulator run

spark-ingest: ## Start the Spark ingest app
	$(COMPOSE) --profile pipeline up -d spark-ingest

spark-realtime: ## Start the Spark realtime app
	$(COMPOSE) --profile pipeline up -d spark-realtime

landing: ## List landing-zone batches waiting for the loader
	$(COMPOSE) --profile pipeline exec spark-ingest sh -c 'find /app/data/landing -name _SUCCESS | sort | tail -20'

load: ## Run one Snowflake load cycle now (containerised)
	$(COMPOSE) --profile snowflake run --rm loader retail-loader once

snowflake-migrate: ## Apply Snowflake migrations + repeatable scripts (as RETAIL_ADMIN)
	$(UV) run retail-snowflake-migrate apply

snowflake-tasks-resume: ## Resume the Snowflake task graph (starts consuming credits on schedule)
	$(UV) run retail-snowflake-migrate tasks --resume

snowflake-tasks-suspend: ## Suspend the Snowflake task graph
	$(UV) run retail-snowflake-migrate tasks --suspend

reconcile: ## Run end-to-end reconciliation and DQ report
	$(UV) run retail-reconcile report

dlq: ## Summarise DLQ contents (make dlq t=pos.transactions)
	$(UV) run retail-dlq inspect --topic $(or $(t),pos.transactions)

lint: ## ruff lint + format check
	$(UV) run ruff check src tests
	$(UV) run ruff format --check src tests

format: ## Auto-format code
	$(UV) run ruff format src tests
	$(UV) run ruff check --fix src tests

typecheck: ## mypy --strict
	$(UV) run mypy

test-unit: ## Unit + contract tests (no external services)
	$(UV) run pytest -m "unit or contract"

test-integration: ## Integration tests (needs `make up`)
	$(UV) run pytest -m integration

test-spark: ## Spark tests inside the Spark image (JDK 21)
	$(COMPOSE) --profile pipeline build spark-ingest
	docker build -f docker/spark/Dockerfile --target test -t retail-platform/spark-test:local .
	docker run --rm retail-platform/spark-test:local pytest -m spark -p no:cacheprovider

test-e2e: ## End-to-end tests (needs `make up-pipeline`)
	$(UV) run pytest -m e2e

audit: ## Dependency vulnerability audit
	$(UV) export --no-hashes --format requirements-txt --extra snowflake --extra spark > .audit-requirements.txt
	$(UV) run pip-audit -r .audit-requirements.txt --strict
	@rm -f .audit-requirements.txt

check: lint typecheck test-unit ## Everything CI runs locally: lint, typecheck, unit tests
