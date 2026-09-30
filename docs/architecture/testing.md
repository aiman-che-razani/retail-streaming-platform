# Test Strategy

The suite follows the test pyramid: many fast tests with no dependencies, fewer tests against real services, and a few full-pipeline tests. Markers select each layer.

| Layer | Marker | Needs | Command | What it proves |
|---|---|---|---|---|
| Unit | `unit` | nothing | `make test-unit` | Business logic, simulator realism and faults, loader/migration/reconciliation logic with fakes, metrics mapping |
| Contract | `contract` | nothing | `make test-unit` | Schemas ↔ examples ↔ topics ↔ Python models ↔ COPY columns ↔ RAW DDL agree; all Snowflake SQL parses (sqlglot) |
| Spark | `spark` | JVM | `make test-spark` | Every validation rule, output projections, windows, watermark drops, idempotent landing |
| Integration | `integration` | `make up` | `make test-integration` | Python ↔ Kafka and Schema Registry (round trip, schema rejection, evolution rules, redrive, provisioning, broker outage) |
| Spark integration | `spark and integration` | `make up` | `make test-spark-integration` | Kafka → Spark → landing/DLQ, restart from checkpoint, Spark → PostgreSQL idempotent upsert |
| End-to-end | `e2e` | `make up-pipeline` | `make test-e2e` | Producer → Kafka → both Spark apps → PostgreSQL and DLQ, with dedupe, late drop and DLQ in one scenario |
| Snowflake (live) | `snowflake` | a Snowflake account | manual | Not automated: needs credentials. Verify with `docs/runbooks/snowflake-setup.md` |

Integration and e2e tests **skip with a clear message** when their services aren't running, so `pytest` never fails just because Docker is down.

## Failure modes → verification

| FM | Failure | Verified by |
|---|---|---|
| FM-01 | Kafka unavailable (producer) | `integration/test_kafka.py::test_unavailable_broker_is_reported_not_silently_dropped` |
| FM-02/03/17 | Spark crash / restart | `spark/test_kafka_integration.py::test_restart_from_checkpoint_processes_only_new_records`; `spark/test_outputs_and_streaming.py::test_ingest_writer_is_idempotent_per_batch`; manual SIGKILL drill (Phase 5: zero duplicate offsets) |
| FM-04 | Malformed events | `spark/test_validation.py::test_malformed_json_is_env_002`, `::test_unframed_value_is_env_001`; `e2e` |
| FM-05/06 | Semantic violations, missing fields | `spark/test_validation.py::test_invalid_examples_are_rejected_with_their_rule` (+ one test per rule) |
| FM-07 | Duplicate events | `spark/test_outputs_and_streaming.py::test_windowed_aggregations`; `e2e` (duplicate counted once); STAGING MERGE (static SQL tests) |
| FM-08 | Business duplicates | `R__200_staging_procedures.sql` (static parse); live: `OPS.BUSINESS_DUPLICATES` |
| FM-09 | Late events | `spark/test_outputs_and_streaming.py::test_watermark_drops_late_rows_and_counts_them`; `spark/test_validation.py::test_old_events_are_valid_on_the_ingest_path`; `e2e` |
| FM-10 | Late-arriving dimension | `unit/simulator` (unknown-product fault is schema-valid); SCD2 procedures (static parse) |
| FM-11/12 | Snowflake unavailable / loader crash | `unit/test_loader.py` (outage keeps batches, backoff, replay idempotent) |
| FM-14/15 | Compatible / incompatible schema change | `integration/test_kafka.py::test_registry_enforces_backward_transitive_rules`; `spark/test_validation.py::test_unknown_extra_fields_are_tolerated` |
| FM-16 | Producer bypasses the registry | `spark/test_validation.py::test_unframed_value_is_env_001` |
| FM-20 | PostgreSQL replay | `spark/test_kafka_integration.py::test_postgres_upsert_is_idempotent_on_replay` |
| FM-22 | Poison record | `spark/test_validation.py::test_wrong_type_field_is_rejected_not_crashing` |
| FM-13, 18, 19, 21 | Task failure, checkpoint loss, retention expiry, full disk | Manual drills (runbooks); require live Snowflake or deliberately destructive setups |

## Conventions

- Seeded RNGs and injected clocks. There are no `sleep`s in unit tests; integration tests poll with a deadline.
- Every integration test uses unique topic names, SKUs or checkpoint directories and cleans up after itself, so tests never disturb the running pipeline or each other.
- `collect()` appears only in tests, on a handful of rows.
