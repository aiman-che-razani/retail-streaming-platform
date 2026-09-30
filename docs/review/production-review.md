# Production Review (Phase 12)

A whole-repository review by the `code-reviewer` agent (`.claude/agents/code-reviewer.md`). It checked conformance to the architecture contracts, correctness, reliability, security, performance, testing, data quality, observability, documentation and Snowflake cost. Every CRITICAL and HIGH finding was fixed; most MEDIUM and LOW findings were fixed as well. The remaining items are recorded below as known limitations.

**Important caveat.** No Snowflake account was available during development, so the Snowflake SQL has been checked statically only: it is parsed in the Snowflake dialect by `tests/contract/test_snowflake_sql.py`, and its column contracts are cross-checked. It has not been executed. The first live deployment should follow [`docs/runbooks/snowflake-setup.md`](../runbooks/snowflake-setup.md) and then verify the checklist at the end of this document.

## Findings and resolutions

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | CRITICAL | Task ownership was transferred task by task. Every task in a graph needs the same owner, and per-task transfer can sever predecessor links, which would leave the child tasks unscheduled | `R__800_tasks.sql` ends with `GRANT OWNERSHIP ON ALL TASKS IN SCHEMA OPS … COPY CURRENT GRANTS`, transferring the whole graph atomically on every deploy of that script |
| 2 | HIGH | `CREATE OR REPLACE PROCEDURE/VIEW` drops grants, and the grants script only re-ran when its own checksum changed. That could leave the pipeline with insufficient privileges and auto-suspended tasks | `COPY GRANTS` added to every replaced procedure, function and view. The migration runner now treats `R__9xx` scripts as **post-deploy**, re-applied after any other script (unit-tested) |
| 3 | HIGH | Bootstrap dropped the `PUBLIC` schema as `RETAIL_ADMIN`, which doesn't own it, so the script would abort before creating the loader user | `PUBLIC` is dropped while still `ACCOUNTADMIN` |
| 4 | HIGH | The daily inventory snapshot was built once and never saw late movements | The nightly task rebuilds the **last 3 local business days** (`SP_BUILD_RECENT_INVENTORY_SNAPSHOTS(3)`). The documented bound is that older corrections need `SP_BACKFILL_INVENTORY_SNAPSHOTS` |
| 5 | HIGH | The inventory turnover query averaged per product, inflating turnover about N× | Rewritten as a two-step aggregation for a semi-additive measure: sum per (category, day), then average over days |
| 6 | HIGH | Docs and code disagreed on landing timestamp types, FM-12 re-PUT behaviour, and the SF-004 and SF-006 definitions | Docs updated to match the implementation, including why timestamps are ISO strings |
| 7 | MEDIUM | A partial load cycle could re-upload already-loaded files | Each dataset's batches are archived right after its own COPY succeeds (unit-tested) |
| 8 | MEDIUM | A business duplicate with extra lines could produce a mixed basket | The staging MERGE never extends an already-staged transaction |
| 9 | MEDIUM | DIM_PRODUCT NOT NULL columns weren't guarded in Spark, so one bad event could stall the task graph | PRD-001 now requires brand, subcategory and unit of measure (contract updated first; Spark test added) |
| 10 | MEDIUM | DQ checks scanned all history every 15 minutes, and SF-001 would stay FAIL forever | Checks are windowed to rows loaded in the last 48 h. Full reconciliation stays on demand via `retail-reconcile` |
| 11 | MEDIUM | V004 seed inserts weren't safe to re-run after a partial failure | Guarded with `NOT EXISTS`; DIM_DATE uses a gap-free `ROW_NUMBER` |
| 12 | MEDIUM | Loader freshness alert fired on an idle pipeline and missed a crash-looping loader | Idle cycles mark datasets fresh; new `RetailLoaderCrashLooping` alert |
| 13 | MEDIUM | Host ports were published on all interfaces, and Prometheus had its lifecycle API enabled | All ports bound to `127.0.0.1`; lifecycle API removed |
| 14 | MEDIUM | Several failure modes lack automated tests, and there's no live Snowflake execution | Partly addressed (see [testing.md](../architecture/testing.md)). The live verification checklist is below |
| 15 | LOW | `SEQ4()` gaps, a stale file reference, a misleading dedupe comment, business duplicates re-logged on redelivery | Fixed |
| 16 | LOW | AGE_BAND is frozen when a customer's rows are rebuilt | Deferred: it's recomputed whenever the customer changes; moving it to a view is noted as a follow-up |
| 17 | LOW | Facts with an unknown store are never re-keyed | Deferred: stores come from the versioned CSV loaded before any facts; SF-004 alerts if it ever happens |
| 18 | LOW | Base images pinned by tag rather than digest | Deferred: Dependabot tracks tags; digest pinning is recommended for production |
| 19 | QUESTION | `FUTURE` grants may require `MANAGE GRANTS` | Removed. The post-deploy grants script re-grants on every deploy instead |

## Verification pass

The reviewer re-checked every CRITICAL and HIGH fix. All were confirmed except one: the fix for #2 had introduced a new defect, a duplicated `COPY GRANTS` clause in every view. Snowflake rejects that, and it would have blocked the whole deployment. It was fixed, and `tests/contract/test_snowflake_sql.py::test_replaced_objects_keep_grants_exactly_once` now guards against it. The reviewer also noted that V004 was edited in place. That is acceptable only because it has never been applied anywhere. From the first live deployment onwards, changes go into new `V###` files (the runner enforces this with checksums).

## Live Snowflake verification checklist (first deployment)

1. `make snowflake-migrate`, then `retail-snowflake-migrate status` shows nothing pending.
2. `SHOW TASKS IN SCHEMA OPS;`: all tasks are owned by `RETAIL_TRANSFORMER`; `TASK_DIMENSIONS` has predecessor `TASK_PIPELINE_ROOT`, and so on down the graph.
3. `SHOW GRANTS ON PROCEDURE STAGING.SP_STAGE_ALL();` includes `USAGE` for `RETAIL_TRANSFORMER`.
4. `make load`, then `EXECUTE TASK OPS.TASK_PIPELINE_ROOT;`, then check `TABLE(INFORMATION_SCHEMA.TASK_HISTORY())` for success of all four tasks.
5. `SELECT COUNT(*) FROM ANALYTICS.FACT_SALES;` > 0; run `snowflake/analytics/*.sql`.
6. `make reconcile` exits 0 once the grace period has passed.
