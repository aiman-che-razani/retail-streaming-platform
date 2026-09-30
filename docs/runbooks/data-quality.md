# Runbook: Data quality, reconciliation and DLQ triage

Data quality is enforced in three places, each for what it can see (event-contracts.md §5):

| Layer | What it checks | Where failures go | How to look |
|---|---|---|---|
| Producer | JSON Schema (types, required, patterns, ranges) | refused, never sent: `retail_producer_validation_failures_total` | simulator logs `event_rejected_by_schema` |
| Spark (record-level) | ENV/POS/INV/CUS/PRD rules | `<topic>.dlq` + `RAW.DEAD_LETTERS` (REJECT); `DQ_WARNINGS` column (WARN) | `make dlq t=pos.transactions`, Grafana *Pipeline Overview* |
| Snowflake (set-level) | SF-001..SF-008: duplicates, integrity, reconciliation, freshness | `OPS.DQ_RESULTS` (history), `OPS.VW_DQ_LATEST` (current) | `make reconcile` |

## Daily check

```bash
make reconcile           # JSON report; exit code 1 if anything needs attention
```

The report has two parts:

- **`topics`**: accounting from Kafka through to STAGING per primary topic. Every gap is put in a category. `not_yet_processed` means Spark lag. `rejected_to_dlq` means validation rejects. `not_yet_loaded` means loader lag. `duplicates_removed_in_staging` means duplicates were deliberately dropped. `unexplained` means **investigate**.
- **`data_quality`**: the latest result of each SF rule.

## Rule-by-rule triage

| Rule | Severity | A FAIL means | First steps |
|---|---|---|---|
| SF-001 | INFO | The same `event_id` was loaded to RAW more than once (replay, checkpoint reset, loader re-run) | Expected occasionally; STAGING removes them. If it grows continuously, check for a Spark checkpoint being reset repeatedly (FM-18). |
| SF-002 | WARN | A source re-sent a business transaction with a *new* `event_id` | `SELECT * FROM OPS.BUSINESS_DUPLICATES ORDER BY DETECTED_AT DESC;` Compare payloads in RAW. It's a source-system bug if the payloads differ. |
| SF-003 | WARN | Facts point at inferred products/customers (master data arrived late or never) | `SELECT PRODUCT_ID FROM ANALYTICS.DIM_PRODUCT WHERE IS_INFERRED;` A short-lived non-zero value is normal. If persistent, the product feed is missing items. With the simulator, `SIMULATOR_UNKNOWN_PRODUCT_RATE` creates these on purpose. |
| SF-004 | ERROR | Facts with an Unknown store/date/product | Shouldn't happen: Spark rejects missing keys. Check `STG_STORES` for a store missing from `stores.csv`, or dates outside DIM_DATE (2020–2030). |
| SF-005 | ERROR | RAW rows for a Spark batch ≠ Spark's valid count | `details.mismatches` lists batches. Check the loader logs for a failed COPY and `data/landing` for stuck batches. Re-run `make load`: COPY is idempotent. |
| SF-006 | ERROR | Rows stuck between layers for longer than the 30-min grace period | `SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC;` Look for a failing procedure. Fix it; the next run reprocesses from the stream (FM-13). |
| SF-007 | WARN | The newest sale in FACT_SALES is > 60 min old | Is the simulator running? Are Spark, the loader and the tasks resumed? Check `OPS.VW_PIPELINE_FRESHNESS` to see which layer is behind. |
| SF-008 | WARN | The source's reported stock level doesn't move by exactly the movement delta | This detects source inconsistencies (lost movements, or a source reset). After a simulator restart it is expected, because stock levels restart. |

## DLQ triage and redrive

```bash
make dlq t=pos.transactions                     # summary by rule id and stage, sample records
uv run retail-dlq redrive --topic pos.transactions --error-code POS-006 --dry-run
uv run retail-dlq redrive --topic pos.transactions --error-code POS-006
```

1. **Group by rule id** (`by_error_code`). One dominant code usually means one root cause.
2. **Decide whose bug it is.**
   - *Producer bug* (the data really is wrong, e.g. a total mismatch): fix the source. Redrive won't help; the record would be rejected again.
   - *Our bug* (a rule or parser rejected valid data): fix and deploy the consumer, **then** redrive. Redriven records go to `<topic>.retry`, through the same validation, and `event_id` deduplication makes redriving an already-loaded record harmless.
3. The loop guard refuses records redriven 3 times unless you pass `--force`.

SQL view of rejects (after loading):

```sql
SELECT ERROR_CODE, ERROR_STAGE, COUNT(*) AS n, MIN(FAILED_AT), MAX(FAILED_AT)
FROM RAW.DEAD_LETTERS GROUP BY 1, 2 ORDER BY n DESC;
```

## Streams going stale

A stream becomes *stale* if it isn't consumed within the table's data retention plus `MAX_DATA_EXTENSION_TIME_IN_DAYS` (14 days by default). That happens if tasks stay suspended for weeks while data keeps loading. Recovery: recreate the stale stream (`CREATE OR REPLACE STREAM ... AT (TIMESTAMP => ...)`, or `SHOW_INITIAL_ROWS = TRUE` for a full reload); STAGING's MERGEs are idempotent. Avoid it by suspending the **loader** as well when parking the project.
