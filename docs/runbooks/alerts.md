# Runbook: Alerts

Alert rules live in [`monitoring/prometheus/rules/alerts.yml`](../../monitoring/prometheus/rules/alerts.yml). Open Prometheus at <http://localhost:9090/alerts> and Grafana at <http://localhost:3030> (all ports are bound to 127.0.0.1).

Useful commands: `make ps`, `make logs s=<service>`, `make dlq t=<topic>`, `make reconcile`.

## RetailProducerDeliveryFailures
**Meaning:** records were enqueued but never acknowledged within `delivery.timeout.ms` (120 s). **Those records are lost at the source.**
**Check:** is the `kafka` container healthy? `make logs s=simulator | grep delivery_failed`.
**Fix:** restore the broker. The simulator resumes automatically; a real POS would resend the same `event_id`s from local storage.

## RetailProducerValidationFailures
**Meaning:** the producer refused events that violate the registered schema (nothing bad was sent).
**Check:** simulator logs for `event_rejected_by_schema`. Is the registered schema version the one the code expects? `make schemas-check`.

## RetailSparkQueryFailed
**Meaning:** a streaming query died. The app exits and Docker restarts it; it resumes from the checkpoint.
**Check:** `make logs s=spark-ingest | grep query_failed`. The same error after every restart is a crash loop (FM-22): a code bug, fix and redeploy. No data is lost; Kafka keeps it within retention.

## RetailSparkNoProgress
**Meaning:** a query has emitted neither a progress nor an idle event for 3 minutes. The JVM is stuck, or the container is down.
**Check:** Spark UI (<http://localhost:4040> ingest, <http://localhost:4041> realtime), then `docker compose restart spark-ingest`.

## RetailSparkConsumerLagHigh
**Meaning:** Spark is > 20k offsets behind the end of the topic.
**Check:** is it catching up after downtime (lag falling)? `maxOffsetsPerTrigger` bounds batch size, so catch-up takes several batches. If lag keeps rising, input exceeds throughput: lower `SIMULATOR_TRANSACTIONS_PER_SECOND` or give Spark more cores/memory.

## RetailRejectRateHigh
**Meaning:** more than 5% of records are going to the DLQ.
**Check:** `make dlq t=pos.transactions` and group by `error_code`. Follow [data-quality.md → DLQ triage](data-quality.md#dlq-triage-and-redrive).

## RetailLateEventsDropped
**Meaning:** events arrived more than 10 minutes behind the realtime watermark, so realtime dashboards undercount.
**Check:** is a till or source replaying old data (backfill, redrive, offline store)? This is expected during `make backfill`. The warehouse is unaffected.

## RetailLoaderStale
**Meaning:** no successful Snowflake load for 45 minutes.
**Check:** `make logs s=loader`. Look for `snowflake_unavailable` (outage or network: it retries with backoff), a configuration error, or the resource monitor suspending the warehouse (see [snowflake-setup.md](snowflake-setup.md#troubleshooting)).

## RetailLandingBacklogGrowing
**Meaning:** completed batches are piling up in `data/landing`.
**Fix:** same causes as RetailLoaderStale. Once Snowflake is back, `make load` drains the backlog. Loads are idempotent.

## RetailLoaderCrashLooping
**Meaning:** the loader container keeps restarting, usually a configuration error (credentials, key path). Idle-but-healthy loaders do NOT alert.
**Check:** `make logs s=loader | grep fatal_error` - the message names the missing/invalid setting.

## RetailPipelineComponentDown
**Meaning:** Prometheus can't scrape a Spark app.
**Check:** `make ps`. A container restarting repeatedly points to RetailSparkQueryFailed.

## RetailDlqVolumeHigh
**Meaning:** a DLQ is receiving more than 30 records/min (measured from Kafka offsets by kafka-exporter).
**Check:** as for RetailRejectRateHigh. With `make simulate-faults` this is expected.
