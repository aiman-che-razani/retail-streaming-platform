# Concept Guide

Every concept below is explained in the same five parts: **What** it is, **Why** it matters, **How** it works, **What can go wrong**, and **How production systems handle it**. Each section ends with where to see it in this repository. It's written for someone learning data engineering by reading this code.

---

## Kafka

### Partitions
- **What:** a topic is split into partitions, each an ordered, append-only log.
- **Why:** partitions are Kafka's unit of parallelism (one consumer per partition within a group) and of ordering (only within a partition).
- **How:** the producer picks a partition by hashing the record key; brokers store and replicate each partition independently.
- **What can go wrong:** too few partitions caps throughput; too many costs memory and makes rebalances slower. *Adding* partitions later changes `hash(key) mod N`, so keys move and per-key ordering breaks for in-flight data.
- **Production:** size for peak parallelism up front, measure per-partition throughput, and migrate to a new topic rather than resizing.
- **Here:** 6 partitions for high-volume topics, 3 or 1 elsewhere ([topics.yaml](../../kafka/config/topics.yaml), [ADR-003](../adr/ADR-003-kafka-partitioning.md)). The provisioner *refuses* partition changes.

### Message keys
- **What:** the key is an optional byte string that decides the partition.
- **Why:** records with the same key are always ordered relative to each other. Choosing the key means choosing *what must stay in order*.
- **How:** `murmur2(key) mod partitions`.
- **What can go wrong:** low-cardinality keys cause hot partitions (our 30 stores produced partitions ranging from 31 to 165 records). Null keys lose ordering. The Python client (librdkafka) and the Java client hash keys *differently* by default.
- **Production:** key by the entity whose sequence matters; watch partition skew; set a Java-compatible partitioner everywhere.
- **Here:** `store_id` for POS, `store_id:product_id` for inventory (evenly spread), `partitioner=murmur2_random` in [producer.py](../../src/retail_platform/messaging/producer.py).

### Consumer groups
- **What:** consumers sharing a `group.id` divide a topic's partitions among themselves.
- **Why:** scaling out, plus automatic failover when a member dies.
- **How:** a group coordinator assigns partitions. A member joining or leaving triggers a rebalance.
- **What can go wrong:** frequent rebalances pause consumption; one slow consumer lags its partitions.
- **Production:** cooperative rebalancing, static membership, and lag alerting per group.
- **Here:** the DLQ redrive tool is a classic group consumer. **Spark is not.** It assigns partitions itself and tracks offsets in its checkpoint, so consumer-group lag tools show nothing for it ([ADR-009](../adr/ADR-009-observability.md)).

### Offsets
- **What:** an offset is a record's position in its partition. A consumer's committed offset says "processed up to here".
- **Why:** it enables restart without loss, and **replay**: rewinding lets you reprocess data.
- **How:** Kafka keeps data for `retention.ms` regardless of consumption; consumers just move their pointer.
- **What can go wrong:** commit before processing and a crash loses data; commit after and a crash reprocesses data (duplicates). If you fall further behind than the retention period, the data is gone.
- **Production:** commit only after effects are durable, make processing idempotent, and alert on lag well before retention runs out.
- **Here:** the redrive tool commits only after the republished records are acknowledged. Spark fails loudly (`failOnDataLoss=true`) if retention deleted unprocessed data (FM-19).

### Delivery semantics
- **What:** at-most-once (may lose), at-least-once (may duplicate), exactly-once (neither).
- **Why:** revenue must be neither lost nor double-counted.
- **How:** exactly-once is achieved as **at-least-once delivery plus idempotent (or transactional) processing at every hop**.
- **What can go wrong:** one non-idempotent sink breaks the chain, for example `revenue = revenue + x`.
- **Production:** idempotent producers, idempotent or transactional sinks, deterministic keys.
- **Here:** the per-hop table in [ADR-007](../adr/ADR-007-delivery-and-idempotency.md). The SIGKILL drill produced zero duplicate offsets.

### Schema evolution
- **What:** changing an event's structure while producers and consumers are deployed independently.
- **Why:** you can never upgrade every service at the same instant.
- **How:** Schema Registry stores versions per subject and rejects new versions that break the compatibility level. Consumers ignore unknown fields.
- **What can go wrong:** removing, renaming or retyping a field breaks old data or old consumers. Adding an enum value surprises old consumers.
- **Production:** compatibility gates in CI; breaking changes go to a new topic (`.v2`) with a migration window.
- **Here:** `BACKWARD_TRANSITIVE` with closed schemas ([event-contracts.md §6](../architecture/event-contracts.md#6-compatibility-and-evolution)). The rules are verified against the real registry in integration tests, CI registers the base branch's schemas and checks the PR's schemas against them, and unknown enum values are WARN rather than REJECT.

---

## Spark

### Lazy evaluation
- **What:** transformations only build a plan; nothing runs until an action or a streaming `start()`.
- **Why:** the Catalyst optimizer sees the whole pipeline and prunes columns, pushes filters down and reorders operations.
- **How:** DataFrame operations build a logical plan → optimized plan → physical plan.
- **What can go wrong:** Python UDFs are opaque to the optimizer and ship every row to Python and back. Calling `collect()` pulls a whole dataset into the driver's memory. Reusing a DataFrame in two actions recomputes it.
- **Production:** native functions only, `persist()` when reusing, and never `collect()` unbounded data.
- **Here:** every validation rule is a native expression ([validation.py](../../src/retail_platform/processing/transformations/validation.py)). The batch is `persist()`ed in `foreachBatch` because it feeds four outputs.

### Structured Streaming and checkpoints
- **What:** a stream is treated as an unbounded table and processed in micro-batches.
- **Why:** it's the same API as batch, with fault tolerance built in.
- **How:** each trigger (1) records the planned offset range in the **offset log**, (2) runs the batch, (3) writes the **commit log**. On restart, any planned-but-uncommitted batch re-runs over identical offsets. The checkpoint also holds the state stores and the query id.
- **What can go wrong:** a sink that isn't idempotent (replays duplicate data); a lost checkpoint (reprocessing from `startingOffsets`); an incompatible change to the query plan when resuming from an old checkpoint.
- **Production:** checkpoints on durable object storage, idempotent sinks, and treating a checkpoint reset as a migration.
- **Here:** landing paths are keyed by (checkpoint query id, batch id), and a `_SUCCESS` marker is written last ([sinks.py](../../src/retail_platform/processing/sinks.py)).

### Watermarks, late events and stateful processing
- **What:** a watermark is `max(event time seen) − allowed lateness`.
- **Why:** windows and deduplication need *state*, and state has to be forgotten eventually or memory grows forever.
- **How:** once the watermark passes a window's end, Spark finalizes the window and evicts its state. Later records for that window are **dropped** and counted in `numRowsDroppedByWatermark`.
- **What can go wrong:** too short a watermark drops legitimate late data; too long a watermark means large state and delayed results. Putting a watermark on a path that must be complete silently loses data.
- **Production:** choose lateness from measured data; keep a complete, stateless path to the system of record; alert on drops.
- **Here:** the **ingest** app has *no* watermark, so every valid sale reaches Snowflake. The **realtime** app uses a 10-minute watermark and `dropDuplicatesWithinWatermark`. The e2e test proves both behaviours ([ADR-004](../adr/ADR-004-spark-structured-streaming.md)).

---

## Snowflake

### Warehouses (compute)
- **What:** virtual compute clusters, separate from storage.
- **Why:** you pay per second only while one is running (with a 60-second minimum per resume); each size step doubles the cost.
- **How:** `AUTO_RESUME` starts a warehouse on the first query; `AUTO_SUSPEND` stops it after a period of idleness.
- **What can go wrong:** a trickle of queries every minute keeps a warehouse up forever; one analyst query can starve the pipeline.
- **Production:** separate warehouses per workload, resource monitors, query tags for cost attribution.
- **Here:** two XSMALL warehouses with `AUTO_SUSPEND = 60`, a monthly resource monitor, tasks that skip when no stream has data, and a loader that never connects when idle ([ADR-005](../adr/ADR-005-snowflake-architecture.md)).

### Micro-partitions
- **What:** tables are stored as immutable, compressed, columnar micro-partitions with min/max metadata per column.
- **Why:** **pruning**: a query with `WHERE date_key BETWEEN …` reads only the micro-partitions whose ranges overlap.
- **How:** data arrives in load order; if load order correlates with your filters, the table is "naturally clustered".
- **What can go wrong:** filtering on a column scattered across every micro-partition means a full scan.
- **Production:** clustering keys only on very large tables whose load order doesn't match the filter columns (reclustering costs credits).
- **Here:** facts load roughly in event-time order, so no clustering keys are used ([data-model.md §7](../architecture/data-model.md)).

### Streams and Tasks
- **What:** a stream is an offset into a table's change history; a task is scheduled SQL.
- **Why:** incremental processing without hand-written "last loaded" bookkeeping.
- **How:** reading a stream inside a DML transaction returns only new rows; `COMMIT` advances the offset and `ROLLBACK` doesn't.
- **What can go wrong:** consuming a stream in two separate transactions (the offset advances after the first, so the second misses rows); streams going stale when unused beyond retention.
- **Here:** every multi-statement consumer runs in one explicit transaction inside a stored procedure ([R__200](../../snowflake/transformations/R__200_staging_procedures.sql)).

---

## Modelling and operations

### Dimensional modelling
- **What:** a star schema, with *facts* (measurements at a declared **grain**) surrounded by *dimensions* (descriptive context).
- **Why:** simple, fast and understandable queries.
- **How:** facts hold foreign keys and additive measures; dimensions hold attributes.
- **What can go wrong:** an undeclared or mixed grain means double counting. Summing semi-additive measures (stock on hand) across time is meaningless.
- **Here:** `FACT_SALES` (line grain), `FACT_INVENTORY` (movement grain), `FACT_INVENTORY_SNAPSHOT` (store × product × day), with the grain stated in table comments. The turnover query shows correct semi-additive aggregation ([06_inventory.sql](../../snowflake/analytics/06_inventory.sql)).

### Slowly Changing Dimensions
- **What:** a way to handle attribute changes. **Type 1** overwrites (no history); **Type 2** adds a new *version* row with effective dates.
- **Why:** "revenue by category" should use the category *when the sale happened*, while consent flags must always be current.
- **How:** each version has `effective_from`/`effective_to`, and facts join point-in-time.
- **What can go wrong:** out-of-order changes corrupt the version chain; sequence-generated keys change on every rebuild; facts arrive before their dimension row exists.
- **Production:** Type 2 only where history matters, deterministic keys, inferred members.
- **Here:** hybrid SCD2/SCD1 per attribute. Keys are `MD5_NUMBER_LOWER64(natural_key | effective_from)`. The affected keys' version chains are rebuilt so out-of-order changes are handled. Inferred members share the first version's key, so they fill in place when the real record arrives ([ADR-006](../adr/ADR-006-dimensional-modelling.md), [R__300](../../snowflake/transformations/R__300_dimension_procedures.sql)).

### Idempotency
- **What:** doing an operation twice has the same effect as doing it once.
- **Why:** it's the only practical way to exactly-once *effects* in a system where anything can be retried.
- **How:** deterministic identifiers plus MERGE/upsert with absolute values, or skip-if-done markers.
- **Here:** Spark `_SUCCESS` per (query, batch); COPY load metadata; `MERGE` on `event_id` and business keys; hash surrogate keys; PostgreSQL upserts of absolute totals.

### Data quality
- **What:** checks that data is complete, valid, unique, consistent and fresh.
- **Why:** a pipeline that runs happily on bad data is worse than one that stops.
- **How:** check at the cheapest layer that can see the problem: the producer (schema), the stream processor (record rules), the warehouse (set-level rules and reconciliation).
- **What can go wrong:** checks that only log, checks nobody reads, and "unexplained" differences that get ignored.
- **Here:** 22 record-level rules with stable IDs, a DLQ with the reason on every record, SF-001…SF-008 recorded in `OPS.DQ_RESULTS`, and `retail-reconcile`, which explains every gap between layers ([data-quality runbook](../runbooks/data-quality.md)).

### Observability
- **What:** being able to answer "is it flowing, fresh, correct, and where is it stuck?" without adding new code.
- **How:** structured logs with correlation IDs, metrics with low-cardinality labels, dashboards per question, alerts linked to runbooks.
- **What can go wrong:** IDs used as metric labels (cardinality explosion), alerts nobody can act on, idle systems that look dead (we fixed exactly this for low-volume Spark queries).
- **Here:** [ADR-009](../adr/ADR-009-observability.md), the Grafana dashboards, and 12 alert rules.
