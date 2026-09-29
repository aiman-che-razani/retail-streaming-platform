# Kafka Topology and Configuration

> Status: **Accepted** (Phase 1). Machine-readable source of truth: [`kafka/config/topics.yaml`](../../kafka/config/topics.yaml).
> Decisions: [ADR-001](../adr/ADR-001-kafka-streaming-backbone.md), [ADR-003](../adr/ADR-003-kafka-partitioning.md), [ADR-007](../adr/ADR-007-delivery-and-idempotency.md), [ADR-008](../adr/ADR-008-dead-letter-and-retry.md).

## 1. Concepts in one page

- **Topic.** A named, append-only log of records, split into **partitions**.
- **Partition.** An ordered, immutable sequence. Each record gets a monotonically increasing **offset** within its partition. Ordering is guaranteed only *within* a partition, never across partitions. Partitions are also the unit of parallelism: a consumer group can have at most one active consumer per partition, and Spark creates one task per partition per micro-batch by default.
- **Key.** Decides the partition: `partition = murmur2(key) mod partition_count`. Records with the same key always go to the same partition, so they are ordered relative to each other. The key therefore defines the **ordering scope** and the **distribution**.
- **Replication.** Each partition has a leader and followers on other brokers. `acks=all` means the leader acknowledges only after all *in-sync* replicas have the record. `min.insync.replicas` sets how many must be in sync for a write to be accepted. Together they decide whether an acknowledged write can be lost when a broker dies.
- **Consumer group.** Consumers sharing a `group.id` split a topic's partitions among themselves and commit offsets to Kafka (`__consumer_offsets`) so a restart resumes where it left off. Different groups read independently, so the same topic can feed many applications.
- **Offsets and replay.** Because the log is retained (by time or size) and consumers only move a pointer, any consumer can rewind and **replay**. This is Kafka's main difference from a queue that deletes messages on consumption.
- **Retention.** Records are deleted after `retention.ms`, whether or not anyone read them. A consumer that falls behind by more than the retention window loses data. Spark detects this (`failOnDataLoss=true`) and fails loudly instead of skipping silently.

## 2. Topics

| Topic | Role | Partitions | Key | Retention | Ordering guarantee we rely on |
|---|---|---|---|---|---|
| `pos.transactions` | primary | 6 | `store_id` | 7 d | Per-store sequence of sales |
| `inventory.events` | primary | 6 | `store_id:product_id` | 7 d | Per SKU-location movement sequence (`quantity_on_hand_after` progression) |
| `customer.events` | primary | 3 | `customer_id` | 30 d | Per-customer profile versions in order |
| `product.updates` | primary | 3 | `product_id` | 30 d | Per-product versions in order (SCD2) |
| `<primary>.retry` | redrive | 3 / 1 | same as primary | 3 d | Same key keeps redriven records on a consistent partition |
| `<primary>.dlq` | dead letter | 3 / 1 | same as primary | 14 d | None needed |

Retry topics do **not** register their own Schema Registry subject. The redrive tool republishes the original bytes, whose embedded schema id already points at the primary subject.

### Why these partition counts

Partition count is hard to change later: adding partitions changes `hash(key) mod N`, which breaks per-key ordering for in-flight keys. We size for peak consumer parallelism with headroom, not for today's load.

- **6 for POS and inventory.** These are the high-volume topics. 6 partitions allow up to 6 parallel Spark tasks, which comfortably covers the N1 throughput target locally. In production we would size from measured per-partition throughput (typically 5–10 MB/s per partition).
- **3 for customer and product.** Low volume (hundreds per hour). More partitions would only add overhead: file handles, replication traffic, rebalancing time.
- **Retry and DLQ: 1–3.** Low volume by design. Fewer partitions keep them cheap.

### Why these keys (see ADR-003)

- `pos.transactions` → `store_id`. Sales aggregations are commutative, so no consumer *needs* cross-transaction ordering. Keying by store still gives per-store ordering for free, which matters for anything sequential per till/store (e.g., till reconciliation, fraud rules), and it keeps a store's traffic together.
  **Trade-off:** 30 keys over 6 partitions is low cardinality, so hash collisions and flagship stores can skew partitions. We accept this at this scale, measure it (per-partition offset rates in Grafana), and document the mitigation: re-key by `transaction_id` if a single partition becomes hot.
- `inventory.events` → `store_id:product_id`. Ordering matters here because stock levels are a sequence. High cardinality (30 × 500) also gives an even spread.
- `customer.events` → `customer_id` and `product.updates` → `product_id`. Entity changes must apply in order, or the dimension ends up with the wrong "current" version.

**Cross-language gotcha.** Java clients hash keys with murmur2. librdkafka (and so `confluent-kafka` Python) defaults to `consistent_random` (CRC32). If a Python and a Java producer write the same key with defaults, the key lands on **different partitions** and per-key ordering silently breaks. We set `partitioner=murmur2_random` in all Python producers.

## 3. Producer configuration (simulator)

| Setting | Value | Why | What goes wrong otherwise |
|---|---|---|---|
| `acks` | `all` | The write is acknowledged only after all in-sync replicas have it | `acks=1`: the leader acks, dies before replication, and the "acknowledged" record is gone |
| `enable.idempotence` | `true` | The broker de-duplicates producer retries using (producer id, sequence number), giving exactly-once *per partition per producer session* | A network timeout after a successful write makes the retry create a duplicate |
| `max.in.flight.requests.per.connection` | `5` | The maximum allowed with idempotence; ordering is still preserved | With idempotence off and > 1 in flight, retries can reorder records |
| `retries` | default (∞) bounded by `delivery.timeout.ms` | Let the client ride out leader elections and brief outages | — |
| `delivery.timeout.ms` | `120000` | Upper bound for "sent or failed". After it, the delivery callback reports failure and we count/log it | Infinite blocking, or silent drop if callbacks are ignored |
| `linger.ms` | `20` | Wait up to 20 ms to fill batches, giving better throughput and compression at a negligible latency cost | `0`: many tiny requests |
| `batch.size` | `65536` | 64 KiB batches | — |
| `compression.type` | `zstd` | Best ratio for JSON; decompressed by the consumer | JSON is verbose; uncompressed uses 3–5× the network and disk |
| `partitioner` | `murmur2_random` | Java-compatible key hashing | See gotcha above |
| `client.id` | `retail-simulator-<hostname>` | Identifies the client in broker logs and metrics | — |

Delivery callbacks are mandatory. `produce()` is asynchronous and only *enqueues*. Success or failure is known only in the callback, which updates `retail_producer_messages_total{status}` and logs failures with `event_id`. `flush()` runs on shutdown so buffered records are not lost.

We do **not** use Kafka transactions. Each simulator event is a single write to a single topic, so there is no multi-write atomicity to protect. Idempotence already removes retry duplicates, and application-level duplicates are handled downstream by `event_id` (ADR-007).

## 4. Broker and topic configuration (local)

| Setting | Value | Why |
|---|---|---|
| `auto.create.topics.enable` | `false` | A typo in a topic name must fail, not silently create a 1-partition topic with default retention |
| `default.replication.factor`, `min.insync.replicas` | 1 / 1 locally | Single broker. Production: 3 / 2 (see `topics.yaml` profiles) |
| `cleanup.policy` | `delete` everywhere | Compaction would keep only the latest record per key and destroy the change history SCD2 needs |
| `message.timestamp.type` | `CreateTime` | The record timestamp is set by the producer (ingestion time). Event time is in the envelope |
| `num.partitions` | irrelevant | Every topic is created explicitly |
| `group.initial.rebalance.delay.ms` | `0` locally | Faster test startup |

## 5. Consumption by Spark

Spark's Kafka source is **not** a classic consumer-group consumer:

- It assigns partitions itself and tracks offsets in its **checkpoint** (`offsets/` and `commits/` directories), not in `__consumer_offsets`.
- Each micro-batch first writes the offset range it *will* process to the offset log (write-ahead). After the sink succeeds, it writes the commit log. On restart, an uncommitted batch is re-run with *exactly the same offset range*. This is what makes idempotent sinks sufficient for exactly-once *effects*.
- Consequently, `kafka-consumer-groups.sh` and kafka-exporter show **no lag** for Spark. We compute lag from Spark's own progress metrics (`avgOffsetsBehindLatest`, `maxOffsetsBehindLatest` on the Kafka source) and export them (ADR-009).

| Source option | Value | Why |
|---|---|---|
| `subscribe` | `<primary>,<primary>.retry` | Redriven records go through the identical code path |
| `startingOffsets` | `earliest` | Only used when there is no checkpoint (first start); after that the checkpoint wins |
| `failOnDataLoss` | `true` | If retention deleted records we have not processed (or a topic was recreated), stop and page someone instead of skipping data |
| `maxOffsetsPerTrigger` | `50000` (ingest), `20000` (realtime) | Back-pressure: bounds batch size after downtime so catch-up doesn't blow memory or produce giant files |
| `kafka.isolation.level` | `read_committed` | No-op today (no transactional producers), and correct if transactional producers are ever added |
| `includeHeaders` | `true` | Headers are copied into DLQ records and used for diagnostics |

The DLQ redrive CLI *is* a classic consumer (`group.id = dlq-redrive`), with `enable.auto.commit=false`. It commits an offset only after the record has been republished and acknowledged, which gives at-least-once.

## 6. Delivery semantics end to end

| Hop | Guarantee | Mechanism |
|---|---|---|
| Simulator → Kafka | exactly-once per partition per producer session; at-least-once across restarts | idempotent producer; app resends keep the same `event_id` |
| Kafka → Spark ingest → landing | exactly-once per batch | offset write-ahead log + idempotent `foreachBatch` keyed by (`query_id`, `batch_id`) |
| Spark → DLQ topic | at-least-once | Kafka sink; duplicates identifiable by `dlq.source.topic/partition/offset` |
| Landing → Snowflake RAW | effectively-once | COPY load metadata skips already-loaded files |
| RAW → STAGING → facts | exactly-once *effect* | `MERGE` on `event_id` and business keys; deterministic surrogate keys |
| Kafka → Spark realtime → PostgreSQL | exactly-once *effect* for accepted events; late events beyond the watermark are dropped (counted) | state store + upsert on (key, window_start) |

"Exactly-once" is never a property of one component. It comes from a replayable source, deterministic reprocessing and idempotent sinks together.

## 7. Graceful shutdown

- **Simulator:** traps SIGINT/SIGTERM, stops generating, `flush(timeout)`, then logs how many records were still unacknowledged (non-zero means possible loss, and is reported).
- **Spark:** SIGTERM → `query.stop()` for each query. An in-flight batch is either committed or re-run on restart; there are no partial effects because sinks are idempotent.
- **Redrive CLI:** finishes the in-flight record, commits, closes the consumer (triggering a clean group rebalance rather than a session timeout).
