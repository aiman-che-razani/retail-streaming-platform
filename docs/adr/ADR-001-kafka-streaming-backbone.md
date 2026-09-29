# ADR-001: Kafka as the streaming backbone

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect

## Context

Thirty stores produce a continuous stream of sales, stock movements and master-data changes. Several consumers need the same events for different purposes: warehouse ingestion, real-time aggregation, DLQ redrive, and future consumers such as fraud detection or replenishment. Requirements:

- durable buffering, so producers are not coupled to consumer availability (a Snowflake or Spark outage must not stop tills);
- **replay**, to reprocess after a bug fix or bootstrap a new consumer;
- per-entity ordering (a product's versions, a SKU-location's stock movements);
- horizontal scalability and a large ecosystem (Spark connector, schema registry, monitoring);
- runs locally in Docker at zero cost.

## Decision

Use **Apache Kafka 4.3 in KRaft mode** (no ZooKeeper) as the event backbone. Locally this is a single broker (`apache/kafka:4.3.1`) with combined broker and controller roles. Topics are declared in `kafka/config/topics.yaml` and created by a bootstrap job; broker auto-creation is disabled.

## Alternatives

| Option | Why not chosen |
|---|---|
| RabbitMQ / traditional queue | Messages are deleted on consumption: no replay, no independent consumers at different positions. Ordering and partitioned scaling are weaker. |
| Direct producer → Snowflake (Snowpipe Streaming) | Couples tills to warehouse availability. No place for streaming validation or real-time aggregation. One consumer only. |
| Redpanda | Kafka-API compatible and lighter. A reasonable choice, but Apache Kafka is the industry reference and the learning goal is Kafka itself. The code would work unchanged against Redpanda. |
| Managed cloud streaming (Kinesis, Pub/Sub, Confluent Cloud) | Costs money and needs cloud accounts; the project must run locally for free. Documented as the production option. |
| Multi-broker local cluster (3 brokers) | Demonstrates replication for real, but costs ~2 GB more RAM next to Spark on a 16 GB laptop. Replication settings are declared for production in `topics.yaml` and explained instead. |

## Consequences

**Positive**
- Producers and consumers are decoupled in time and in failure.
- Any consumer can replay within the retention window; new consumers can start from `earliest`.
- Per-key ordering is available where the domain needs it.

**Negative / accepted trade-offs**
- A single local broker has no fault tolerance, so `acks=all` is only meaningful in production. The configuration is still set correctly so behaviour doesn't change on promotion.
- Kafka adds operational weight (monitoring lag, retention, partition planning) that a simple queue wouldn't.
- Kafka retention (7–30 days) is *not* the long-term archive; Snowflake RAW is (see ADR-005).

**Follow-ups**
- Production: 3+ brokers across availability zones, `min.insync.replicas=2`, SASL_SSL + ACLs per topic owner, tiered storage for longer retention.
