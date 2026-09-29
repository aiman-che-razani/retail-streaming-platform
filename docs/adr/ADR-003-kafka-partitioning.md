# ADR-003: Kafka partitioning and keying strategy

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, kafka-engineer

## Context

The record key determines the partition, and so the **ordering scope** and **load distribution**. The partition count determines maximum consumer parallelism and is costly to change later, because adding partitions remaps keys and breaks per-key ordering for in-flight data. Different event types have different ordering needs:

- POS sales feed commutative aggregations; no consumer needs global ordering.
- Stock movements are a sequence per SKU-location (`quantity_on_hand_after` must progress in order).
- Customer and product changes are versioned entities; applying versions out of order corrupts the current state.

## Decision

| Topic | Key | Partitions |
|---|---|---|
| `pos.transactions` | `store_id` | 6 |
| `inventory.events` | `store_id:product_id` | 6 |
| `customer.events` | `customer_id` | 3 |
| `product.updates` | `product_id` | 3 |
| `*.retry` | same as primary | 3 (POS, inventory) / 1 |
| `*.dlq` | original key | 3 (POS, inventory) / 1 |

- Keys are never null on these topics. A null key would round-robin and destroy per-entity ordering.
- All Python producers use `partitioner=murmur2_random`, so the key → partition mapping matches Java clients.
- Partition counts are sized for peak parallelism with headroom, and are fixed. If the count ever has to grow, create a new topic and migrate; don't add partitions in place.

## Alternatives

| Option | Why not chosen |
|---|---|
| POS keyed by `transaction_id` | Best distribution (high cardinality) but no useful ordering scope. Store-level ordering is free with `store_id` and useful to per-store consumers. Kept as the documented mitigation if store skew becomes a problem. |
| POS keyed by `store_id:register_id` | Finer ordering than we need; spreads a store's traffic across partitions for no consumer benefit. |
| Null keys (round-robin / sticky) | Best throughput, but no ordering at all. Unacceptable for inventory and entity topics. |
| Many partitions (e.g. 24+) "for future scale" | Each partition costs file handles, memory, replication traffic and longer rebalances/elections. Spark would schedule many tiny tasks locally. |
| One topic for all event types | Couples retention, scaling and schemas across domains; consumers filter what they don't need; one bad producer affects everyone. |

## Consequences

**Positive**
- Ordering guarantees match domain needs exactly, and are documented per topic.
- Spark can process up to 6 partitions of the busiest topics in parallel.

**Negative / accepted trade-offs**
- 30 store keys over 6 partitions gives uneven partitions (hash collisions, flagship stores). We monitor per-partition rates. The simulator's store weights make this skew visible on purpose.
- Ordering holds only *within* a topic. A sale and its inventory depletion are on different topics and can be processed in either order. Downstream logic never assumes cross-topic ordering; this is why the model uses inferred members (ADR-006).
- Retry topics have fewer partitions than primaries, so a key's partition differs between the primary and the retry topic. Acceptable: redriven records are exceptions and are deduplicated by `event_id`.
