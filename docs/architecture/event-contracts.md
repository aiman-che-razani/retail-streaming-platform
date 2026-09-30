# Event Contracts

> Status: **Accepted** (Phase 1). Machine-readable source of truth: [`kafka/schemas/*.schema.json`](../../kafka/schemas/). This document explains them. If they ever disagree, the schema file wins and this document is a bug.
> Decision record: [ADR-002](../adr/ADR-002-event-serialization.md).

## 1. Principles

1. **Envelope + payload.** Every event is `{ "metadata": {...}, "payload": {...} }`. Metadata is generic, identical for all topics, and consumed by infrastructure (dedupe, tracing, routing). The payload is business data owned by the source domain.
2. **Full state, not deltas, for entities.** Customer and product events carry the full current entity state (event-carried state transfer). A consumer never needs earlier events to interpret one, so SCD2 processing and replays are simple and idempotent.
3. **One schema per topic.** Subject `<topic>-value` (TopicNameStrategy). A topic can carry several event types (for example `customer.registered` and `customer.profile_updated`) if they share one payload shape.
4. **UTC on the wire.** Timestamps are RFC 3339 with a `Z` suffix. The schema regex enforces the `Z`. Local business dates are derived later from the store's timezone.
5. **Money as decimal.** Amounts are JSON numbers with at most 2 fractional digits. Every consumer parses them as a fixed-point decimal (`DECIMAL(12,2)`), never binary float. Currency is explicit.
6. **Validated twice.** Producers validate against the registered schema before sending. Spark validates again, because a contract only holds if someone checks it at the receiving end.

## 2. Wire format

```
byte 0      : 0x00                  magic byte (Confluent wire format)
bytes 1..4  : int32 big-endian      Schema Registry schema id
bytes 5..   : UTF-8 JSON            the envelope
```

Produced by `confluent_kafka.schema_registry.json_schema.JSONSerializer`. Spark decodes it with native functions only: it checks the magic byte, extracts the schema id with `conv(hex(substring(value, 2, 4)), 16, 10)`, and parses `substring(value, 6, ...)` with `from_json`. A record whose first byte is not `0x00` is rejected with `WIRE_FORMAT_INVALID`.

**Kafka record key:** UTF-8 string, per topic (see [kafka-topology.md](kafka-topology.md#topics)).

**Kafka headers** (convenience copies for tooling; the envelope is authoritative):

| Header | Example |
|---|---|
| `event_type` | `pos.transaction.completed` |
| `schema_version` | `1.0` |
| `correlation_id` | `7f5e…` |

DLQ records add `dlq.*` headers (see [ADR-008](../adr/ADR-008-dead-letter-and-retry.md)).

## 3. Envelope metadata

Identical in every schema (a contract test asserts the four `definitions.metadata` blocks are equal).

| Field | Type | Required | Rules | Meaning |
|---|---|---|---|---|
| `event_id` | string (UUID, lower-case) | yes | UUID pattern | Globally unique ID of *this event*. The deduplication key across the whole platform. A producer retry re-sends the same `event_id`. |
| `event_type` | string | yes | per-topic enum | What happened, `<domain>.<entity>.<verb_past_tense>` style. |
| `schema_version` | string `MAJOR.MINOR` | yes | `^1\.[0-9]+$` in v1 schemas | Contract version the producer used. The major version is pinned by the schema. |
| `event_timestamp` | string (UTC) | yes | `…Z` | **Event time**: when the business fact happened. |
| `produced_at` | string (UTC) | yes | `…Z` | When the producer serialized the event. `produced_at - event_timestamp` reveals source-side delay (for example an offline till). |
| `producer` | string | yes | `name/semver` | Emitting application and version, e.g. `retail-simulator/0.1.0`. |
| `correlation_id` | string (UUID) | yes | UUID pattern | Groups events of one business flow. A sale and the inventory depletion it causes share it. |
| `causation_id` | string (UUID) or null | no | UUID pattern | `event_id` of the event that directly caused this one (inventory `SALE` → the POS event). |

## 4. Event catalog

| Topic | Schema file | Event types | Key | Payload semantics |
|---|---|---|---|---|
| `pos.transactions` | `pos-transactions.schema.json` | `pos.transaction.completed` | `store_id` | One completed checkout (basket) with 1..200 line items |
| `inventory.events` | `inventory-events.schema.json` | `inventory.movement.recorded` | `store_id:product_id` | One stock movement for one SKU at one store |
| `customer.events` | `customer-events.schema.json` | `customer.registered`, `customer.profile_updated` | `customer_id` | Full current customer profile (no direct PII) |
| `product.updates` | `product-updates.schema.json` | `product.created`, `product.updated` | `product_id` | Full current product master record |

Store master data is small, static reference data. It is shipped as a versioned CSV (`src/retail_platform/simulator/reference/stores.csv`) and loaded into `RAW.STORE_REFERENCE` by the loader, not streamed ([data-model.md](data-model.md#dim_store)).

### Why a POS event is a basket, not a line

A real POS emits one event when the checkout completes. A basket is atomic: the lines are paid together, with one payment method and one total. If each line were a separate event, a consumer could never know when a basket was complete. Average basket value and items-per-basket would then need guesswork, and a partially delivered basket would look like a smaller sale. Spark and Snowflake **explode** the basket into lines. `FACT_SALES` is at line grain; basket metrics use `COUNT(DISTINCT transaction_id)`.

### 4.1 `pos.transaction.completed`

| Field | Type | Required | Constraints |
|---|---|---|---|
| `transaction_id` | string | yes | `^TXN-[A-Z0-9-]{4,40}$` — business key of the sale |
| `store_id` | string | yes | `^[A-Z]{2,6}-[0-9]{2}$` (e.g. `KLCC-01`) |
| `register_id` | string | yes | `^REG-[0-9]{2}$` |
| `customer_id` | string or null | yes (nullable) | `^CUST-[0-9]{7}$`; `null` = guest checkout |
| `payment_method` | enum | yes | `CASH`, `CARD`, `EWALLET` |
| `currency` | string | yes | ISO 4217 `^[A-Z]{3}$` (always `MYR` today) |
| `line_items` | array | yes | 1..200 items |
| `line_items[].line_number` | integer | yes | ≥ 1, unique within the transaction |
| `line_items[].product_id` | string | yes | `^SKU-[0-9]{5}$` |
| `line_items[].quantity` | integer | yes | 1..999 |
| `line_items[].unit_price` | number | yes | 0..100000, ≤ 2 dp |
| `line_items[].discount_amount` | number | yes | ≥ 0, ≤ 2 dp, ≤ quantity × unit_price |
| `total_amount` | number | yes | ≥ 0, = Σ(quantity × unit_price − discount_amount) |

### 4.2 `inventory.movement.recorded`

| Field | Type | Required | Constraints |
|---|---|---|---|
| `movement_id` | string | yes | `^MOV-[A-Z0-9-]{4,40}$` — business key |
| `store_id` | string | yes | store pattern |
| `product_id` | string | yes | SKU pattern |
| `movement_type` | enum | yes | `RECEIPT` (replenishment delivered), `SALE` (depletion by POS), `ADJUSTMENT` (damage, expiry, shrinkage, count correction) |
| `quantity_delta` | integer | yes | ≠ 0; sign must match type (`RECEIPT` > 0, `SALE` < 0, `ADJUSTMENT` either) |
| `quantity_on_hand_after` | integer | yes | Source system's stock level after the movement. It may be negative in real retail (sync lag); flagged, not rejected. |
| `reason_code` | enum or null | yes (nullable) | `REPLENISHMENT`, `POS_SALE`, `DAMAGED`, `EXPIRED`, `SHRINKAGE`, `STOCK_COUNT_CORRECTION` |
| `reference_id` | string or null | yes (nullable) | Purchase order (`PO-…`) or transaction (`TXN-…`) |
| `unit_cost` | number or null | yes (nullable) | ≥ 0, ≤ 2 dp; present on `RECEIPT` |

### 4.3 `customer.registered` / `customer.profile_updated`

| Field | Type | Required | Constraints |
|---|---|---|---|
| `customer_id` | string | yes | `^CUST-[0-9]{7}$` |
| `loyalty_tier` | enum | yes | `BASIC`, `SILVER`, `GOLD`, `PLATINUM` |
| `home_store_id` | string | yes | store pattern |
| `city` | string | yes | 1..100 chars |
| `state` | string | yes | 1..100 chars |
| `signup_date` | string (date) | yes | `YYYY-MM-DD` |
| `birth_year` | integer or null | yes (nullable) | 1900..2015 |
| `email_sha256` | string or null | yes (nullable) | 64 lowercase hex. Hashed at source; raw email never leaves the source system. |
| `marketing_opt_in` | boolean | yes | |

### 4.4 `product.created` / `product.updated`

| Field | Type | Required | Constraints |
|---|---|---|---|
| `product_id` | string | yes | SKU pattern |
| `product_name` | string | yes | 1..200 chars |
| `brand` | string | yes | 1..100 chars |
| `category` | string | yes | 1..100 chars |
| `subcategory` | string | yes | 1..100 chars |
| `list_price` | number | yes | 0..100000, ≤ 2 dp |
| `unit_cost` | number | yes | 0..100000, ≤ 2 dp |
| `currency` | string | yes | ISO 4217 |
| `unit_of_measure` | enum | yes | `EACH`, `KG`, `LITRE`, `PACK` |
| `is_active` | boolean | yes | `false` = discontinued |

## 5. Semantic validation rules

Schema validation covers types, required fields, patterns and ranges. The rules below also cover what JSON Schema cannot express, and they are applied again at the consumer. **REJECT** means the record is routed to the DLQ with `dlq.error.code = <rule id>`. **WARN** means the record is loaded and the rule id is appended to `dq_warnings`.

| Rule ID | Applies to | Check | Severity | Layer |
|---|---|---|---|---|
| `ENV-001` | all | Value has the Confluent wire format (magic byte 0, ≥ 6 bytes) | REJECT | Spark |
| `ENV-002` | all | Body parses as a JSON object with `metadata` and `payload` objects | REJECT | Spark |
| `ENV-003` | all | All required metadata fields present and non-null | REJECT | Spark |
| `ENV-004` | all | `event_type` is allowed for the topic | REJECT | Spark |
| `ENV-005` | all | `schema_version` major = 1 | REJECT | Spark |
| `ENV-006` | all | `event_timestamp` parseable and ≤ processing time + 5 min (clock-skew tolerance) | REJECT | Spark |
| `POS-001` | pos | `transaction_id`, `store_id`, `register_id`, `payment_method`, `currency`, `total_amount` non-null | REJECT | Spark |
| `POS-002` | pos | `line_items` non-empty; every line has non-null `line_number`, `product_id`, `quantity`, `unit_price`, `discount_amount` | REJECT | Spark |
| `POS-003` | pos | `quantity > 0` | REJECT | Spark |
| `POS-004` | pos | `unit_price >= 0`, `discount_amount >= 0` | REJECT | Spark |
| `POS-005` | pos | `discount_amount <= quantity × unit_price` | REJECT | Spark |
| `POS-006` | pos | `abs(total_amount − Σ line net) <= 0.01` | REJECT | Spark |
| `POS-007` | pos | Money values have ≤ 2 decimal places | REJECT | Spark |
| `POS-008` | pos | `payment_method` in known enum | WARN | Spark |
| `POS-009` | pos | `line_number` unique within the transaction | REJECT | Spark |
| `INV-001` | inventory | `movement_id`, `store_id`, `product_id`, `movement_type`, `quantity_delta`, `quantity_on_hand_after` non-null | REJECT | Spark |
| `INV-002` | inventory | `quantity_delta != 0` and sign consistent with `movement_type` | REJECT | Spark |
| `INV-003` | inventory | `quantity_on_hand_after >= 0` | WARN | Spark |
| `CUS-001` | customer | `customer_id`, `loyalty_tier`, `home_store_id`, `signup_date` non-null | REJECT | Spark |
| `CUS-002` | customer | `loyalty_tier` in known enum | WARN | Spark |
| `PRD-001` | product | `product_id`, `product_name`, `brand`, `category`, `subcategory`, `list_price`, `unit_cost`, `unit_of_measure`, `is_active` non-null (all are NOT NULL or version-defining in DIM_PRODUCT) | REJECT | Spark |
| `PRD-002` | product | `list_price >= 0`, `unit_cost >= 0` | REJECT | Spark |
| `SF-001` | RAW | Duplicate `event_id` count among rows loaded in the last 48 h (transport duplicates) | INFO | Snowflake |
| `SF-002` | STAGING | Same `transaction_id` / `movement_id` with different `event_id` (business duplicates) | WARN | Snowflake |
| `SF-003` | facts | Rows mapped to inferred (late-arriving) product/customer members | WARN | Snowflake |
| `SF-004` | facts | Rows with an Unknown (-1) store, date, product or customer key | ERROR | Snowflake |
| `SF-005` | reconciliation | Spark audit valid count = distinct Kafka offsets in RAW, per dataset and batch (batches loaded > 30 min and < 48 h ago) | ERROR | Snowflake |
| `SF-006` | reconciliation | Completeness between layers: every RAW business key (`transaction_id`, `movement_id`) is in STAGING, and every staged POS line is in `FACT_SALES` (rows older than the 30-min grace period, within 48 h) | ERROR | Snowflake |
| `SF-007` | freshness | `max(event_timestamp)` in `FACT_SALES` within the freshness SLO | WARN | Snowflake |
| `SF-008` | inventory | Σ deltas per store/product consistent with the latest `quantity_on_hand_after` | WARN | Snowflake |

The DLQ error code is the rule ID (`ENV-002`, `POS-003`, …), so a DLQ is always explainable by grouping on one header.

## 6. Compatibility and evolution

### Registry compatibility level: `BACKWARD_TRANSITIVE`

The registry rejects a new schema version unless it can read data written with **every** earlier version of that subject. For our JSON Schemas, whose objects are closed (`additionalProperties: false`), this means:

| Change | Allowed? | Why |
|---|---|---|
| Add an **optional** property | ✅ | Old data lacks it; the new schema doesn't require it |
| Add a value to an enum | ✅ | Old values are still valid |
| Widen a numeric range / relax a pattern | ✅ | Old data still valid |
| Remove a property | ❌ | Old data containing it would violate the closed model |
| Rename a property | ❌ | = remove + add |
| Change a property's type | ❌ | Old data invalid |
| Make an optional property required | ❌ | Old data lacks it |
| Remove an enum value / narrow a range | ❌ | Old data invalid |

**Our consumers are more tolerant than the registry requires.** Strictly, BACKWARD compatibility says "upgrade consumers first". Spark parses JSON by field name with `from_json` and ignores fields it does not know. Snowflake RAW stores the whole envelope as `VARIANT`. So a producer can ship a new optional field before any consumer is upgraded. The field is preserved in RAW and can be surfaced in STAGING later, without replay. This is the practical reason for choosing JSON over a positional binary format for this project (ADR-002).

**Enum caveat.** Adding an enum value is registry-compatible, but an old consumer will see a value it doesn't know. That's why unknown enum values are `WARN`, not `REJECT` (`POS-008`, `CUS-002`), and why dimensions map them to an explicit `UNKNOWN` bucket.

### Breaking changes

A breaking change never goes into an existing topic. The process:

1. Create the new contract `schema_version: "2.0"` in a new schema file and a new topic `<topic>.v2`.
2. Producers dual-write v1 and v2 for a migration window, or a translator republishes v1 → v2.
3. Consumers migrate to v2. v1 is retired after its retention expires.

### Evolution process (checklist)

1. The architect classifies the change using the table above and updates the schema file, bumping `schema_version` MINOR (compatible) or MAJOR (breaking).
2. Add or adjust examples in `kafka/schemas/examples/`.
3. `make schemas-check` runs the registry compatibility check against the running registry (`POST /compatibility/subjects/<subject>/versions/latest`). CI fails on incompatibility.
4. Update Python models, Spark `StructType`s and STAGING SQL. Contract tests fail until they agree.
5. Register the schema (`make schemas`), then deploy producers.

Example of a planned compatible change (it doubles as a demo exercise): v1.1 of `pos-transactions` adds an optional `channel` enum (`IN_STORE`, `CLICK_AND_COLLECT`).

## 7. Examples

Valid and deliberately invalid examples live in [`kafka/schemas/examples/`](../../kafka/schemas/examples/). Invalid examples are named after the rule they break (`pos-transactions.POS-003.negative-quantity.json`). Unit and contract tests use them as fixtures.
