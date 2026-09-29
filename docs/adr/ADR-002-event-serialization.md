# ADR-002: Event serialization — JSON Schema with Schema Registry, envelope/payload

- **Status:** Accepted
- **Date:** 2026-09-29
- **Deciders:** architect, kafka-engineer, spark-engineer

## Context

Producers and consumers are deployed independently, so we need:

1. a **formal, machine-checked contract** for every event;
2. **evolution** without breaking running consumers, enforced *before* a bad schema reaches production;
3. efficient parsing in **PySpark without Python UDFs** and without extra JVM libraries;
4. storage in Snowflake that keeps *all* fields, including ones added later;
5. readable events, because this is a learning project and humans will inspect topics and DLQs.

Spark's built-in `from_avro` does not understand the Confluent wire format (5-byte header), and it needs the exact *writer* schema for each record to resolve evolution. Doing this properly with Avro in open-source PySpark means extra JVM dependencies (e.g. ABRiS) or custom per-schema-id logic.

## Decision

- **Format:** JSON validated by **JSON Schema (draft-07)**, registered in **Confluent Schema Registry** (`cp-schema-registry:8.2.4`) under subject `<topic>-value` (TopicNameStrategy).
- **Wire format:** Confluent framing (`0x00` + 4-byte schema id + UTF-8 JSON), produced by `confluent-kafka`'s `JSONSerializer`, which also **validates each event against the schema before sending**.
- **Compatibility:** `BACKWARD_TRANSITIVE` per subject. All objects are closed (`additionalProperties: false`), which makes "add optional field" the compatible change and turns field typos into errors.
- **Shape:** envelope `{metadata, payload}`. `metadata` is identical across all schemas (event_id, event_type, schema_version, event_timestamp, produced_at, producer, correlation_id, optional causation_id); a contract test enforces this.
- **Spark decoding:** native functions only. Check the magic byte, extract the schema id, and `from_json` the remainder with a `StructType` that mirrors the schema. Unknown fields are ignored.
- **Snowflake:** RAW keeps the entire envelope as `VARIANT`.

## Alternatives

| Option | Why not chosen |
|---|---|
| **Avro + Schema Registry** | The industry default for JVM stacks: compact and with strong evolution rules. In OSS PySpark it needs wire-format stripping, and correct evolution requires fetching the writer schema per schema id (extra library or custom code). Records are not human-readable in tools. **We would choose it** in a JVM/Databricks environment (native Confluent Avro support), or when volume makes JSON's size matter. |
| Protobuf + Schema Registry | Strong typing and good evolution (field numbers). Same Spark integration friction as Avro (`from_protobuf` needs descriptor files), plus a code-generation toolchain. Overkill here. |
| Plain JSON, no registry | No enforcement: a producer can change a field type and consumers discover it in production. Rejected. |
| JSON Schema files in the repo, validated only in code | Nothing stops an incompatible schema going live; there's no central version history. The registry adds compatibility gating for the cost of one container. |
| Metadata in Kafka headers only | Headers are easy to lose (many tools and sinks drop them) and aren't part of the schema. We keep metadata in the body (authoritative) and copy a few fields into headers for tooling. |

## Consequences

**Positive**
- The compatibility gate is enforced by the registry and in CI (`make schemas-check`).
- Consumers are more tolerant than required: Spark ignores unknown fields and RAW preserves them. Producers can add optional fields first.
- Events are human-readable in Kafka UI and in the DLQ, which is valuable for debugging and learning.
- No Python UDFs or extra JVM jars for decoding.

**Negative / accepted trade-offs**
- JSON is 2–4× larger than Avro before compression. zstd compression narrows most of the gap. Acceptable at this volume.
- JSON numbers can be parsed as floats by careless consumers. Mitigated by the "money as decimal" rule and by parsing to `DecimalType` in Spark and `NUMBER(12,2)` in Snowflake.
- Confluent's JSON Schema compatibility rules are subtler than Avro's (open vs closed content models). We use closed models throughout and document the allowed changes in `event-contracts.md`.
- The Spark `StructType` duplicates the schema structure. Contract tests keep them aligned.
- `cp-schema-registry` is under the Confluent Community License: free to use, not OSI open source. Apicurio Registry (Apache 2.0, Confluent-compatible API) is a drop-in alternative.
