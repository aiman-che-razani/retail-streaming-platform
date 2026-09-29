"""Projections from a validated batch to the ingest outputs (overview.md §6 landing contract).

Timestamps in the landing zone are ISO-8601 UTC *strings* (`...Z`) rather than Parquet
timestamp types: Parquet timestamp encodings (INT96 vs INT64, UTC-adjusted or not) are a
classic source of silent timezone shifts between engines. A string with an explicit `Z` is
unambiguous; Snowflake converts it with TO_TIMESTAMP_NTZ at load time.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from retail_platform.contracts import dlq

ISO_UTC = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"


def iso(column: Column) -> Column:
    return F.date_format(column, ISO_UTC)


def _decoded(column: str) -> Column:
    return F.expr(f"decode({column}, 'UTF-8')")


def landing_events(df: DataFrame, query_id: str, batch_id: int) -> DataFrame:
    """Valid events -> one row per event, full envelope preserved in `event_json`."""
    return df.filter(F.col("is_valid")).select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        iso(F.col("timestamp")).alias("kafka_timestamp"),
        _decoded("key").alias("kafka_key"),
        F.col("schema_id"),
        F.col("metadata.event_id").alias("event_id"),
        F.col("metadata.event_type").alias("event_type"),
        F.col("metadata.schema_version").alias("schema_version"),
        iso(F.col("event_ts")).alias("event_timestamp"),
        iso(F.expr("try_cast(metadata.produced_at AS TIMESTAMP)")).alias("produced_at"),
        F.col("metadata.producer").alias("producer"),
        F.col("metadata.correlation_id").alias("correlation_id"),
        F.col("metadata.causation_id").alias("causation_id"),
        F.col("body").alias("event_json"),
        # JSON text, not a Parquet LIST: nested-list encodings differ between engines.
        F.to_json("dq_warnings").alias("dq_warnings"),
        iso(F.current_timestamp()).alias("ingested_at"),
        F.lit(query_id).alias("spark_query_id"),
        F.lit(batch_id).cast("long").alias("spark_batch_id"),
    )


def _header(name: str, value: Column) -> Column:
    return F.struct(
        F.lit(name).alias("key"), F.encode(value.cast("string"), "UTF-8").alias("value")
    )


def dlq_records(df: DataFrame, dlq_topic: str, app: str) -> DataFrame:
    """Rejected records -> Kafka sink rows: original key/value, diagnostics in headers."""
    rejected = df.filter(~F.col("is_valid"))
    diagnostics = F.array(
        _header(dlq.ERROR_CODE, F.col("rejection.rule_id")),
        _header(dlq.ERROR_MESSAGE, F.col("rejection.message")),
        _header(dlq.ERROR_STAGE, F.col("rejection.stage")),
        _header(dlq.SOURCE_TOPIC, F.col("topic")),
        _header(dlq.SOURCE_PARTITION, F.col("partition")),
        _header(dlq.SOURCE_OFFSET, F.col("offset")),
        _header(dlq.FAILED_AT, iso(F.current_timestamp())),
        _header(dlq.APP, F.lit(app)),
    )
    # Keep the original headers (e.g. a previous dlq.redrive_count) except ones we overwrite.
    overwritten = [h for h in dlq.ALL_HEADERS if h != dlq.REDRIVE_COUNT]
    original = F.filter(
        F.coalesce(F.col("headers"), F.array().cast("array<struct<key:string,value:binary>>")),
        lambda h: ~h["key"].isin(overwritten),
    )
    return rejected.select(
        F.lit(dlq_topic).alias("topic"),
        F.col("key"),
        F.col("value"),
        F.concat(original, diagnostics).alias("headers"),
    )


def landing_dead_letters(df: DataFrame, query_id: str, batch_id: int) -> DataFrame:
    """Rejected records for SQL analysis in Snowflake RAW.DEAD_LETTERS."""
    return df.filter(~F.col("is_valid")).select(
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        iso(F.col("timestamp")).alias("kafka_timestamp"),
        _decoded("key").alias("kafka_key"),
        F.col("rejection.rule_id").alias("error_code"),
        F.col("rejection.stage").alias("error_stage"),
        F.col("rejection.message").alias("error_message"),
        F.col("metadata.event_id").alias("event_id"),
        F.base64("value").alias("raw_value_base64"),
        F.substring(_decoded("value"), 1, 2000).alias("raw_value_preview"),
        iso(F.current_timestamp()).alias("failed_at"),
        F.lit(query_id).alias("spark_query_id"),
        F.lit(batch_id).cast("long").alias("spark_batch_id"),
    )


def ingest_audit(df: DataFrame, dataset: str, query_id: str, batch_id: int) -> DataFrame:
    """Per topic-partition accounting for the batch: read = valid + rejected (reconciliation)."""
    return (
        df.groupBy("topic", "partition")
        .agg(
            F.min("offset").alias("min_offset"),
            F.max("offset").alias("max_offset"),
            F.count(F.lit(1)).alias("records_read"),
            F.sum(F.col("is_valid").cast("long")).alias("records_valid"),
            F.sum((~F.col("is_valid")).cast("long")).alias("records_rejected"),
            F.sum((F.col("is_valid") & (F.size("dq_warnings") > 0)).cast("long")).alias(
                "records_warned"
            ),
            iso(F.min(F.when(F.col("is_valid"), F.col("event_ts")))).alias("min_event_timestamp"),
            iso(F.max(F.when(F.col("is_valid"), F.col("event_ts")))).alias("max_event_timestamp"),
        )
        .select(
            F.lit(dataset).alias("dataset"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            "min_offset",
            "max_offset",
            "records_read",
            "records_valid",
            "records_rejected",
            "records_warned",
            "min_event_timestamp",
            "max_event_timestamp",
            F.lit(query_id).alias("spark_query_id"),
            F.lit(batch_id).cast("long").alias("spark_batch_id"),
            iso(F.current_timestamp()).alias("processed_at"),
        )
    )
