"""Streaming query assembly for the two Spark applications (overview.md §6, ADR-004).

Lazy evaluation in practice: everything up to `.start()` only builds a logical plan. Nothing
reads Kafka until a query starts; then each trigger plans a micro-batch over a fixed offset
range and Catalyst optimises the whole pipeline (column pruning, predicate pushdown) at once.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from retail_platform.config.settings import KafkaSettings, PostgresSettings, SparkSettings
from retail_platform.contracts.catalog import Dataset, TopicCatalog
from retail_platform.errors import ProcessingError
from retail_platform.observability.logging import get_logger
from retail_platform.processing.sinks import IngestBatchWriter, PostgresUpsertWriter
from retail_platform.processing.transformations.aggregations import (
    product_units_1h,
    store_revenue_5m,
    valid_pos_sales,
)
from retail_platform.processing.transformations.envelope import decode_and_parse
from retail_platform.processing.transformations.validation import apply_rules, rules_for

log = get_logger(__name__)

INGEST_OBSERVATION = "ingest"
REALTIME_OBSERVATION = "realtime"


def kafka_source(
    spark: SparkSession,
    topics: list[str],
    kafka: KafkaSettings,
    *,
    max_offsets_per_trigger: int,
    starting_offsets: str,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", ",".join(topics))
        # Only used on the very first start; afterwards the checkpoint decides.
        .option("startingOffsets", starting_offsets)
        # Retention deleted unprocessed data / topic recreated -> fail loudly (FM-19).
        .option("failOnDataLoss", "true")
        # Back-pressure: bounded batches when catching up after downtime.
        .option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        .option("kafka.isolation.level", "read_committed")
        .option("includeHeaders", "true")
    )
    for key, value in kafka.client_security_config().items():
        reader = reader.option(f"kafka.{key}", value)
    return reader.load()


def validated_stream(
    spark: SparkSession,
    dataset: Dataset,
    catalog: TopicCatalog,
    settings: SparkSettings,
    kafka: KafkaSettings,
    *,
    max_offsets_per_trigger: int,
) -> DataFrame:
    primary = catalog.primary_for_dataset(dataset)
    source = kafka_source(
        spark,
        [primary, catalog.retry_for(primary).name],
        kafka,
        max_offsets_per_trigger=max_offsets_per_trigger,
        starting_offsets=settings.starting_offsets,
    )
    rules = rules_for(
        dataset, catalog.allowed_event_types(primary), settings.future_tolerance_minutes
    )
    return apply_rules(decode_and_parse(source, dataset), rules)


def observe_batch(df: DataFrame, name: str) -> DataFrame:
    """Per-batch counters computed inside the same job (no extra Spark action)."""
    lag = F.when(
        F.col("is_valid"),
        F.current_timestamp().cast("double") - F.col("event_ts").cast("double"),
    )
    return df.observe(
        name,
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.col("is_valid").cast("long")).alias("valid"),
        F.sum((~F.col("is_valid")).cast("long")).alias("rejected"),
        F.sum((F.col("is_valid") & (F.size("dq_warnings") > 0)).cast("long")).alias("warned"),
        F.max(lag).alias("max_lag_s"),
        F.avg(lag).alias("avg_lag_s"),
    )


def start_ingest_query(
    spark: SparkSession,
    dataset: Dataset,
    catalog: TopicCatalog,
    settings: SparkSettings,
    kafka: KafkaSettings,
) -> StreamingQuery:
    checkpoint = settings.checkpoint_dir / "ingest" / dataset.value
    validated = validated_stream(
        spark,
        dataset,
        catalog,
        settings,
        kafka,
        max_offsets_per_trigger=settings.ingest_max_offsets_per_trigger,
    )
    writer = IngestBatchWriter(
        dataset=dataset,
        dlq_topic=catalog.dlq_for(catalog.primary_for_dataset(dataset)).name,
        landing_dir=settings.landing_dir,
        checkpoint_location=checkpoint,
        kafka_settings=kafka,
    )
    return (
        observe_batch(validated, INGEST_OBSERVATION)
        .writeStream.queryName(f"ingest_{dataset.value}")
        .foreachBatch(writer)
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=f"{settings.ingest_trigger_seconds} seconds")
        .start()
    )


def start_realtime_queries(
    spark: SparkSession,
    catalog: TopicCatalog,
    settings: SparkSettings,
    kafka: KafkaSettings,
    postgres: PostgresSettings,
) -> list[StreamingQuery]:
    queries = []
    specs = (
        ("store_revenue_5m", store_revenue_5m, ("store_id", "window_start"),
         ("window_end", "revenue", "transactions", "units")),
        ("product_units_1h", product_units_1h, ("product_id", "window_start"),
         ("window_end", "units", "revenue")),
    )  # fmt: skip
    for table, aggregate, keys, values in specs:
        # Each query has its own Kafka reader and checkpoint: independent progress and restarts.
        validated = validated_stream(
            spark,
            Dataset.POS_TRANSACTIONS,
            catalog,
            settings,
            kafka,
            max_offsets_per_trigger=settings.realtime_max_offsets_per_trigger,
        )
        sales = valid_pos_sales(
            observe_batch(validated, REALTIME_OBSERVATION), settings.watermark_minutes
        )
        writer = PostgresUpsertWriter(
            table=table,
            keys=keys,
            values=values,
            timestamp_columns=("window_start", "window_end"),
            settings=postgres,
        )
        queries.append(
            aggregate(sales)
            .writeStream.queryName(f"rt_{table}")
            .outputMode("update")
            .foreachBatch(writer)
            .option("checkpointLocation", str(settings.checkpoint_dir / "realtime" / table))
            .trigger(processingTime=f"{settings.realtime_trigger_seconds} seconds")
            .start()
        )
    return queries


class StopFlag(Protocol):
    @property
    def requested(self) -> bool: ...


def run_until_stopped(
    spark: SparkSession, queries: list[StreamingQuery], stop: StopFlag, poll_seconds: float = 5.0
) -> None:
    """Block until shutdown is requested or any query fails (fail-fast: FM-02/FM-03)."""
    try:
        while not stop.requested:
            spark.streams.awaitAnyTermination(timeout=int(poll_seconds))
            for query in queries:
                if not query.isActive:
                    error = query.exception()
                    raise ProcessingError(
                        f"query {query.name} terminated: {error}" if error else
                        f"query {query.name} stopped unexpectedly"
                    )  # fmt: skip
    finally:
        for query in queries:
            if query.isActive:
                log.info("stopping_query", query=query.name)
                query.stop()  # an in-flight batch is re-run from the checkpoint on restart


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
