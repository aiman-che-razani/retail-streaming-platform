"""Realtime (stateful) transformations for the POS stream (ADR-004).

Watermark + dedupe + tumbling windows:

* `withWatermark("event_ts", "10 minutes")` — Spark tracks max(event_ts) seen and treats
  anything older than (max - 10 min) as too late. Late rows are dropped by the stateful
  operators (and counted in `numRowsDroppedByWatermark`), which is what lets Spark evict
  old state and keep memory bounded.
* `dropDuplicatesWithinWatermark(["event_id"])` — remembers event ids only while they could
  still be duplicated within the watermark horizon (Spark >= 3.5).
* `window(event_ts, "5 minutes")` — tumbling event-time windows; a sale at 12:03:59 counts
  in [12:00, 12:05) no matter when it was processed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NET = "CAST(l.quantity * l.unit_price - l.discount_amount AS DECIMAL(38, 6))"


def valid_pos_sales(validated: DataFrame, watermark_minutes: int) -> DataFrame:
    sales = (
        validated.filter(F.col("is_valid"))
        .select(
            F.col("event_ts"),
            F.col("metadata.event_id").alias("event_id"),
            F.col("payload.store_id").alias("store_id"),
            F.col("payload.total_amount").alias("total_amount"),
            F.col("payload.line_items").alias("line_items"),
        )
        .withWatermark("event_ts", f"{watermark_minutes} minutes")
    )
    if not sales.isStreaming:
        # Bounded input (tests, backfills): exact dedupe needs no watermark-bounded state.
        return sales.dropDuplicates(["event_id"])
    return sales.dropDuplicatesWithinWatermark(["event_id"])


def store_revenue_5m(sales: DataFrame) -> DataFrame:
    """Grain: store x 5-minute event-time window."""
    return (
        sales.groupBy(F.window("event_ts", "5 minutes"), F.col("store_id"))
        .agg(
            F.sum("total_amount").cast("decimal(14,2)").alias("revenue"),
            F.count(F.lit(1)).alias("transactions"),
            F.sum(F.expr("aggregate(line_items, 0L, (acc, l) -> acc + l.quantity)")).alias("units"),
        )
        .select(
            "store_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "revenue",
            "transactions",
            "units",
        )
    )


def product_units_1h(sales: DataFrame) -> DataFrame:
    """Grain: product x 1-hour event-time window."""
    lines = sales.select("event_ts", F.explode("line_items").alias("l"))
    return (
        lines.groupBy(F.window("event_ts", "1 hour"), F.col("l.product_id").alias("product_id"))
        .agg(
            F.sum("l.quantity").alias("units"),
            F.sum(F.expr(NET)).cast("decimal(14,2)").alias("revenue"),
        )
        .select(
            "product_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "units",
            "revenue",
        )
    )
