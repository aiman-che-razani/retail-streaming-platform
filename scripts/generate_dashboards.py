"""Generate the Grafana dashboards (dashboards as code).

    python scripts/generate_dashboards.py      # writes monitoring/grafana/dashboards/*.json

Each dashboard answers operational questions: Is data flowing? Is it fresh? Is it correct?
Where is it stuck? Edit this file, not the generated JSON.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "monitoring" / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
PG = {"type": "grafana-postgresql-datasource", "uid": "realtime-postgres"}


class Layout:
    """Places panels left-to-right on a 24-column grid."""

    def __init__(self) -> None:
        self.x = 0
        self.y = 0
        self.row_height = 0
        self.next_id = 1

    def place(self, w: int, h: int) -> dict[str, int]:
        if self.x + w > 24:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_height = max(self.row_height, h)
        return pos

    def new_row(self) -> None:
        if self.x:
            self.x, self.y, self.row_height = 0, self.y + self.row_height, 0

    def pid(self) -> int:
        self.next_id += 1
        return self.next_id


def prom_target(
    expr: str, legend: str = "", ref: str = "A", instant: bool = False
) -> dict[str, Any]:
    return {
        "datasource": PROM,
        "expr": expr,
        "legendFormat": legend,
        "refId": ref,
        "instant": instant,
    }


def row(layout: Layout, title: str) -> dict[str, Any]:
    layout.new_row()
    return {"type": "row", "title": title, "id": layout.pid(), "gridPos": layout.place(24, 1),
            "collapsed": False, "panels": []}  # fmt: skip


def stat(layout: Layout, title: str, expr: str, unit: str = "short", w: int = 4,
         thresholds: list[tuple[str, float | None]] | None = None, desc: str = "") -> dict[str, Any]:  # fmt: skip
    steps = [{"color": c, "value": v} for c, v in (thresholds or [("green", None)])]
    return {
        "type": "stat", "title": title, "description": desc, "id": layout.pid(),
        "gridPos": layout.place(w, 4), "datasource": PROM,
        "targets": [prom_target(expr, instant=True)],
        "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps},
                                     "color": {"mode": "thresholds"}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background",
                    "graphMode": "area", "textMode": "auto"},
    }  # fmt: skip


def timeseries(layout: Layout, title: str, targets: list[dict[str, Any]], unit: str = "short",
               w: int = 12, h: int = 8, desc: str = "", datasource: dict[str, str] = PROM,
               stack: bool = False) -> dict[str, Any]:  # fmt: skip
    return {
        "type": "timeseries", "title": title, "description": desc, "id": layout.pid(),
        "gridPos": layout.place(w, h), "datasource": datasource, "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "drawStyle": "line", "lineWidth": 1, "fillOpacity": 10, "showPoints": "never",
            "stacking": {"mode": "normal" if stack else "none", "group": "A"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }  # fmt: skip


def table(
    layout: Layout, title: str, sql: str, w: int = 12, h: int = 8, desc: str = ""
) -> dict[str, Any]:
    return {
        "type": "table", "title": title, "description": desc, "id": layout.pid(),
        "gridPos": layout.place(w, h), "datasource": PG,
        "targets": [{"datasource": PG, "rawQuery": True, "editorMode": "code", "format": "table",
                     "rawSql": sql, "refId": "A"}],
        "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True},
    }  # fmt: skip


def pg_series(sql: str, ref: str = "A") -> dict[str, Any]:
    return {"datasource": PG, "rawQuery": True, "editorMode": "code", "format": "time_series",
            "rawSql": sql, "refId": ref}  # fmt: skip


def dashboard(uid: str, title: str, panels: list[dict[str, Any]], refresh: str = "10s",
              time_from: str = "now-1h", tags: list[str] | None = None) -> dict[str, Any]:  # fmt: skip
    return {
        "uid": uid, "title": title, "tags": tags or ["retail-platform"], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "editable": False, "refresh": refresh,
        "time": {"from": time_from, "to": "now"}, "panels": panels,
        "templating": {"list": []}, "annotations": {"list": []},
    }  # fmt: skip


def pipeline_overview() -> dict[str, Any]:
    L = Layout()  # noqa: N806
    rejected = 'sum(rate(retail_spark_records_total{outcome="rejected"}[5m]))'
    read = "sum(rate(retail_spark_input_rows_total[5m]))"
    panels = [
        row(L, "Is data flowing?"),
        stat(L, "Produced / s", 'sum(rate(retail_producer_messages_total{status="acked"}[1m]))', "ops"),
        stat(L, "Delivery failures (5m)", 'sum(increase(retail_producer_messages_total{status="failed"}[5m]))',
             thresholds=[("green", None), ("red", 1)]),
        stat(L, "Spark ingest rows / s", 'sum(rate(retail_spark_input_rows_total{query=~"ingest_.*"}[1m]))', "ops"),
        stat(L, "Reject rate", f"{rejected} / clamp_min({read}, 1e-9)", "percentunit",
             thresholds=[("green", None), ("orange", 0.01), ("red", 0.05)]),
        stat(L, "Max consumer lag (offsets)", 'max(retail_spark_kafka_offsets_behind_latest{stat="max"})',
             thresholds=[("green", None), ("orange", 5000), ("red", 20000)],
             desc="Spark tracks offsets in checkpoints, not consumer groups; this is the Kafka source's own lag."),
        stat(L, "Loader last success", "time() - max(retail_loader_last_success_timestamp_seconds)", "s",
             thresholds=[("green", None), ("orange", 1200), ("red", 2700)]),
        timeseries(L, "Kafka topic throughput (records/s)",
                   [prom_target('sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"_.*|.*dlq|.*retry|it\\\\..*"}[1m]))', "{{topic}}")],
                   "ops"),
        timeseries(L, "Spark records by outcome (records/s)",
                   [prom_target("sum by (outcome) (rate(retail_spark_records_total[1m]))", "{{outcome}}")],
                   "ops", stack=True),
        row(L, "Is it fresh?"),
        timeseries(L, "Event lag: processing time - event time (max)",
                   [prom_target('retail_spark_event_lag_seconds{stat="max"}', "{{query}}")], "s",
                   desc="Includes deliberately late (fault-injected) events."),
        timeseries(L, "Spark micro-batch duration",
                   [prom_target("retail_spark_batch_duration_seconds", "{{query}}")], "s"),
        row(L, "Is it correct?"),
        timeseries(L, "DLQ volume (records/min)",
                   [prom_target('sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~".*\\\\.dlq"}[5m])) * 60', "{{topic}}")]),
        timeseries(L, "Late rows dropped by the realtime watermark (per min)",
                   [prom_target("sum by (query) (rate(retail_spark_late_rows_dropped_total[5m])) * 60", "{{query}}")]),
        row(L, "Where is it stuck?"),
        timeseries(L, "Spark consumer lag (offsets behind latest)",
                   [prom_target('retail_spark_kafka_offsets_behind_latest{stat="max"}', "{{query}}")], w=8),
        timeseries(L, "Landing backlog (batches waiting for Snowflake)",
                   [prom_target("retail_loader_landing_backlog_batches", "{{dataset}}")], w=8),
        timeseries(L, "Realtime state store rows",
                   [prom_target("retail_spark_state_rows", "{{query}}")], w=8,
                   desc="Bounded by the watermark: should plateau, not grow forever."),
        timeseries(L, "Producer delivery latency p95",
                   [prom_target("histogram_quantile(0.95, sum by (le, topic) (rate(retail_producer_delivery_latency_seconds_bucket[5m])))", "{{topic}}")],
                   "s", w=12),
        timeseries(L, "Rows loaded into Snowflake RAW (per 15 min)",
                   [prom_target("sum by (dataset) (increase(retail_loader_rows_loaded_total[15m]))", "{{dataset}}")], w=12),
    ]  # fmt: skip
    return dashboard("retail-pipeline-overview", "Retail Pipeline Overview", panels)


def realtime_sales() -> dict[str, Any]:
    L = Layout()  # noqa: N806
    panels = [
        row(L, "Real-time sales (Spark realtime app -> PostgreSQL; late data > 10 min excluded)"),
        timeseries(
            L, "Revenue per 5-minute window - top 8 stores",
            [pg_series(
                "SELECT window_start AS time, store_id AS metric, revenue AS value\n"
                "FROM realtime.store_revenue_5m\n"
                "WHERE $__timeFilter(window_start) AND store_id IN (\n"
                "  SELECT store_id FROM realtime.store_revenue_5m WHERE $__timeFilter(window_start)\n"
                "  GROUP BY store_id ORDER BY SUM(revenue) DESC LIMIT 8)\n"
                "ORDER BY 1")],
            "currencyMYR", w=24, h=9, datasource=PG),
        table(L, "Store leaderboard - current hour",
              "SELECT store_id, SUM(revenue) AS revenue_myr, SUM(transactions) AS transactions,\n"
              "       ROUND(SUM(revenue) / NULLIF(SUM(transactions), 0), 2) AS avg_basket_myr\n"
              "FROM realtime.store_revenue_5m\n"
              "WHERE window_start >= date_trunc('hour', now())\n"
              "GROUP BY store_id ORDER BY revenue_myr DESC"),
        table(L, "Top products - current hour window",
              "SELECT product_id, units, revenue AS revenue_myr, updated_at\n"
              "FROM realtime.product_units_1h\n"
              "WHERE window_start = date_trunc('hour', now())\n"
              "ORDER BY units DESC LIMIT 15"),
        timeseries(
            L, "Total revenue per 5 minutes (all stores)",
            [pg_series(
                "SELECT window_start AS time, SUM(revenue) AS revenue\n"
                "FROM realtime.store_revenue_5m WHERE $__timeFilter(window_start)\n"
                "GROUP BY 1 ORDER BY 1")],
            "currencyMYR", w=24, h=7, datasource=PG),
    ]  # fmt: skip
    return dashboard("retail-realtime-sales", "Retail Real-time Sales", panels, refresh="10s",
                     time_from="now-3h")  # fmt: skip


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, board in (
        ("pipeline-overview", pipeline_overview()),
        ("realtime-sales", realtime_sales()),
    ):
        (OUT / f"{name}.json").write_text(json.dumps(board, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
