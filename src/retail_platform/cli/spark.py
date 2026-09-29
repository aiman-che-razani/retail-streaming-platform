"""Entry points for the Spark applications (run inside the Spark image).

retail-spark-ingest     # stateless: validate, route, land (4 queries)
retail-spark-realtime   # stateful: watermark, dedupe, windows -> PostgreSQL (2 queries)
"""

from __future__ import annotations

import sys

from retail_platform.cli.common import EXIT_OK, ShutdownSignal, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings, PostgresSettings, SparkSettings
from retail_platform.contracts.catalog import Dataset
from retail_platform.observability.logging import get_logger
from retail_platform.observability.metrics import new_registry, serve_metrics


def ingest_main() -> int:
    def body() -> int:
        from retail_platform.messaging.clients import admin_client, wait_for_kafka
        from retail_platform.processing.jobs import (
            INGEST_OBSERVATION,
            ensure_dirs,
            run_until_stopped,
            start_ingest_query,
        )
        from retail_platform.processing.listener import (
            PrometheusStreamingListener,
            StreamingMetrics,
        )
        from retail_platform.processing.session import build_session

        settings, kafka = SparkSettings(), KafkaSettings()
        catalog = load_catalog(kafka)
        stop = ShutdownSignal().install()
        wait_for_kafka(admin_client(kafka), kafka.connect_timeout_seconds)
        ensure_dirs(settings.landing_dir, settings.checkpoint_dir)

        registry = new_registry()
        spark = build_session(settings, "ingest")
        spark.streams.addListener(
            PrometheusStreamingListener(StreamingMetrics(registry), INGEST_OBSERVATION)
        )
        serve_metrics(registry, settings.metrics_port)
        queries = [
            start_ingest_query(spark, dataset, catalog, settings, kafka) for dataset in Dataset
        ]
        get_logger("spark-ingest").info("ingest_running", queries=[q.name for q in queries])
        run_until_stopped(spark, queries, stop)
        spark.stop()
        return EXIT_OK

    return run_main("spark-ingest", body)


def realtime_main() -> int:
    def body() -> int:
        from retail_platform.messaging.clients import admin_client, wait_for_kafka
        from retail_platform.processing.jobs import (
            REALTIME_OBSERVATION,
            ensure_dirs,
            run_until_stopped,
            start_realtime_queries,
        )
        from retail_platform.processing.listener import (
            PrometheusStreamingListener,
            StreamingMetrics,
        )
        from retail_platform.processing.session import build_session

        settings, kafka, postgres = SparkSettings(), KafkaSettings(), PostgresSettings()
        catalog = load_catalog(kafka)
        stop = ShutdownSignal().install()
        wait_for_kafka(admin_client(kafka), kafka.connect_timeout_seconds)
        ensure_dirs(settings.checkpoint_dir)

        registry = new_registry()
        spark = build_session(settings, "realtime")
        spark.streams.addListener(
            PrometheusStreamingListener(StreamingMetrics(registry), REALTIME_OBSERVATION)
        )
        serve_metrics(registry, settings.metrics_port)
        queries = start_realtime_queries(spark, catalog, settings, kafka, postgres)
        get_logger("spark-realtime").info("realtime_running", queries=[q.name for q in queries])
        run_until_stopped(spark, queries, stop)
        spark.stop()
        return EXIT_OK

    return run_main("spark-realtime", body)


if __name__ == "__main__":
    sys.exit(ingest_main() if "ingest" in sys.argv[-1] else realtime_main())
