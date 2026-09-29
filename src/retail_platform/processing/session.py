"""SparkSession construction."""

from __future__ import annotations

from pyspark.sql import SparkSession

from retail_platform.config.settings import SparkSettings


def build_session(settings: SparkSettings, app: str) -> SparkSession:
    return (
        SparkSession.builder.master(settings.master)
        .appName(f"{settings.app_name_prefix}-{app}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
        # ANSI mode (default ON in Spark 4) turns a bad cast or overflow into an exception that
        # fails the WHOLE micro-batch. Our input is untrusted, so one malformed record would
        # become a poison pill that crash-loops the query (FM-22). With ANSI off such values
        # become NULL, which the validation rules treat as violations -> DLQ. Our own derived
        # arithmetic uses explicit DECIMAL casts and try_cast where input is untrusted.
        .config("spark.sql.ansi.enabled", "false")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
