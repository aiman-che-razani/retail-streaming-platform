"""`retail-loader` — load completed landing batches into Snowflake RAW.

retail-loader run    # loop every LOADER_INTERVAL_SECONDS (the loader container)
retail-loader once   # one cycle, then exit (make load)
"""

from __future__ import annotations

import argparse
import sys

from retail_platform.cli.common import EXIT_OK, ShutdownSignal, run_main
from retail_platform.config.settings import LoaderSettings, SnowflakeSettings
from retail_platform.errors import ConfigurationError
from retail_platform.observability.metrics import new_registry, serve_metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-loader", description=__doc__)
    parser.add_argument("command", choices=["run", "once"])
    args = parser.parse_args(argv)

    def body() -> int:
        from retail_platform.loader.loader import LoaderMetrics, SnowflakeLoader
        from retail_platform.simulator.reference_data import stores_csv_path
        from retail_platform.warehouse.connection import (
            classify,
            connect,
            connection_params,
            connector_error,
        )

        account, settings = SnowflakeSettings(), LoaderSettings()
        if not account.is_configured:
            raise ConfigurationError(
                "Snowflake is not configured: set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER and "
                "SNOWFLAKE_PRIVATE_KEY_PATH (see docs/runbooks/snowflake-setup.md)"
            )
        params = connection_params(account, account, component="loader")
        registry = new_registry()
        loader = SnowflakeLoader(
            connect=lambda: connect(params),
            settings=settings,
            metrics=LoaderMetrics(registry),
            stores_csv=stores_csv_path(),
            classify_error=classify,
            driver_error=connector_error(),
        )
        if args.command == "once":
            loader.run_cycle()
            return EXIT_OK
        serve_metrics(registry, settings.metrics_port)
        loader.run_forever(ShutdownSignal().install())
        return EXIT_OK

    return run_main("retail-loader", body)


if __name__ == "__main__":
    sys.exit(main())
