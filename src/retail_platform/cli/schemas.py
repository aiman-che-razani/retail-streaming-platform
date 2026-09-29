"""`retail-schemas` — check compatibility of, or register, the event schemas."""

from __future__ import annotations

import argparse
import sys

from retail_platform.cli.common import EXIT_FAILURE, EXIT_OK, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings
from retail_platform.observability.logging import get_logger


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-schemas", description=__doc__)
    parser.add_argument(
        "command",
        choices=["check", "register"],
        help="check: compatibility only (CI gate); register: set compatibility and register",
    )
    args = parser.parse_args(argv)

    def body() -> int:
        # Imported after logging is configured so library warnings are rendered as JSON.
        from retail_platform.messaging.clients import (
            schema_registry_client,
            wait_for_schema_registry,
        )
        from retail_platform.messaging.schemas import SchemaManager

        settings = KafkaSettings()
        client = schema_registry_client(settings)
        wait_for_schema_registry(client, settings.connect_timeout_seconds)
        manager = SchemaManager(client, load_catalog(settings))
        if args.command == "register":
            manager.register_all()
            return EXIT_OK
        results = manager.check_all()
        bad = [r.subject for r in results if not r.compatible]
        log = get_logger("retail-schemas")
        if bad:
            log.error("schemas_incompatible", subjects=bad)
            return EXIT_FAILURE
        log.info("schemas_compatible", subjects=[r.subject for r in results])
        return EXIT_OK

    return run_main("retail-schemas", body)


if __name__ == "__main__":
    sys.exit(main())
