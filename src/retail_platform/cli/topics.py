"""`retail-topics` — create/verify Kafka topics from kafka/config/topics.yaml."""

from __future__ import annotations

import argparse
import sys

from retail_platform.cli.common import EXIT_OK, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-topics", description=__doc__)
    parser.add_argument("command", choices=["ensure"], help="create missing topics, fix drift")
    parser.parse_args(argv)

    def body() -> int:
        # Imported after logging is configured so library warnings are rendered as JSON.
        from retail_platform.messaging.clients import admin_client, wait_for_kafka
        from retail_platform.messaging.topics import TopicProvisioner

        settings = KafkaSettings()
        catalog = load_catalog(settings)
        admin = admin_client(settings)
        wait_for_kafka(admin, settings.connect_timeout_seconds)
        profile = catalog.profiles[settings.environment_profile]
        TopicProvisioner(admin, catalog, profile).ensure_topics()
        return EXIT_OK

    return run_main("retail-topics", body)


if __name__ == "__main__":
    sys.exit(main())
