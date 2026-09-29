"""`retail-dlq` — inspect dead-letter topics and redrive records after remediation (ADR-008).

Examples:
    retail-dlq inspect --topic pos.transactions
    retail-dlq redrive --topic pos.transactions --error-code POS-006 --dry-run
    retail-dlq redrive --topic pos.transactions --error-code ENV-003 --max 100
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid

from retail_platform.cli.common import EXIT_OK, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings
from retail_platform.contracts.catalog import TopicRole


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-dlq", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("inspect", "redrive"):
        p = sub.add_parser(name)
        p.add_argument("--topic", required=True, help="PRIMARY topic, e.g. pos.transactions")
    sub.choices["inspect"].add_argument("--samples", type=int, default=5)
    redrive = sub.choices["redrive"]
    redrive.add_argument("--error-code", action="append", help="only these rule ids (repeatable)")
    redrive.add_argument("--max", type=int, default=None, help="max records to read")
    redrive.add_argument("--dry-run", action="store_true")
    redrive.add_argument("--force", action="store_true", help="ignore the redrive loop guard")
    redrive.add_argument(
        "--from-beginning", action="store_true", help="re-scan the whole DLQ, ignoring commits"
    )
    args = parser.parse_args(argv)

    def body() -> int:
        from confluent_kafka import Consumer, Producer

        from retail_platform.messaging.dlq import Redriver, consumer_config, inspect
        from retail_platform.messaging.producer import default_client_id, producer_config

        settings = KafkaSettings()
        catalog = load_catalog(settings)
        primary = catalog.get(args.topic)
        if primary.role is not TopicRole.PRIMARY:
            raise SystemExit(f"--topic must be a primary topic, got {args.topic}")
        dlq_topic = catalog.dlq_for(primary.name).name

        if args.command == "inspect":
            # Throwaway group: inspection never moves the redrive group's offsets.
            consumer = Consumer(consumer_config(settings, f"dlq-inspect-{uuid.uuid4().hex[:8]}"))
            try:
                summary = inspect(consumer, dlq_topic, sample_limit=args.samples)
            finally:
                consumer.close()
            sys.stdout.write(json.dumps(summary.as_dict(), indent=2) + "\n")
            return EXIT_OK

        codes = set(args.error_code or [])
        consumer = Consumer(consumer_config(settings, f"dlq-redrive.{dlq_topic}"))
        producer = Producer(producer_config(settings, default_client_id("retail-dlq-redrive")))
        try:
            report = Redriver(consumer, producer).redrive(
                dlq_topic=dlq_topic,
                retry_topic=catalog.retry_for(primary.name).name,
                predicate=lambda r: not codes or r.error_code in codes,
                max_records=args.max,
                dry_run=args.dry_run,
                force=args.force,
                from_beginning=args.from_beginning,
            )
        finally:
            consumer.close()
        sys.stdout.write(json.dumps(report.__dict__, indent=2) + "\n")
        return EXIT_OK

    return run_main("retail-dlq", body, logs_to_stderr=True)


if __name__ == "__main__":
    sys.exit(main())
