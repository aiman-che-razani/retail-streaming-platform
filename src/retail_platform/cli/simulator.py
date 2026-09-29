"""`retail-simulator` — generate retail events into Kafka (or validate them with --dry-run).

Examples:
    retail-simulator run                                  # realtime, settings from env/.env
    SIMULATOR_MODE=backfill retail-simulator run          # N days of history, max speed
    retail-simulator run --dry-run --max-events 2000      # no Kafka: validate against schemas
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import Counter
from datetime import UTC, datetime

from retail_platform.cli.common import EXIT_FAILURE, EXIT_OK, ShutdownSignal, load_catalog, run_main
from retail_platform.config.settings import KafkaSettings, SimulatorMode, SimulatorSettings
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.contracts.validation import SchemaValidator
from retail_platform.messaging.records import OutgoingEvent
from retail_platform.observability.logging import get_logger
from retail_platform.observability.metrics import new_registry, serve_metrics
from retail_platform.simulator.catalog import build_catalog
from retail_platform.simulator.customers import build_customers
from retail_platform.simulator.engine import RetailEngine
from retail_platform.simulator.faults import FaultInjector
from retail_platform.simulator.reference_data import Store, load_stores
from retail_platform.simulator.runner import SimulationRunner, SimulatorMetrics

log = get_logger("retail-simulator")


class DryRunSink:
    """Validates every event against its JSON Schema and counts outcomes; sends nothing."""

    def __init__(self, catalog: TopicCatalog) -> None:
        self._validator = SchemaValidator(catalog)
        self.valid: Counter[str] = Counter()
        self.invalid: Counter[str] = Counter()
        self.raw: Counter[str] = Counter()

    def send(self, event: OutgoingEvent) -> None:
        if event.envelope is None:
            self.raw[f"{event.topic}:{event.fault}"] += 1
            return
        errors = self._validator.errors(event.topic, event.envelope.to_wire_dict())
        (self.invalid if errors else self.valid)[event.topic] += 1
        if errors:
            log.warning("dry_run_invalid_event", topic=event.topic, errors=errors[:3])

    def poll(self) -> None:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="retail-simulator", description=__doc__)
    parser.add_argument("command", choices=["run"])
    parser.add_argument("--dry-run", action="store_true", help="validate only, no Kafka")
    parser.add_argument(
        "--max-events", type=int, default=None, help="override SIMULATOR_MAX_EVENTS"
    )
    args = parser.parse_args(argv)

    def body() -> int:
        settings = SimulatorSettings()
        max_events = args.max_events if args.max_events is not None else settings.max_events
        if args.dry_run and max_events == 0:
            max_events = 5000  # a dry run never paces itself, so it must be bounded
        settings = settings.model_copy(update={"max_events": max_events})
        kafka_settings = KafkaSettings()
        catalog = load_catalog(kafka_settings)
        rng = random.Random(settings.seed)
        stores = load_stores()
        products = build_catalog(rng, settings.product_count)
        customers = build_customers(rng, settings.customer_count, stores, datetime.now(UTC).date())
        registry = new_registry()
        metrics = SimulatorMetrics(registry)
        engine = RetailEngine(
            stores=stores,
            products=products,
            customers=customers,
            rng=rng,
            wall_clock=lambda: datetime.now(UTC),
        )
        faults = FaultInjector(
            settings,
            rng,
            on_fault=lambda topic, fault: metrics.faults_injected.labels(topic, fault).inc(),
        )
        stop = ShutdownSignal().install()
        log.info(
            "simulator_starting",
            mode=settings.mode.value,
            dry_run=args.dry_run,
            stores=len(stores),
            products=len(products),
            customers=len(customers),
            faults_enabled=faults.enabled,
        )

        if args.dry_run:
            dry = DryRunSink(catalog)
            runner = SimulationRunner(
                engine=engine, faults=faults, sink=dry, settings=settings, rng=rng, stop=stop,
                metrics=metrics, sleep=lambda _s: None,
            )  # fmt: skip
            _run(runner, settings, stores)
            log.info(
                "dry_run_complete",
                valid=dict(dry.valid),
                invalid=dict(dry.invalid),
                raw_faults=dict(dry.raw),
                transactions=runner.stats.transactions,
            )
            return EXIT_FAILURE if dry.invalid else EXIT_OK

        # Imported lazily so logging is configured before client libraries load.
        from retail_platform.messaging.clients import (
            schema_registry_client,
            wait_for_schema_registry,
        )
        from retail_platform.messaging.producer import (
            KafkaEventSink,
            ProducerMetrics,
            default_client_id,
        )

        sr_client = schema_registry_client(kafka_settings)
        wait_for_schema_registry(sr_client, kafka_settings.connect_timeout_seconds)
        serve_metrics(registry, settings.metrics_port)
        with KafkaEventSink(
            settings=kafka_settings,
            catalog=catalog,
            registry_client=sr_client,
            metrics=ProducerMetrics(registry),
            client_id=default_client_id("retail-simulator"),
        ) as sink:
            runner = SimulationRunner(
                engine=engine, faults=faults, sink=sink, settings=settings, rng=rng, stop=stop,
                metrics=metrics,
            )  # fmt: skip
            _run(runner, settings, stores)
        log.info(
            "simulator_stopped",
            transactions=runner.stats.transactions,
            events_sent=runner.stats.events_sent,
        )
        return EXIT_OK

    return run_main("retail-simulator", body)


def _run(runner: SimulationRunner, settings: SimulatorSettings, stores: tuple[Store, ...]) -> None:
    if settings.mode is SimulatorMode.BACKFILL:
        runner.run_backfill(stores)
    else:
        runner.run_realtime()


if __name__ == "__main__":
    sys.exit(main())
