from __future__ import annotations

import json
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from retail_platform.config.settings import SimulatorSettings
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.contracts.models import PosTransaction
from retail_platform.contracts.validation import SchemaValidator
from retail_platform.messaging.records import OutgoingEvent
from retail_platform.simulator.faults import FaultInjector
from retail_platform.simulator.reference_data import load_stores
from retail_platform.simulator.runner import SimulationRunner, poisson
from tests.factories import T0, make_engine

pytestmark = pytest.mark.unit


def settings(**overrides: object) -> SimulatorSettings:
    # model_copy(update=...) silently accepts unknown keys, so guard against typos.
    unknown = set(overrides) - set(SimulatorSettings.model_fields)
    assert not unknown, f"unknown settings: {unknown}"
    return SimulatorSettings().model_copy(update=overrides)


class ListSink:
    def __init__(self) -> None:
        self.events: list[OutgoingEvent] = []

    def send(self, event: OutgoingEvent) -> None:
        self.events.append(event)

    def poll(self) -> None:
        return None


class NeverStop:
    requested = False


def only_fault(fault: str) -> SimulatorSettings:
    field = "late_event_rate" if fault == "late" else f"{fault}_rate"
    return settings(**{field: 1.0})


def pos_event() -> OutgoingEvent:
    return make_engine().sale(T0)[0]


def test_no_faults_is_passthrough() -> None:
    events = make_engine().sale(T0)
    assert FaultInjector(settings(), random.Random(1)).apply(events) == events


def test_duplicate_repeats_same_event_id() -> None:
    out = FaultInjector(only_fault("duplicate"), random.Random(1)).apply([pos_event()])
    assert len(out) == 2
    assert out[0].event_id == out[1].event_id


def test_late_event_is_shifted_back_and_still_valid(catalog: TopicCatalog) -> None:
    original = pos_event()
    (late,) = FaultInjector(only_fault("late"), random.Random(1)).apply([original])
    assert late.envelope is not None and original.envelope is not None
    lag = original.envelope.metadata.event_timestamp - late.envelope.metadata.event_timestamp
    assert lag >= timedelta(minutes=15)
    assert SchemaValidator(catalog).errors(late.topic, late.envelope.to_wire_dict()) == []


def test_unknown_product_is_schema_valid_but_not_in_catalog(catalog: TopicCatalog) -> None:
    (event,) = FaultInjector(only_fault("unknown_product"), random.Random(1)).apply([pos_event()])
    assert event.envelope is not None
    payload = event.envelope.payload
    assert isinstance(payload, PosTransaction)
    assert payload.line_items[0].product_id.startswith("SKU-99")
    assert SchemaValidator(catalog).errors(event.topic, event.envelope.to_wire_dict()) == []


def test_missing_field_is_raw_and_violates_schema(catalog: TopicCatalog) -> None:
    (event,) = FaultInjector(only_fault("missing_field"), random.Random(1)).apply([pos_event()])
    assert event.raw_body is not None and event.framed
    assert SchemaValidator(catalog).errors(event.topic, json.loads(event.raw_body))


def test_malformed_is_not_parseable_or_unframed() -> None:
    injector = FaultInjector(only_fault("malformed"), random.Random(3))
    outcomes = Counter()
    for _ in range(40):
        (event,) = injector.apply([pos_event()])
        assert event.raw_body is not None
        if not event.framed:
            outcomes["unframed"] += 1
            continue
        with pytest.raises(json.JSONDecodeError):
            json.loads(event.raw_body)
        outcomes["truncated"] += 1
    assert outcomes["unframed"] and outcomes["truncated"]


def test_invalid_value_mixes_semantic_and_schema_violations(catalog: TopicCatalog) -> None:
    injector = FaultInjector(only_fault("invalid_value"), random.Random(5))
    validator = SchemaValidator(catalog)
    kinds = Counter()
    for _ in range(40):
        (event,) = injector.apply([pos_event()])
        if event.envelope is not None:  # semantic: schema-valid, wrong total (POS-006)
            payload = event.envelope.payload
            assert isinstance(payload, PosTransaction)
            assert payload.total_amount != sum(line.net_amount for line in payload.line_items)
            assert validator.errors(event.topic, event.envelope.to_wire_dict()) == []
            kinds["semantic"] += 1
        else:
            assert event.raw_body is not None
            assert validator.errors(event.topic, json.loads(event.raw_body))
            kinds["schema"] += 1
    assert kinds["semantic"] and kinds["schema"]


def test_fault_callback_counts_faults() -> None:
    seen: list[tuple[str, str]] = []
    FaultInjector(
        only_fault("duplicate"), random.Random(1), on_fault=lambda t, f: seen.append((t, f))
    ).apply([pos_event()])
    assert seen == [("pos.transactions", "duplicate")]


def test_poisson_mean_is_close_to_lambda() -> None:
    rng = random.Random(11)
    samples = [poisson(rng, 2.5) for _ in range(20000)]
    assert abs(sum(samples) / len(samples) - 2.5) < 0.1
    assert poisson(rng, 0) == 0


def test_realtime_run_respects_max_events_after_bootstrap() -> None:
    sink = ListSink()
    engine = make_engine()
    runner = SimulationRunner(
        engine=engine,
        faults=FaultInjector(settings(), random.Random(1)),
        sink=sink,
        settings=settings(max_events=50, transactions_per_second=1000.0),
        rng=random.Random(1),
        stop=NeverStop(),
        now=lambda: T0,
        sleep=lambda _s: None,
    )
    stats = runner.run_realtime()
    assert stats.bootstrap_events > 0
    assert 50 <= stats.events_sent - stats.bootstrap_events < 80
    assert stats.transactions > 0


def test_backfill_only_sells_during_opening_hours() -> None:
    sink = ListSink()
    runner = SimulationRunner(
        engine=make_engine(),
        faults=FaultInjector(settings(), random.Random(1)),
        sink=sink,
        settings=settings(
            backfill_days=1, backfill_transactions_per_store_per_day=40, bootstrap_master_data=False
        ),
        rng=random.Random(2),
        stop=NeverStop(),
        now=lambda: datetime(2026, 9, 29, 16, 0, tzinfo=UTC),
    )
    stats = runner.run_backfill(load_stores())
    assert stats.transactions > 500
    kl = ZoneInfo("Asia/Kuala_Lumpur")
    for event in sink.events:
        if event.topic == "pos.transactions" and event.envelope:
            local_hour = event.envelope.metadata.event_timestamp.astimezone(kl).hour
            assert 8 <= local_hour < 23
