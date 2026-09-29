from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.contracts.models import (
    InventoryMovement,
    MovementType,
    PosTransaction,
    ReasonCode,
)
from retail_platform.contracts.validation import SchemaValidator
from retail_platform.simulator.reference_data import load_stores
from tests.factories import T0, make_engine

pytestmark = pytest.mark.unit


def test_thirty_stores_loaded() -> None:
    stores = load_stores()
    assert len(stores) == 30
    assert len({s.store_id for s in stores}) == 30


def test_same_seed_produces_identical_events() -> None:
    a = [e.envelope.to_wire_dict() for e in make_engine().sale(T0) if e.envelope]
    b = [e.envelope.to_wire_dict() for e in make_engine().sale(T0) if e.envelope]
    assert a == b


def test_sale_basket_is_internally_consistent() -> None:
    engine = make_engine()
    for i in range(200):
        events = engine.sale(T0 + timedelta(seconds=i))
        pos = events[0].envelope
        assert pos is not None
        payload = pos.payload
        assert isinstance(payload, PosTransaction)
        assert events[0].key == payload.store_id
        net = sum((line.net_amount for line in payload.line_items), Decimal(0))
        assert payload.total_amount == net
        for line in payload.line_items:
            assert Decimal(0) <= line.discount_amount <= line.quantity * line.unit_price


def test_every_sale_line_depletes_stock_with_lineage() -> None:
    engine = make_engine()
    events = engine.sale(T0)
    pos = events[0].envelope
    assert pos is not None and isinstance(pos.payload, PosTransaction)
    sales = [
        e.envelope
        for e in events[1:]
        if e.envelope
        and isinstance(e.envelope.payload, InventoryMovement)
        and e.envelope.payload.movement_type is MovementType.SALE
    ]
    assert len(sales) == len(pos.payload.line_items)
    for movement, line in zip(sales, pos.payload.line_items, strict=True):
        assert isinstance(movement.payload, InventoryMovement)
        assert movement.payload.quantity_delta == -line.quantity
        assert movement.payload.reference_id == pos.payload.transaction_id
        assert movement.metadata.correlation_id == pos.metadata.correlation_id
        assert movement.metadata.causation_id == pos.metadata.event_id


def test_low_stock_triggers_delivery_after_lead_time() -> None:
    engine = make_engine(products=10)
    engine.bootstrap(T0)
    now = T0
    for i in range(3000):
        now = T0 + timedelta(seconds=i)
        engine.sale(now, store=load_stores()[0])
    receipts = engine.due_deliveries(now + timedelta(hours=4))
    assert receipts, "sustained sales must trigger replenishment"
    for event in receipts:
        assert event.envelope is not None
        movement = event.envelope.payload
        assert isinstance(movement, InventoryMovement)
        assert movement.movement_type is MovementType.RECEIPT
        assert movement.reason_code is ReasonCode.REPLENISHMENT
        assert movement.quantity_delta > 0
        assert movement.reference_id is not None and movement.reference_id.startswith("PO-")


def test_guest_checkouts_have_null_customer() -> None:
    engine = make_engine()
    customers = [
        e.envelope.payload.customer_id
        for i in range(300)
        for e in engine.sale(T0 + timedelta(seconds=i))[:1]
        if e.envelope and isinstance(e.envelope.payload, PosTransaction)
    ]
    guests = sum(1 for c in customers if c is None)
    assert 0.25 < guests / len(customers) < 0.55


def test_all_generated_events_satisfy_the_contracts(catalog: TopicCatalog) -> None:
    validator = SchemaValidator(catalog)
    engine = make_engine()
    events = engine.bootstrap(T0)
    for i in range(300):
        at = T0 + timedelta(seconds=i)
        events += engine.sale(at)
        events.append(engine.product_change(at))
        events.append(engine.customer_change(at))
        events.append(engine.adjustment(at))
    events += engine.due_deliveries(T0 + timedelta(days=1))
    for event in events:
        assert event.envelope is not None
        assert validator.errors(event.topic, event.envelope.to_wire_dict()) == []


def test_business_ids_are_unique() -> None:
    engine = make_engine()
    ids = []
    for i in range(500):
        for e in engine.sale(T0 + timedelta(seconds=i)):
            assert e.envelope is not None
            payload = e.envelope.payload
            ids.append(
                payload.transaction_id
                if isinstance(payload, PosTransaction)
                else payload.movement_id  # type: ignore[attr-defined]
            )
    assert len(ids) == len(set(ids))
