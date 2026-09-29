"""Fault injection (F3): turn a fraction of clean events into realistic bad data.

| fault | how it is sent | expected handling downstream |
| --- | --- | --- |
| duplicate | same envelope twice (same event_id) | dedupe by event_id (FM-07) |
| late | valid, event_timestamp shifted back | loaded; dropped from realtime (FM-09) |
| unknown_product | valid POS, SKU not in catalog | inferred dimension member (FM-10) |
| invalid_value | schema-valid semantic error, or raw | DLQ with POS-006 / INV-002 / schema |
| missing_field | raw framed JSON without a required key | DLQ ENV-003 / POS-001 ... |
| malformed | truncated JSON, or unframed bytes | DLQ ENV-002 / ENV-001 |

"raw" events bypass the producer's schema validation — they simulate an upstream system with
a bug, which is exactly why the consumer validates again (event-contracts.md §1.6).
"""

from __future__ import annotations

import json
import random
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from retail_platform.config.settings import SimulatorSettings
from retail_platform.contracts.models import Envelope, InventoryMovement, PosTransaction
from retail_platform.messaging.records import OutgoingEvent

FAULTS = ("malformed", "missing_field", "invalid_value", "duplicate", "late", "unknown_product")

_REQUIRED_TO_DROP: dict[str, list[tuple[str, str]]] = {
    "pos.transactions": [("metadata", "event_id"), ("payload", "store_id"),
                         ("payload", "transaction_id"), ("metadata", "event_timestamp")],
    "inventory.events": [("payload", "product_id"), ("payload", "movement_id"),
                         ("metadata", "event_id")],
    "customer.events": [("payload", "customer_id"), ("payload", "loyalty_tier")],
    "product.updates": [("payload", "product_id"), ("payload", "list_price")],
}  # fmt: skip


class FaultInjector:
    def __init__(
        self,
        settings: SimulatorSettings,
        rng: random.Random,
        on_fault: Callable[[str, str], None] | None = None,
    ) -> None:
        self._rng = rng
        self._late_max = settings.late_event_max_minutes
        self._rates = {
            "malformed": settings.malformed_rate,
            "missing_field": settings.missing_field_rate,
            "invalid_value": settings.invalid_value_rate,
            "duplicate": settings.duplicate_rate,
            "late": settings.late_event_rate,
            "unknown_product": settings.unknown_product_rate,
        }
        self._on_fault = on_fault or (lambda _topic, _fault: None)

    @property
    def enabled(self) -> bool:
        return any(rate > 0 for rate in self._rates.values())

    def _choose(self, topic: str) -> str | None:
        roll = self._rng.random()
        cumulative = 0.0
        for fault in FAULTS:
            if fault == "unknown_product" and topic != "pos.transactions":
                continue
            cumulative += self._rates[fault]
            if roll < cumulative:
                return fault
        return None

    def apply(self, events: list[OutgoingEvent]) -> list[OutgoingEvent]:
        if not self.enabled:
            return events
        out: list[OutgoingEvent] = []
        for event in events:
            fault = self._choose(event.topic) if event.envelope is not None else None
            if fault is None:
                out.append(event)
                continue
            self._on_fault(event.topic, fault)
            out.extend(getattr(self, f"_{fault}")(event))
        return out

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _env(event: OutgoingEvent) -> Envelope[Any]:
        if event.envelope is None:  # apply() only passes enveloped events
            raise ValueError("fault injection requires an enveloped event")
        return event.envelope

    def _wire(self, event: OutgoingEvent) -> dict[str, Any]:
        return self._env(event).to_wire_dict()

    @staticmethod
    def _raw(
        event: OutgoingEvent, body: bytes, fault: str, *, framed: bool = True
    ) -> OutgoingEvent:
        return OutgoingEvent(event.topic, event.key, raw_body=body, framed=framed, fault=fault)

    @staticmethod
    def _with_envelope(event: OutgoingEvent, envelope: Envelope[Any], fault: str) -> OutgoingEvent:
        return OutgoingEvent(event.topic, event.key, envelope=envelope, fault=fault)

    # ------------------------------------------------------------------ faults
    def _duplicate(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        return [event, self._with_envelope(event, self._env(event), "duplicate")]

    def _late(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        envelope = self._env(event)
        shift = timedelta(minutes=self._rng.randint(15, max(15, self._late_max)))
        metadata = envelope.metadata.model_copy(
            update={"event_timestamp": envelope.metadata.event_timestamp - shift}
        )
        late = envelope.model_copy(update={"metadata": metadata})
        return [self._with_envelope(event, late, "late")]

    def _unknown_product(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        envelope = self._env(event)
        payload = envelope.payload
        if not isinstance(payload, PosTransaction):
            return [event]
        unknown = f"SKU-{self._rng.randint(99000, 99999):05d}"
        first, *rest = payload.line_items
        lines = (first.model_copy(update={"product_id": unknown}), *rest)
        changed = envelope.model_copy(
            update={"payload": payload.model_copy(update={"line_items": lines})}
        )
        return [self._with_envelope(event, changed, "unknown_product")]

    def _invalid_value(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        envelope = self._env(event)
        payload = envelope.payload
        semantic = self._rng.random() < 0.5
        bad: PosTransaction | InventoryMovement | None = None
        if semantic and isinstance(payload, PosTransaction):
            # Schema-valid but wrong total: only consumer-side rule POS-006 catches it.
            bad = payload.model_copy(update={"total_amount": payload.total_amount + 7})
        elif semantic and isinstance(payload, InventoryMovement):
            # Schema-valid but sign contradicts movement type: rule INV-002.
            bad = payload.model_copy(update={"quantity_delta": -payload.quantity_delta})
        if bad is not None:
            changed = envelope.model_copy(update={"payload": bad})
            return [self._with_envelope(event, changed, "invalid_value")]
        wire = self._wire(event)
        body = wire["payload"]
        if "line_items" in body:
            body["line_items"][0]["quantity"] = -1
        elif "quantity_on_hand_after" in body:
            body["movement_type"] = "TELEPORTED"
        elif "loyalty_tier" in body:
            body["loyalty_tier"] = "DIAMOND"
        elif "list_price" in body:
            body["list_price"] = -5.0
        return [self._raw(event, json.dumps(wire).encode(), "invalid_value")]

    def _missing_field(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        wire = self._wire(event)
        section, field = self._rng.choice(_REQUIRED_TO_DROP[event.topic])
        wire[section].pop(field, None)
        return [self._raw(event, json.dumps(wire).encode(), "missing_field")]

    def _malformed(self, event: OutgoingEvent) -> list[OutgoingEvent]:
        text = json.dumps(self._wire(event))
        if self._rng.random() < 0.5:
            cut = self._rng.randint(10, max(11, len(text) - 10))
            return [self._raw(event, text[:cut].encode(), "malformed")]
        # Valid JSON but sent without the Confluent wire-format header (producer bypassed SR).
        return [self._raw(event, text.encode(), "malformed", framed=False)]
