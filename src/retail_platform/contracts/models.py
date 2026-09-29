"""Pydantic wire models mirroring `kafka/schemas/*.schema.json`.

The JSON Schemas are authoritative; these models give producers typed construction.
Contract tests assert that every model serialises to JSON that validates against its schema
and that field names match exactly.

Serialisation rules (event-contracts.md §1):
* timestamps -> RFC 3339 UTC with millisecond precision and a `Z` suffix;
* money -> JSON number (Decimal -> float). Values have <= 2 dp, and float's shortest
  round-trip repr ("18.9") is parsed back exactly as a decimal by consumers.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, field_validator

SCHEMA_VERSION: Final = "1.0"


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


UtcTimestamp = Annotated[datetime, PlainSerializer(format_utc, return_type=str)]
Money = Annotated[
    Decimal,
    Field(ge=0, max_digits=12, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]
OptionalMoney = Annotated[
    Decimal | None,
    Field(ge=0, max_digits=12, decimal_places=2),
    PlainSerializer(lambda v: None if v is None else float(v), when_used="json"),
]
LowerUuid = Annotated[UUID, PlainSerializer(str, return_type=str)]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ------------------------------------------------------------------ enums
class EventType(StrEnum):
    POS_TRANSACTION_COMPLETED = "pos.transaction.completed"
    INVENTORY_MOVEMENT_RECORDED = "inventory.movement.recorded"
    CUSTOMER_REGISTERED = "customer.registered"
    CUSTOMER_PROFILE_UPDATED = "customer.profile_updated"
    PRODUCT_CREATED = "product.created"
    PRODUCT_UPDATED = "product.updated"


class PaymentMethod(StrEnum):
    CASH = "CASH"
    CARD = "CARD"
    EWALLET = "EWALLET"


class MovementType(StrEnum):
    RECEIPT = "RECEIPT"
    SALE = "SALE"
    ADJUSTMENT = "ADJUSTMENT"


class ReasonCode(StrEnum):
    REPLENISHMENT = "REPLENISHMENT"
    POS_SALE = "POS_SALE"
    DAMAGED = "DAMAGED"
    EXPIRED = "EXPIRED"
    SHRINKAGE = "SHRINKAGE"
    STOCK_COUNT_CORRECTION = "STOCK_COUNT_CORRECTION"


class LoyaltyTier(StrEnum):
    BASIC = "BASIC"
    SILVER = "SILVER"
    GOLD = "GOLD"
    PLATINUM = "PLATINUM"


class UnitOfMeasure(StrEnum):
    EACH = "EACH"
    KG = "KG"
    LITRE = "LITRE"
    PACK = "PACK"


# ------------------------------------------------------------------ envelope
class EventMetadata(_Frozen):
    event_id: LowerUuid
    event_type: EventType
    schema_version: str = SCHEMA_VERSION
    event_timestamp: UtcTimestamp
    produced_at: UtcTimestamp
    producer: str
    correlation_id: LowerUuid
    causation_id: LowerUuid | None = None


class Envelope[P: BaseModel](_Frozen):
    metadata: EventMetadata
    payload: P

    def to_wire_dict(self) -> dict[str, Any]:
        """JSON-compatible dict exactly as it goes on the wire (optional None fields omitted
        from metadata only; payload nullables are required-and-nullable by contract)."""
        data = self.model_dump(mode="json")
        if data["metadata"].get("causation_id") is None:
            data["metadata"].pop("causation_id", None)
        return data


# ------------------------------------------------------------------ payloads
class PosLineItem(_Frozen):
    line_number: int = Field(ge=1)
    product_id: str
    quantity: int = Field(ge=1, le=999)
    unit_price: Money
    discount_amount: Money = Decimal("0.00")

    @property
    def net_amount(self) -> Decimal:
        return self.quantity * self.unit_price - self.discount_amount


class PosTransaction(_Frozen):
    transaction_id: str
    store_id: str
    register_id: str
    customer_id: str | None
    payment_method: PaymentMethod
    currency: str = "MYR"
    line_items: tuple[PosLineItem, ...] = Field(min_length=1, max_length=200)
    total_amount: Money

    @field_validator("line_items")
    @classmethod
    def _unique_line_numbers(cls, items: tuple[PosLineItem, ...]) -> tuple[PosLineItem, ...]:
        numbers = [i.line_number for i in items]
        if len(numbers) != len(set(numbers)):
            raise ValueError("line_number must be unique within a transaction")
        return items


class InventoryMovement(_Frozen):
    movement_id: str
    store_id: str
    product_id: str
    movement_type: MovementType
    quantity_delta: int
    quantity_on_hand_after: int
    reason_code: ReasonCode | None
    reference_id: str | None
    unit_cost: OptionalMoney


class CustomerProfile(_Frozen):
    customer_id: str
    loyalty_tier: LoyaltyTier
    home_store_id: str
    city: str
    state: str
    signup_date: date
    birth_year: int | None
    email_sha256: str | None
    marketing_opt_in: bool


class ProductRecord(_Frozen):
    product_id: str
    product_name: str
    brand: str
    category: str
    subcategory: str
    list_price: Money
    unit_cost: Money
    currency: str = "MYR"
    unit_of_measure: UnitOfMeasure
    is_active: bool


PosTransactionEvent = Envelope[PosTransaction]
InventoryMovementEvent = Envelope[InventoryMovement]
CustomerEvent = Envelope[CustomerProfile]
ProductEvent = Envelope[ProductRecord]
