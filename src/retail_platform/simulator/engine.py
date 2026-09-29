"""Stateful retail domain engine.

The engine owns all simulator state (stock levels, pending deliveries, id counters) in one
instance — no module globals — and turns "something happened at time T" into contract-
conforming events. It knows nothing about Kafka; a runner decides *when* things happen and a
sink decides *where* events go.

Business behaviour modelled:
* baskets of 1..N lines, popularity-weighted products, occasional promotions;
* loyalty customers prefer their home store; ~40% guest checkouts (customer_id = null);
* every sale line depletes stock (SALE movement caused by the POS event);
* stock below reorder point triggers a purchase order delivered after a lead time (RECEIPT);
* random shrinkage/damage/count corrections (ADJUSTMENT);
* product master changes (price, cost, discontinuation) and customer changes (tier, moves).
"""

from __future__ import annotations

import heapq
import random
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from retail_platform import __version__
from retail_platform.contracts.models import (
    CustomerProfile,
    Envelope,
    EventMetadata,
    EventType,
    InventoryMovement,
    MovementType,
    PaymentMethod,
    PosLineItem,
    PosTransaction,
    ProductRecord,
    ReasonCode,
)
from retail_platform.messaging.records import OutgoingEvent
from retail_platform.simulator.catalog import CENT, ProductState, money
from retail_platform.simulator.customers import TIER_ORDER, CustomerState
from retail_platform.simulator.reference_data import Store

PRODUCER = f"retail-simulator/{__version__}"

POS_TOPIC = "pos.transactions"
INVENTORY_TOPIC = "inventory.events"
CUSTOMER_TOPIC = "customer.events"
PRODUCT_TOPIC = "product.updates"

PAYMENT_WEIGHTS = {PaymentMethod.CARD: 0.45, PaymentMethod.EWALLET: 0.35, PaymentMethod.CASH: 0.2}


@dataclass(frozen=True, slots=True)
class EngineConfig:
    guest_checkout_ratio: float = 0.4
    home_store_affinity: float = 0.7
    registers_per_store: int = 6
    promo_line_probability: float = 0.08
    adjustment_probability_per_sale: float = 0.02
    lead_time_minutes: tuple[int, int] = (30, 180)
    initial_stock_multiplier: int = 3


@dataclass(order=True, frozen=True, slots=True)
class _Delivery:
    arrival: datetime
    store_id: str
    product_id: str
    quantity: int
    purchase_order: str
    correlation_id: uuid.UUID


class RetailEngine:
    def __init__(
        self,
        *,
        stores: tuple[Store, ...],
        products: list[ProductState],
        customers: list[CustomerState],
        rng: random.Random,
        wall_clock: Callable[[], datetime],
        config: EngineConfig | None = None,
    ) -> None:
        if not stores or not products:
            raise ValueError("stores and products must not be empty")
        self._stores = stores
        self._store_by_id = {s.store_id: s for s in stores}
        self._store_weights = [s.traffic_weight for s in stores]
        self._products = products
        self._product_by_id = {p.product_id: p for p in products}
        self._customers = customers
        self._customers_by_store: dict[str, list[CustomerState]] = defaultdict(list)
        for c in customers:
            self._customers_by_store[c.home_store_id].append(c)
        self._rng = rng
        self._wall_clock = wall_clock  # real time for metadata.produced_at
        self._config = config or EngineConfig()
        self._on_hand: dict[tuple[str, str], int] = {}
        self._pending: list[_Delivery] = []
        self._pending_keys: set[tuple[str, str]] = set()
        self._counters: dict[tuple[str, str, date], int] = defaultdict(int)

    # ------------------------------------------------------------------ helpers
    @property
    def products(self) -> list[ProductState]:
        return self._products

    def on_hand(self, store_id: str, product_id: str) -> int:
        return self._on_hand.get((store_id, product_id), 0)

    def _uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self._rng.getrandbits(128), version=4)

    def _metadata(
        self,
        event_type: EventType,
        at: datetime,
        correlation_id: uuid.UUID,
        causation_id: uuid.UUID | None = None,
    ) -> EventMetadata:
        return EventMetadata(
            event_id=self._uuid(),
            event_type=event_type,
            event_timestamp=at,
            produced_at=self._wall_clock(),
            producer=PRODUCER,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    def _next_id(self, prefix: str, store: Store, at: datetime) -> str:
        key = (prefix, store.store_id, at.date())
        self._counters[key] += 1
        return f"{prefix}-{store.compact_id}-{at:%Y%m%d}-{self._counters[key]:06d}"

    def _active_products(self) -> list[ProductState]:
        return [p for p in self._products if p.is_active]

    # ------------------------------------------------------------------ master data
    def product_event(
        self, product: ProductState, at: datetime, event_type: EventType
    ) -> OutgoingEvent:
        payload = ProductRecord(
            product_id=product.product_id,
            product_name=product.product_name,
            brand=product.brand,
            category=product.category,
            subcategory=product.subcategory,
            list_price=product.list_price,
            unit_cost=product.unit_cost,
            unit_of_measure=product.unit_of_measure,
            is_active=product.is_active,
        )
        envelope = Envelope[ProductRecord](
            metadata=self._metadata(event_type, at, self._uuid()), payload=payload
        )
        return OutgoingEvent(PRODUCT_TOPIC, product.product_id, envelope)

    def customer_event(
        self, customer: CustomerState, at: datetime, event_type: EventType
    ) -> OutgoingEvent:
        payload = CustomerProfile(
            customer_id=customer.customer_id,
            loyalty_tier=customer.loyalty_tier,
            home_store_id=customer.home_store_id,
            city=customer.city,
            state=customer.state,
            signup_date=customer.signup_date,
            birth_year=customer.birth_year,
            email_sha256=customer.email_sha256,
            marketing_opt_in=customer.marketing_opt_in,
        )
        envelope = Envelope[CustomerProfile](
            metadata=self._metadata(event_type, at, self._uuid()), payload=payload
        )
        return OutgoingEvent(CUSTOMER_TOPIC, customer.customer_id, envelope)

    def bootstrap(self, at: datetime) -> list[OutgoingEvent]:
        """Initial master data and opening stock for every store x product."""
        events = [self.product_event(p, at, EventType.PRODUCT_CREATED) for p in self._products]
        events += [
            self.customer_event(c, at, EventType.CUSTOMER_REGISTERED) for c in self._customers
        ]
        for store in self._stores:
            correlation = self._uuid()
            po = self._next_id("PO", store, at)
            for product in self._products:
                qty = product.reorder_point * self._config.initial_stock_multiplier
                events.append(
                    self._receipt(
                        store,
                        product,
                        quantity=qty,
                        purchase_order=po,
                        at=at,
                        correlation=correlation,
                    )
                )
        return events

    # ------------------------------------------------------------------ sales
    def sale(self, at: datetime, store: Store | None = None) -> list[OutgoingEvent]:
        """One completed checkout plus the stock movements it causes."""
        store = store or self._rng.choices(self._stores, weights=self._store_weights, k=1)[0]
        products = self._active_products()
        n_lines = min(len(products), max(1, int(self._rng.expovariate(1 / 3.2)) + 1), 25)
        chosen = self._pick_products(products, n_lines)
        lines = []
        for number, product in enumerate(chosen, start=1):
            quantity = 1 if self._rng.random() < 0.7 else self._rng.randint(2, 4)
            gross = product.list_price * quantity
            discount = Decimal("0.00")
            if self._rng.random() < self._config.promo_line_probability:
                discount = money(gross * Decimal(str(self._rng.choice((0.05, 0.1, 0.15, 0.2)))))
            lines.append(
                PosLineItem(
                    line_number=number,
                    product_id=product.product_id,
                    quantity=quantity,
                    unit_price=product.list_price,
                    discount_amount=discount,
                )
            )
        total = sum((line.net_amount for line in lines), Decimal("0.00")).quantize(CENT)
        transaction = PosTransaction(
            transaction_id=self._next_id("TXN", store, at),
            store_id=store.store_id,
            register_id=f"REG-{self._rng.randint(1, self._config.registers_per_store):02d}",
            customer_id=self._pick_customer(store),
            payment_method=self._rng.choices(
                list(PAYMENT_WEIGHTS), weights=list(PAYMENT_WEIGHTS.values()), k=1
            )[0],
            line_items=tuple(lines),
            total_amount=total,
        )
        correlation = self._uuid()
        pos_meta = self._metadata(EventType.POS_TRANSACTION_COMPLETED, at, correlation)
        events = [
            OutgoingEvent(
                POS_TOPIC,
                store.store_id,
                Envelope[PosTransaction](metadata=pos_meta, payload=transaction),
            )
        ]
        for line in lines:
            product = self._product_by_id[line.product_id]
            events.append(
                self._movement(
                    store,
                    product,
                    movement_type=MovementType.SALE,
                    delta=-line.quantity,
                    reason=ReasonCode.POS_SALE,
                    reference=transaction.transaction_id,
                    unit_cost=None,
                    at=at,
                    correlation=correlation,
                    causation=pos_meta.event_id,
                )
            )
            self._maybe_reorder(store, product, at)
        if self._rng.random() < self._config.adjustment_probability_per_sale:
            events.append(self.adjustment(at, store))
        return events

    def _pick_products(self, products: list[ProductState], n: int) -> list[ProductState]:
        weights = [p.popularity for p in products]
        picked: dict[str, ProductState] = {}
        # Sampling with replacement then de-duplicating keeps popular items popular.
        while len(picked) < n:
            product = self._rng.choices(products, weights=weights, k=1)[0]
            picked.setdefault(product.product_id, product)
        return list(picked.values())

    def _pick_customer(self, store: Store) -> str | None:
        if self._rng.random() < self._config.guest_checkout_ratio or not self._customers:
            return None
        locals_ = self._customers_by_store.get(store.store_id)
        if locals_ and self._rng.random() < self._config.home_store_affinity:
            return self._rng.choice(locals_).customer_id
        return self._rng.choice(self._customers).customer_id

    # ------------------------------------------------------------------ inventory
    def _movement(
        self,
        store: Store,
        product: ProductState,
        *,
        movement_type: MovementType,
        delta: int,
        reason: ReasonCode | None,
        reference: str | None,
        unit_cost: Decimal | None,
        at: datetime,
        correlation: uuid.UUID,
        causation: uuid.UUID | None = None,
    ) -> OutgoingEvent:
        key = (store.store_id, product.product_id)
        self._on_hand[key] = self._on_hand.get(key, 0) + delta
        payload = InventoryMovement(
            movement_id=self._next_id("MOV", store, at),
            store_id=store.store_id,
            product_id=product.product_id,
            movement_type=movement_type,
            quantity_delta=delta,
            quantity_on_hand_after=self._on_hand[key],
            reason_code=reason,
            reference_id=reference,
            unit_cost=unit_cost,
        )
        envelope = Envelope[InventoryMovement](
            metadata=self._metadata(
                EventType.INVENTORY_MOVEMENT_RECORDED, at, correlation, causation
            ),
            payload=payload,
        )
        return OutgoingEvent(INVENTORY_TOPIC, f"{store.store_id}:{product.product_id}", envelope)

    def _receipt(
        self,
        store: Store,
        product: ProductState,
        *,
        quantity: int,
        purchase_order: str,
        at: datetime,
        correlation: uuid.UUID,
    ) -> OutgoingEvent:
        return self._movement(
            store,
            product,
            movement_type=MovementType.RECEIPT,
            delta=quantity,
            reason=ReasonCode.REPLENISHMENT,
            reference=purchase_order,
            unit_cost=product.unit_cost,
            at=at,
            correlation=correlation,
        )

    def _maybe_reorder(self, store: Store, product: ProductState, at: datetime) -> None:
        key = (store.store_id, product.product_id)
        if self.on_hand(*key) > product.reorder_point or key in self._pending_keys:
            return
        low, high = self._config.lead_time_minutes
        delivery = _Delivery(
            arrival=at + timedelta(minutes=self._rng.randint(low, high)),
            store_id=store.store_id,
            product_id=product.product_id,
            quantity=product.reorder_quantity,
            purchase_order=self._next_id("PO", store, at),
            correlation_id=self._uuid(),
        )
        heapq.heappush(self._pending, delivery)
        self._pending_keys.add(key)

    def due_deliveries(self, now: datetime) -> list[OutgoingEvent]:
        """RECEIPT events for purchase orders whose lead time has elapsed."""
        events = []
        while self._pending and self._pending[0].arrival <= now:
            d = heapq.heappop(self._pending)
            self._pending_keys.discard((d.store_id, d.product_id))
            events.append(
                self._receipt(
                    self._store_by_id[d.store_id],
                    self._product_by_id[d.product_id],
                    quantity=d.quantity,
                    purchase_order=d.purchase_order,
                    at=d.arrival,
                    correlation=d.correlation_id,
                )
            )
        return events

    def adjustment(self, at: datetime, store: Store | None = None) -> OutgoingEvent:
        store = store or self._rng.choices(self._stores, weights=self._store_weights, k=1)[0]
        product = self._rng.choice(self._products)
        reason = self._rng.choices(
            [
                ReasonCode.DAMAGED,
                ReasonCode.EXPIRED,
                ReasonCode.SHRINKAGE,
                ReasonCode.STOCK_COUNT_CORRECTION,
            ],
            weights=[0.3, 0.2, 0.3, 0.2],
            k=1,
        )[0]
        if reason is ReasonCode.STOCK_COUNT_CORRECTION:
            delta = self._rng.choice([-3, -2, -1, 1, 2, 3])
        else:
            delta = -self._rng.randint(1, 3)
        return self._movement(
            store,
            product,
            movement_type=MovementType.ADJUSTMENT,
            delta=delta,
            reason=reason,
            reference=None,
            unit_cost=None,
            at=at,
            correlation=self._uuid(),
        )

    # ------------------------------------------------------------------ master-data changes
    def product_change(self, at: datetime) -> OutgoingEvent:
        product = self._rng.choice(self._products)
        roll = self._rng.random()
        if roll < 0.75:  # price change, cost follows loosely
            factor = Decimal(str(self._rng.choice((0.9, 0.95, 1.05, 1.1, 1.15))))
            product.list_price = money(product.list_price * factor)
            product.unit_cost = min(
                money(product.unit_cost * Decimal(str(self._rng.uniform(0.98, 1.06)))),
                product.list_price,
            )
        elif roll < 0.9:  # name correction (SCD Type 1 attribute)
            product.product_name = product.product_name.replace("(", "- ", 1).replace(")", "")
        elif roll < 0.97:  # recategorisation within the same category (SCD Type 2)
            premium = " Premium"
            product.subcategory = (
                product.subcategory.removesuffix(premium)
                if product.subcategory.endswith(premium)
                else product.subcategory + premium
            )
        else:  # discontinue or reactivate
            product.is_active = not product.is_active
        return self.product_event(product, at, EventType.PRODUCT_UPDATED)

    def customer_change(self, at: datetime) -> OutgoingEvent:
        customer = self._rng.choice(self._customers)
        roll = self._rng.random()
        if roll < 0.6:  # tier move (mostly upgrades)
            idx = TIER_ORDER.index(customer.loyalty_tier)
            step = 1 if self._rng.random() < 0.75 else -1
            customer.loyalty_tier = TIER_ORDER[max(0, min(len(TIER_ORDER) - 1, idx + step))]
        elif roll < 0.8:  # moved: new home store and location
            store = self._rng.choice(self._stores)
            customer.home_store_id, customer.city, customer.state = (
                store.store_id,
                store.city,
                store.state,
            )
        else:  # consent change (Type 1)
            customer.marketing_opt_in = not customer.marketing_opt_in
        return self.customer_event(customer, at, EventType.CUSTOMER_PROFILE_UPDATED)
