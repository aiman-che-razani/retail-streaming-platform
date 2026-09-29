"""Deterministic loyalty-customer population. No direct PII: only a hashed email."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date, timedelta

from retail_platform.contracts.models import LoyaltyTier
from retail_platform.simulator.reference_data import Store

TIER_WEIGHTS = {
    LoyaltyTier.BASIC: 0.55,
    LoyaltyTier.SILVER: 0.28,
    LoyaltyTier.GOLD: 0.13,
    LoyaltyTier.PLATINUM: 0.04,
}
TIER_ORDER = list(TIER_WEIGHTS)


@dataclass(slots=True)
class CustomerState:
    customer_id: str
    loyalty_tier: LoyaltyTier
    home_store_id: str
    city: str
    state: str
    signup_date: date
    birth_year: int | None
    email_sha256: str | None
    marketing_opt_in: bool


def build_customers(
    rng: random.Random, count: int, stores: tuple[Store, ...], as_of: date
) -> list[CustomerState]:
    weights = [s.traffic_weight for s in stores]
    customers: list[CustomerState] = []
    for n in range(1, count + 1):
        store = rng.choices(stores, weights=weights, k=1)[0]
        tier = rng.choices(TIER_ORDER, weights=list(TIER_WEIGHTS.values()), k=1)[0]
        has_email = rng.random() < 0.85
        email = f"customer{n:07d}@example.invalid"
        customers.append(
            CustomerState(
                customer_id=f"CUST-{n:07d}",
                loyalty_tier=tier,
                home_store_id=store.store_id,
                city=store.city,
                state=store.state,
                signup_date=as_of - timedelta(days=rng.randint(1, 5 * 365)),
                birth_year=rng.randint(1950, 2008) if rng.random() < 0.9 else None,
                email_sha256=hashlib.sha256(email.encode()).hexdigest() if has_email else None,
                marketing_opt_in=rng.random() < 0.6,
            )
        )
    return customers
