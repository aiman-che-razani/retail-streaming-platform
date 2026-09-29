"""Test data factories shared by unit and integration tests."""

from __future__ import annotations

import random
from datetime import UTC, datetime

from retail_platform.simulator.catalog import build_catalog
from retail_platform.simulator.customers import build_customers
from retail_platform.simulator.engine import RetailEngine
from retail_platform.simulator.reference_data import load_stores

T0 = datetime(2026, 9, 29, 4, 0, tzinfo=UTC)  # 12:00 in Kuala Lumpur


def make_engine(seed: int = 7, products: int = 40, customers: int = 200) -> RetailEngine:
    rng = random.Random(seed)
    stores = load_stores()
    return RetailEngine(
        stores=stores,
        products=build_catalog(rng, products),
        customers=build_customers(rng, customers, stores, T0.date()),
        rng=rng,
        wall_clock=lambda: T0,
    )
