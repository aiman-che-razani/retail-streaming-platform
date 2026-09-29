"""Demand shape used in backfill mode: store opening hours, intraday curve, weekend uplift.

Realtime mode deliberately uses a flat rate instead (a demo run at 02:00 Malaysian time
should still show data flowing); backfill produces realistic history for the warehouse.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

OPENING_HOUR = 8
CLOSING_HOUR = 23  # exclusive

# Relative traffic per local hour (08:00..22:00); lunch and after-work peaks.
HOURLY_PROFILE: dict[int, float] = {
    8: 0.35, 9: 0.5, 10: 0.65, 11: 0.85, 12: 1.3, 13: 1.25, 14: 0.8, 15: 0.75,
    16: 0.85, 17: 1.1, 18: 1.45, 19: 1.5, 20: 1.3, 21: 0.9, 22: 0.55,
}  # fmt: skip
_PROFILE_SUM = sum(HOURLY_PROFILE.values())

WEEKDAY_MULTIPLIER = (0.95, 0.9, 0.92, 0.95, 1.1, 1.3, 1.2)  # Mon..Sun


def hourly_share(local: datetime) -> float:
    """Fraction of a store's daily transactions that fall in this local hour (0 if closed)."""
    weight = HOURLY_PROFILE.get(local.hour, 0.0)
    return weight / _PROFILE_SUM * WEEKDAY_MULTIPLIER[local.weekday()]


def expected_transactions_per_minute(
    utc_minute: datetime, timezone: str, per_store_per_day: float
) -> float:
    local = utc_minute.astimezone(ZoneInfo(timezone))
    return per_store_per_day * hourly_share(local) / 60.0
