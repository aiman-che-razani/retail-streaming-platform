"""Static reference data shipped with the package (stores)."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from importlib import resources
from pathlib import Path

STORES_RESOURCE = "stores.csv"


@dataclass(frozen=True, slots=True)
class Store:
    store_id: str
    store_name: str
    city: str
    state: str
    region: str
    store_format: str
    size_sqm: int
    opened_date: date
    timezone: str
    traffic_weight: float

    @property
    def compact_id(self) -> str:
        """`KLCC-01` -> `KLCC01`, used inside business identifiers."""
        return self.store_id.replace("-", "")


def stores_csv_path() -> Path:
    """Filesystem path of the packaged stores.csv (also loaded into Snowflake RAW)."""
    # The package is installed as regular files (wheel/editable), so this is a real path.
    return Path(str(resources.files(__package__).joinpath("reference", STORES_RESOURCE)))


def load_stores() -> tuple[Store, ...]:
    text = resources.files(__package__).joinpath("reference", STORES_RESOURCE).read_text("utf-8")
    rows = csv.DictReader(text.splitlines())
    return tuple(
        Store(
            store_id=r["store_id"],
            store_name=r["store_name"],
            city=r["city"],
            state=r["state"],
            region=r["region"],
            store_format=r["store_format"],
            size_sqm=int(r["size_sqm"]),
            opened_date=date.fromisoformat(r["opened_date"]),
            timezone=r["timezone"],
            traffic_weight=float(r["traffic_weight"]),
        )
        for r in rows
    )
