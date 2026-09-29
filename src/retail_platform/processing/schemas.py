"""Spark schemas mirroring `kafka/schemas/*.schema.json` (verified by tests/contract).

Design choices:
* Enums are parsed as strings so an unknown value from a newer producer is *seen* (and turned
  into a WARN) instead of silently becoming null.
* Money is parsed as DECIMAL(18,6): wide enough to detect values with more than 2 decimal
  places (rule POS-007) without binary floating point ever being involved.
* Timestamps are parsed as strings and converted explicitly, so an unparseable timestamp is
  distinguishable from a missing one.
* Fields present in the JSON but absent here are ignored by `from_json` — this is what makes
  consumers tolerant of compatible schema evolution (event-contracts.md §6).
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from retail_platform.contracts.catalog import Dataset

MONEY = DecimalType(18, 6)


def _fields(*specs: tuple[str, DataType]) -> StructType:
    return StructType([StructField(name, dtype, nullable=True) for name, dtype in specs])


METADATA = _fields(
    ("event_id", StringType()),
    ("event_type", StringType()),
    ("schema_version", StringType()),
    ("event_timestamp", StringType()),
    ("produced_at", StringType()),
    ("producer", StringType()),
    ("correlation_id", StringType()),
    ("causation_id", StringType()),
)

POS_LINE_ITEM = _fields(
    ("line_number", LongType()),
    ("product_id", StringType()),
    ("quantity", LongType()),
    ("unit_price", MONEY),
    ("discount_amount", MONEY),
)

POS_PAYLOAD = _fields(
    ("transaction_id", StringType()),
    ("store_id", StringType()),
    ("register_id", StringType()),
    ("customer_id", StringType()),
    ("payment_method", StringType()),
    ("currency", StringType()),
    ("line_items", ArrayType(POS_LINE_ITEM, containsNull=True)),
    ("total_amount", MONEY),
)

INVENTORY_PAYLOAD = _fields(
    ("movement_id", StringType()),
    ("store_id", StringType()),
    ("product_id", StringType()),
    ("movement_type", StringType()),
    ("quantity_delta", LongType()),
    ("quantity_on_hand_after", LongType()),
    ("reason_code", StringType()),
    ("reference_id", StringType()),
    ("unit_cost", MONEY),
)

CUSTOMER_PAYLOAD = _fields(
    ("customer_id", StringType()),
    ("loyalty_tier", StringType()),
    ("home_store_id", StringType()),
    ("city", StringType()),
    ("state", StringType()),
    ("signup_date", StringType()),
    ("birth_year", LongType()),
    ("email_sha256", StringType()),
    ("marketing_opt_in", BooleanType()),
)

PRODUCT_PAYLOAD = _fields(
    ("product_id", StringType()),
    ("product_name", StringType()),
    ("brand", StringType()),
    ("category", StringType()),
    ("subcategory", StringType()),
    ("list_price", MONEY),
    ("unit_cost", MONEY),
    ("currency", StringType()),
    ("unit_of_measure", StringType()),
    ("is_active", BooleanType()),
)

PAYLOADS: dict[Dataset, StructType] = {
    Dataset.POS_TRANSACTIONS: POS_PAYLOAD,
    Dataset.INVENTORY_MOVEMENTS: INVENTORY_PAYLOAD,
    Dataset.CUSTOMER_EVENTS: CUSTOMER_PAYLOAD,
    Dataset.PRODUCT_EVENTS: PRODUCT_PAYLOAD,
}


def envelope_schema(dataset: Dataset) -> StructType:
    return StructType(
        [
            StructField("metadata", METADATA, nullable=True),
            StructField("payload", PAYLOADS[dataset], nullable=True),
        ]
    )
