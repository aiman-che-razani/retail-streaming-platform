"""Record-level validation rules (event-contracts.md §5) as native Spark expressions.

Each rule is a Column that is TRUE when the record *violates* the rule. The first violated
REJECT rule (in catalog order) becomes the record's `rejection` (rule id, stage, message);
all violated WARN rules are collected into `dq_warnings`.

Null handling is deliberately fail-safe: a REJECT condition that evaluates to NULL (because
some input was unexpectedly null) counts as a violation; a WARN condition that is NULL does
not warn. Earlier rules check required fields, so later rules normally see non-null inputs.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from retail_platform.contracts.catalog import Dataset
from retail_platform.contracts.dlq import ErrorStage


class Severity(StrEnum):
    REJECT = "REJECT"
    WARN = "WARN"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    severity: Severity
    stage: ErrorStage
    message: str
    violation: Column


def _any_null(prefix: str, fields: Sequence[str]) -> Column:
    return F.greatest(*[F.col(f"{prefix}.{f}").isNull() for f in fields], F.lit(False))


REQUIRED_METADATA = (
    "event_id",
    "event_type",
    "schema_version",
    "event_timestamp",
    "produced_at",
    "producer",
    "correlation_id",
)


def envelope_rules(allowed_event_types: Sequence[str], future_tolerance_minutes: int) -> list[Rule]:
    return [
        Rule(
            "ENV-001",
            Severity.REJECT,
            ErrorStage.DECODE,
            "value is not in Confluent wire format",
            ~F.col("wire_ok"),
        ),
        Rule(
            "ENV-002",
            Severity.REJECT,
            ErrorStage.PARSE,
            "body is not a JSON object with metadata and payload objects",
            ~F.col("is_envelope") | F.col("metadata").isNull() | F.col("payload").isNull(),
        ),
        Rule(
            "ENV-003",
            Severity.REJECT,
            ErrorStage.VALIDATE,
            "required metadata field missing or null",
            _any_null("metadata", REQUIRED_METADATA),
        ),
        Rule(
            "ENV-004",
            Severity.REJECT,
            ErrorStage.VALIDATE,
            "event_type not allowed on this topic",
            ~F.col("metadata.event_type").isin(list(allowed_event_types)),
        ),
        Rule(
            "ENV-005",
            Severity.REJECT,
            ErrorStage.VALIDATE,
            "unsupported schema_version (major must be 1)",
            ~F.col("metadata.schema_version").rlike(r"^1\.[0-9]+$"),
        ),
        Rule(
            "ENV-006",
            Severity.REJECT,
            ErrorStage.VALIDATE,
            "event_timestamp unparseable or in the future",
            F.col("event_ts").isNull()
            | (
                F.col("event_ts")
                > F.current_timestamp() + F.expr(f"INTERVAL {future_tolerance_minutes} MINUTES")
            ),
        ),
    ]


_LINE_NET = "CAST(l.quantity * l.unit_price - l.discount_amount AS DECIMAL(38, 6))"


def pos_rules() -> list[Rule]:
    lines = "payload.line_items"
    return [
        Rule(
            "POS-001", Severity.REJECT, ErrorStage.VALIDATE, "required transaction field missing",
            _any_null("payload", ("transaction_id", "store_id", "register_id", "payment_method",
                                  "currency", "total_amount")),
        ),
        Rule(
            "POS-002", Severity.REJECT, ErrorStage.VALIDATE,
            "line_items empty or line field missing",
            F.col(lines).isNull()
            | (F.size(lines) == 0)
            | F.expr(f"exists({lines}, l -> l IS NULL OR l.line_number IS NULL"
                     " OR l.product_id IS NULL OR l.quantity IS NULL"
                     " OR l.unit_price IS NULL OR l.discount_amount IS NULL)"),
        ),
        Rule(
            "POS-003", Severity.REJECT, ErrorStage.VALIDATE, "quantity must be > 0",
            F.expr(f"exists({lines}, l -> l.quantity <= 0)"),
        ),
        Rule(
            "POS-004", Severity.REJECT, ErrorStage.VALIDATE, "negative unit_price or discount",
            F.expr(f"exists({lines}, l -> l.unit_price < 0 OR l.discount_amount < 0)"),
        ),
        Rule(
            "POS-005", Severity.REJECT, ErrorStage.VALIDATE, "discount exceeds line gross amount",
            F.expr(f"exists({lines}, l -> l.discount_amount > l.quantity * l.unit_price)"),
        ),
        Rule(
            "POS-006", Severity.REJECT, ErrorStage.VALIDATE, "total_amount != sum of line nets",
            F.abs(
                F.col("payload.total_amount")
                - F.expr(f"aggregate({lines}, CAST(0 AS DECIMAL(38, 6)), "
                         f"(acc, l) -> CAST(acc + {_LINE_NET} AS DECIMAL(38, 6)))")
            ) > F.lit(0.01),
        ),
        Rule(
            "POS-007", Severity.REJECT, ErrorStage.VALIDATE, "money value has > 2 decimal places",
            (F.col("payload.total_amount") != F.round("payload.total_amount", 2))
            | F.expr(f"exists({lines}, l -> l.unit_price != round(l.unit_price, 2)"
                     " OR l.discount_amount != round(l.discount_amount, 2))"),
        ),
        Rule(
            "POS-009", Severity.REJECT, ErrorStage.VALIDATE, "duplicate line_number",
            F.size(F.array_distinct(F.expr(f"transform({lines}, l -> l.line_number)")))
            != F.size(lines),
        ),
        Rule(
            "POS-008", Severity.WARN, ErrorStage.VALIDATE, "unknown payment_method",
            ~F.col("payload.payment_method").isin("CASH", "CARD", "EWALLET"),
        ),
    ]  # fmt: skip


def inventory_rules() -> list[Rule]:
    delta, mtype = F.col("payload.quantity_delta"), F.col("payload.movement_type")
    return [
        Rule(
            "INV-001", Severity.REJECT, ErrorStage.VALIDATE, "required movement field missing",
            _any_null("payload", ("movement_id", "store_id", "product_id", "movement_type",
                                  "quantity_delta", "quantity_on_hand_after")),
        ),
        Rule(
            "INV-002", Severity.REJECT, ErrorStage.VALIDATE, "quantity_delta sign invalid for type",
            (delta == 0)
            | ~mtype.isin("RECEIPT", "SALE", "ADJUSTMENT")
            | ((mtype == "RECEIPT") & (delta < 0))
            | ((mtype == "SALE") & (delta > 0)),
        ),
        Rule(
            "INV-003", Severity.WARN, ErrorStage.VALIDATE, "negative stock on hand",
            F.col("payload.quantity_on_hand_after") < 0,
        ),
    ]  # fmt: skip


def customer_rules() -> list[Rule]:
    return [
        Rule(
            "CUS-001", Severity.REJECT, ErrorStage.VALIDATE, "required customer field missing",
            _any_null("payload", ("customer_id", "loyalty_tier", "home_store_id", "signup_date")),
        ),
        Rule(
            "CUS-002", Severity.WARN, ErrorStage.VALIDATE, "unknown loyalty_tier",
            ~F.col("payload.loyalty_tier").isin("BASIC", "SILVER", "GOLD", "PLATINUM"),
        ),
    ]  # fmt: skip


def product_rules() -> list[Rule]:
    return [
        Rule(
            "PRD-001", Severity.REJECT, ErrorStage.VALIDATE, "required product field missing",
            _any_null("payload", ("product_id", "product_name", "brand", "category",
                                  "subcategory", "list_price", "unit_cost", "unit_of_measure",
                                  "is_active")),
        ),
        Rule(
            "PRD-002", Severity.REJECT, ErrorStage.VALIDATE, "negative price or cost",
            (F.col("payload.list_price") < 0) | (F.col("payload.unit_cost") < 0),
        ),
    ]  # fmt: skip


DATASET_RULES = {
    Dataset.POS_TRANSACTIONS: pos_rules,
    Dataset.INVENTORY_MOVEMENTS: inventory_rules,
    Dataset.CUSTOMER_EVENTS: customer_rules,
    Dataset.PRODUCT_EVENTS: product_rules,
}


def rules_for(
    dataset: Dataset, allowed_event_types: Sequence[str], future_tolerance_minutes: int
) -> list[Rule]:
    return [
        *envelope_rules(allowed_event_types, future_tolerance_minutes),
        *DATASET_RULES[dataset](),
    ]


def apply_rules(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Add `rejection` (struct or null), `dq_warnings` (array<string>) and `is_valid`."""
    rejection_type = "struct<rule_id:string,stage:string,message:string>"
    rejection: Column = F.lit(None).cast(rejection_type)
    # Build back-to-front so the FIRST rule in catalog order wins.
    for rule in reversed([r for r in rules if r.severity is Severity.REJECT]):
        rejection = F.when(
            F.coalesce(rule.violation, F.lit(True)),
            F.struct(
                F.lit(rule.rule_id).alias("rule_id"),
                F.lit(rule.stage.value).alias("stage"),
                F.lit(rule.message).alias("message"),
            ),
        ).otherwise(rejection)
    warnings = [
        F.when(F.coalesce(r.violation, F.lit(False)), F.lit(r.rule_id))
        for r in rules
        if r.severity is Severity.WARN
    ]
    dq_warnings = (
        F.array_compact(F.array(*warnings)) if warnings else F.array().cast("array<string>")
    )
    return df.withColumns({"rejection": rejection, "dq_warnings": dq_warnings}).withColumn(
        "is_valid", F.col("rejection").isNull()
    )
