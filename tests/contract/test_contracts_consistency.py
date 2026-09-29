"""Contract tests: the JSON Schemas, examples and topic declarations must agree.

These tests guard the architecture's sources of truth (docs/architecture/event-contracts.md,
kafka/config/topics.yaml). They have no runtime dependencies beyond jsonschema and PyYAML.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft7Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = REPO_ROOT / "kafka" / "schemas"
EXAMPLES_DIR = SCHEMA_DIR / "examples"
TOPICS_FILE = REPO_ROOT / "kafka" / "config" / "topics.yaml"

pytestmark = pytest.mark.contract


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_files() -> list[Path]:
    return sorted(SCHEMA_DIR.glob("*.schema.json"))


def _schema_for_example(example: Path) -> dict[str, Any]:
    # Example files are named "<schema-stem>.<description>.json"
    schema_stem = example.name.split(".")[0]
    return _load_json(SCHEMA_DIR / f"{schema_stem}.schema.json")


def _topics() -> list[dict[str, Any]]:
    return yaml.safe_load(TOPICS_FILE.read_text(encoding="utf-8"))["topics"]


@pytest.mark.parametrize("schema_path", _schema_files(), ids=lambda p: p.name)
def test_schema_is_valid_draft7(schema_path: Path) -> None:
    Draft7Validator.check_schema(_load_json(schema_path))


@pytest.mark.parametrize("schema_path", _schema_files(), ids=lambda p: p.name)
def test_schema_objects_are_closed(schema_path: Path) -> None:
    """Closed content models make 'add optional field' the only compatible change (ADR-002)."""
    schema = _load_json(schema_path)
    assert schema["additionalProperties"] is False
    assert schema["definitions"]["metadata"]["additionalProperties"] is False
    assert schema["definitions"]["payload"]["additionalProperties"] is False


def test_metadata_definition_identical_across_schemas() -> None:
    definitions = {p.name: _load_json(p)["definitions"]["metadata"] for p in _schema_files()}
    reference_name, reference = next(iter(definitions.items()))
    for name, definition in definitions.items():
        assert definition == reference, f"metadata in {name} differs from {reference_name}"


@pytest.mark.parametrize(
    "example", sorted((EXAMPLES_DIR / "valid").glob("*.json")), ids=lambda p: p.name
)
def test_valid_examples_pass_schema(example: Path) -> None:
    validator = Draft7Validator(_schema_for_example(example))
    errors = [e.message for e in validator.iter_errors(_load_json(example))]
    assert errors == []


# Rule IDs whose violations JSON Schema can detect. Others (e.g. POS-006 total mismatch,
# INV-002 sign vs movement type) are schema-valid and must be caught by consumer-side rules.
SCHEMA_DETECTABLE_RULES = {"ENV-003", "POS-003"}


@pytest.mark.parametrize(
    "example", sorted((EXAMPLES_DIR / "invalid").glob("*.json")), ids=lambda p: p.name
)
def test_invalid_examples_are_classified_correctly(example: Path) -> None:
    rule_id = example.name.split(".")[1]
    validator = Draft7Validator(_schema_for_example(example))
    schema_errors = list(validator.iter_errors(_load_json(example)))
    if rule_id in SCHEMA_DETECTABLE_RULES:
        assert schema_errors, f"{example.name} should fail JSON Schema validation"
    else:
        assert not schema_errors, (
            f"{example.name} is expected to be schema-valid (semantic rule {rule_id} only)"
        )


def test_malformed_example_is_not_json() -> None:
    for path in (EXAMPLES_DIR / "invalid").glob("*.txt"):
        with pytest.raises(json.JSONDecodeError):
            json.loads(path.read_text(encoding="utf-8"))


def test_every_primary_topic_has_retry_and_dlq_with_same_key() -> None:
    by_name = {t["name"]: t for t in _topics()}
    primaries = [t for t in by_name.values() if t["role"] == "primary"]
    assert len(primaries) == 4
    for primary in primaries:
        for suffix in ("retry", "dlq"):
            companion = by_name.get(f"{primary['name']}.{suffix}")
            assert companion is not None, f"missing {primary['name']}.{suffix}"
            assert companion["role"] == suffix
            assert companion["key"] == primary["key"]


def test_topic_value_schemas_exist() -> None:
    for topic in _topics():
        if topic["value_schema"] is not None:
            assert (SCHEMA_DIR / topic["value_schema"]).is_file(), topic["name"]
        else:
            assert topic["role"] == "dlq", "only DLQ topics may carry schema-less raw bytes"


def test_topic_configs_never_compact() -> None:
    """Compaction would destroy the change history SCD2 relies on (kafka-topology.md §4)."""
    for topic in _topics():
        assert topic["config"]["cleanup.policy"] == "delete", topic["name"]
