"""Shared fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from retail_platform.contracts.catalog import TopicCatalog

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def catalog() -> TopicCatalog:
    return TopicCatalog.load(
        REPO_ROOT / "kafka" / "config" / "topics.yaml", REPO_ROOT / "kafka" / "schemas"
    )
