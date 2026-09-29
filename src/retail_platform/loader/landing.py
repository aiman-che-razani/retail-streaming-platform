"""Landing-zone discovery and archiving (the Spark <-> loader contract, overview.md §6)."""

from __future__ import annotations

import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from retail_platform.observability.logging import get_logger

log = get_logger(__name__)

SUCCESS_MARKER = "_SUCCESS"
_QUERY = re.compile(r"^query_id=([0-9a-fA-F-]+)$")
_BATCH = re.compile(r"^batch_id=(\d{12})$")


@dataclass(frozen=True, slots=True)
class LandingBatch:
    dataset: str
    query_id: str
    batch_id: int
    path: Path
    files: tuple[Path, ...]

    @property
    def relative(self) -> str:
        """`<dataset>/query_id=<id>/batch_id=<n>` — also the stage sub-path."""
        return f"{self.dataset}/query_id={self.query_id}/batch_id={self.batch_id:012d}"


def discover(landing_dir: Path, datasets: list[str], limit: int) -> list[LandingBatch]:
    """Completed batches (those with `_SUCCESS`), oldest batch id first, per dataset.

    Directories without `_SUCCESS` are still being written (or were abandoned by a crashed
    attempt that Spark will overwrite on replay) and are never touched.
    """
    batches: list[LandingBatch] = []
    for dataset in datasets:
        root = landing_dir / dataset
        if not root.is_dir():
            continue
        for query_dir in sorted(root.iterdir()):
            query = _QUERY.match(query_dir.name)
            if not query or not query_dir.is_dir():
                continue
            for batch_path in sorted(query_dir.iterdir()):
                batch = _BATCH.match(batch_path.name)
                if not batch or not (batch_path / SUCCESS_MARKER).exists():
                    continue
                files = tuple(sorted(batch_path.glob("*.parquet")))
                batches.append(
                    LandingBatch(dataset, query.group(1), int(batch.group(1)), batch_path, files)
                )
    batches.sort(key=lambda b: (b.batch_id, b.dataset))
    return batches[:limit]


def backlog(landing_dir: Path, datasets: list[str]) -> dict[str, int]:
    counts = dict.fromkeys(datasets, 0)
    for batch in discover(landing_dir, datasets, limit=10**9):
        counts[batch.dataset] += 1
    return counts


def archive(batch: LandingBatch, landing_dir: Path, archive_dir: Path) -> Path:
    target = archive_dir / batch.path.relative_to(landing_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():  # re-archiving after a crash between copy and delete
        shutil.rmtree(target)
    shutil.move(str(batch.path), str(target))
    return target


def purge_archive(archive_dir: Path, retention_days: int, now: float | None = None) -> int:
    """Delete archived batch directories older than the retention period."""
    if not archive_dir.is_dir():
        return 0
    cutoff = (now or time.time()) - retention_days * 86400
    removed = 0
    for success in archive_dir.glob(f"*/query_id=*/batch_id=*/{SUCCESS_MARKER}"):
        batch_dir = success.parent
        if success.stat().st_mtime < cutoff:
            shutil.rmtree(batch_dir, ignore_errors=True)
            removed += 1
    if removed:
        log.info("archive_purged", batches=removed, retention_days=retention_days)
    return removed
