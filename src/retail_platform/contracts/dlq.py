"""Dead-letter record contract (ADR-008), shared by the Spark writer and the redrive tool.

A DLQ record keeps the original key and value bytes untouched; all diagnostics travel in
headers so the record can be replayed byte-for-byte.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final

ERROR_CODE: Final = "dlq.error.code"
ERROR_MESSAGE: Final = "dlq.error.message"
ERROR_STAGE: Final = "dlq.error.stage"
SOURCE_TOPIC: Final = "dlq.source.topic"
SOURCE_PARTITION: Final = "dlq.source.partition"
SOURCE_OFFSET: Final = "dlq.source.offset"
FAILED_AT: Final = "dlq.failed_at"
APP: Final = "dlq.app"
REDRIVE_COUNT: Final = "dlq.redrive_count"

ALL_HEADERS: Final = (
    ERROR_CODE,
    ERROR_MESSAGE,
    ERROR_STAGE,
    SOURCE_TOPIC,
    SOURCE_PARTITION,
    SOURCE_OFFSET,
    FAILED_AT,
    APP,
    REDRIVE_COUNT,
)

MAX_REDRIVES: Final = 3


class ErrorStage(StrEnum):
    DECODE = "decode"  # not Confluent wire format
    PARSE = "parse"  # not a JSON envelope
    VALIDATE = "validate"  # parsed, but violates a REJECT rule
