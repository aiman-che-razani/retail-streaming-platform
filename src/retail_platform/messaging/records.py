"""Records emitted by the simulator, before serialisation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from retail_platform.contracts.models import Envelope


@dataclass(frozen=True, slots=True)
class OutgoingEvent:
    """One record to publish.

    Normal events carry an `envelope` and go through the schema-validating serializer.
    Fault-injected events that must *bypass* validation (simulating a buggy upstream)
    carry `raw_body` instead; `framed` says whether the sink prepends a valid Confluent
    wire-format header (schema id of the topic) or sends the bytes as-is.
    """

    topic: str
    key: str
    envelope: Envelope[Any] | None = None
    raw_body: bytes | None = None
    framed: bool = True
    fault: str | None = None

    def __post_init__(self) -> None:
        if (self.envelope is None) == (self.raw_body is None):
            raise ValueError("exactly one of envelope or raw_body must be set")

    @property
    def event_id(self) -> str | None:
        return str(self.envelope.metadata.event_id) if self.envelope else None

    @property
    def event_type(self) -> str | None:
        return self.envelope.metadata.event_type.value if self.envelope else None

    @property
    def payload(self) -> BaseModel | None:
        return self.envelope.payload if self.envelope else None
