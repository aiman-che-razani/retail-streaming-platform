"""Confluent wire format: 0x00 magic byte + 4-byte big-endian schema id + payload bytes."""

from __future__ import annotations

import struct
from dataclasses import dataclass

MAGIC_BYTE = 0
HEADER_SIZE = 5


@dataclass(frozen=True, slots=True)
class Framed:
    schema_id: int
    payload: bytes


def encode(schema_id: int, payload: bytes) -> bytes:
    return struct.pack(">bI", MAGIC_BYTE, schema_id) + payload


def decode(value: bytes) -> Framed:
    """Split a framed value. Raises ValueError if the bytes are not Confluent-framed."""
    if len(value) < HEADER_SIZE + 1 or value[0] != MAGIC_BYTE:
        raise ValueError("value is not in Confluent wire format")
    (schema_id,) = struct.unpack(">I", value[1:HEADER_SIZE])
    return Framed(schema_id=schema_id, payload=value[HEADER_SIZE:])
