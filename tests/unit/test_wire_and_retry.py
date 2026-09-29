from __future__ import annotations

import pytest

from retail_platform.messaging import wire
from retail_platform.messaging.clients import retry_until

pytestmark = pytest.mark.unit


def test_wire_round_trip() -> None:
    framed = wire.encode(42, b'{"a":1}')
    assert framed[0] == 0
    decoded = wire.decode(framed)
    assert decoded.schema_id == 42
    assert decoded.payload == b'{"a":1}'


@pytest.mark.parametrize(
    "value", [b"", b"\x00\x00\x00\x00\x01", b'{"a":1}', b"\x01\x00\x00\x00\x01{}"]
)
def test_wire_rejects_unframed_values(value: bytes) -> None:
    with pytest.raises(ValueError, match="wire format"):
        wire.decode(value)


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def test_retry_until_succeeds_after_transient_failures() -> None:
    clock = _FakeClock()
    attempts = iter([ConnectionError("down"), ConnectionError("down"), "ok"])

    def probe() -> str:
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    assert (
        retry_until(
            probe,
            what="dep",
            timeout_seconds=30,
            retry_on=(ConnectionError,),
            sleep=clock.sleep,
            clock=clock.time,
        )
        == "ok"
    )
    assert clock.sleeps == [0.5, 1.0]  # exponential backoff


def test_retry_until_gives_up_after_timeout() -> None:
    clock = _FakeClock()

    def probe() -> None:
        raise ConnectionError("still down")

    with pytest.raises(TimeoutError, match="dep not ready"):
        retry_until(
            probe,
            what="dep",
            timeout_seconds=10,
            retry_on=(ConnectionError,),
            sleep=clock.sleep,
            clock=clock.time,
        )
    assert clock.now <= 10


def test_retry_until_does_not_retry_unexpected_errors() -> None:
    def probe() -> None:
        raise ValueError("bug")

    with pytest.raises(ValueError, match="bug"):
        retry_until(probe, what="dep", timeout_seconds=10, retry_on=(ConnectionError,))
