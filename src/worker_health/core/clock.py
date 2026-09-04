"""Clock protocol plus the two implementations.

All interval and staleness arithmetic uses ``monotonic()``.  ``wall()`` is
display only.
"""
from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def wall(self) -> float: ...


class MonotonicClock:
    __slots__ = ()

    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> float:
        return time.time()


class FakeClock:
    """Deterministic clock for the unit tier.

    ``advance_wall`` moves only the epoch, which is what an NTP correction
    does.  A wall-clock implementation would report every check stale at that
    moment; a monotonic one is unaffected, and the test proves it.
    """

    __slots__ = ("_mono", "_wall")

    def __init__(self, mono: float = 1000.0, wall: float = 1_700_000_000.0) -> None:
        self._mono = mono
        self._wall = wall

    def monotonic(self) -> float:
        return self._mono

    def wall(self) -> float:
        return self._wall

    def advance(self, seconds: float) -> None:
        self._mono += seconds
        self._wall += seconds

    def advance_wall(self, seconds: float) -> None:
        self._wall += seconds
