"""Timing instrumentation.

Three questions this answers, which are genuinely different:

1. How long does the *worker* take to handle a message?
2. How long does a *health check* take to run?
3. How far behind the worker is the health signal?

(3) is the one that has no equivalent in any library surveyed, and it is the
number that tells you whether a green light is current or merely recent.  A
check that takes 2ms but last observed the worker 90 seconds ago is not a
2ms-fresh signal.
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable


def _percentile(ordered: list[float], q: float) -> float:
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    idx = int(round(q * (len(ordered) - 1)))
    return ordered[max(0, min(idx, len(ordered) - 1))]


@dataclass
class Window:
    """Bounded rolling window of durations, in milliseconds."""

    maxlen: int = 512
    _samples: Deque[float] = field(default_factory=lambda: deque(maxlen=512))
    _count: int = 0
    _total: float = 0.0

    def __post_init__(self) -> None:
        self._samples = deque(maxlen=self.maxlen)

    def add(self, ms: float) -> None:
        self._samples.append(ms)
        self._count += 1
        self._total += ms

    def summary(self) -> Dict[str, float | int]:
        ordered = sorted(self._samples)
        return {
            "count": self._count,
            "p50_ms": round(_percentile(ordered, 0.50), 3),
            "p95_ms": round(_percentile(ordered, 0.95), 3),
            "p99_ms": round(_percentile(ordered, 0.99), 3),
            "max_ms": round(max(ordered), 3) if ordered else 0.0,
            "mean_ms": round(self._total / self._count, 3) if self._count else 0.0,
        }

    @property
    def last(self) -> float | None:
        return self._samples[-1] if self._samples else None


class Timings:
    """Thread-safe collection of named windows.

    Both runners write here from different threads, and the HTTP transport
    reads from a third, so every mutation takes the lock.  The lock is held
    for a deque append and two float adds -- it is never held across I/O.
    """

    def __init__(self, maxlen: int = 512) -> None:
        self._lock = threading.Lock()
        self._maxlen = maxlen
        self._windows: Dict[str, Window] = {}

    def observe(self, key: str, ms: float) -> None:
        with self._lock:
            w = self._windows.get(key)
            if w is None:
                w = self._windows[key] = Window(maxlen=self._maxlen)
            w.add(ms)

    def summary(self, key: str) -> Dict[str, float | int] | None:
        with self._lock:
            w = self._windows.get(key)
            return w.summary() if w else None

    def last(self, key: str) -> float | None:
        with self._lock:
            w = self._windows.get(key)
            return w.last if w else None

    def keys(self) -> Iterable[str]:
        with self._lock:
            return tuple(self._windows)

    def export(self) -> Dict[str, Dict[str, float | int]]:
        with self._lock:
            return {k: w.summary() for k, w in self._windows.items()}


# Canonical metric keys, so producers and consumers cannot drift.
def check_duration(name: str) -> str:
    """Wall time the probe itself consumed."""
    return f"check.{name}.duration_ms"


def check_schedule_lag(name: str) -> str:
    """How late a check ran relative to when it was due.

    Rises when the scheduler is starved -- the early warning that the monitor
    itself is the thing in trouble.
    """
    return f"check.{name}.schedule_lag_ms"


def check_evidence_age(name: str) -> str:
    """Age of the signal backing the verdict, at the moment of the verdict."""
    return f"check.{name}.evidence_age_ms"


def handler_duration(queue: str) -> str:
    """Wall time the worker's own handler consumed."""
    return f"worker.{queue}.handler_ms"


def stage_duration(queue: str, stage: str) -> str:
    """Per-stage timing inside a multi-dependency handler."""
    return f"worker.{queue}.stage.{stage}_ms"


def detection_latency(name: str) -> str:
    """Fault onset to reported FAILING.  Populated by the fault harness."""
    return f"check.{name}.detection_ms"


# The headline delta.
WORKER_HEALTH_DELTA = "delta.worker_to_health_ms"
LOOP_LAG = "runtime.loop_lag_ms"
SNAPSHOT_BUILD = "runtime.snapshot_build_ms"
