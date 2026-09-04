"""Processing health -- the check with no equivalent in any surveyed library.

Everything else answers "can I reach my dependencies".  This answers "am I
still doing work", and the hard part is that a quiet queue and a wedged
consumer look identical from inside the process.  Only queue depth
separates them, which is why this check takes the broker state as an input
rather than guessing from timers alone.
"""
from __future__ import annotations

import threading
import time

from ..core.model import CheckResult, ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


class ProcessingState:
    """Counters written by the @track.handler decorator."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.received = 0
        self.succeeded = 0
        self.failed = 0
        self.in_flight = 0
        self.last_received_at: float | None = None
        self.last_success_at: float | None = None
        self.last_failure_at: float | None = None
        self.consecutive_failures = 0
        self.last_duration_ms: float | None = None
        self.started_at = time.monotonic()

    def on_receive(self) -> float:
        with self._lock:
            self.received += 1
            self.in_flight += 1
            self.last_received_at = time.monotonic()
            return self.last_received_at

    def on_success(self, duration_ms: float) -> None:
        with self._lock:
            self.succeeded += 1
            self.in_flight = max(0, self.in_flight - 1)
            self.consecutive_failures = 0
            self.last_success_at = time.monotonic()
            self.last_duration_ms = duration_ms

    def on_failure(self, duration_ms: float) -> None:
        with self._lock:
            self.failed += 1
            self.in_flight = max(0, self.in_flight - 1)
            self.consecutive_failures += 1
            self.last_failure_at = time.monotonic()
            self.last_duration_ms = duration_ms

    def read(self) -> dict:
        with self._lock:
            return dict(
                received=self.received, succeeded=self.succeeded, failed=self.failed,
                in_flight=self.in_flight, last_received_at=self.last_received_at,
                last_success_at=self.last_success_at, last_failure_at=self.last_failure_at,
                consecutive_failures=self.consecutive_failures,
                last_duration_ms=self.last_duration_ms, started_at=self.started_at,
            )


class ProcessingCheck(BaseCheck):
    def __init__(
        self,
        state: ProcessingState,
        *,
        name: str = "processing",
        broker_state=None,
        max_idle: float = 60.0,
        max_since_success: float = 120.0,
        poison_threshold: int = 10,
    ) -> None:
        self.name = name
        self.dependency = ""
        self._state = state
        self._broker = broker_state
        self._max_idle = max_idle
        self._max_since_success = max_since_success
        self._poison_threshold = poison_threshold

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        started = time.perf_counter()
        s = self._state.read()

        depth = None
        if self._broker is not None:
            depth = self._broker.read().get("queue_depth")

        observed = {
            "received": s["received"], "succeeded": s["succeeded"],
            "failed": s["failed"], "in_flight": s["in_flight"],
            "consecutive_failures": s["consecutive_failures"],
        }
        if depth is not None:
            observed["queue_depth"] = depth
        if s["last_duration_ms"] is not None:
            observed["last_duration_ms"] = round(s["last_duration_ms"], 3)

        idle_for = None
        if s["last_received_at"] is not None:
            idle_for = ctx.now - s["last_received_at"]
            observed["idle_seconds"] = round(idle_for, 2)

        age_ms = round(idle_for * 1000.0, 3) if idle_for is not None else None

        # A handler failing over and over on the same message.
        if s["consecutive_failures"] >= self._poison_threshold:
            return self.fail(
                ctx, ErrorCategory.POISON_LOOP, started, evidence=Evidence.OBSERVED,
                detail="handler is failing repeatedly", **observed,
            )

        # Nothing has ever arrived.  Not an error -- a worker that has been
        # up for ten seconds on a quiet queue is perfectly healthy.
        if s["last_received_at"] is None:
            if depth and depth > 0 and (ctx.now - s["started_at"]) > self._max_idle:
                return self.fail(
                    ctx, ErrorCategory.NOT_CONSUMING, started,
                    evidence=Evidence.OBSERVED,
                    detail="messages are queued but none have been received",
                    **observed,
                )
            return CheckResult(
                name=self.name, status=Status.OK, checked_at=ctx.now,
                wall_clock=ctx.wall, evidence=Evidence.OBSERVED,
                latency_ms=_ms(started), evidence_age_ms=None,
                detail="idle, nothing received yet", observed=observed,
            )

        # THE discrimination.  Idle with an empty queue is healthy, forever.
        # Idle with a backlog is a stuck consumer.
        if idle_for is not None and idle_for > self._max_idle:
            if depth is not None and depth > 0:
                return self.fail(
                    ctx, ErrorCategory.NOT_CONSUMING, started,
                    evidence=Evidence.OBSERVED,
                    detail="backlog present but nothing received recently",
                    **observed,
                )
            # Quiet queue.  Explicitly OK.
            return CheckResult(
                name=self.name, status=Status.OK, checked_at=ctx.now,
                wall_clock=ctx.wall, evidence=Evidence.OBSERVED,
                latency_ms=_ms(started), evidence_age_ms=age_ms,
                detail="idle, queue is empty", observed=observed,
            )

        # Receiving but never completing: work is arriving and dying.
        if s["last_success_at"] is not None:
            since_success = ctx.now - s["last_success_at"]
            observed["seconds_since_success"] = round(since_success, 2)
            if since_success > self._max_since_success and s["received"] > s["succeeded"]:
                return self.fail(
                    ctx, ErrorCategory.STALLED, started, evidence=Evidence.OBSERVED,
                    detail="messages received but none completed recently", **observed,
                )
        elif s["received"] > 0 and (ctx.now - s["started_at"]) > self._max_since_success:
            return self.fail(
                ctx, ErrorCategory.STALLED, started, evidence=Evidence.OBSERVED,
                detail="messages received but none have ever completed", **observed,
            )

        return CheckResult(
            name=self.name, status=Status.OK, checked_at=ctx.now, wall_clock=ctx.wall,
            evidence=Evidence.OBSERVED, latency_ms=_ms(started),
            evidence_age_ms=age_ms, observed=observed,
        )


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)
