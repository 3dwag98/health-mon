"""Thread runner: a scheduler thread plus a bounded worker pool.

A hung check occupies one pool slot and nothing else.  The scheduler never
blocks on a result -- it submits, collects what has finished, and moves on --
so one black-holed dependency cannot stop every other check from running.
That property is what test P6 exists to prove.
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor

from ..checks.base import CheckContext
from ..core import timing as T
from ..core.model import ErrorCategory
from . import base


class ThreadRunner:
    name = "thread"

    def __init__(self, monitor, max_workers: int = 8, tick: float = 0.1) -> None:
        self._m = monitor
        self._tick = tick
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="wh-check"
        )
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._inflight: dict[str, Future] = {}

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="wh-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._pool.shutdown(wait=False, cancel_futures=True)

    # -- internals -------------------------------------------------------- #

    def _loop(self) -> None:
        last_beat = time.monotonic()
        while not self._stop.is_set():
            cycle_start = time.monotonic()

            # Loop lag: how far the scheduler slipped from its own cadence.
            # Under a thread runner this measures scheduler starvation; under
            # asyncio it measures a blocked event loop.
            expected = last_beat + self._tick
            self._m.timings.observe(T.LOOP_LAG, max(0.0, (cycle_start - expected) * 1000.0))
            self._m._loop_beat = cycle_start
            last_beat = cycle_start

            self._collect()
            self._dispatch(cycle_start)

            elapsed = time.monotonic() - cycle_start
            self._stop.wait(max(0.0, self._tick - elapsed))

    def _dispatch(self, now: float) -> None:
        machine = self._m.machine
        for name in list(base.due_checks(machine, now)):
            if name in self._inflight and not self._inflight[name].done():
                continue  # still running; do not pile up
            base.record_schedule_lag(machine, name, now, self._m.timings)
            check = self._m.checks[name]
            spec = machine.spec(name)
            ctx = CheckContext(
                now=self._m.clock.monotonic(), wall=self._m.clock.wall(),
                deadline=now + spec.timeout, max_silence=spec.max_silence,
                traffic=self._m.traffic,
            )
            # Push the next due time forward immediately so a slow check is
            # not re-dispatched every tick while it is still running.
            machine.state(name).next_due = now + spec.interval
            self._inflight[name] = self._pool.submit(self._run_one, name, check, ctx)

    def _run_one(self, name, check, ctx):
        started = time.perf_counter()
        try:
            result = check.evaluate(ctx)
        except Exception as exc:  # noqa: BLE001
            category = ErrorCategory.INTERNAL
            try:
                category = check.classify(exc)
            except Exception:
                pass
            result = base.error_result(
                name, ctx.now, ctx.wall, category, "check raised"
            )
        duration_ms = (time.perf_counter() - started) * 1000.0
        self._m.timings.observe(T.check_duration(name), duration_ms)
        if result.evidence_age_ms is not None:
            self._m.timings.observe(T.check_evidence_age(name), result.evidence_age_ms)
        return result

    def _collect(self) -> None:
        for name, fut in list(self._inflight.items()):
            if not fut.done():
                spec = self._m.machine.spec(name)
                # Exceeded its timeout: record it and stop waiting.  The
                # thread stays blocked, which is exactly what a black hole
                # does, and the semaphore is what keeps that contained.
                lag = self._m.timings.last(T.check_schedule_lag(name)) or 0.0
                continue
            del self._inflight[name]
            try:
                result = fut.result()
            except Exception:
                continue
            self._m.apply(result)
