"""Asyncio runner.

Same policy core, different mechanics: checks run in the default executor so
a blocking driver call cannot stall the loop, and the loop-lag probe measures
what it is actually supposed to measure here -- a wedged event loop.
"""
from __future__ import annotations

import asyncio
import threading
import time

from ..checks.base import CheckContext
from ..core import timing as T
from ..core.model import ErrorCategory
from . import base


class AsyncioRunner:
    name = "asyncio"

    def __init__(self, monitor, tick: float = 0.1, loop=None) -> None:
        self._m = monitor
        self._tick = tick
        self._loop = loop
        self._own_loop = loop is None
        self._thread: threading.Thread | None = None
        self._task = None
        self._stopping = asyncio.Event() if loop is not None else None
        self._inflight: set[str] = set()

    def start(self) -> None:
        if self._own_loop:
            self._thread = threading.Thread(
                target=self._run_forever, name="wh-loop", daemon=True
            )
            self._thread.start()
            # Wait for the loop to exist before anything schedules onto it.
            for _ in range(200):
                if self._loop is not None and self._loop.is_running():
                    break
                time.sleep(0.01)
        else:
            self._task = asyncio.run_coroutine_threadsafe(self._main(), self._loop)

    def _run_forever(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.create_task(self._main())
        self._loop.run_forever()

    def stop(self, timeout: float = 5.0) -> None:
        loop = self._loop
        if loop is None:
            return
        self._stop_requested = True
        if not self._own_loop:
            return

        def _shutdown():
            # Cancel outstanding work before stopping, or the loop is torn
            # down with tasks still pending and asyncio complains at exit.
            for task in asyncio.all_tasks(loop):
                task.cancel()
            loop.call_later(0, loop.stop)

        try:
            loop.call_soon_threadsafe(_shutdown)
        except RuntimeError:
            return
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        try:
            if not loop.is_running():
                loop.close()
        except Exception:
            pass

    _stop_requested = False

    async def _main(self) -> None:
        last_beat = time.monotonic()
        while not self._stop_requested:
            cycle_start = time.monotonic()
            expected = last_beat + self._tick
            # On asyncio this is the real signal: if a coroutine blocks the
            # loop, this number is the length of the block.
            self._m.timings.observe(T.LOOP_LAG, max(0.0, (cycle_start - expected) * 1000.0))
            self._m._loop_beat = cycle_start
            last_beat = cycle_start

            self._dispatch(cycle_start)
            try:
                await asyncio.sleep(
                    max(0.0, self._tick - (time.monotonic() - cycle_start))
                )
            except asyncio.CancelledError:
                return

    def _dispatch(self, now: float) -> None:
        machine = self._m.machine
        for name in list(base.due_checks(machine, now)):
            if name in self._inflight:
                continue
            base.record_schedule_lag(machine, name, now, self._m.timings)
            spec = machine.spec(name)
            machine.state(name).next_due = now + spec.interval
            self._inflight.add(name)
            asyncio.ensure_future(self._run_one(name, spec))

    async def _run_one(self, name, spec) -> None:
        check = self._m.checks[name]
        ctx = CheckContext(
            now=self._m.clock.monotonic(), wall=self._m.clock.wall(),
            deadline=time.monotonic() + spec.timeout, max_silence=spec.max_silence,
            traffic=self._m.traffic,
        )
        started = time.perf_counter()
        loop = asyncio.get_running_loop()
        try:
            if asyncio.iscoroutinefunction(getattr(check, "evaluate_async", None)):
                coro = check.evaluate_async(ctx)
            else:
                coro = loop.run_in_executor(None, check.evaluate, ctx)
            result = await asyncio.wait_for(coro, timeout=spec.timeout)
        except asyncio.TimeoutError:
            result = base.timeout_result(name, ctx.now, ctx.wall, spec.timeout)
        except Exception as exc:  # noqa: BLE001
            category = ErrorCategory.INTERNAL
            try:
                category = check.classify(exc)
            except Exception:
                pass
            result = base.error_result(name, ctx.now, ctx.wall, category, "check raised")
        finally:
            self._inflight.discard(name)

        duration_ms = (time.perf_counter() - started) * 1000.0
        self._m.timings.observe(T.check_duration(name), duration_ms)
        if result.evidence_age_ms is not None:
            self._m.timings.observe(T.check_evidence_age(name), result.evidence_age_ms)
        self._m.apply(result)
