"""The integration surface: one decorator is the whole required change.

``@track.handler`` detects whether it wrapped a coroutine and adapts, so the
same import works under both runners.  It records message received,
processing start and end, outcome, duration and error class -- which is the
entire input to the processing check.

Cost per message: one monotonic clock read, a few integer increments under a
lock, and one deque append.  At the soak load of 200 msg/s that is not
measurable.
"""
from __future__ import annotations

import contextlib
import functools
import inspect
import time

from .checks.processing import ProcessingState
from .core import timing as T
from .core.model import ErrorCategory


class Tracker:
    """Binds a monitor to the processing state the decorators write into."""

    def __init__(self, monitor, state: ProcessingState | None = None,
                 default_queue: str = "default") -> None:
        self.monitor = monitor
        self.state = state or ProcessingState()
        self.default_queue = default_queue

    # -- handler decorator ------------------------------------------------ #

    def handler(self, _fn=None, *, queue: str | None = None):
        q = queue or self.default_queue

        def deco(fn):
            if inspect.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def awrapper(*args, **kwargs):
                    started = self._begin()
                    try:
                        out = await fn(*args, **kwargs)
                    except Exception:
                        self._end(q, started, ok=False)
                        raise
                    self._end(q, started, ok=True)
                    return out
                return awrapper

            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                started = self._begin()
                try:
                    out = fn(*args, **kwargs)
                except Exception:
                    self._end(q, started, ok=False)
                    raise
                self._end(q, started, ok=True)
                return out
            return wrapper

        return deco(_fn) if _fn is not None else deco

    def _begin(self) -> float:
        self.state.on_receive()
        self.monitor.note_activity()
        return time.perf_counter()

    def _end(self, queue: str, started: float, *, ok: bool) -> None:
        ms = (time.perf_counter() - started) * 1000.0
        if ok:
            self.state.on_success(ms)
        else:
            self.state.on_failure(ms)
        self.monitor.timings.observe(T.handler_duration(queue), ms)
        self.monitor.note_activity()

    # -- context managers for code that cannot be decorated ---------------- #

    @contextlib.contextmanager
    def processing(self, queue: str | None = None):
        q = queue or self.default_queue
        started = self._begin()
        try:
            yield
        except Exception:
            self._end(q, started, ok=False)
            raise
        self._end(q, started, ok=True)

    @contextlib.contextmanager
    def stage(self, queue: str, stage: str):
        """Times one step inside a multi-dependency handler."""
        started = time.perf_counter()
        try:
            yield
        finally:
            self.monitor.timings.observe(
                T.stage_duration(queue, stage),
                (time.perf_counter() - started) * 1000.0,
            )

    @contextlib.contextmanager
    def dependency(self, name: str, classify=None):
        """Records a real dependency call into the traffic log.

        This is what makes passive observation possible: the health check
        for `name` can then stand on the worker's own successful calls
        instead of issuing a synthetic probe.
        """
        started = time.perf_counter()
        try:
            yield
        except Exception as exc:
            category = ErrorCategory.UNKNOWN
            if classify is not None:
                try:
                    category = classify(exc)
                except Exception:
                    pass
            self.monitor.traffic.failure(name, category)
            raise
        self.monitor.traffic.success(
            name, (time.perf_counter() - started) * 1000.0
        )
