"""The integration surface: one decorator is the whole required change.

``@tracker.handler`` detects whether it wrapped a coroutine and adapts, so
the same import works under both runners.  It records message received,
processing start and end, outcome, duration and queue -- which is the
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
                 default_queue: str = "default", broker_state=None) -> None:
        self.monitor = monitor
        self.state = state or ProcessingState()
        self.default_queue = default_queue
        # Registering here is what makes per-queue message metrics appear on
        # /metrics without the worker having to wire anything else up.
        monitor.attach_processing(default_queue, self.state, broker_state)

    # -- handler decorator ------------------------------------------------ #

    def handler(self, _fn=None, *, queue: str | None = None):
        """Wrap one unit of work so the processing check can see it.

        Applies to more shapes than a plain function, because real consumers
        are not all plain functions:

            @tracker.handler(queue="billing.in")
            def handle(msg): ...                     # function

            @tracker.handler()
            async def handle(msg): ...               # coroutine

            @tracker.handler()
            def stream(msg):                         # generator -- one unit
                yield from split(msg)                # of work, not one per
                                                     # yield

            @tracker.handler()
            class Handler:                           # callable instance:
                def __call__(self, msg): ...         # __call__ is wrapped,
                                                     # the class is returned

        A class-based consumer is the shape Django worker commands reach for
        most often (state on the instance, ``__call__`` as the entry point),
        and ``inspect.iscoroutinefunction`` returns False for an instance
        with an ``async def __call__`` -- which silently produced a wrapper
        that timed how long it took to CREATE the coroutine, not to run it.
        """
        q = self._ensure_queue(queue)

        def deco(target):
            if isinstance(target, type):
                return self._wrap_class(target, q)
            return self._wrap_callable(target, q)

        return deco(_fn) if _fn is not None else deco

    def consumer_class(self, _cls=None, *, queue: str | None = None,
                       method: str = "__call__"):
        """Track one method of a class-based consumer.

            @tracker.consumer_class(queue="billing.in", method="handle")
            class BillingConsumer:
                def handle(self, msg): ...

        ``handler`` applied to a class does the same thing for ``__call__``;
        this is for consumers whose entry point has a name.
        """
        q = self._ensure_queue(queue)

        def deco(cls):
            return self._wrap_class(cls, q, method=method)

        return deco(_cls) if _cls is not None else deco

    # -- wrapping ---------------------------------------------------------- #

    def _ensure_queue(self, queue: str | None) -> str:
        q = queue or self.default_queue
        if q not in self.monitor.processing:
            existing = self.monitor.processing.get(self.default_queue)
            self.monitor.attach_processing(
                q, self.state, existing.broker_state if existing else None
            )
        return q

    def _wrap_class(self, cls, q: str, method: str = "__call__"):
        """Replace one method on the class, in place.

        In place rather than by subclassing so that ``isinstance`` checks,
        registries keyed on the class, and pickling all keep working -- a
        consumer class is usually referenced by name somewhere else.
        """
        original = getattr(cls, method, None)
        if original is None:
            raise TypeError(
                f"@handler on {cls.__name__} needs a {method}() to wrap"
            )
        if getattr(original, "__worker_health_tracked__", False):
            return cls
        setattr(cls, method, self._wrap_callable(original, q))
        return cls

    def _wrap_callable(self, fn, q: str):
        """Dispatch on what the target actually is when called."""
        if getattr(fn, "__worker_health_tracked__", False):
            return fn

        target = fn
        instance = False
        if not (inspect.isfunction(fn) or inspect.ismethod(fn)
                or inspect.isbuiltin(fn)):
            # A callable instance: what runs is type(fn).__call__, and that
            # is what has to be inspected.  Dispatching on the instance
            # instead is the bug this exists to avoid -- for an
            # ``async def __call__`` every inspect predicate says False, so
            # the sync wrapper was chosen and it timed how long it took to
            # CREATE the coroutine rather than to run it: every message
            # looked like it took eight microseconds.
            call = getattr(type(fn), "__call__", None)
            if call is not None and callable(fn):
                target, instance = call, True

        if inspect.isasyncgenfunction(target):
            wrapper = self._async_gen_wrapper(fn, q)
        elif inspect.iscoroutinefunction(target):
            wrapper = self._async_wrapper(fn, q)
        elif inspect.isgeneratorfunction(target):
            wrapper = self._gen_wrapper(fn, q)
        else:
            wrapper = self._sync_wrapper(fn, q)

        if instance:
            # Hand back something that is still the caller's object: a bare
            # function would drop every other attribute and method on it.
            return _TrackedCallable(fn, wrapper)
        wrapper.__worker_health_tracked__ = True
        return wrapper

    def _sync_wrapper(self, fn, q: str):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            started = self._begin(q)
            try:
                out = fn(*args, **kwargs)
            except Exception:
                self._end(q, started, ok=False)
                raise
            self._end(q, started, ok=True)
            return out
        return wrapper

    def _async_wrapper(self, fn, q: str):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            started = self._begin(q)
            try:
                out = await fn(*args, **kwargs)
            except Exception:
                self._end(q, started, ok=False)
                raise
            self._end(q, started, ok=True)
            return out
        return wrapper

    def _gen_wrapper(self, fn, q: str):
        """One message in, many values out -- still one unit of work.

        Timed across the whole iteration, so a handler that streams rows for
        four seconds reports four seconds.  ``yield from`` rather than a
        for-loop keeps ``send()``, ``throw()`` and ``close()`` working, which
        a naive re-yield quietly breaks.
        """
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            started = self._begin(q)
            ok = True
            try:
                result = yield from fn(*args, **kwargs)
            except Exception:
                ok = False
                raise
            finally:
                # GeneratorExit is a BaseException and is deliberately not
                # caught: a consumer that stops reading early has not failed.
                self._end(q, started, ok=ok)
            return result
        return wrapper

    def _async_gen_wrapper(self, fn, q: str):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            started = self._begin(q)
            ok = True
            agen = fn(*args, **kwargs)
            try:
                async for item in agen:
                    yield item
            except Exception:
                ok = False
                raise
            finally:
                self._end(q, started, ok=ok)
        return wrapper

    def _begin(self, queue: str) -> float:
        self.state.on_receive(queue)
        self.monitor.note_activity()
        return time.perf_counter()

    def _end(self, queue: str, started: float, *, ok: bool) -> None:
        ms = (time.perf_counter() - started) * 1000.0
        if ok:
            self.state.on_success(ms, queue)
        else:
            self.state.on_failure(ms, queue)
        self.monitor.timings.observe(T.handler_duration(queue), ms)
        self.monitor.note_activity()

    # -- context managers for code that cannot be decorated ---------------- #

    @contextlib.contextmanager
    def processing(self, queue: str | None = None):
        q = queue or self.default_queue
        started = self._begin(q)
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

        Rarely needed now: ``worker_health.instrument`` records the same
        thing automatically for SQLAlchemy, Django, redis-py and pika.  This
        remains for a client the SDK has no adapter for -- an SDK for a
        vendor API, say -- where one context manager is still cheaper than
        writing an adapter.
        """
        from .instrument.context import is_health_probe_active

        started = time.perf_counter()
        try:
            yield
        except Exception as exc:
            if is_health_probe_active():
                raise
            category = ErrorCategory.UNKNOWN
            if classify is not None:
                try:
                    category = classify(exc)
                except Exception:
                    pass
            self.monitor.traffic.failure(name, category)
            raise
        if not is_health_probe_active():
            self.monitor.traffic.success(
                name, (time.perf_counter() - started) * 1000.0
            )


class _TrackedCallable:
    """A callable instance, timed, without losing the instance.

    ``@tracker.handler`` applied to a class patches the class in place and
    keeps everything; applied to an already-built object there is no such
    seam, because ``__call__`` is looked up on the type.  This proxy is the
    seam: it forwards attribute access to the object it wraps, so a consumer
    with ``.close()`` or ``.stats`` still has them.
    """

    __slots__ = ("_wh_target", "_wh_call")
    __worker_health_tracked__ = True

    def __init__(self, target, call) -> None:
        object.__setattr__(self, "_wh_target", target)
        object.__setattr__(self, "_wh_call", call)

    def __call__(self, *args, **kwargs):
        return self._wh_call(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._wh_target, name)

    def __repr__(self) -> str:
        return f"<worker-health tracked {self._wh_target!r}>"
