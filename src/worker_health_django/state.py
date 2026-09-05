"""Process-global handle on the wired health objects.

A Django management command cannot receive the monitor as an argument --
Django constructs the command -- so the objects wired during ``AppConfig.
ready()`` have to be reachable from anywhere.  This is that reach, kept to
one module so there is exactly one global in the package and it is obvious
where it lives.
"""
from __future__ import annotations

import threading

_LOCK = threading.Lock()
_STATE: dict = {"health": None}


def set_health_state(health=None, *, monitor=None, tracker=None) -> None:
    """Record the wired objects.  Called by ``autowire``.

    Accepts either the ``WorkerHealth`` bundle or the individual pieces, so
    a project that builds its own monitor can still publish it here and get
    ``get_tracker()`` working in its commands.
    """
    with _LOCK:
        if health is not None:
            _STATE["health"] = health
        elif monitor is not None or tracker is not None:
            _STATE["health"] = _Partial(monitor, tracker)


def get_health():
    return _STATE["health"]


def get_monitor():
    health = _STATE["health"]
    return getattr(health, "monitor", None) if health else None


def get_tracker():
    """The tracker whose ``@handler`` decorator a worker command uses.

    Returns a no-op tracker rather than None when health is disabled, so a
    management command decorated with ``@tracker.handler`` still runs in a
    context where WORKER_HEALTH is off -- a test suite, a local shell, a
    migration container.  Health being switched off must not change whether
    business code executes.
    """
    health = _STATE["health"]
    tracker = getattr(health, "tracker", None) if health else None
    return tracker if tracker is not None else _NullTracker()


def reset() -> None:
    with _LOCK:
        _STATE["health"] = None


class _Partial:
    __slots__ = ("monitor", "tracker")

    def __init__(self, monitor, tracker) -> None:
        self.monitor = monitor
        self.tracker = tracker


class _NullTracker:
    """Every method a worker calls, doing nothing, costing nothing."""

    default_queue = "default"
    state = None
    monitor = None

    def handler(self, _fn=None, *, queue: str | None = None):
        def deco(fn):
            return fn
        return deco(_fn) if _fn is not None else deco

    def processing(self, queue: str | None = None):
        return _null_context()

    def stage(self, queue: str, stage: str):
        return _null_context()

    def dependency(self, name: str, classify=None):
        return _null_context()


def _null_context():
    import contextlib

    @contextlib.contextmanager
    def cm():
        yield
    return cm()
