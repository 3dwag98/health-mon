"""The probe-suppression flag.

Auto-instrumentation records every database query, every Redis command and
every broker event as *observed* traffic.  A health probe issues database
queries and Redis commands too -- so without a flag, the monitor would
watch its own probes, decide the worker has recent traffic, and stop
probing.  The evidence label would read `observed` when nothing but the
health check had touched the dependency all day, which is precisely the
false green this package exists to prevent.

A ``ContextVar`` rather than a thread-local because it is correct under
both runners: it is set inside the worker thread that executes the probe
(thread runner) or inside the task/executor call that awaits it (asyncio
runner), and in neither case does it leak into the application's own code.
"""
from __future__ import annotations

import contextlib
import contextvars

_PROBE_ACTIVE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "worker_health_probe_active", default=False
)


def is_health_probe_active() -> bool:
    """True while a synthetic probe is running on this thread/task.

    Instrumentation hooks consult this and record nothing when it is set.
    """
    return _PROBE_ACTIVE.get()


@contextlib.contextmanager
def probe_scope():
    """Mark everything executed inside as health-probe traffic."""
    token = _PROBE_ACTIVE.set(True)
    try:
        yield
    finally:
        _PROBE_ACTIVE.reset(token)
