"""Redis auto-instrumentation, sync and async.

redis-py has no event hooks, so the only place to observe every command is
``execute_command`` -- every high-level method (``get``, ``setex``,
``pipeline.execute``) funnels through it.

That method lives on the CLASS, so the patch is necessarily global to the
process.  Two consequences are handled here rather than left as surprises:

* **Per-client routing.**  A worker with a cache client and a lock client
  wants two dependency names, not one.  The wrapper reads the target off
  the INSTANCE, so each client reports under its own name and any
  un-registered client is simply ignored.
* **Idempotence.**  Patching twice would double-count every command and,
  worse, nest the wrappers so the second unpatch could not undo the first.
  A flag on the class prevents it.
"""
from __future__ import annotations

import time

from ..checks.redis_ import classify_redis
from .recorder import TrafficRecorder

_ATTR = "_worker_health_target"
_FLAG = "_worker_health_instrumented"


def instrument_redis_sync(redis_client, monitor, dependency_name: str = "redis"):
    """Observe every command issued by ``redis_client``."""
    setattr(redis_client, _ATTR, TrafficRecorder(monitor, dependency_name, classify_redis))

    client_class = redis_client.__class__
    if getattr(client_class, _FLAG, False):
        return redis_client

    original = client_class.execute_command

    def wrapper(self, *args, **kwargs):
        recorder = getattr(self, _ATTR, None)
        if recorder is None:
            return original(self, *args, **kwargs)
        started = time.perf_counter()
        try:
            result = original(self, *args, **kwargs)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return result

    client_class.execute_command = wrapper
    client_class._worker_health_original = original
    setattr(client_class, _FLAG, True)
    return redis_client


def instrument_redis_async(redis_client, monitor, dependency_name: str = "redis"):
    """The same, for ``redis.asyncio``."""
    setattr(redis_client, _ATTR, TrafficRecorder(monitor, dependency_name, classify_redis))

    client_class = redis_client.__class__
    if getattr(client_class, _FLAG, False):
        return redis_client

    original = client_class.execute_command

    async def wrapper(self, *args, **kwargs):
        recorder = getattr(self, _ATTR, None)
        if recorder is None:
            return await original(self, *args, **kwargs)
        started = time.perf_counter()
        try:
            result = await original(self, *args, **kwargs)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return result

    client_class.execute_command = wrapper
    client_class._worker_health_original = original
    setattr(client_class, _FLAG, True)
    return redis_client


def instrument_redis(redis_client, monitor, dependency_name: str = "redis"):
    """Pick sync or async by inspecting the client, so callers need not.

    ``redis.asyncio.Redis`` and ``redis.Redis`` share a method name and
    differ only in whether it returns an awaitable; the module path is the
    reliable discriminator.
    """
    module = type(redis_client).__module__ or ""
    if "asyncio" in module or _is_async(redis_client):
        return instrument_redis_async(redis_client, monitor, dependency_name)
    return instrument_redis_sync(redis_client, monitor, dependency_name)


def _is_async(client) -> bool:
    import inspect

    return inspect.iscoroutinefunction(getattr(client, "execute_command", None))


def uninstrument_redis(client_class) -> None:
    """Restore the original method.  For fixtures and long-lived test runs."""
    original = getattr(client_class, "_worker_health_original", None)
    if original is not None:
        client_class.execute_command = original
        delattr(client_class, "_worker_health_original")
    if hasattr(client_class, _FLAG):
        delattr(client_class, _FLAG)
