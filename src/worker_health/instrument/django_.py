"""Django ORM auto-instrumentation.

Django has no query event hook, so ``CursorWrapper.execute`` /
``executemany`` are the funnel -- every ORM query, every ``raw()``, and
every ``connection.cursor()`` call goes through them.

The class is shared by all databases in the process, so the wrapper routes
by connection alias: ``default`` and ``replica`` report as two dependencies
if they were registered as two.  A query on an alias nobody registered is
passed straight through and costs one dict lookup.
"""
from __future__ import annotations

import time

from ..checks.postgres import classify_postgres
from .context import is_health_probe_active
from .recorder import TrafficRecorder

_FLAG = "_worker_health_instrumented"
_TARGETS: dict[str, TrafficRecorder] = {}


def instrument_django_db(monitor, dependency_name: str = "postgres",
                         alias: str = "default"):
    """Record every ORM query on ``alias`` as observed traffic."""
    from django.db.backends.utils import CursorWrapper

    _TARGETS[alias] = TrafficRecorder(monitor, dependency_name, classify_postgres)

    if getattr(CursorWrapper, _FLAG, False):
        return

    original_execute = CursorWrapper.execute
    original_executemany = CursorWrapper.executemany

    def _recorder(self) -> TrafficRecorder | None:
        db = getattr(self, "db", None)
        return _TARGETS.get(getattr(db, "alias", "default"))

    def execute_wrapper(self, sql, params=None):
        recorder = _recorder(self)
        # The health probe issues SELECT 1 through this same wrapper.
        # Counting it would make a silent worker look busy -- the exact
        # false green the evidence ladder exists to prevent.
        if recorder is None or is_health_probe_active():
            return original_execute(self, sql, params)

        started = time.perf_counter()
        try:
            result = original_execute(self, sql, params)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return result

    def executemany_wrapper(self, sql, param_list):
        recorder = _recorder(self)
        if recorder is None or is_health_probe_active():
            return original_executemany(self, sql, param_list)

        started = time.perf_counter()
        try:
            result = original_executemany(self, sql, param_list)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return result

    CursorWrapper.execute = execute_wrapper
    CursorWrapper.executemany = executemany_wrapper
    CursorWrapper._worker_health_original = (original_execute, original_executemany)
    setattr(CursorWrapper, _FLAG, True)


def instrument_django_cache(monitor, dependency_name: str = "redis",
                            alias: str = "default"):
    """Observe Django's cache backend when it is Redis-backed.

    Reaches the underlying redis-py client through the documented
    ``cache.client.get_client`` surface of django-redis / Django's own
    ``RedisCache``; if neither shape is present it does nothing rather than
    guessing at internals.
    """
    from django.core.cache import caches

    cache = caches[alias]
    client = _underlying_redis(cache)
    if client is None:
        return None

    from .redis_ import instrument_redis

    instrument_redis(client, monitor, dependency_name)
    return client


def _underlying_redis(cache):
    getter = getattr(getattr(cache, "client", None), "get_client", None)
    if getter is not None:
        try:
            return getter(write=True)
        except TypeError:
            return getter()
        except Exception:
            return None
    # Django >= 4.0 built-in RedisCache
    for attr in ("_cache", "_client"):
        candidate = getattr(cache, attr, None)
        client = getattr(candidate, "get_client", None)
        if client is not None:
            try:
                return client(None, write=True)
            except Exception:
                continue
    return None


def uninstrument_django_db() -> None:
    from django.db.backends.utils import CursorWrapper

    original = getattr(CursorWrapper, "_worker_health_original", None)
    if original is not None:
        CursorWrapper.execute, CursorWrapper.executemany = original
        delattr(CursorWrapper, "_worker_health_original")
    _TARGETS.clear()
    if hasattr(CursorWrapper, _FLAG):
        delattr(CursorWrapper, _FLAG)
