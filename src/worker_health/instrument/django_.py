"""Django ORM auto-instrumentation.

Uses ``connection.execute_wrappers`` -- Django's own documented database
instrumentation hook, added in 2.0 -- rather than monkeypatching
``CursorWrapper.execute``.  Four things that buys:

* it is a supported API, so a Django upgrade that reorganises the cursor
  internals does not silently stop the instrumentation;
* it covers ``executemany`` through the ``many`` flag, with no second patch;
* it composes.  Django Debug Toolbar, django-silk and Scout all append to
  the same list, so worker-health sits alongside them instead of fighting
  over one method;
* it is per-connection, so routing by alias is natural rather than a lookup
  bolted onto a shared class.

The wrapper is installed from a ``connection_created`` receiver, and also
directly onto any connection that already exists -- Django will usually
have connected before an AppConfig's ``ready()`` runs, and a receiver alone
would miss those.
"""
from __future__ import annotations

import time

from ..checks.postgres import classify_postgres
from .context import is_health_probe_active
from .recorder import TrafficRecorder

DISPATCH_UID = "worker_health.instrument.django"

# alias -> recorder.  Read at query time, so registering a second alias does
# not need the wrapper to be reinstalled.
_TARGETS: dict[str, TrafficRecorder] = {}
_INSTALLED = False


class _QueryWrapper:
    """The execute wrapper itself.

    One instance is shared by every connection: it routes on the alias in
    the context Django hands it, so a `default` and a `replica` connection
    report as two dependencies without two wrappers.
    """

    __slots__ = ()

    def __call__(self, execute, sql, params, many, context):
        recorder = _TARGETS.get(_alias(context))
        # The health probe issues SELECT 1 through this same path.  Counting
        # it would make a silent worker look busy -- the exact false green
        # the evidence ladder exists to prevent.
        if recorder is None or is_health_probe_active():
            return execute(sql, params, many, context)

        started = time.perf_counter()
        try:
            result = execute(sql, params, many, context)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return result

    def __eq__(self, other) -> bool:
        # So an "is it already installed" check works across instances.
        return isinstance(other, _QueryWrapper)

    def __hash__(self) -> int:
        return hash(_QueryWrapper)


_WRAPPER = _QueryWrapper()


def _alias(context) -> str:
    connection = (context or {}).get("connection")
    return getattr(connection, "alias", "default")


def _install_on(connection) -> None:
    wrappers = getattr(connection, "execute_wrappers", None)
    if wrappers is None or _WRAPPER in wrappers:
        return
    wrappers.append(_WRAPPER)


def _on_connection_created(sender=None, connection=None, **kwargs) -> None:
    if connection is not None:
        _install_on(connection)


def instrument_django_db(monitor, dependency_name: str = "postgres",
                         alias: str = "default"):
    """Record every ORM query on ``alias`` as observed traffic."""
    global _INSTALLED

    from django.db import connections
    from django.db.backends.signals import connection_created

    _TARGETS[alias] = TrafficRecorder(monitor, dependency_name, classify_postgres)

    if _INSTALLED:
        return
    # New connections, including the ones a thread opens later.
    connection_created.connect(_on_connection_created, dispatch_uid=DISPATCH_UID)
    # And the ones already open: by the time an AppConfig is ready, Django
    # has usually connected at least once.
    for connection in connections.all():
        _install_on(connection)
    _INSTALLED = True


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
    """Remove the wrapper and the receiver.  For fixtures and test runs."""
    global _INSTALLED

    try:
        from django.db import connections
        from django.db.backends.signals import connection_created
    except Exception:
        _TARGETS.clear()
        _INSTALLED = False
        return

    connection_created.disconnect(dispatch_uid=DISPATCH_UID)
    for connection in connections.all():
        wrappers = getattr(connection, "execute_wrappers", None)
        if wrappers:
            connection.execute_wrappers = [w for w in wrappers
                                           if not isinstance(w, _QueryWrapper)]
    _TARGETS.clear()
    _INSTALLED = False
