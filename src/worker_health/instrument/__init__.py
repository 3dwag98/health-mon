"""Auto-instrumentation: the reason business code needs one decorator.

Without this layer, "observed" evidence costs a context manager around
every query, every cache read and every publish -- which is exactly the
per-call wrapping the brief rules out, and which nobody keeps up to date
anyway.  With it, a worker hands the SDK the client objects it already has
and every real call becomes evidence.

Each adapter lives in its own module and imports its driver lazily, so a
worker that uses Redis but not SQLAlchemy never imports SQLAlchemy.
"""
from __future__ import annotations

from .context import is_health_probe_active, probe_scope
from .recorder import TrafficRecorder, recorder_for

__all__ = [
    "instrument_sqlalchemy", "instrument_redis", "instrument_redis_sync",
    "instrument_redis_async", "instrument_pika_connection",
    "instrument_pika_channel", "instrument_django_db", "instrument_django_cache",
    "autowire_context", "is_health_probe_active", "probe_scope",
    "TrafficRecorder", "recorder_for",
]


def __getattr__(name):
    """Lazy re-export, so importing this package imports no drivers."""
    if name == "instrument_sqlalchemy":
        from .sqlalchemy_ import instrument_sqlalchemy
        return instrument_sqlalchemy
    if name in ("instrument_redis", "instrument_redis_sync", "instrument_redis_async"):
        from . import redis_
        return getattr(redis_, name)
    if name in ("instrument_pika_connection", "instrument_pika_channel"):
        from . import pika_
        return getattr(pika_, name)
    if name in ("instrument_django_db", "instrument_django_cache"):
        from . import django_
        return getattr(django_, name)
    raise AttributeError(name)


# Context keys that carry a client the SDK knows how to instrument.  The
# names match the ones the YAML config uses for `@references`, so one
# context dict serves both purposes.
_SQLALCHEMY_KEYS = ("db_engine", "engine", "sqlalchemy_engine")
_REDIS_KEYS = ("redis_client", "redis", "cache_client")
_PIKA_CONNECTION_KEYS = ("amqp_connection", "connection", "broker_connection", "pika_connection")
_PIKA_CHANNEL_KEYS = ("amqp_channel", "channel", "consumer_channel")


def autowire_context(monitor, context: dict, *, names: dict | None = None) -> dict:
    """Instrument every client the context holds, by shape not by name.

    Returns a map of ``{context key: dependency name}`` describing what was
    wired, which the setup facade logs once at boot.  Detection is on duck
    type -- a SQLAlchemy engine has ``.pool`` and ``.dialect``, a pika
    connection has one of the ``add_on_connection_*_callback`` registrars --
    because a worker team's variable names are their own business.
    """
    names = names or {}
    wired: dict[str, str] = {}
    if not context:
        return wired

    for key, value in context.items():
        if value is None:
            continue
        dependency = names.get(key) or _default_name(key, value)
        try:
            if _is_sqlalchemy_engine(value):
                from .sqlalchemy_ import instrument_sqlalchemy

                instrument_sqlalchemy(value, monitor, dependency)
                wired[key] = dependency
            elif _is_redis_client(value):
                from .redis_ import instrument_redis

                instrument_redis(value, monitor, dependency)
                wired[key] = dependency
            elif _is_pika_connection(value):
                from .pika_ import instrument_pika_connection

                instrument_pika_connection(value, monitor,
                                           context.get("broker_state"), dependency)
                wired[key] = dependency
        except Exception:
            # Instrumentation is an optimisation of evidence quality, never
            # a precondition for starting.  A worker whose driver version
            # moved the method we patch still boots, still probes, and still
            # reports -- with `probed` evidence instead of `observed`.
            continue
    return wired


def _default_name(key: str, value) -> str:
    if key in _SQLALCHEMY_KEYS or _is_sqlalchemy_engine(value):
        return "postgres"
    if key in _REDIS_KEYS or _is_redis_client(value):
        return "redis"
    if key in _PIKA_CONNECTION_KEYS or _is_pika_connection(value):
        return "rabbitmq"
    return key


def _is_sqlalchemy_engine(value) -> bool:
    target = getattr(value, "sync_engine", value)
    return hasattr(target, "pool") and hasattr(target, "dialect")


def _is_redis_client(value) -> bool:
    return (
        hasattr(value, "execute_command")
        and hasattr(value, "connection_pool")
        and "redis" in (type(value).__module__ or "")
    )


# BlockingConnection and the async connections expose DIFFERENT subsets of
# these: BlockingConnection has the blocked/unblocked pair but no closed
# callback (it raises on a closed connection instead), while SelectConnection
# and friends have all three.  Detecting on any one of them is what makes the
# common blocking-consumer worker instrumentable at all.
_PIKA_CALLBACKS = (
    "add_on_connection_blocked_callback",
    "add_on_connection_unblocked_callback",
    "add_on_connection_closed_callback",
)


def _is_pika_connection(value) -> bool:
    return any(hasattr(value, name) for name in _PIKA_CALLBACKS)
