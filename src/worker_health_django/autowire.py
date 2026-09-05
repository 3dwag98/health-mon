"""Turn ``settings.WORKER_HEALTH`` into a running monitor.

Everything a Django worker needs, decided from settings:

    ORM queries observed          (CursorWrapper instrumentation)
    cache commands observed       (redis-py instrumentation, when Redis-backed)
    probes installed              (the probe factory, from PROBES)
    health server started         (its own thread, never Django's)

The one thing this module worries about that a plain script does not: it
runs inside ``AppConfig.ready()``, which fires for EVERY Django entry point
-- ``migrate``, ``collectstatic``, ``shell``, the autoreloader's parent
process, a pytest run.  Starting an HTTP server and a scheduler in all of
those is wrong, so the guards below decide whether this process is actually
a worker before wiring anything.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any, Mapping

from worker_health import setup_worker_health
from worker_health.config import HealthConfig

from .state import set_health_state

logger = logging.getLogger("worker_health")

# Entry points that must never start a health server: they are short-lived,
# they often run many at once (so the port would collide), and none of them
# consumes messages.
_SKIP_COMMANDS = frozenset({
    "migrate", "makemigrations", "collectstatic", "shell", "shell_plus",
    "dbshell", "createsuperuser", "test", "check", "showmigrations",
    "loaddata", "dumpdata", "compilemessages", "makemessages", "sqlmigrate",
    "diffsettings", "flush", "squashmigrations",
})


def should_wire(config: Mapping[str, Any], argv: list[str] | None = None) -> bool:
    """Decide whether THIS process should run a health monitor."""
    if not config.get("ENABLED", False):
        return False

    argv = sys.argv if argv is None else argv
    command = argv[1] if len(argv) > 1 else ""

    # The autoreloader runs the real process as a child with
    # RUN_MAIN=true; wiring in the parent as well would bind the port twice.
    if os.environ.get("RUN_MAIN") == "false":
        return False

    allowed = config.get("COMMANDS")
    if allowed:
        # An explicit allow-list is the precise answer, and the one a team
        # should reach for once they have more than one worker command.
        return command in set(allowed)

    return command not in _SKIP_COMMANDS


def build_config(config: Mapping[str, Any]) -> HealthConfig:
    """Django's UPPER_CASE settings onto the SDK's config object."""
    resolved = HealthConfig.from_mapping(config)
    resolved = HealthConfig.from_env(base=resolved)
    if not resolved.service or resolved.service == "worker":
        resolved.service = str(config.get("SERVICE", "django-worker"))
    # Django's request/response cycle is not what runs here; the worker's
    # own loop is, and it is a thread.
    resolved.runner = str(config.get("RUNNER", resolved.runner or "thread"))
    return resolved


def build_context(config: Mapping[str, Any]) -> dict:
    """Resolve the objects that ``"@name"`` references point at.

    Import paths rather than objects, because Django settings are imported
    long before an engine or a client exists.  ``"myapp.deps:redis_client"``
    is resolved here, at ``ready()`` time, when the app registry is loaded
    and importing application modules is finally safe.
    """
    context: dict[str, Any] = {}

    for name, target in (config.get("CONTEXT") or {}).items():
        context[name] = _resolve(target)

    if "redis_client" not in context:
        client = _default_cache_client(config.get("CACHE_ALIAS", "default"))
        if client is not None:
            context["redis_client"] = client
    return context


def autowire(config: Mapping[str, Any]):
    """Wire and start.  Returns the ``WorkerHealth`` bundle, or None."""
    resolved = build_config(config)
    context = build_context(config)

    health = setup_worker_health(
        config=resolved,
        context=context,
        # Django's ORM and cache are instrumented explicitly below; the
        # generic autowiring has no engine or client to find in the context
        # because Django hides both behind its own handles.
        instrument=True,
    )

    _instrument_django(health.monitor, config)
    _adopt_health_check_plugins(health.monitor, config)
    set_health_state(health)
    return health


def _adopt_health_check_plugins(monitor, config: Mapping[str, Any]) -> None:
    """Register existing django-health-check backends, if asked to.

        WORKER_HEALTH = {
            "ADOPT_HEALTH_CHECK_PLUGINS": True,
            # The SDK's own django_db probe covers the database read-only;
            # django-health-check's writes a row on every check.
            "HEALTH_CHECK_SKIP": ["DatabaseBackend", "CacheBackend"],
        }
    """
    if not config.get("ADOPT_HEALTH_CHECK_PLUGINS"):
        return
    try:
        from .compat import install_health_check_plugins

        install_health_check_plugins(
            monitor,
            interval=float(config.get("HEALTH_CHECK_INTERVAL", 30.0)),
            timeout=float(config.get("HEALTH_CHECK_TIMEOUT", 5.0)),
            skip=tuple(config.get("HEALTH_CHECK_SKIP", ())),
        )
    except Exception:
        # An adoption failure must not stop a worker from starting; the
        # SDK's own probes are unaffected.
        logger.exception("could not adopt django-health-check plugins")


def _instrument_django(monitor, config: Mapping[str, Any]) -> None:
    from worker_health.instrument.django_ import (
        instrument_django_cache,
        instrument_django_db,
    )

    for alias, dependency in (config.get("DATABASES") or {"default": "postgres"}).items():
        try:
            instrument_django_db(monitor, dependency_name=dependency, alias=alias)
        except Exception:
            continue

    cache_alias = config.get("CACHE_ALIAS", "default")
    if cache_alias:
        try:
            instrument_django_cache(monitor, dependency_name=config.get(
                "CACHE_DEPENDENCY", "redis"), alias=cache_alias)
        except Exception:
            # No cache configured, or not a Redis backend.  The redis probe
            # (if any) still works; it just reports `probed` rather than
            # `observed`.
            pass


def _resolve(target: Any) -> Any:
    """Accept a live object, a callable, or a "module:attribute" path."""
    if not isinstance(target, str):
        return target() if callable(target) and not hasattr(target, "execute_command") else target

    module_name, _, attribute = target.partition(":")
    if not attribute:
        raise ValueError(f"WORKER_HEALTH CONTEXT entry {target!r} must be 'module:attribute'")
    import importlib

    value = getattr(importlib.import_module(module_name), attribute)
    # A factory function is called; an already-built client is used as-is.
    return value() if callable(value) and not hasattr(value, "execute_command") else value


def _default_cache_client(alias: str):
    try:
        from worker_health.instrument.django_ import _underlying_redis
        from django.core.cache import caches

        return _underlying_redis(caches[alias])
    except Exception:
        return None
