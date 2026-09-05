"""Run existing ``django-health-check`` backends as worker-health checks.

Most Django projects that need this SDK already have health checks: a few
``BaseHealthCheckBackend`` subclasses registered with `plugin_dir`,
accumulated over years and encoding real knowledge about the deployment.
Asking a team to rewrite those as a condition of adopting worker-health is
how an adoption stalls.

So they are adapted rather than replaced. An existing backend keeps working
exactly as written, and gains the things it did not have:

* it runs on the SDK's scheduler thread instead of inside the request, so a
  slow backend no longer makes the health endpoint slow -- django-health-check
  runs every backend on every request, uncached, which is why its own docs
  warn the endpoint is a denial-of-service vector;
* it gets a timeout, thresholds and hysteresis, so one blip is not an outage;
* it gets backoff, so a failing dependency is not asked every few seconds;
* its ``critical_service`` flag maps onto worker-health's `critical`, which
  means a non-critical failure degrades rather than 503s -- django-health-check
  returns 500 for a critical failure and has no degraded state at all.

What is deliberately NOT carried over: django-health-check's database
backend performs a full write cycle -- create a row, update it, delete it --
on every check. worker-health's guardrail is that probes are read-only, so
if that backend is adapted it keeps its own behaviour and this docstring is
the warning. Prefer the SDK's own `django_db` probe for the database.
"""
from __future__ import annotations

import time

from worker_health.checks.base import BaseCheck, CheckContext
from worker_health.core.model import CheckResult, ErrorCategory, Evidence, Status


class DjangoHealthCheckAdapter(BaseCheck):
    """Wraps one ``BaseHealthCheckBackend`` subclass."""

    def __init__(self, backend, *, name: str = "", dependency: str = "") -> None:
        self._backend_factory = backend if isinstance(backend, type) else type(backend)
        self.name = name or _identifier(backend)
        self.dependency = dependency

    @property
    def critical(self) -> bool:
        """The backend's own opinion, for the caller to register with."""
        return bool(getattr(self._backend_factory, "critical_service", True))

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        started = time.perf_counter()

        # A fresh instance per run: `errors` accumulates on the instance, so
        # a shared one would report a failure from ten minutes ago forever.
        backend = self._backend_factory()
        try:
            backend.run_check()
        except Exception as exc:      # noqa: BLE001 - a backend that raises
            return self.fail(ctx, _classify(exc), started,
                             evidence=Evidence.PROBED,
                             detail="health check backend raised")

        errors = list(getattr(backend, "errors", ()) or ())
        if not errors:
            return self.ok(ctx, started, evidence=Evidence.PROBED,
                           backend=type(backend).__name__)

        # The message can be anything the backend chose to put in it, so it
        # goes through the same redaction as every other detail string.
        return self.fail(
            ctx, _classify(errors[0]), started, evidence=Evidence.PROBED,
            detail=_first_message(errors), backend=type(backend).__name__,
            error_count=len(errors),
        )

    def classify(self, exc: BaseException) -> ErrorCategory:
        return _classify(exc)


def install_health_check_plugins(monitor, *, interval: float = 30.0,
                                 timeout: float = 5.0, prefix: str = "",
                                 skip: tuple[str, ...] = (),
                                 **spec_kwargs) -> list[str]:
    """Register every ``plugin_dir`` backend as a worker-health check.

    Each keeps its own ``critical_service`` setting. Returns the names
    registered, so a caller can log or assert on them.

    ``skip`` takes identifiers to leave out -- typically
    ``("DatabaseBackend",)``, because the SDK's own `django_db` probe covers
    the database read-only, and django-health-check's writes.
    """
    names: list[str] = []
    for backend in discover_backends():
        identifier = _identifier(backend)
        if identifier in skip:
            continue
        check = DjangoHealthCheckAdapter(backend, name=f"{prefix}{identifier}")
        monitor.register(
            check, name=check.name, critical=check.critical,
            interval=interval, timeout=timeout, **spec_kwargs,
        )
        names.append(check.name)
    return names


def discover_backends() -> list:
    """Every backend registered with django-health-check, or an empty list.

    The registry's internal shape has changed across releases, so this reads
    it defensively: an unrecognised shape means no adapters, never an
    exception at boot.
    """
    try:
        from health_check.plugins import plugin_dir
    except Exception:
        return []

    registry = getattr(plugin_dir, "_registry", None)
    if registry is None:
        registry = plugin_dir
    try:
        entries = list(registry)
    except TypeError:
        return []

    backends = []
    for entry in entries:
        # Historically a set of classes; later a dict of {class: options}.
        candidate = entry[0] if isinstance(entry, tuple) else entry
        if isinstance(candidate, type):
            backends.append(candidate)
    return backends


def _identifier(backend) -> str:
    identify = getattr(backend, "identifier", None)
    if callable(identify):
        try:
            return str(identify(backend) if isinstance(backend, type) else identify())
        except Exception:
            pass
    return backend.__name__ if isinstance(backend, type) else type(backend).__name__


def _first_message(errors) -> str:
    first = errors[0]
    message = getattr(first, "message", None)
    return str(message if message is not None else first)


def _classify(error) -> ErrorCategory:
    """Map a HealthCheckException onto the closed taxonomy.

    django-health-check's exception classes are the vocabulary here:
    ServiceUnavailable, ServiceReturnedUnexpectedResult,
    ServiceWarning. They carry no structured cause, so this reads the class
    name and the message rather than inventing precision that is not there.
    """
    name = type(error).__name__.lower()
    text = str(error).lower()

    if "warning" in name:
        return ErrorCategory.STALE
    if "timeout" in text or "timed out" in text:
        return ErrorCategory.TIMEOUT
    if "refused" in text:
        return ErrorCategory.CONNECTION_REFUSED
    if "unexpected" in name or "unexpected" in text:
        return ErrorCategory.PROTOCOL_ERROR
    if "unavailable" in name or "unavailable" in text:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.UNKNOWN
