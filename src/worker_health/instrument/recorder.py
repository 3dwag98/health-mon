"""The single funnel every instrumentation hook writes through.

Centralised for three reasons that each caused a bug when they were not:

1. Probe suppression has to be checked in exactly one place, or one
   forgotten hook makes every probe look like real traffic.
2. Classification has to go through the closed taxonomy, or a driver
   message reaches the traffic log and from there a log line.
3. Latency has to land in the timing windows as well as the traffic log,
   or the dependency percentiles on the dashboard stay empty while the
   evidence says `observed`.
"""
from __future__ import annotations

import time

from ..core import timing as T
from ..core.model import ErrorCategory
from .context import is_health_probe_active


class TrafficRecorder:
    """Binds one dependency name to one monitor's traffic log."""

    __slots__ = ("monitor", "dependency", "_classify")

    def __init__(self, monitor, dependency: str, classify=None) -> None:
        self.monitor = monitor
        self.dependency = dependency
        self._classify = classify

    # -- outcomes -------------------------------------------------------- #

    def success(self, duration_ms: float) -> None:
        if is_health_probe_active():
            return                      # our own probe: never counts as traffic
        self.monitor.traffic.success(self.dependency, duration_ms)
        self.monitor.timings.observe(T.dependency_duration(self.dependency), duration_ms)

    def failure(self, exc: BaseException | None = None,
                category: ErrorCategory | None = None) -> None:
        if is_health_probe_active():
            return
        self.monitor.traffic.failure(self.dependency, category or self.classify(exc))

    def classify(self, exc: BaseException | None) -> ErrorCategory:
        if exc is None:
            return ErrorCategory.UNKNOWN
        if self._classify is None:
            return ErrorCategory.UNKNOWN
        try:
            return self._classify(exc)
        except Exception:
            return ErrorCategory.UNKNOWN

    # -- convenience ----------------------------------------------------- #

    def wrap(self, fn):
        """Wrap a synchronous callable so its outcome is recorded."""
        def call(*args, **kwargs):
            started = time.perf_counter()
            try:
                out = fn(*args, **kwargs)
            except Exception as exc:
                self.failure(exc)
                raise
            self.success((time.perf_counter() - started) * 1000.0)
            return out
        return call


def recorder_for(monitor, dependency: str) -> TrafficRecorder:
    """Build a recorder with the classifier that matches the dependency.

    The classifier is chosen by name because that is the only thing the
    caller reliably knows at wiring time; an unknown name still records,
    it just reports `unknown` as the category rather than guessing wrong.
    """
    from ..checks.postgres import classify_postgres
    from ..checks.rabbitmq import classify_amqp
    from ..checks.redis_ import classify_redis

    lowered = dependency.lower()
    if "postgres" in lowered or lowered in ("db", "database", "sql"):
        return TrafficRecorder(monitor, dependency, classify_postgres)
    if "redis" in lowered or "cache" in lowered:
        return TrafficRecorder(monitor, dependency, classify_redis)
    if "rabbit" in lowered or "amqp" in lowered or "broker" in lowered:
        return TrafficRecorder(monitor, dependency, classify_amqp)
    return TrafficRecorder(monitor, dependency)
