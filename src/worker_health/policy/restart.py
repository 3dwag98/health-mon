"""Configurable controlled restart.

The library never kills anything.  The only action available to it is
exiting its own process with a chosen code, at which point PM2 applies its
own restart policy.  That keeps the restart decision in the supervisor,
where it belongs, and means this package cannot become the thing that takes
the fleet down.

The default trigger list deliberately excludes dependency failures.
Restarting a worker because Postgres is down does not fix Postgres -- it
converts one database outage into forty crash-looping processes hammering a
database that is already in trouble, and destroys the in-flight work each
was holding.  The failure modes a restart genuinely repairs are
self-inflicted, and those are what is listed below.
"""
from __future__ import annotations

import os
import random
import threading
import time

from ..core.model import ErrorCategory, Status

# Categories a restart can plausibly repair.  All self-inflicted.
SELF_FAULTS = frozenset({
    ErrorCategory.STALLED,
    ErrorCategory.NOT_CONSUMING,
    ErrorCategory.NOT_SUBSCRIBED,
    ErrorCategory.CREDIT_EXHAUSTED,
    ErrorCategory.CONNECTION_LOST,
    ErrorCategory.POISON_LOOP,
})

# Categories where restarting makes things actively worse.
DEPENDENCY_FAULTS = frozenset({
    ErrorCategory.CONNECTION_REFUSED,
    ErrorCategory.TIMEOUT,
    ErrorCategory.SLOW,
    ErrorCategory.AUTH_FAILED,
    ErrorCategory.BROKER_ALARM,
    ErrorCategory.BROKER_SHUTDOWN,
    ErrorCategory.MEMORY_PRESSURE,
    ErrorCategory.POOL_EXHAUSTED,
    ErrorCategory.READ_ONLY,
})


class RestartPolicy:
    def __init__(
        self,
        *,
        enabled: bool = False,
        triggers=SELF_FAULTS,
        after_cycles: int = 5,
        min_uptime: float = 120.0,
        cooldown: float = 600.0,
        max_per_hour: int = 3,
        jitter: float = 0.3,
        drain_timeout: float = 30.0,
        exit_code: int = 70,
        on_restart=None,
        logger=None,
    ) -> None:
        self.enabled = enabled
        self.triggers = frozenset(triggers)
        self.after_cycles = after_cycles
        self.min_uptime = min_uptime
        self.cooldown = cooldown
        self.max_per_hour = max_per_hour
        self.jitter = jitter
        self.drain_timeout = drain_timeout
        self.exit_code = exit_code
        self.on_restart = on_restart
        self.logger = logger

        self._monitor = None
        self._streak = 0
        self._last_restart: float | None = None
        self._restarts: list[float] = []
        self._latched = False
        self._lock = threading.Lock()

    def bind(self, monitor) -> None:
        self._monitor = monitor

    @property
    def latched(self) -> bool:
        """True once the hourly budget is spent.

        A latched policy stays up and reports failing, which is strictly more
        useful than a process that is never alive long enough to inspect.
        """
        return self._latched

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "streak": self._streak,
            "after_cycles": self.after_cycles,
            "restarts_in_window": len(self._restarts),
            "max_per_hour": self.max_per_hour,
            "latched": self._latched,
            "triggers": sorted(c.value for c in self.triggers),
        }

    def observe(self, monitor) -> None:
        if not self.enabled or self._latched:
            return
        with self._lock:
            snap = monitor.snapshot()
            hit = self._triggering(snap)
            self._streak = self._streak + 1 if hit else 0
            if not hit or self._streak < self.after_cycles:
                return
            if not self._eligible(snap):
                return
            self._fire(snap)

    def _triggering(self, snap) -> bool:
        for r in snap.results.values():
            if r.status is Status.FAILING and r.category in self.triggers:
                return True
        return False

    def _eligible(self, snap) -> bool:
        now = time.monotonic()
        if snap.uptime_s < self.min_uptime:
            return False   # never restart a process that just booted
        if self._last_restart is not None and (now - self._last_restart) < self.cooldown:
            return False
        cutoff = now - 3600.0
        self._restarts = [t for t in self._restarts if t > cutoff]
        if len(self._restarts) >= self.max_per_hour:
            self._latched = True
            if self.logger:
                self.logger.error(
                    "restart budget exhausted; latching and staying up",
                    extra={"service": snap.service, "instance": snap.instance},
                )
            return False
        return True

    def _fire(self, snap) -> None:
        now = time.monotonic()
        self._last_restart = now
        self._restarts.append(now)
        categories = sorted({
            r.category.value for r in snap.results.values()
            if r.status is Status.FAILING and r.category in self.triggers
        })
        if self.logger:
            self.logger.error(
                "restart policy triggered",
                extra={"service": snap.service, "instance": snap.instance,
                       "category": ",".join(categories)},
            )
        if self.on_restart is not None:
            self.on_restart(snap)
            return
        # Jittered so forty workers hitting the same condition do not all
        # exit in the same second.
        delay = self.drain_timeout * (1.0 + random.uniform(-self.jitter, self.jitter))
        threading.Timer(max(0.0, delay), self._exit).start()

    def _exit(self) -> None:
        os._exit(self.exit_code)
