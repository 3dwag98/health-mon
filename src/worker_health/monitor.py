"""HealthMonitor: registration, scheduling, snapshot assembly, serialisation."""
from __future__ import annotations

import os
import random
import threading
import time

from .checks.base import BaseCheck, TrafficLog
from .checks.custom import CustomCheck
from .core import timing as T
from .core.aggregate import aggregate, boot_complete
from .core.clock import Clock, MonotonicClock
from .core.machine import CheckSpec, StateMachine
from .core.model import (
    LIVE_CODE,
    READY_CODE,
    WIRE,
    CheckResult,
    ErrorCategory,
    Evidence,
    Snapshot,
    Status,
)
from .core.timing import Timings


class HealthMonitor:
    def __init__(
        self,
        service: str,
        *,
        version: str = "0.0.0",
        instance: str | None = None,
        clock: Clock | None = None,
        runner: str = "thread",
        tick: float = 0.1,
        max_workers: int = 8,
        loop_lag_threshold_ms: float = 2000.0,
        seed: int | None = None,
        logger=None,
    ) -> None:
        self.service = service
        self.version = version
        self.instance = instance or os.getenv("HEALTH_INSTANCE") or f"{service}-{os.getpid()}"
        self.clock = clock or MonotonicClock()
        self.timings = Timings()
        self.traffic = TrafficLog()
        self.checks: dict[str, BaseCheck] = {}
        self.machine = StateMachine([], self.clock, rng=random.Random(seed))
        self.logger = logger

        self._started_at = self.clock.monotonic()
        self._boot_deadline: float | None = None
        self._boot_done = False
        self._loop_beat = self.clock.monotonic()
        self._loop_lag_threshold_ms = loop_lag_threshold_ms
        self._last_activity: float | None = None
        self._lock = threading.Lock()
        self._results: dict[str, CheckResult] = {}
        self._listeners: list = []
        self._restart_policy = None
        self._runner_name = runner

        if runner == "asyncio":
            from .runners.asyncio_ import AsyncioRunner
            self._runner = AsyncioRunner(self, tick=tick)
        else:
            from .runners.thread_ import ThreadRunner
            self._runner = ThreadRunner(self, max_workers=max_workers, tick=tick)

    # -- registration ----------------------------------------------------- #

    def register(self, check, *, name: str | None = None, critical: bool = True,
                 **spec_kwargs) -> "HealthMonitor":
        n = name or getattr(check, "name", None) or f"check-{len(self.checks)}"
        check.name = n
        self.checks[n] = check
        self.machine.add(CheckSpec(name=n, critical=critical, **spec_kwargs))
        return self

    def check_fn(self, name: str, *, critical: bool = False, **spec_kwargs):
        """Decorator form: @health.check("vendor-api", interval=30)."""
        def deco(fn):
            self.register(CustomCheck(fn, name=name), name=name,
                          critical=critical, **spec_kwargs)
            return fn
        return deco

    # Alias so the documented `@health.check(...)` spelling works.
    check = check_fn

    def set_restart_policy(self, policy) -> None:
        self._restart_policy = policy
        policy.bind(self)

    def on_transition(self, fn) -> None:
        self._listeners.append(fn)

    # -- lifecycle -------------------------------------------------------- #

    def start(self, boot_grace: float = 30.0) -> "HealthMonitor":
        self._started_at = self.clock.monotonic()
        self._boot_deadline = self._started_at + boot_grace if boot_grace else None
        self._loop_beat = self.clock.monotonic()
        self._runner.start()
        return self

    def stop(self, timeout: float = 5.0) -> None:
        """Idempotent -- tests stop the monitor and the fixture stops it again."""
        try:
            self._runner.stop(timeout=timeout)
        except Exception:
            pass
        for check in self.checks.values():
            try:
                check.close()
            except Exception:
                pass

    # -- result intake ---------------------------------------------------- #

    def apply(self, result: CheckResult) -> None:
        with self._lock:
            previous = self.machine.state(result.name).effective
            self.machine.apply(result.name, result)
            self._results[result.name] = result
            current = self.machine.state(result.name).effective
        if current is not previous:
            for fn in self._listeners:
                try:
                    fn(result.name, previous, current, result)
                except Exception:
                    pass
        if self._restart_policy is not None:
            try:
                self._restart_policy.observe(self)
            except Exception:
                pass

    def note_activity(self, at: float | None = None) -> None:
        """The worker did real work.  Drives the worker-to-health delta."""
        self._last_activity = at if at is not None else self.clock.monotonic()

    # -- reads ------------------------------------------------------------ #

    def live_status(self) -> Status:
        """Liveness answers exactly one question: is our loop responsive.

        Dependencies are deliberately not consulted.  A failed dependency
        does not mean a dead process, and returning 503 here would restart
        the entire fleet against a database that is already struggling.
        """
        lag_ms = (self.clock.monotonic() - self._loop_beat) * 1000.0
        if lag_ms > self._loop_lag_threshold_ms:
            return Status.FAILING
        return Status.OK

    def loop_lag_ms(self) -> float:
        return round((self.clock.monotonic() - self._loop_beat) * 1000.0, 3)

    def snapshot(self) -> Snapshot:
        """Serves a cached view.

        No I/O happens on this path.  A health endpoint that probes inline is
        slow exactly when everything else is already on fire.
        """
        build_started = time.perf_counter()
        now = self.clock.monotonic()

        with self._lock:
            results = dict(self._results)
            if not self._boot_done and boot_complete(self.machine):
                self._boot_done = True
            deadline = None if self._boot_done else self._boot_deadline
            status = aggregate(self.machine, self.clock, deadline)
            # Re-project each stored result through the state machine so the
            # reported status is the EFFECTIVE one (thresholds + TTL applied),
            # not the raw last sample.
            projected = {}
            for name, r in results.items():
                projected[name] = _with_status(r, self.machine.effective(name))

        timing = self._timing_block(now, projected)
        build_ms = (time.perf_counter() - build_started) * 1000.0
        self.timings.observe(T.SNAPSHOT_BUILD, build_ms)
        timing["snapshot_build_ms"] = round(build_ms, 3)

        return Snapshot(
            status=status,
            live_status=self.live_status(),
            results=projected,
            built_at=now,
            wall_clock=self.clock.wall(),
            service=self.service,
            instance=self.instance,
            version=self.version,
            uptime_s=round(now - self._started_at, 2),
            timing=timing,
        )

    def _timing_block(self, now: float, results) -> dict:
        """The worker/health timing relationship, measured rather than assumed.

        ``worker_to_health_delta_ms`` is the one with no equivalent elsewhere:
        how old the worker's signal already was at the moment health last
        looked at it.  A check that runs in 2ms but is standing on a
        90-second-old observation is not a 2ms-fresh signal, and this is the
        number that says so.
        """
        block: dict[str, float | int | str] = {
            "loop_lag_ms": self.loop_lag_ms(),
            "runner": self._runner_name,
        }

        if self._last_activity is not None:
            block["worker_last_activity_age_ms"] = round(
                (now - self._last_activity) * 1000.0, 3
            )

        if results:
            newest = max(r.checked_at for r in results.values())
            oldest = min(r.checked_at for r in results.values())
            block["health_eval_age_ms"] = round((now - newest) * 1000.0, 3)
            block["health_oldest_eval_age_ms"] = round((now - oldest) * 1000.0, 3)

            observed = [
                r for r in results.values()
                if r.evidence is Evidence.OBSERVED and r.evidence_age_ms is not None
            ]
            if observed:
                freshest = min(observed, key=lambda r: r.evidence_age_ms)
                block["worker_to_health_delta_ms"] = round(freshest.evidence_age_ms, 3)
                self.timings.observe(T.WORKER_HEALTH_DELTA, freshest.evidence_age_ms)

        snap = self.timings.summary(T.SNAPSHOT_BUILD)
        if snap:
            block["snapshot_p99_ms"] = snap["p99_ms"]
        return block

    def transitions(self, name: str) -> int:
        return self.machine.transitions(name)

    # -- serialisation ---------------------------------------------------- #

    def snapshot_dict(self, *, include_timings: bool = True) -> dict:
        s = self.snapshot()
        checks = {}
        for name, r in s.results.items():
            entry = {
                "status": WIRE[r.status],
                "internal_status": r.status.value,
                "evidence": r.evidence.value,
                "time": r.wall_clock,
                "critical": self.machine.spec(name).critical,
            }
            if r.latency_ms is not None:
                entry["latency_ms"] = round(r.latency_ms, 3)
            if r.evidence_age_ms is not None:
                entry["evidence_age_ms"] = round(r.evidence_age_ms, 3)
            if r.category is not None:
                entry["category"] = r.category.value
            if r.detail:
                entry["detail"] = r.detail
            if r.observed:
                entry["observed"] = dict(r.observed)
            entry["transitions"] = self.machine.transitions(name)
            checks[name] = entry

        body = {
            "status": s.status.value,
            "wire_status": WIRE[s.status],
            "live": s.live_status.value,
            "service": s.service,
            "instance": s.instance,
            "version": s.version,
            "uptime_s": s.uptime_s,
            "time": s.wall_clock,
            "checks": checks,
            "timing": dict(s.timing),
        }
        if include_timings:
            body["metrics"] = self.timings.export()
        return body

    def ready_code(self) -> int:
        return READY_CODE[self.snapshot().status]

    def live_code(self) -> int:
        return LIVE_CODE[self.live_status()]


def _with_status(r: CheckResult, status: Status) -> CheckResult:
    if r.status is status:
        return r
    return CheckResult(
        name=r.name, status=status, checked_at=r.checked_at, wall_clock=r.wall_clock,
        evidence=r.evidence, latency_ms=r.latency_ms,
        category=r.category if status is not Status.OK else None,
        evidence_age_ms=r.evidence_age_ms,
        detail=r.detail if status is not Status.OK else None,
        observed=r.observed,
    )
