"""Check protocol and the evidence-ordering that every adapter shares.

The ordering is the point.  A synthetic probe answers "can this process
reach the dependency right now"; the question that matters is "is the
worker's own connection working".  Those diverge exactly when it counts --
a worker sitting on a dead pooled connection while a fresh probe succeeds
cheerfully.  So real traffic is consulted first, connection state second,
and a probe is what we fall back to when the worker has been silent long
enough that we have no recent evidence either way.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, replace
from typing import Protocol

from ..core.model import CheckResult, ErrorCategory, Evidence, Status
from ..instrument.context import probe_scope


@dataclass
class TrafficRecord:
    """What the worker's own traffic has told us about one dependency."""

    successes: int = 0
    failures: int = 0
    last_success_at: float | None = None
    last_failure_at: float | None = None
    last_category: ErrorCategory | None = None
    last_latency_ms: float | None = None
    consecutive_failures: int = 0


class TrafficLog:
    """Thread-safe record of real dependency usage.

    Written by the decorators, the SQLAlchemy event hooks and the recording
    client wrappers; read by the checks.  Never written by a probe -- if a
    probe counted as traffic, a silent worker would look busy.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._records: dict[str, TrafficRecord] = {}

    def _rec(self, dep: str) -> TrafficRecord:
        r = self._records.get(dep)
        if r is None:
            r = self._records[dep] = TrafficRecord()
        return r

    def success(self, dep: str, latency_ms: float | None = None, *, now: float | None = None) -> None:
        with self._lock:
            r = self._rec(dep)
            r.successes += 1
            r.consecutive_failures = 0
            r.last_success_at = now if now is not None else time.monotonic()
            r.last_latency_ms = latency_ms
            r.last_category = None

    def failure(self, dep: str, category: ErrorCategory, *, now: float | None = None) -> None:
        with self._lock:
            r = self._rec(dep)
            r.failures += 1
            r.consecutive_failures += 1
            r.last_failure_at = now if now is not None else time.monotonic()
            r.last_category = category

    def get(self, dep: str) -> TrafficRecord | None:
        with self._lock:
            r = self._records.get(dep)
            if r is None:
                return None
            # Copy: callers must never mutate shared state.
            return TrafficRecord(**vars(r))

    def snapshot(self) -> dict[str, TrafficRecord]:
        with self._lock:
            return {k: TrafficRecord(**vars(v)) for k, v in self._records.items()}


@dataclass
class CheckContext:
    now: float
    wall: float
    deadline: float
    max_silence: float
    traffic: TrafficLog


class Check(Protocol):
    name: str

    def evaluate(self, ctx: CheckContext) -> CheckResult: ...

    def close(self) -> None: ...


class BaseCheck:
    """Shared machinery: result construction and the evidence ladder."""

    name: str = "unnamed"
    dependency: str = ""

    # Latency policy, set from the ProbeSpec by the factory.  Applied to
    # every rung of the ladder that produced an OK, so a dependency judged
    # healthy from real traffic is held to the same bar as one judged from
    # a probe -- the two must not disagree about what "fast enough" means.
    #
    # None disables the judgement entirely, which is the default: a latency
    # bar the library picked for you is a pager that goes off at 3am about
    # a threshold nobody chose.
    latency_warn_ms: float | None = None
    latency_critical_ms: float | None = None

    # -- to be provided by adapters ------------------------------------- #

    def introspect(self, ctx: CheckContext) -> CheckResult | None:
        """Local connection/pool state.  Zero I/O.  None when inconclusive."""
        return None

    def probe(self, ctx: CheckContext) -> CheckResult:
        """Synthetic request.  Only called when nothing else is conclusive."""
        raise NotImplementedError

    # -- the ladder ------------------------------------------------------ #

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        started = time.perf_counter()

        # 1. Introspection first: it costs nothing and it is the only thing
        #    that can distinguish "pool exhausted" from "server unreachable".
        introspected = self.introspect(ctx)
        if introspected is not None and introspected.status is not Status.OK:
            return introspected

        # 2. Real traffic.  The strongest evidence there is, and free.
        observed = self.from_traffic(ctx)
        if observed is not None:
            return self.apply_latency_policy(observed)

        # 3. Nothing recent to go on.  Probe, and say so.
        #
        # `probe_scope` marks everything the probe does as health traffic,
        # so auto-instrumentation ignores it.  Without that, the probe's own
        # SELECT 1 would land in the traffic log, the next evaluation would
        # find "recent traffic", and a silent worker would report `observed`
        # on the strength of nothing but its own health checks.
        try:
            with probe_scope():
                result = self.probe(ctx)
        except Exception as exc:  # noqa: BLE001 - classified, never re-raised
            result = self.fail(ctx, self.classify(exc), started)
        if introspected is not None and result.status is Status.OK:
            # Carry introspected observations forward onto the probe result.
            merged = dict(introspected.observed)
            merged.update(result.observed)
            result = _replace_observed(result, merged)
        return self.apply_latency_policy(result)

    def apply_latency_policy(self, result: CheckResult) -> CheckResult:
        """Downgrade an otherwise-OK verdict that took too long.

        Only OK results are touched.  A check that already failed has a
        cause worth more than its latency, and overwriting `pool_exhausted`
        with `slow` would send on-call to the wrong team.
        """
        if result.status is not Status.OK or result.latency_ms is None:
            return result
        if self.latency_critical_ms is None and self.latency_warn_ms is None:
            return result

        latency = result.latency_ms
        if self.latency_critical_ms is not None and latency >= self.latency_critical_ms:
            status, threshold = Status.FAILING, self.latency_critical_ms
        elif self.latency_warn_ms is not None and latency >= self.latency_warn_ms:
            status, threshold = Status.DEGRADED, self.latency_warn_ms
        else:
            return result

        observed = dict(result.observed)
        observed["latency_threshold_ms"] = threshold
        return replace(
            result,
            status=status,
            category=ErrorCategory.SLOW,
            detail=f"{round(latency, 1)}ms exceeds the {round(threshold, 1)}ms threshold",
            observed=observed,
        )

    def from_traffic(self, ctx: CheckContext) -> CheckResult | None:
        """Verdict from the worker's own recent traffic, or None if stale."""
        if not self.dependency:
            return None
        rec = ctx.traffic.get(self.dependency)
        if rec is None:
            return None

        newest = max(
            rec.last_success_at or -1.0,
            rec.last_failure_at or -1.0,
        )
        if newest < 0:
            return None
        age_ms = (ctx.now - newest) * 1000.0
        if age_ms > ctx.max_silence * 1000.0:
            return None   # too old to stand on; fall through to a probe

        observed = {
            "successes": rec.successes,
            "failures": rec.failures,
            "consecutive_failures": rec.consecutive_failures,
        }
        if rec.last_latency_ms is not None:
            observed["last_latency_ms"] = round(rec.last_latency_ms, 3)

        failing = (
            rec.last_failure_at is not None
            and (rec.last_success_at is None or rec.last_failure_at > rec.last_success_at)
        )
        return CheckResult(
            name=self.name,
            status=Status.FAILING if failing else Status.OK,
            checked_at=ctx.now,
            wall_clock=ctx.wall,
            evidence=Evidence.OBSERVED,
            latency_ms=rec.last_latency_ms,
            category=rec.last_category if failing else None,
            evidence_age_ms=round(age_ms, 3),
            observed=observed,
        )

    # -- helpers --------------------------------------------------------- #

    def ok(self, ctx, started, evidence=Evidence.PROBED, **observed) -> CheckResult:
        return CheckResult(
            name=self.name, status=Status.OK, checked_at=ctx.now, wall_clock=ctx.wall,
            evidence=evidence, latency_ms=_ms(started), evidence_age_ms=0.0,
            observed=observed,
        )

    def degraded(self, ctx, category, started, evidence=Evidence.INTROSPECTED,
                 detail=None, **observed) -> CheckResult:
        return CheckResult(
            name=self.name, status=Status.DEGRADED, checked_at=ctx.now, wall_clock=ctx.wall,
            evidence=evidence, latency_ms=_ms(started), category=category,
            evidence_age_ms=0.0, detail=detail, observed=observed,
        )

    def fail(self, ctx, category, started, evidence=Evidence.PROBED,
             detail=None, **observed) -> CheckResult:
        return CheckResult(
            name=self.name, status=Status.FAILING, checked_at=ctx.now, wall_clock=ctx.wall,
            evidence=evidence, latency_ms=_ms(started), category=category,
            evidence_age_ms=0.0, detail=detail, observed=observed,
        )

    def classify(self, exc: BaseException) -> ErrorCategory:
        return ErrorCategory.UNKNOWN

    def close(self) -> None:
        return None


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)


def _replace_observed(result: CheckResult, observed) -> CheckResult:
    return CheckResult(
        name=result.name, status=result.status, checked_at=result.checked_at,
        wall_clock=result.wall_clock, evidence=result.evidence,
        latency_ms=result.latency_ms, category=result.category,
        evidence_age_ms=result.evidence_age_ms, detail=result.detail,
        observed=observed,
    )
