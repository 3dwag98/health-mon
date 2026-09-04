"""Shared scheduling policy.

The runners own only the mechanics of "call this thing, with a timeout,
concurrently".  Every decision -- when a check is next due, whether the
breaker opens, how jitter is applied -- lives in core.machine, which is
synchronous and pure.  If policy lived in each runner they would drift, and
every row of the scenario matrix would need testing twice with no guarantee
the two agreed.
"""
from __future__ import annotations

import time
from typing import Iterable

from ..core import timing as T
from ..core.machine import StateMachine
from ..core.model import CheckResult, ErrorCategory, Evidence, Status


def due_checks(machine: StateMachine, now: float) -> Iterable[str]:
    for spec in machine.specs:
        if now >= machine.state(spec.name).next_due:
            yield spec.name


def record_schedule_lag(machine: StateMachine, name: str, now: float, timings) -> None:
    """How late this check ran relative to when it was due.

    Rises when the scheduler is starved, which is the early warning that the
    monitor itself is the thing in trouble.
    """
    due = machine.state(name).next_due
    if due:
        timings.observe(T.check_schedule_lag(name), max(0.0, (now - due) * 1000.0))


def timeout_result(name: str, now: float, wall: float, timeout: float) -> CheckResult:
    return CheckResult(
        name=name, status=Status.FAILING, checked_at=now, wall_clock=wall,
        evidence=Evidence.PROBED, latency_ms=timeout * 1000.0,
        category=ErrorCategory.TIMEOUT, evidence_age_ms=0.0,
        detail="check exceeded its timeout",
    )


def error_result(name: str, now: float, wall: float, category: ErrorCategory,
                 detail: str | None = None) -> CheckResult:
    """A check that raised is isolated: UNKNOWN, and nothing else is affected."""
    return CheckResult(
        name=name, status=Status.UNKNOWN, checked_at=now, wall_clock=wall,
        evidence=Evidence.NONE, category=category, evidence_age_ms=0.0,
        detail=detail,
    )
