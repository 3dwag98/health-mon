"""User-supplied checks -- the 'extensible custom checks' the brief asks for."""
from __future__ import annotations

import time

from ..core.model import CheckResult, ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


class CustomCheck(BaseCheck):
    """Wraps a plain callable.

    The callable may return a bool, a Status, or a full CheckResult.  It may
    also raise -- an exception is caught, classified as INTERNAL and isolated
    to this check, so a broken custom check can never take down the monitor
    or affect any other check's result.
    """

    def __init__(self, fn, *, name: str) -> None:
        self.name = name
        self.dependency = ""
        self._fn = fn

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        started = time.perf_counter()
        value = self._fn()

        if isinstance(value, CheckResult):
            return value
        if isinstance(value, Status):
            status = value
        elif isinstance(value, bool):
            status = Status.OK if value else Status.FAILING
        elif value is None:
            status = Status.OK
        else:
            status = Status.OK

        return CheckResult(
            name=self.name, status=status, checked_at=ctx.now, wall_clock=ctx.wall,
            evidence=Evidence.PROBED,
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            category=None if status is Status.OK else ErrorCategory.UNKNOWN,
            evidence_age_ms=0.0,
        )

    def classify(self, exc: BaseException) -> ErrorCategory:
        return ErrorCategory.INTERNAL
