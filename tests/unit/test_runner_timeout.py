"""L0: the runner must bound a check that never returns.

A black-holed dependency does not raise and does not close the socket -- the
call simply never comes back.  If the runner only ever waits for a result, a
check like this stops reporting entirely: the last result ages past its TTL,
``effective()`` turns it into UNKNOWN, and ``aggregate`` maps UNKNOWN onto
DEGRADED.  A total outage then reads as "degraded", and because the check is
never re-dispatched it can never observe the dependency coming back either --
only a process restart clears it.

These tests pin both halves: the timeout is reported as a real failure, and
the check stays schedulable so recovery is possible without a restart.

The slow-but-working case is here too.  It is the opposite error: a
dependency answering in 400ms is healthy, and a monitor that reports it as
an outage -- or flaps it -- is worse than no monitor, because people stop
believing it.  Healthy / slow / failed are the three cases the brief asks
for, and only two of them are failures.
"""
from __future__ import annotations

import threading
import time

import pytest

from worker_health import ErrorCategory, Status
from worker_health.monitor import HealthMonitor

pytestmark = pytest.mark.unit


class HangingCheck:
    """Mocks a black hole: blocks until released, never raises."""

    name = "hang"
    dependency = ""

    def __init__(self) -> None:
        self.release = threading.Event()
        self.calls = 0

    def evaluate(self, ctx):
        self.calls += 1
        # Bounded so a failing test cannot wedge the suite; far longer than
        # the check's own timeout, which is the point.
        self.release.wait(timeout=30.0)
        from worker_health import CheckResult, Evidence

        return CheckResult(
            name=self.name, status=Status.OK, checked_at=ctx.now,
            wall_clock=ctx.wall, evidence=Evidence.PROBED, evidence_age_ms=0.0,
        )

    def classify(self, exc):
        return ErrorCategory.UNKNOWN

    def close(self):
        self.release.set()


def _wait(predicate, timeout=8.0, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_hung_check_is_reported_failing_not_unknown():
    check = HangingCheck()
    m = HealthMonitor("t", runner="thread", tick=0.05)
    m.register(check, name="hang", timeout=0.3, interval=0.2, ttl=30.0)
    m.start(boot_grace=0)
    try:
        assert _wait(lambda: m.machine.effective("hang") is Status.FAILING), (
            "a check that never returns must be timed out and reported "
            f"FAILING, got {m.machine.effective('hang')}"
        )
        result = m.machine.state("hang").last_result
        assert result.category is ErrorCategory.TIMEOUT
    finally:
        m.stop()


def test_hung_check_recovers_without_a_restart():
    check = HangingCheck()
    m = HealthMonitor("t", runner="thread", tick=0.05)
    m.register(check, name="hang", timeout=0.3, interval=0.2, ttl=30.0)
    m.start(boot_grace=0)
    try:
        assert _wait(lambda: m.machine.effective("hang") is Status.FAILING)
        before = check.calls

        # The dependency comes back.  The check must be dispatched again --
        # if the runner is still waiting on the original call, it never is.
        check.release.set()
        assert _wait(lambda: check.calls > before), (
            "check was never re-dispatched after its timeout; recovery would "
            "require restarting the process"
        )
        assert _wait(lambda: m.machine.effective("hang") is Status.OK), (
            "check did not return to OK after the dependency recovered"
        )
    finally:
        m.stop()


class SlowCheck:
    """Mocks a slow but entirely healthy dependency."""

    name = "slow"
    dependency = ""

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.calls = 0

    def evaluate(self, ctx):
        self.calls += 1
        time.sleep(self.delay)
        from worker_health import CheckResult, Evidence

        return CheckResult(
            name=self.name, status=Status.OK, checked_at=ctx.now,
            wall_clock=ctx.wall, evidence=Evidence.PROBED, evidence_age_ms=0.0,
        )

    def classify(self, exc):
        return ErrorCategory.UNKNOWN

    def close(self):
        return None


def test_slow_but_healthy_dependency_stays_ok_and_does_not_flap():
    """400ms against a 3s timeout is healthy.  Reporting it as anything else
    is the false positive that teaches an on-call to ignore the page."""
    check = SlowCheck(delay=0.4)
    m = HealthMonitor("t", runner="thread", tick=0.05)
    m.register(check, name="slow", timeout=3.0, interval=0.2, ttl=30.0)
    m.start(boot_grace=0)
    try:
        assert _wait(lambda: m.machine.effective("slow") is Status.OK), (
            "a dependency answering well inside its timeout must read OK"
        )
        # Let it run several cycles: the status must hold, not oscillate.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            assert m.machine.effective("slow") is Status.OK, "slow check flapped"
            time.sleep(0.1)
        assert check.calls > 1, "check never ran a second time"
        assert m.machine.state("slow").transitions == 1, (
            f"expected one transition into OK, saw "
            f"{m.machine.state('slow').transitions} -- the check is flapping"
        )
    finally:
        m.stop()


def test_dependency_slower_than_its_timeout_is_a_failure():
    """The same check, with the timeout tightened below the latency."""
    check = SlowCheck(delay=1.0)
    m = HealthMonitor("t", runner="thread", tick=0.05)
    m.register(check, name="slow", timeout=0.3, interval=0.2, ttl=30.0)
    m.start(boot_grace=0)
    try:
        assert _wait(lambda: m.machine.effective("slow") is Status.FAILING), (
            "a dependency slower than its configured timeout must fail"
        )
        assert m.machine.state("slow").last_result.category is ErrorCategory.TIMEOUT
    finally:
        m.stop()
