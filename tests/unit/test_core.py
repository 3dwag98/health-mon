"""L0: no containers. This tier must pass with the docker daemon stopped."""
from __future__ import annotations

import pytest

from worker_health import (
    LIVE_CODE,
    READY_CODE,
    SEVERITY,
    WIRE,
    CheckResult,
    CheckSpec,
    ErrorCategory,
    Evidence,
    FakeClock,
    StateMachine,
    Status,
)
from worker_health.core.aggregate import aggregate

pytestmark = pytest.mark.unit


def _result(status, at, name="c"):
    return CheckResult(name=name, status=status, checked_at=at, wall_clock=0.0)


def _machine(clock, **kw):
    spec = CheckSpec(name="c", **kw)
    return StateMachine([spec], clock), spec


# -- status model ------------------------------------------------------------ #

def test_every_status_maps_completely():
    for s in Status:
        assert s in WIRE and s in SEVERITY and s in LIVE_CODE and s in READY_CODE


def test_unknown_is_not_worse_than_failing():
    """No measurement is not an outage. Treating it as one produces a false
    alarm on every deploy and every slow check."""
    assert SEVERITY[Status.UNKNOWN] < SEVERITY[Status.FAILING]


def test_liveness_never_503s_on_a_dependency_failure():
    """The whole fleet must not restart because a shared database is down."""
    assert LIVE_CODE[Status.FAILING] == 200
    assert READY_CODE[Status.FAILING] == 503


def test_liveness_survives_boot():
    """A 503 on /live during boot makes the supervisor kill every worker
    mid-startup, and the restart never converges."""
    assert LIVE_CODE[Status.STARTING] == 200
    assert READY_CODE[Status.STARTING] == 503


def test_degraded_is_still_serving():
    assert READY_CODE[Status.DEGRADED] == 200


# -- state machine ----------------------------------------------------------- #

def test_failure_threshold_absorbs_a_single_blip():
    clock = FakeClock()
    m, _ = _machine(clock, failure_threshold=3, interval=1.0)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    clock.advance(1)
    m.apply("c", _result(Status.FAILING, clock.monotonic()))
    assert m.effective("c") is Status.OK      # one packet lost is not an outage
    clock.advance(1)
    m.apply("c", _result(Status.FAILING, clock.monotonic()))
    clock.advance(1)
    m.apply("c", _result(Status.FAILING, clock.monotonic()))
    assert m.effective("c") is Status.FAILING


def test_recovery_is_confirmed_not_assumed():
    clock = FakeClock()
    m, _ = _machine(clock, failure_threshold=1, success_threshold=2, interval=1.0)
    m.apply("c", _result(Status.FAILING, clock.monotonic()))
    assert m.effective("c") is Status.FAILING
    clock.advance(1)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.effective("c") is Status.FAILING   # one success is not recovery
    clock.advance(1)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.effective("c") is Status.OK


def test_degraded_also_has_hysteresis():
    """The spec this replaces let DEGRADED -> OK happen on a single sample,
    which gave degraded checks no damping at all."""
    clock = FakeClock()
    m, _ = _machine(clock, success_threshold=2, interval=1.0)
    m.apply("c", _result(Status.DEGRADED, clock.monotonic()))
    assert m.effective("c") is Status.DEGRADED
    clock.advance(1)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.effective("c") is Status.DEGRADED
    clock.advance(1)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.effective("c") is Status.OK


def test_flapping_produces_few_transitions():
    clock = FakeClock()
    m, _ = _machine(clock, failure_threshold=3, success_threshold=2, interval=1.0)
    for _ in range(10):
        clock.advance(1)
        m.apply("c", _result(Status.FAILING, clock.monotonic()))
        clock.advance(1)
        m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.state("c").transitions <= 4


def test_ttl_expiry_yields_unknown():
    """Applied at read time, which is what makes a wedged scheduler visible."""
    clock = FakeClock()
    m, _ = _machine(clock, ttl=5.0)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    clock.advance(6.0)
    assert m.effective("c") is Status.UNKNOWN


def test_clock_step_does_not_create_staleness():
    """An NTP correction must not make every check look stale at once."""
    clock = FakeClock()
    m, _ = _machine(clock, ttl=30.0)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    clock.advance_wall(3600)
    assert m.effective("c") is Status.OK


def test_backoff_rises_then_resets_on_success():
    clock = FakeClock()
    m, _ = _machine(clock, breaker_threshold=2, interval=1.0)
    steps = []
    for _ in range(6):
        clock.advance(1.0)
        m.apply("c", _result(Status.FAILING, clock.monotonic()))
        steps.append(m.state("c").backoff_step)
    assert steps == sorted(steps) and steps[-1] > 0
    m.apply("c", _result(Status.OK, clock.monotonic()))
    assert m.state("c").backoff_step == 0


def test_breaker_never_changes_reported_status():
    """It changes probe frequency only. One that reported UNKNOWN while open
    would hide the outage it exists to survive."""
    clock = FakeClock()
    m, _ = _machine(clock, breaker_threshold=2, failure_threshold=2, interval=1.0)
    for _ in range(6):
        clock.advance(1.0)
        m.apply("c", _result(Status.FAILING, clock.monotonic()))
    assert m.effective("c") is Status.FAILING


def test_mark_unknown_reschedules():
    """Or a check marked unknown can stop being scheduled entirely."""
    clock = FakeClock()
    m, _ = _machine(clock, interval=5.0)
    m.apply("c", _result(Status.OK, clock.monotonic()))
    m.mark_unknown("c")
    assert m.state("c").next_due > clock.monotonic()


# -- aggregation ------------------------------------------------------------- #

def _agg(clock, specs, statuses, boot=None):
    m = StateMachine(specs, clock)
    for name, st in statuses.items():
        m.apply(name, _result(st, clock.monotonic(), name))
        m.state(name).effective = st
    return aggregate(m, clock, boot)


def test_non_critical_failure_degrades_but_does_not_fail():
    clock = FakeClock()
    specs = [CheckSpec("db", critical=True, failure_threshold=1),
             CheckSpec("cache", critical=False, failure_threshold=1)]
    assert _agg(clock, specs, {"db": Status.OK, "cache": Status.FAILING}) is Status.DEGRADED


def test_critical_failure_fails_the_whole():
    clock = FakeClock()
    specs = [CheckSpec("db", critical=True, failure_threshold=1),
             CheckSpec("cache", critical=False, failure_threshold=1)]
    assert _agg(clock, specs, {"db": Status.FAILING, "cache": Status.OK}) is Status.FAILING


def test_a_critical_check_with_no_measurement_is_not_ready():
    """Once boot grace is over, "we have never managed to ask" is not a
    reason to route work here.  A check that has never answered, or whose
    last answer aged past its ttl, reads the same from outside: nobody
    knows, and nobody-knows is not ready."""
    clock = FakeClock()
    specs = [CheckSpec("db", critical=True)]
    assert _agg(clock, specs, {"db": Status.UNKNOWN}) is Status.FAILING


def test_a_non_critical_check_with_no_measurement_only_degrades():
    clock = FakeClock()
    specs = [CheckSpec("cache", critical=False)]
    assert _agg(clock, specs, {"cache": Status.UNKNOWN}) is Status.DEGRADED


def test_boot_grace_reports_starting():
    clock = FakeClock()
    specs = [CheckSpec("db", critical=True)]
    boot = clock.monotonic() + 10
    assert _agg(clock, specs, {"db": Status.UNKNOWN}, boot=boot) is Status.STARTING


# -- taxonomy ---------------------------------------------------------------- #

def test_error_categories_are_unique_and_closed():
    values = [c.value for c in ErrorCategory]
    assert len(values) == len(set(values))


def test_resp3_mismatch_has_its_own_category():
    """The fix is a client setting, not a network or server problem, so it
    must not be reported as connection_refused."""
    from worker_health import classify_redis
    exc = Exception("unknown command `HELLO`, with args beginning with: `3`")
    assert classify_redis(exc) is ErrorCategory.DEPENDENCY_VERSION


def test_evidence_classes_are_distinct():
    assert len({e.value for e in Evidence}) == 4
