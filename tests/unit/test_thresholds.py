"""L0: the operator-tunable thresholds -- validation, latency, backoff, staleness.

These are the knobs an operator turns without touching code, so the tests
are written from that angle: what happens when the value is wrong, and does
the value actually reach the thing it is supposed to control.
"""
from __future__ import annotations

import time

import pytest

from worker_health import ErrorCategory, Evidence, HealthMonitor, Status, TrafficLog
from worker_health.checks.base import BaseCheck, CheckContext
from worker_health.checks.processing import ProcessingCheck, ProcessingState
from worker_health.probes import ProbeConfigError, ProbeSpec, default_factory

pytestmark = pytest.mark.unit


def _ctx(traffic=None, now=100.0, max_silence=10.0):
    return CheckContext(now=now, wall=0.0, deadline=now + 1,
                        max_silence=max_silence, traffic=traffic or TrafficLog())


class SlowProbe(BaseCheck):
    """A check whose probe reports a latency we choose."""

    name = "dep"
    dependency = "dep"

    def __init__(self, latency_ms: float):
        self._latency_ms = latency_ms

    def probe(self, ctx):
        result = self.ok(ctx, time.perf_counter())
        return type(result)(
            name=result.name, status=result.status, checked_at=result.checked_at,
            wall_clock=result.wall_clock, evidence=Evidence.PROBED,
            latency_ms=self._latency_ms, evidence_age_ms=0.0, observed={},
        )


# -- configuration validation ------------------------------------------------ #

def test_a_timeout_that_does_not_fit_inside_the_interval_is_refused():
    """Overlapping evaluations make a check run slower than configured, and
    nothing in the worker's output would ever say so."""
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "http", "name": "vendor",
                            "interval": 1.0, "timeout": 2.0})
    # The probe must be named: a config with twelve probes and an unnamed
    # error is a puzzle, not a diagnostic.
    assert "vendor" in str(exc.value)
    assert "interval" in str(exc.value)


def test_an_unset_timeout_is_derived_from_the_interval():
    """Setting only a short interval must not silently inherit a 2s timeout
    that is longer than the interval itself."""
    assert ProbeSpec.from_raw({"type": "http", "name": "fast",
                               "interval": 1.0}).timeout == 0.5
    # The historical default is preserved at the default cadence.
    assert ProbeSpec.from_raw({"type": "http", "name": "normal"}).timeout == 2.0


def test_a_backoff_multiplier_below_one_is_refused():
    """Below 1.0 the interval shrinks on every failure -- the exact retry
    storm that backoff exists to prevent."""
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "http", "name": "vendor",
                            "backoff_multiplier": 0.5})
    assert "vendor" in str(exc.value)


def test_a_backoff_ceiling_below_its_floor_is_refused():
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "http", "name": "vendor",
                            "backoff_initial": 30.0, "backoff_max": 10.0})
    assert "vendor" in str(exc.value)


def test_a_critical_latency_below_the_warning_is_refused():
    """Otherwise the warning threshold could never fire."""
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "http", "name": "vendor",
                            "latency_warn_ms": 500, "latency_critical_ms": 100})
    assert "vendor" in str(exc.value)


def test_a_negative_staleness_window_is_refused():
    with pytest.raises(ProbeConfigError):
        ProbeSpec.from_raw({"type": "http", "name": "vendor", "max_silence": -1})


def test_one_bad_probe_does_not_stop_the_worker_when_strict_is_off():
    """A misconfigured probe is reported and skipped; the worker still boots
    with everything else, because a health library must never be the reason
    a fleet fails to start."""
    monitor = HealthMonitor("test", instance="t-1")
    installed = default_factory().install_from_config(
        monitor,
        [
            {"type": "function", "name": "good", "interval": 5,
             "params": {"fn": lambda: True}},
            {"type": "function", "name": "bad", "interval": 1, "timeout": 5,
             "params": {"fn": lambda: True}},
        ],
        strict=False,
    )
    assert [c.name for c in installed] == ["good"]
    # Skipped, but not invisible: a named failing check points at the config
    # file, where a check that simply does not exist points at nothing.
    assert monitor.checks["bad"] is not None


# -- the values actually reach the engine ------------------------------------ #

def test_backoff_settings_reach_the_state_machine():
    """The engine has always supported per-check backoff; until it could be
    written in a config file, every probe used the same hardcoded curve."""
    monitor = HealthMonitor("test", instance="t-1")
    default_factory().install(
        monitor,
        ProbeSpec.from_raw({
            "type": "function", "name": "vendor", "interval": 5,
            "backoff_initial": 1.0, "backoff_max": 4.0, "backoff_multiplier": 2.0,
            "params": {"fn": lambda: True},
        }),
    )
    spec = monitor.machine.spec("vendor")
    assert (spec.backoff_initial, spec.backoff_max, spec.backoff_multiplier) == \
        (1.0, 4.0, 2.0)


def test_latency_thresholds_reach_the_check():
    monitor = HealthMonitor("test", instance="t-1")
    check = default_factory().install(
        monitor,
        ProbeSpec.from_raw({
            "type": "function", "name": "vendor", "interval": 5,
            "latency_warn_ms": 150, "latency_critical_ms": 500,
            "params": {"fn": lambda: True},
        }),
    )
    assert check.latency_warn_ms == 150.0
    assert check.latency_critical_ms == 500.0


def test_a_probe_spec_keeps_every_field_through_the_factory():
    """The factory used to rebuild the spec field by field, which silently
    dropped every field added afterwards."""
    spec = ProbeSpec.from_raw({
        "type": "function", "name": "vendor", "interval": 5,
        "backoff_max": 42.0, "latency_warn_ms": 99.0,
        "params": {"fn": lambda: True},
    })
    check = default_factory().create(spec)
    assert check.latency_warn_ms == 99.0
    assert spec.registration_kwargs()["backoff_max"] == 42.0


# -- latency policy ----------------------------------------------------------- #

def test_a_slow_but_successful_probe_degrades():
    check = SlowProbe(latency_ms=400.0)
    check.latency_warn_ms = 150.0
    check.latency_critical_ms = 1000.0
    result = check.evaluate(_ctx())
    assert result.status is Status.DEGRADED
    assert result.category is ErrorCategory.SLOW
    assert result.observed["latency_threshold_ms"] == 150.0


def test_a_very_slow_probe_fails():
    check = SlowProbe(latency_ms=1200.0)
    check.latency_warn_ms = 150.0
    check.latency_critical_ms = 1000.0
    assert check.evaluate(_ctx()).status is Status.FAILING


def test_latency_is_judged_the_same_way_on_observed_traffic():
    """A dependency judged healthy from real traffic is held to the same bar
    as one judged from a probe.  The two disagreeing about what 'fast enough'
    means is how a worker reports OK on evidence a probe would have failed."""
    log = TrafficLog()
    log.success("dep", 400.0, now=99.5)
    check = SlowProbe(latency_ms=1.0)
    check.latency_warn_ms = 150.0
    result = check.evaluate(_ctx(log))
    assert result.evidence is Evidence.OBSERVED
    assert result.status is Status.DEGRADED
    assert result.category is ErrorCategory.SLOW


def test_no_thresholds_means_no_latency_judgement():
    """The default must stay silent: a latency bar the library picked is a
    pager going off about a threshold nobody chose."""
    assert SlowProbe(latency_ms=5000.0).evaluate(_ctx()).status is Status.OK


def test_latency_never_overwrites_a_real_failure():
    """`pool_exhausted` outranks `slow`; they go to different teams."""
    class Failing(SlowProbe):
        def probe(self, ctx):
            return self.fail(ctx, ErrorCategory.POOL_EXHAUSTED, time.perf_counter())

    check = Failing(latency_ms=9000.0)
    check.latency_critical_ms = 1.0
    result = check.evaluate(_ctx())
    assert result.status is Status.FAILING
    assert result.category is ErrorCategory.POOL_EXHAUSTED


# -- staleness: the discrimination that needs queue depth --------------------- #

class FakeBroker:
    def __init__(self, depth):
        self._depth = depth

    def read(self):
        return {"queue_depth": self._depth}


def _processing(depth, *, last_received_ago, now=1000.0, max_idle=30.0):
    state = ProcessingState()
    state.received = 1
    state.succeeded = 1
    state.started_at = now - 600.0
    state.last_received_at = now - last_received_ago
    state.last_success_at = now - last_received_ago
    check = ProcessingCheck(state, broker_state=FakeBroker(depth),
                            max_idle=max_idle, max_since_success=1e9)
    return check.evaluate(_ctx(now=now))


def test_silence_with_a_backlog_is_a_stuck_consumer():
    result = _processing(1000, last_received_ago=60.0)
    assert result.status is Status.FAILING
    assert result.category is ErrorCategory.NOT_CONSUMING
    assert result.observed["queue_depth"] == 1000


def test_silence_with_an_empty_queue_is_healthy_forever():
    """An idle worker on a quiet queue is the single most common false
    positive in every library that only watches a timer."""
    result = _processing(0, last_received_ago=3600.0)
    assert result.status is Status.OK
    assert "empty" in (result.detail or "")


def test_a_backlog_alone_is_not_a_fault():
    """Work is arriving and being handled; a queue with items in it is what
    a busy worker looks like."""
    assert _processing(1000, last_received_ago=1.0).status is Status.OK


# -- backoff must not change the reported status ------------------------------ #

def test_a_backed_off_check_keeps_reporting_failing_not_unknown():
    """Backoff changes probe FREQUENCY, never reported status.  With a fixed
    TTL it changed both: the check is asked every 60s, the result ages past a
    5s TTL in between, and a definitively failing dependency reads as
    'no current measurement' -- losing the category and the alert written
    against it, exactly during the outage."""
    from worker_health.core.clock import Clock
    from worker_health.core.machine import CheckSpec, StateMachine
    from worker_health.core.model import CheckResult, Evidence

    class FakeClock(Clock):
        def __init__(self):
            self.t = 1000.0

        def monotonic(self):
            return self.t

        def time(self):
            return self.t

    clock = FakeClock()
    spec = CheckSpec(name="db", interval=2.0, timeout=1.0, ttl=5.0,
                     failure_threshold=1, backoff_initial=30.0, backoff_max=60.0)
    machine = StateMachine([spec], clock)

    machine.apply("db", CheckResult(name="db", status=Status.FAILING,
                                    checked_at=clock.t, wall_clock=clock.t,
                                    evidence=Evidence.PROBED,
                                    category=ErrorCategory.CONNECTION_REFUSED))
    assert machine.effective("db") is Status.FAILING

    # Well past the 5s TTL, but inside the 30s the check is now waiting.
    clock.t += 20.0
    assert machine.effective("db") is Status.FAILING
    assert machine.state("db").last_result.category is ErrorCategory.CONNECTION_REFUSED

    # Past even the backed-off interval: now there genuinely is no current
    # measurement, and saying so is correct.
    clock.t += 100.0
    assert machine.effective("db") is Status.UNKNOWN


def test_a_healthy_check_keeps_its_tight_ttl():
    """The widened TTL must not blunt the signal it exists for: a scheduler
    that stops writing results has to become visible quickly."""
    from worker_health.core.clock import Clock
    from worker_health.core.machine import CheckSpec, StateMachine
    from worker_health.core.model import CheckResult, Evidence

    class FakeClock(Clock):
        def __init__(self):
            self.t = 1000.0

        def monotonic(self):
            return self.t

        def time(self):
            return self.t

    clock = FakeClock()
    machine = StateMachine([CheckSpec(name="db", interval=2.0, timeout=1.0, ttl=5.0)],
                           clock)
    machine.apply("db", CheckResult(name="db", status=Status.OK, checked_at=clock.t,
                                    wall_clock=clock.t, evidence=Evidence.PROBED))
    assert machine.effective("db") is Status.OK
    clock.t += 6.0
    assert machine.effective("db") is Status.UNKNOWN


# -- liveness: what a restart can and cannot fix ------------------------------ #

def _wedged_monitor(category, *, live_on_self_fault=True):
    """A monitor with one critical check reporting `category` as failing."""
    from worker_health.core.model import CheckResult, Evidence

    monitor = HealthMonitor("test", instance="t-1",
                            live_on_self_fault=live_on_self_fault)
    monitor.register(_Never(), name="worker", critical=True,
                     interval=1.0, timeout=0.5, ttl=1e9, failure_threshold=1)
    monitor.apply(CheckResult(
        name="worker", status=Status.FAILING, checked_at=monitor.clock.monotonic(),
        wall_clock=0.0, evidence=Evidence.OBSERVED, category=category,
        observed={"queue_depth": 1000, "idle_seconds": 60},
    ))
    return monitor


class _Never(BaseCheck):
    name = "worker"

    def probe(self, ctx):
        return self.ok(ctx, time.perf_counter())


def test_a_backlog_nobody_is_consuming_flips_live_to_503():
    """The process is running and will sit there forever. A restart is the
    actual remedy, and /live is the only signal a supervisor watches."""
    monitor = _wedged_monitor(ErrorCategory.NOT_CONSUMING)
    assert monitor.live_code() == 503
    assert any("not_consuming" in r for r in monitor.live_reasons())


def test_a_poison_loop_flips_live_to_503():
    assert _wedged_monitor(ErrorCategory.POISON_LOOP).live_code() == 503


def test_a_lost_dependency_never_flips_live():
    """The distinction the whole package rests on. Restarting a worker
    because the broker went away does not bring the broker back -- it turns
    one outage into a crash-looping fleet."""
    for category in (ErrorCategory.CONNECTION_LOST,
                     ErrorCategory.CONNECTION_REFUSED,
                     ErrorCategory.TIMEOUT,
                     ErrorCategory.POOL_EXHAUSTED,
                     ErrorCategory.SLOW):
        monitor = _wedged_monitor(category)
        assert monitor.live_code() == 200, category
        # ...while readiness does follow it down.
        assert monitor.ready_code() == 503, category


def test_an_idle_worker_on_a_quiet_queue_stays_live():
    """No false positives for idle workers: nothing is failing, so nothing
    is wedged."""
    monitor = HealthMonitor("test", instance="t-1")
    monitor.register(_Never(), name="worker", critical=True,
                     interval=1.0, timeout=0.5, ttl=1e9)
    assert monitor.live_code() == 200
    assert monitor.live_reasons() == ()


def test_liveness_can_be_kept_purely_about_loop_lag():
    monitor = _wedged_monitor(ErrorCategory.NOT_CONSUMING, live_on_self_fault=False)
    assert monitor.live_code() == 200


# -- pool ratios -------------------------------------------------------------- #

class _Pool:
    def __init__(self, checked_out, size=10, overflow=0):
        self._c, self._s, self._o = checked_out, size, overflow

    def checkedout(self):
        return self._c

    def size(self):
        return self._s

    def overflow(self):
        return self._o


class _Engine:
    def __init__(self, pool):
        self.pool = pool


def _postgres(checked_out, **kwargs):
    from worker_health.checks.postgres import PostgresCheck
    check = PostgresCheck(app_engine=_Engine(_Pool(checked_out)), **kwargs)
    return check.introspect(_ctx())


def test_pool_critical_ratio_catches_exhaustion_before_it_is_total():
    """By the time the pool is completely full, work has already been
    waiting behind it."""
    result = _postgres(8, pool_warn_ratio=0.6, pool_critical_ratio=0.8)
    assert result.status is Status.DEGRADED
    assert result.category is ErrorCategory.POOL_EXHAUSTED


def test_pool_warn_ratio_is_reported_without_changing_status():
    result = _postgres(7, pool_warn_ratio=0.6, pool_critical_ratio=0.9)
    assert result.status is Status.OK
    assert result.observed["pool_pressure"] is True


def test_the_default_pool_behaviour_is_unchanged():
    """1.0 means 'only when completely full', which is what this did before
    the ratio became configurable."""
    assert _postgres(9).status is Status.OK
    assert _postgres(10).status is Status.DEGRADED


def test_a_pool_ratio_outside_zero_to_one_is_refused():
    with pytest.raises(ProbeConfigError):
        ProbeSpec.from_raw({"type": "postgres", "name": "db", "pool_warn_ratio": 1.5})


def test_a_critical_pool_ratio_below_the_warning_is_refused():
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "postgres", "name": "db",
                            "pool_warn_ratio": 0.9, "pool_critical_ratio": 0.5})
    assert "db" in str(exc.value)


# -- spelling ------------------------------------------------------------------ #

def test_the_plans_field_spellings_are_accepted():
    """Config written against one naming convention must not silently become
    a param no builder reads -- which is how a threshold set in good faith
    ends up doing nothing at all."""
    spec = ProbeSpec.from_raw({
        "type": "rabbitmq", "name": "broker", "interval": 10.0,
        "max_backoff_seconds": 60.0, "backoff_multiplier": 2.0,
        "stale_after": 30.0,
    })
    assert spec.backoff_max == 60.0
    assert spec.stale_after_seconds == 30.0
    assert "max_backoff_seconds" not in spec.params


def test_spec_level_staleness_reaches_the_check():
    spec = ProbeSpec.from_raw({
        "type": "rabbitmq", "name": "broker", "interval": 10.0,
        "stale_after_seconds": 30.0,
        "params": {"broker_state": object(), "queue_name": "billing.in"},
    })
    check = default_factory().create(spec)
    assert check._stale_after == 30.0
    # `queue_name` is the Django-settings spelling; `queue` is the SDK's.
    assert check.queue == "billing.in"


def test_an_explicit_param_beats_the_spec_level_threshold():
    """Someone who wrote the low-level spelling meant it."""
    spec = ProbeSpec.from_raw({
        "type": "rabbitmq", "name": "broker", "interval": 10.0,
        "stale_after_seconds": 30.0,
        "params": {"broker_state": object(), "queue": "q", "stale_after": 5.0},
    })
    assert default_factory().create(spec)._stale_after == 5.0


# -- the Django settings shape ------------------------------------------------ #

def test_django_upper_case_settings_reach_every_new_field():
    """Operators tune this from `settings.WORKER_HEALTH` without touching
    code, so the UPPER_CASE spelling has to land on the same fields the YAML
    spelling does."""
    from worker_health.config import HealthConfig

    config = HealthConfig.from_mapping({
        "SERVICE": "billing_worker",
        "ENVIRONMENT": "production",
        "OTEL_ENDPOINT": "http://otel-collector:4318",
        "OTEL_INTERVAL": 10.0,
        "LIVE_ON_SELF_FAULT": False,
        "PROBES": [
            {"type": "django_db", "name": "primary_db", "critical": True,
             "interval": 15.0, "timeout": 2.0,
             "latency_warn_ms": 150, "latency_critical_ms": 500,
             "pool_warn_ratio": 0.80, "max_silence": 60.0},
            {"type": "rabbitmq", "name": "broker", "critical": True,
             "interval": 10.0, "stale_after_seconds": 30.0,
             "backoff_multiplier": 2.0, "max_backoff_seconds": 60.0,
             "params": {"queue_name": "billing.in"}},
            {"type": "redis", "name": "cache", "critical": False,
             "interval": 30.0},
        ],
    })

    assert config.service == "billing_worker"
    assert config.environment == "production"
    assert config.otel_endpoint == "http://otel-collector:4318"
    assert config.otel_interval == 10.0
    assert config.live_on_self_fault is False

    db = config.probe("primary_db")
    assert (db.latency_warn_ms, db.latency_critical_ms) == (150.0, 500.0)
    assert db.pool_warn_ratio == 0.80

    broker = config.probe("broker")
    assert broker.stale_after_seconds == 30.0
    assert broker.backoff_max == 60.0          # written as max_backoff_seconds
    assert broker.backoff_multiplier == 2.0

    cache = config.probe("cache")
    assert cache.critical is False
    # Nothing was said about its timeout, so it derives rather than
    # inheriting a 2s default it never chose.
    assert cache.timeout == 2.0 and cache.interval == 30.0


def test_a_dependency_outage_outranks_a_self_fault_for_liveness():
    """The subtle restart storm. A handler failing on every message because
    the database is refusing connections trips the poison-loop threshold in
    seconds -- and that is a correct process in front of a broken dependency,
    not a wedged one. Restarting it does not fix the database, which fails
    the next ten messages, which restarts it again."""
    from worker_health.core.model import CheckResult, Evidence

    monitor = HealthMonitor("test", instance="t-1")
    for name in ("db", "processing"):
        monitor.register(_Never(), name=name, critical=True,
                         interval=1.0, timeout=0.5, ttl=1e9, failure_threshold=1)
    now = monitor.clock.monotonic()

    def report(name, category):
        monitor.apply(CheckResult(
            name=name, status=Status.FAILING, checked_at=now, wall_clock=0.0,
            evidence=Evidence.OBSERVED, category=category))

    # The wedge alone restarts.
    report("processing", ErrorCategory.POISON_LOOP)
    assert monitor.live_code() == 503

    # The same wedge, with the database down, does not.
    report("db", ErrorCategory.CONNECTION_REFUSED)
    assert monitor.live_code() == 200, "a dependency outage must never restart"
    assert monitor.ready_code() == 503, "but it must still be unready"

    # Database back, wedge still there: now a restart is the right answer.
    monitor.apply(CheckResult(name="db", status=Status.OK, checked_at=now,
                              wall_clock=0.0, evidence=Evidence.OBSERVED))
    monitor.apply(CheckResult(name="db", status=Status.OK, checked_at=now,
                              wall_clock=0.0, evidence=Evidence.OBSERVED))
    assert monitor.live_code() == 503
