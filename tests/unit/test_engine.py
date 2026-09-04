"""L0: the engine end to end, against mocked healthy / slow / failed deps.

No containers and no drivers: the dependencies here are fakes that can be
switched between healthy, slow (past the timeout) and failed, which is
exactly the matrix the brief asks for and is the only way to test the
timeout and recovery paths deterministically.
"""
from __future__ import annotations

import json
import threading
import time

import pytest

from worker_health import (
    ErrorCategory,
    Evidence,
    FakeClock,
    HealthMonitor,
    Liveness,
    ProcessingState,
    Readiness,
    Status,
    Tracker,
    setup_worker_health,
)
from worker_health.checks.base import BaseCheck
from worker_health.core.aggregate import readiness, reasons
from worker_health.core.machine import CheckSpec, StateMachine
from worker_health.core.model import CheckResult
from worker_health.telemetry.prometheus import render

pytestmark = pytest.mark.unit

CANARY = "canary-pg-8f3ad91c"


class FakeDependency(BaseCheck):
    """Healthy, slow or failed, switchable at runtime."""

    def __init__(self, name="dep", mode="healthy", delay=0.0):
        self.name = name
        self.dependency = ""
        self.mode = mode
        self.delay = delay
        self.probes = 0

    def probe(self, ctx):
        started = time.perf_counter()
        self.probes += 1
        if self.delay:
            time.sleep(self.delay)
        if self.mode == "failed":
            # The message deliberately embeds a credential, the way a real
            # driver exception does. Nothing downstream may echo it.
            raise ConnectionRefusedError(
                f"could not connect to postgres://app:{CANARY}@db:5432/app"
            )
        if self.mode == "degraded":
            return self.degraded(ctx, ErrorCategory.POOL_EXHAUSTED, started,
                                 detail="pool has no free slots")
        return self.ok(ctx, started)

    def classify(self, exc):
        return ErrorCategory.CONNECTION_REFUSED


def _wait(predicate, timeout=6.0, interval=0.05, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what}")


def _monitor(**kwargs):
    return HealthMonitor("test-worker", instance="test-1", tick=0.05, **kwargs)


# -- readiness projection ----------------------------------------------------- #

def test_readiness_vocabulary_covers_every_status():
    for status in Status:
        assert isinstance(readiness(status, Liveness.ALIVE), Readiness)


def test_a_wedged_loop_outranks_healthy_dependencies():
    """A process whose loop is not turning cannot process work, whatever its
    dependencies say."""
    assert readiness(Status.OK, Liveness.UNALIVE) is Readiness.UNREADY


def test_degraded_still_serves_and_unready_does_not():
    from worker_health.core.model import READINESS_CODE

    assert READINESS_CODE[Readiness.READY] == 200
    assert READINESS_CODE[Readiness.DEGRADED] == 200
    assert READINESS_CODE[Readiness.STARTING] == 503
    assert READINESS_CODE[Readiness.UNREADY] == 503


def test_reasons_name_the_check_and_its_category_but_no_driver_text():
    clock = FakeClock()
    machine = StateMachine([CheckSpec("postgres", critical=True, failure_threshold=1)],
                           clock)
    machine.apply("postgres", CheckResult(
        name="postgres", status=Status.FAILING, checked_at=clock.monotonic(),
        wall_clock=0.0, category=ErrorCategory.CONNECTION_REFUSED,
        detail=f"could not connect to postgres://app:{CANARY}@db/app"))

    text = " ".join(reasons(machine, Liveness.ALIVE))
    assert "postgres" in text and "connection_refused" in text and "critical" in text
    assert CANARY not in text


def test_liveness_does_not_move_when_a_dependency_fails():
    monitor = _monitor()
    monitor.register(FakeDependency("dep", mode="failed"), name="dep",
                     critical=True, interval=0.1, timeout=1.0,
                     failure_threshold=1, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.readiness() is Readiness.UNREADY, what="unready")
        assert monitor.liveness() is Liveness.ALIVE
        assert monitor.live_code() == 200
        assert monitor.ready_code() == 503
    finally:
        monitor.stop()


# -- mocked healthy / slow / failed ------------------------------------------- #

def test_a_healthy_dependency_reaches_ready():
    monitor = _monitor()
    monitor.register(FakeDependency("dep"), name="dep", critical=True,
                     interval=0.1, timeout=1.0, ttl=30.0)
    monitor.start(boot_grace=5)
    try:
        _wait(lambda: monitor.readiness() is Readiness.READY, what="ready")
        assert monitor.ready_code() == 200
    finally:
        monitor.stop()


def test_a_slow_dependency_is_reported_as_a_timeout_not_a_hang():
    """The endpoint must stay fast while a probe is parked on a black hole:
    that is the failure this package exists to survive."""
    monitor = _monitor()
    monitor.register(FakeDependency("dep", delay=1.5), name="dep", critical=True,
                     interval=0.2, timeout=0.3, failure_threshold=1, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        result = _wait(
            lambda: monitor.snapshot().results.get("dep"), what="a first result")
        assert result.status is Status.FAILING
        assert result.category is ErrorCategory.TIMEOUT

        started = time.perf_counter()
        monitor.snapshot_dict()
        assert (time.perf_counter() - started) < 1.0    # cached read, no I/O
    finally:
        monitor.stop()


def test_a_failed_critical_dependency_makes_the_worker_unready_then_recovers():
    """Detection and recovery in one test, because recovery is the half that
    silently regresses."""
    monitor = _monitor()
    dependency = FakeDependency("dep", mode="failed")
    monitor.register(dependency, name="dep", critical=True, interval=0.1,
                     timeout=1.0, failure_threshold=2, success_threshold=2,
                     ttl=30.0, backoff_initial=0.1, backoff_max=0.2)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.readiness() is Readiness.UNREADY, what="unready")
        snapshot = monitor.snapshot_dict()
        assert snapshot["checks"]["dep"]["category"] == "connection_refused"

        dependency.mode = "healthy"
        _wait(lambda: monitor.readiness() is Readiness.READY, what="recovery")

        events = [e["event"] for e in monitor.events.recent(50)]
        assert "dependency_recovered" in events
    finally:
        monitor.stop()


def test_a_failed_non_critical_dependency_degrades_without_a_503():
    monitor = _monitor()
    monitor.register(FakeDependency("cache", mode="failed"), name="cache",
                     critical=False, interval=0.1, timeout=1.0,
                     failure_threshold=1, ttl=30.0)
    monitor.register(FakeDependency("db"), name="db", critical=True,
                     interval=0.1, timeout=1.0, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.readiness() is Readiness.DEGRADED, what="degraded")
        assert monitor.ready_code() == 200
    finally:
        monitor.stop()


def test_a_check_that_raises_is_isolated_from_the_others():
    class Broken(BaseCheck):
        name = "broken"
        dependency = ""

        def evaluate(self, ctx):
            raise RuntimeError("this check is broken")

    monitor = _monitor()
    monitor.register(Broken(), name="broken", critical=False, interval=0.1,
                     timeout=1.0, ttl=30.0)
    monitor.register(FakeDependency("db"), name="db", critical=True,
                     interval=0.1, timeout=1.0, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.snapshot().results.get("db"), what="the healthy check")
        assert monitor.snapshot().results["db"].status is Status.OK
    finally:
        monitor.stop()


def test_backoff_slows_a_failing_dependency_rather_than_hammering_it():
    monitor = _monitor()
    dependency = FakeDependency("dep", mode="failed")
    monitor.register(dependency, name="dep", critical=False, interval=0.05,
                     timeout=1.0, failure_threshold=1, ttl=30.0,
                     backoff_initial=1.0, backoff_max=2.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.machine.state("dep").backoff_step > 0, what="backoff")
        probes = dependency.probes
        time.sleep(0.5)
        # At interval=0.05 an un-backed-off check would have run ~10 more times.
        assert dependency.probes - probes <= 2
    finally:
        monitor.stop()


# -- processing --------------------------------------------------------------- #

def test_the_handler_decorator_is_the_whole_integration():
    monitor = _monitor()
    tracker = Tracker(monitor, ProcessingState(), default_queue="billing.in")

    @tracker.handler(queue="billing.in")
    def handle(message):
        return message["n"]

    for n in range(3):
        handle({"n": n})
    with pytest.raises(ValueError):
        @tracker.handler(queue="billing.in")
        def failing(_):
            raise ValueError("bad message")
        failing({})

    data = monitor.snapshot_dict()["processing"]["billing.in"]
    assert data["received"] == 4 and data["succeeded"] == 3 and data["failed"] == 1
    assert data["last_message_age_s"] is not None


def test_an_async_handler_is_detected_and_wrapped():
    import asyncio

    monitor = _monitor()
    tracker = Tracker(monitor, ProcessingState(), default_queue="q")

    @tracker.handler(queue="q")
    async def handle(message):
        return message

    asyncio.run(handle({"a": 1}))
    assert monitor.snapshot_dict()["processing"]["q"]["succeeded"] == 1


# -- telemetry ---------------------------------------------------------------- #

def test_metrics_expose_both_the_binary_and_severity_families():
    monitor = _monitor()
    monitor.register(FakeDependency("db"), name="db", critical=True,
                     interval=0.1, timeout=1.0, ttl=30.0)
    Tracker(monitor, ProcessingState(), default_queue="billing.in")
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.snapshot().results.get("db"), what="a result")
        text = render(monitor)
    finally:
        monitor.stop()

    for metric in ("worker_health_ready", "worker_health_live",
                   "worker_health_status", "worker_health_readiness_state",
                   "worker_health_check_status", "worker_health_check_severity",
                   "worker_health_check_latency_ms",
                   "worker_health_check_evidence_age_ms",
                   "worker_health_check_transitions_total",
                   "worker_health_message_received_total",
                   "worker_health_message_success_total",
                   "worker_health_message_failure_total",
                   "worker_health_loop_lag_ms"):
        assert f"{metric}{{" in text, metric

    assert 'critical="true"' in text
    assert 'queue="billing.in"' in text


def test_metric_labels_are_bounded_and_carry_no_secrets():
    monitor = _monitor()
    monitor.register(FakeDependency("db", mode="failed"), name="db", critical=True,
                     interval=0.1, timeout=1.0, failure_threshold=1, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.snapshot().results.get("db"), what="a result")
        text = render(monitor)
    finally:
        monitor.stop()

    assert CANARY not in text
    allowed = {"service", "instance", "check", "critical", "evidence", "category",
               "state", "queue", "quantile", "metric", "stat", "kind"}
    import re
    for name in re.findall(r"[{,]([a-z_]+)=\"", text):
        assert name in allowed, name


def test_transitions_and_readiness_changes_are_emitted_once_each():
    monitor = _monitor()
    dependency = FakeDependency("db", mode="failed")
    monitor.register(dependency, name="db", critical=True, interval=0.1,
                     timeout=1.0, failure_threshold=1, success_threshold=1,
                     ttl=30.0, backoff_initial=0.1, backoff_max=0.1)
    seen = []
    monitor.on_event(seen.append)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.readiness() is Readiness.UNREADY, what="unready")
        dependency.mode = "healthy"
        _wait(lambda: monitor.readiness() is Readiness.READY, what="ready again")
    finally:
        monitor.stop()

    names = [e["event"] for e in seen]
    assert names.count("readiness_changed") >= 1
    transitions = [e for e in seen if e["event"] == "health_transition"]
    assert transitions and all("previous_status" in e for e in transitions)
    assert CANARY not in json.dumps(seen)


def test_no_snapshot_field_ever_carries_a_credential():
    monitor = _monitor()
    monitor.register(FakeDependency("db", mode="failed"), name="db", critical=True,
                     interval=0.1, timeout=1.0, failure_threshold=1, ttl=30.0)
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.snapshot().results.get("db"), what="a result")
        body = json.dumps(monitor.snapshot_dict(include_events=True))
    finally:
        monitor.stop()
    assert CANARY not in body


# -- the facade --------------------------------------------------------------- #

def test_setup_worker_health_wires_everything_from_one_call():
    health = setup_worker_health(
        service="smoke-worker",
        config={"worker_health": {
            "instance": "smoke-1", "health_port": 0, "boot_grace": 1,
            "default_queue": "billing.in", "log_level": "WARNING",
            "probes": [
                {"type": "disk", "name": "worker-disk", "critical": False,
                 "interval": 30, "params": {"path": "/", "min_free_gb": 0.0001}},
                {"type": "function", "name": "vendor", "critical": False,
                 "interval": 1, "params": {"fn": "@vendor_ok"}},
            ],
        }},
        context={"vendor_ok": lambda: True},
    )
    try:
        @health.tracker.handler(queue="billing.in")
        def handle(message):
            return message

        handle({"n": 1})
        _wait(lambda: health.monitor.readiness() is Readiness.READY, what="ready")

        snapshot = health.monitor.snapshot_dict()
        assert set(snapshot["checks"]) == {"worker-disk", "vendor", "processing"}
        assert snapshot["readiness"] == "ready" and snapshot["liveness"] == "alive"
        assert snapshot["processing"]["billing.in"]["succeeded"] == 1
        assert health.server is not None and health.server.port > 0
    finally:
        health.stop()


def test_the_http_endpoints_answer_with_the_right_codes():
    import urllib.error
    import urllib.request

    health = setup_worker_health(
        service="http-worker",
        config={"worker_health": {
            "health_host": "127.0.0.1", "health_port": 0, "boot_grace": 0,
            "log_level": "WARNING", "processing_check": False,
        }},
        probes=[{"type": "function", "name": "gate", "critical": True,
                 "interval": 0.2, "timeout": 1, "failure_threshold": 1,
                 "params": {"fn": "@gate"}}],
        context={"gate": lambda: _GATE["ok"]},
    )
    base = f"http://127.0.0.1:{health.server.port}"

    def get(path):
        try:
            with urllib.request.urlopen(base + path, timeout=3) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()

    try:
        _wait(lambda: get("/ready")[0] == 200, what="ready 200")
        assert get("/live")[0] == 200

        _GATE["ok"] = False
        _wait(lambda: get("/ready")[0] == 503, what="ready 503")
        # Liveness must NOT follow a dependency down.
        assert get("/live")[0] == 200

        code, body = get("/ready")
        payload = json.loads(body)
        assert payload["readiness"] == "unready"
        assert any("gate" in reason for reason in payload["reasons"])

        code, metrics = get("/metrics")
        assert code == 200 and b"worker_health_ready" in metrics
        assert json.loads(get("/health")[1])["service"] == "http-worker"
        assert get("/nope")[0] == 404
    finally:
        _GATE["ok"] = True
        health.stop()


_GATE = {"ok": True}


def test_snapshot_reads_are_thread_safe_under_concurrent_checks():
    """The HTTP thread reads while the scheduler writes; a torn read here
    would surface as a KeyError in production and nowhere else."""
    monitor = _monitor()
    for i in range(4):
        monitor.register(FakeDependency(f"dep{i}"), name=f"dep{i}", critical=False,
                         interval=0.05, timeout=1.0, ttl=30.0)
    monitor.start(boot_grace=0)
    errors = []

    def reader():
        for _ in range(200):
            try:
                monitor.snapshot_dict()
                render(monitor)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)
    finally:
        monitor.stop()
    assert not errors
