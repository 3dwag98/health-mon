"""The evidence ladder: observation beats introspection beats probing."""
from __future__ import annotations

import pytest

from worker_health import ErrorCategory, Evidence, Status, TrafficLog
from worker_health.checks.base import BaseCheck, CheckContext

pytestmark = pytest.mark.unit


class Probe(BaseCheck):
    name = "dep"
    dependency = "dep"

    def __init__(self):
        self.probed = 0

    def probe(self, ctx):
        import time
        self.probed += 1
        return self.ok(ctx, time.perf_counter())


def _ctx(traffic, now=100.0, max_silence=10.0):
    return CheckContext(now=now, wall=0.0, deadline=now + 1,
                        max_silence=max_silence, traffic=traffic)


def test_fresh_traffic_is_used_and_no_probe_is_issued():
    """Real traffic is the strongest evidence and it is free."""
    log = TrafficLog()
    log.success("dep", 2.0, now=99.0)
    check = Probe()
    r = check.evaluate(_ctx(log))
    assert r.status is Status.OK
    assert r.evidence is Evidence.OBSERVED
    assert check.probed == 0
    assert r.evidence_age_ms == pytest.approx(1000.0)


def test_stale_traffic_falls_back_to_a_labelled_probe():
    log = TrafficLog()
    log.success("dep", 2.0, now=50.0)        # 50s old, past max_silence
    check = Probe()
    r = check.evaluate(_ctx(log, max_silence=10.0))
    assert r.evidence is Evidence.PROBED
    assert check.probed == 1


def test_a_real_failure_is_reported_immediately():
    """No waiting for staleness when the worker's own call already failed."""
    log = TrafficLog()
    log.success("dep", 1.0, now=95.0)
    log.failure("dep", ErrorCategory.CONNECTION_REFUSED, now=99.0)
    check = Probe()
    r = check.evaluate(_ctx(log))
    assert r.status is Status.FAILING
    assert r.evidence is Evidence.OBSERVED
    assert r.category is ErrorCategory.CONNECTION_REFUSED
    assert check.probed == 0


def test_recovery_shows_once_a_success_is_newer_than_the_failure():
    log = TrafficLog()
    log.failure("dep", ErrorCategory.TIMEOUT, now=95.0)
    log.success("dep", 1.0, now=99.0)
    r = Probe().evaluate(_ctx(log))
    assert r.status is Status.OK


def test_no_traffic_at_all_probes():
    check = Probe()
    r = check.evaluate(_ctx(TrafficLog()))
    assert r.evidence is Evidence.PROBED
    assert check.probed == 1


def test_traffic_log_never_hands_out_shared_state():
    log = TrafficLog()
    log.success("dep", 1.0)
    a = log.get("dep")
    a.successes = 9999
    assert log.get("dep").successes == 1
