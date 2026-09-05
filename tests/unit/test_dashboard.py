"""L0: the fleet aggregator -- OTLP ingestion, outage rollup, staleness.

The decode tests deliberately feed the aggregator payloads built by the
SDK's own exporter rather than hand-written fixtures. The two have to agree
about the wire format, and a fixture would let them drift apart quietly:
the encoder would keep passing its tests, the decoder would keep passing
its tests, and the board would go blank in production.
"""
from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request

import pytest

from worker_health import HealthMonitor, Status
from worker_health.checks.base import BaseCheck
from worker_health.core.model import CheckResult, ErrorCategory, Evidence
from worker_health.telemetry.otel import build_metrics

from dashboard.app import (Fleet, as_body, decode_metrics, shared_outages,
                           stale_workers)

pytestmark = pytest.mark.unit


class Dependency(BaseCheck):
    name = "postgres"
    dependency = "postgres"

    def __init__(self, category=None):
        self.category = category

    def probe(self, ctx):
        if self.category is None:
            return self.ok(ctx, time.perf_counter())
        return self.fail(ctx, self.category, time.perf_counter())


def _payload(service="billing", instance="billing-1", category=None,
             environment="production"):
    """A real OTLP payload from a real monitor."""
    monitor = HealthMonitor(service, instance=instance, version="1.2.3",
                            environment=environment)
    check = Dependency(category)
    monitor.register(check, name="postgres", critical=True,
                     interval=0.05, timeout=0.02, ttl=1e9, failure_threshold=1)
    monitor.start(boot_grace=0)
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if monitor.snapshot().results.get("postgres"):
                break
            time.sleep(0.02)
        return build_metrics(monitor)
    finally:
        monitor.stop()


# -- decoding ----------------------------------------------------------------- #

def test_a_pushed_payload_identifies_its_worker():
    views = decode_metrics(_payload())
    assert len(views) == 1
    view = views[0]
    assert view["service"] == "billing"
    assert view["instance"] == "billing-1"
    assert view["environment"] == "production"
    assert view["version"] == "1.2.3"


def test_a_healthy_payload_decodes_to_a_healthy_body():
    body = as_body(decode_metrics(_payload())[0])
    assert body["status"] == "ok"
    assert body["readiness"] == "ready"
    assert body["liveness"] == "alive"
    assert body["checks"]["postgres"]["internal_status"] == "ok"
    assert body["checks"]["postgres"]["critical"] is True
    assert "reasons" not in body


def test_a_failing_payload_carries_the_check_and_its_category():
    body = as_body(decode_metrics(
        _payload(category=ErrorCategory.CONNECTION_REFUSED))[0])
    assert body["status"] == "failing"
    assert body["readiness"] == "unready"
    assert body["checks"]["postgres"]["internal_status"] == "failing"
    assert body["checks"]["postgres"]["category"] == "connection_refused"
    assert any("connection_refused" in r for r in body["reasons"])


def test_a_dependency_failure_still_decodes_as_alive():
    """The distinction the whole package rests on has to survive the wire."""
    body = as_body(decode_metrics(
        _payload(category=ErrorCategory.CONNECTION_REFUSED))[0])
    assert body["liveness"] == "alive"


def test_an_unknown_metric_is_skipped_rather_than_fatal():
    """A dashboard that 500s because a worker shipped a new series is a
    dashboard that goes dark exactly when someone deployed something."""
    payload = _payload()
    metrics = payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
    metrics.append({"name": "worker_health_brand_new_thing",
                    "gauge": {"dataPoints": [{"asDouble": 1.0,
                                              "timeUnixNano": "1"}]}})
    metrics.append({"name": "shaped_like_nothing"})
    assert decode_metrics(payload)[0]["service"] == "billing"


def test_a_payload_with_no_identity_is_ignored():
    assert decode_metrics({"resourceMetrics": [{"resource": {}}]}) == []
    assert decode_metrics({}) == []


# -- the rollup ---------------------------------------------------------------- #

def _entry(name, checks, processing=None):
    return {"name": name, "body": {"checks": checks,
                                   "processing": processing or {}}}


def test_one_dependency_outage_is_one_row_not_fifty():
    """Fifty workers reporting the same database down is ONE database
    outage. Rendering it as fifty sick workers buries the only fact anyone
    can act on."""
    broken = {"postgres": {"internal_status": "failing",
                           "category": "connection_refused", "critical": True}}
    entries = [_entry(f"worker-{i}", dict(broken)) for i in range(50)]

    outages = shared_outages(entries)
    assert len(outages) == 1
    outage = outages[0]
    assert outage["check"] == "postgres"
    assert outage["category"] == "connection_refused"
    assert outage["count"] == 50
    assert outage["shared"] is True
    assert outage["critical"] is True


def test_a_single_sick_worker_is_not_called_a_shared_outage():
    entries = [
        _entry("a", {"redis": {"internal_status": "degraded",
                               "category": "memory_pressure"}}),
        _entry("b", {}),
    ]
    outage = shared_outages(entries)[0]
    assert outage["count"] == 1 and outage["shared"] is False


def test_different_causes_on_one_check_stay_separate():
    """`timeout` and `connection_refused` on the same dependency are two
    different findings and go to two different places."""
    entries = [
        _entry("a", {"pg": {"internal_status": "failing", "category": "timeout"}}),
        _entry("b", {"pg": {"internal_status": "failing", "category": "timeout"}}),
        _entry("c", {"pg": {"internal_status": "failing",
                            "category": "connection_refused"}}),
    ]
    outages = shared_outages(entries)
    assert {(o["category"], o["count"]) for o in outages} == {
        ("timeout", 2), ("connection_refused", 1)}


def test_shared_and_critical_outages_sort_first():
    """The order someone reads under pressure, which is not alphabetical."""
    entries = [
        _entry("a", {"zzz-cache": {"internal_status": "degraded",
                                   "category": "memory_pressure"}}),
        _entry("a2", {"aaa-db": {"internal_status": "failing",
                                 "category": "timeout", "critical": True}}),
        _entry("b2", {"aaa-db": {"internal_status": "failing",
                                 "category": "timeout", "critical": True}}),
    ]
    assert shared_outages(entries)[0]["check"] == "aaa-db"


def test_a_healthy_fleet_has_no_outages():
    assert shared_outages([_entry("a", {"pg": {"internal_status": "ok"}})]) == []


# -- staleness ------------------------------------------------------------------ #

def test_a_backlog_with_silence_is_flagged_stale():
    entries = [_entry("a", {}, {"billing.in": {"queue_lag": 1000,
                                               "last_message_age_s": 120}})]
    stale = stale_workers(entries, stale_after=60)
    assert len(stale) == 1
    assert stale[0]["silent_with_backlog"] is True
    assert stale[0]["queue_lag"] == 1000


def test_an_empty_queue_with_silence_is_never_flagged():
    """The false positive that teaches a team to ignore the board."""
    entries = [_entry("a", {}, {"billing.in": {"queue_lag": 0,
                                               "last_message_age_s": 3600}})]
    assert stale_workers(entries, stale_after=60) == []


def test_a_busy_worker_with_a_backlog_is_never_flagged():
    entries = [_entry("a", {}, {"billing.in": {"queue_lag": 5000,
                                               "last_message_age_s": 1}})]
    assert stale_workers(entries, stale_after=60) == []


def test_a_wedged_category_is_flagged_and_marked_restartable():
    entries = [_entry("a", {"processing": {"internal_status": "failing",
                                           "category": "not_consuming"}})]
    stale = stale_workers(entries)
    assert stale[0]["categories"] == ["not_consuming"]
    assert stale[0]["restart_would_help"] is True


def test_a_dependency_failure_is_not_staleness():
    entries = [_entry("a", {"pg": {"internal_status": "failing",
                                   "category": "connection_refused"}})]
    assert stale_workers(entries) == []


# -- the Fleet model ------------------------------------------------------------ #

def test_a_pushed_worker_appears_without_being_configured():
    """The point of the push path: the board must not need to be told the
    address of every process a supervisor happened to start."""
    fleet = Fleet()
    fleet.ingest_otlp(decode_metrics(_payload(instance="billing-7")))
    names = [w["name"] for w in fleet.snapshot()["workers"]]
    assert names == ["billing-7"]
    assert fleet.snapshot()["workers"][0]["source"] == "otlp"


def test_a_pushed_worker_merges_into_the_polled_one():
    """A pushed `billing-1` and a polled `billing` are one worker; showing
    them as two would double the fleet."""
    fleet = Fleet()
    fleet.update("billing", "http://billing:8080",
                 {"status": "ok", "instance": "billing-1", "checks": {}}, None)
    fleet.ingest_otlp(decode_metrics(_payload(instance="billing-1")))

    workers = fleet.snapshot()["workers"]
    assert len(workers) == 1
    assert workers[0]["name"] == "billing"
    assert workers[0]["source"] == "poll+otlp"
    # The richer polled body wins: it carries reasons and per-check detail
    # that a metric stream cannot.
    assert workers[0]["body"]["status"] == "ok"


def test_a_quiet_pushed_worker_is_forgotten():
    fleet = Fleet()
    fleet.ingest_otlp(decode_metrics(_payload(instance="gone-1")))
    fleet._latest["gone-1"]["otlp_at"] = time.time() - 10_000
    fleet.expire()
    assert fleet.snapshot()["workers"] == []


def test_a_polled_worker_that_stops_answering_stays_on_the_board():
    """A polled worker going silent is a REPORTED failure, not an absence.
    Expiring it would turn an outage into an empty space."""
    fleet = Fleet()
    fleet.update("billing", "http://billing:8080", None, "TimeoutError")
    fleet._latest["billing"]["otlp_at"] = time.time() - 10_000
    fleet.expire()
    assert [w["name"] for w in fleet.snapshot()["workers"]] == ["billing"]


def test_the_snapshot_carries_the_rollup():
    fleet = Fleet()
    fleet.ingest_otlp(decode_metrics(
        _payload(instance="a-1", category=ErrorCategory.CONNECTION_REFUSED)))
    fleet.ingest_otlp(decode_metrics(
        _payload(instance="b-1", category=ErrorCategory.CONNECTION_REFUSED)))

    snapshot = fleet.snapshot()
    assert len(snapshot["workers"]) == 2
    outage = snapshot["outages"][0]
    assert outage["check"] == "postgres" and outage["count"] == 2
    assert outage["shared"] is True


def test_a_wedge_explained_by_a_dependency_is_not_a_restart_candidate():
    """The board and /live have to agree. A handler failing on every message
    because the database is down trips the poison-loop threshold in seconds;
    telling someone to restart is how a dependency outage becomes a crash
    loop."""
    entries = [_entry("a", {
        "postgres": {"internal_status": "failing",
                     "category": "connection_refused", "critical": True},
        "processing": {"internal_status": "failing", "category": "poison_loop"},
    })]
    stale = stale_workers(entries)[0]
    assert stale["categories"] == ["poison_loop"]
    assert stale["restart_would_help"] is False
    assert stale["explained_by_dependency"] is True
