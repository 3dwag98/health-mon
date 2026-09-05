"""L0: OTLP export -- payload shape, bounded queue, and silence under failure.

The exporter's whole contract is that it can never hurt the worker, so most
of these tests are about what it does when the collector misbehaves.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from worker_health import HealthMonitor, Status
from worker_health.checks.base import BaseCheck
from worker_health.telemetry.otel import (LOGS_PATH, METRICS_PATH, OTLPExporter,
                                          build_logs, build_metrics)

pytestmark = pytest.mark.unit


class Collector:
    """A stand-in OTLP/HTTP receiver that records what it was sent."""

    def __init__(self, status: int = 200):
        self.received: list = []
        self._status = status
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                return

            def do_POST(self):  # noqa: N802
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                with outer._lock:
                    outer.received.append((self.path, json.loads(body)))
                self.send_response(outer._status)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"

    def __enter__(self):
        threading.Thread(target=self._server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self._server.shutdown()
        self._server.server_close()

    def paths(self):
        with self._lock:
            return [p for p, _ in self.received]


class Healthy(BaseCheck):
    name = "db"
    dependency = "db"

    def probe(self, ctx):
        return self.ok(ctx, time.perf_counter())


def _monitor():
    monitor = HealthMonitor("otel-worker", instance="otel-1", version="9.9.9")
    monitor.register(Healthy(), name="db", critical=True,
                     interval=0.1, timeout=0.05, ttl=30.0)
    return monitor


def _wait(predicate, timeout=10.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


# -- payload ------------------------------------------------------------------ #

def test_the_metrics_payload_is_valid_otlp_json():
    monitor = _monitor()
    monitor.start(boot_grace=0)
    try:
        _wait(lambda: monitor.snapshot().results.get("db"), what="a result")
        payload = build_metrics(monitor)
    finally:
        monitor.stop()

    resource = payload["resourceMetrics"][0]
    attrs = {kv["key"]: kv["value"] for kv in resource["resource"]["attributes"]}
    assert attrs["service.name"]["stringValue"] == "otel-worker"
    assert attrs["service.instance.id"]["stringValue"] == "otel-1"

    for metric in resource["scopeMetrics"][0]["metrics"]:
        # Exactly one of gauge/sum, and every point carries a timestamp --
        # a point without one is silently dropped by most collectors.
        kind = metric.get("gauge") or metric.get("sum")
        assert kind is not None, metric["name"]
        for point in kind["dataPoints"]:
            assert point["timeUnixNano"]
            assert ("asInt" in point) ^ ("asDouble" in point)


def test_the_logs_payload_carries_events_as_records():
    payload = build_logs(
        [{"event": "health_transition", "level": "error", "check": "db",
          "timestamp": "2026-01-01T00:00:00Z", "category": "connection_refused"}],
        "svc", "inst", "1.0",
    )
    record = payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["body"]["stringValue"] == "health_transition"
    assert record["severityText"] == "ERROR"
    attrs = {kv["key"] for kv in record["attributes"]}
    assert "check" in attrs and "category" in attrs
    # The event name and level became the record's identity; leaving them in
    # the attributes as well would duplicate every field.
    assert "event" not in attrs and "level" not in attrs


# -- delivery ------------------------------------------------------------------ #

def test_metrics_reach_a_live_collector():
    monitor = _monitor()
    monitor.start(boot_grace=0)
    with Collector() as collector:
        exporter = OTLPExporter(monitor, endpoint=collector.url, interval=0.1)
        exporter.start()
        try:
            _wait(lambda: METRICS_PATH in collector.paths(), what="a metrics export")
            assert exporter.exported >= 1
            assert exporter.last_error is None
        finally:
            exporter.stop()
            monitor.stop()


def test_transition_events_are_exported_as_logs():
    monitor = _monitor()
    with Collector() as collector:
        exporter = OTLPExporter(monitor, endpoint=collector.url, interval=0.1)
        exporter.start()
        monitor.start(boot_grace=0)
        try:
            _wait(lambda: LOGS_PATH in collector.paths(), what="a logs export")
        finally:
            exporter.stop()
            monitor.stop()


# -- the collector misbehaving -------------------------------------------------- #

def test_an_unreachable_collector_is_counted_and_never_raised():
    """A collector that is down is not a worker problem.  It must be visible
    on /health and invisible everywhere else."""
    monitor = _monitor()
    monitor.start(boot_grace=0)
    # Port 1 on loopback: nothing listens, and the connection is refused
    # immediately rather than hanging.
    exporter = OTLPExporter(monitor, endpoint="http://127.0.0.1:1",
                            interval=0.1, timeout=0.5)
    exporter.start()
    try:
        _wait(lambda: exporter.failed >= 1, what="a failed export")
        assert exporter.exported == 0
        assert exporter.last_error
        # The worker itself is unaffected.
        assert monitor.live_code() == 200
    finally:
        exporter.stop()
        monitor.stop()


def test_a_rejecting_collector_is_counted_by_status():
    monitor = _monitor()
    monitor.start(boot_grace=0)
    with Collector(status=503) as collector:
        exporter = OTLPExporter(monitor, endpoint=collector.url,
                                interval=0.1, timeout=1.0)
        exporter.start()
        try:
            _wait(lambda: exporter.failed >= 1, what="a rejected export")
            assert exporter.last_error == "http_503"
        finally:
            exporter.stop()
            monitor.stop()


def test_the_queue_is_bounded_and_drops_the_oldest_payload():
    """An unbounded queue is how a health library becomes the reason a
    worker runs out of memory.  Newest-wins, because during an outage the
    useful payload is the one describing the worker now."""
    monitor = _monitor()
    exporter = OTLPExporter(monitor, endpoint="http://127.0.0.1:1", max_queue=3)

    for i in range(10):
        exporter._offer(METRICS_PATH, {"n": i})

    assert exporter._queue.qsize() == 3
    assert exporter.dropped == 7
    remaining = []
    while not exporter._queue.empty():
        remaining.append(exporter._queue.get_nowait()[1]["n"])
    assert remaining == [7, 8, 9]


def test_exporter_status_rides_along_on_health():
    """The exporter is silent by design, so its counters have to surface
    somewhere an operator already looks."""
    monitor = _monitor()
    exporter = OTLPExporter(monitor, endpoint="http://127.0.0.1:1")
    monitor.exporter = exporter
    body = monitor.snapshot_dict()
    assert body["export"]["endpoint"] == "http://127.0.0.1:1"
    assert body["export"]["dropped"] == 0


def test_a_snapshot_that_cannot_be_built_does_not_kill_the_thread():
    """A dead exporter thread is a silent, permanent loss of telemetry."""
    class Broken:
        service = "x"

        def snapshot(self):
            raise RuntimeError("boom")

        def on_event(self, fn):
            return None

    exporter = OTLPExporter(Broken(), endpoint="http://127.0.0.1:1", interval=0.05)
    exporter.start()
    try:
        _wait(lambda: exporter.failed >= 2, what="repeated build failures")
        assert exporter._thread is not None and exporter._thread.is_alive()
    finally:
        exporter.stop()
