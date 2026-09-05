"""OTLP/HTTP export.

Push, not pull.  A worker fleet under PM2 has no stable scrape targets --
processes come and go on ports a supervisor chose, and half of them live
behind a NAT a collector cannot reach.  Pushing inverts that: the worker
needs one outbound URL and nothing needs to discover it.

Three rules hold this together, and all three exist because telemetry must
never be able to hurt the worker it reports on:

* **Bounded queue.**  ``maxsize`` is fixed.  When the collector is slow or
  gone the queue fills, and the OLDEST payload is dropped so the newest
  state still gets through when the link returns.  An unbounded queue is
  how a health library becomes the reason a worker runs out of memory.
* **One background thread, never the worker's.**  Export happens off the
  hot path; a handler never waits on a socket to a collector.
* **Silent failure.**  A collector that is down is not a worker problem.
  Failures are counted and exposed on /health, never raised and never
  logged per-occurrence -- a log line per failed export during a collector
  outage is its own incident.

The payload is OTLP/HTTP with a JSON body, written by hand against the
protobuf-JSON mapping.  That keeps the SDK's dependency count at zero, and
the encoding is small enough to read in one sitting: a resource, a scope,
and a flat list of metrics.
"""
from __future__ import annotations

import json
import queue
import threading
import time
import urllib.error
import urllib.request

from ..core.model import Readiness, Status

# Severity ordinals, kept identical to what the Prometheus exporter used to
# emit so existing dashboards and alert expressions port over unchanged.
SEVERITY_VALUE = {
    Status.OK: 0, Status.DISABLED: 0, Status.STARTING: 1,
    Status.DEGRADED: 2, Status.UNKNOWN: 3, Status.FAILING: 4,
}

READINESS_VALUE = {
    Readiness.READY: 0, Readiness.STARTING: 1,
    Readiness.DEGRADED: 2, Readiness.UNREADY: 3,
}

# Statuses that count as "up" in the binary series.  DISABLED is up because
# a check switched off deliberately must not page anyone.
_UP = (Status.OK, Status.DISABLED)

DEFAULT_ENDPOINT = "http://localhost:4318"
METRICS_PATH = "/v1/metrics"
LOGS_PATH = "/v1/logs"

# OTLP severity numbers (logs data model).
_SEVERITY_NUMBER = {"debug": 5, "info": 9, "warning": 13, "error": 17}


def _attrs(mapping) -> list:
    """Encode a flat dict as OTLP KeyValue list.

    Values are stringified unless they are numeric or boolean.  Every key
    here comes from a closed vocabulary -- check names are registered,
    categories and statuses are enums -- so this cannot emit an unbounded
    attribute set.
    """
    out = []
    for key, value in mapping.items():
        if value is None:
            continue
        if isinstance(value, bool):
            av = {"boolValue": value}
        elif isinstance(value, int):
            av = {"intValue": str(value)}
        elif isinstance(value, float):
            av = {"doubleValue": value}
        else:
            av = {"stringValue": str(value)}
        out.append({"key": key, "value": av})
    return out


def _gauge(name: str, description: str, points: list, unit: str = "") -> dict:
    return {
        "name": name,
        "description": description,
        "unit": unit,
        "gauge": {"dataPoints": points},
    }


def _sum(name: str, description: str, points: list, unit: str = "") -> dict:
    return {
        "name": name,
        "description": description,
        "unit": unit,
        "sum": {
            "dataPoints": points,
            # Monotonic cumulative: these are process-lifetime counters, and
            # saying so is what lets a backend compute a correct rate across
            # a restart instead of reading the reset as a huge negative.
            "aggregationTemporality": 2,
            "isMonotonic": True,
        },
    }


def _point(value, when_ns: int, attributes=None) -> dict:
    key = "asInt" if isinstance(value, int) and not isinstance(value, bool) else "asDouble"
    point = {
        key: str(value) if key == "asInt" else float(value),
        "timeUnixNano": str(when_ns),
        "startTimeUnixNano": str(when_ns),
    }
    if attributes:
        point["attributes"] = _attrs(attributes)
    return point


def build_metrics(monitor) -> dict:
    """One OTLP ExportMetricsServiceRequest for the monitor's current state.

    Reads a single snapshot and derives everything from it, so the numbers
    in one payload are mutually consistent -- a readiness of READY next to a
    failing check is a report nobody can act on.
    """
    snap = monitor.snapshot()
    now_ns = int(time.time() * 1_000_000_000)
    metrics: list = []

    # -- worker verdicts -------------------------------------------------- #

    ready = 1 if snap.readiness in (Readiness.READY, Readiness.DEGRADED) else 0
    metrics.append(_gauge("worker_health_ready",
                          "1 when /ready would return 200", [_point(ready, now_ns)]))
    metrics.append(_gauge("worker_health_live",
                          "1 when /live would return 200",
                          [_point(1 if snap.live_status is Status.OK else 0, now_ns)]))
    metrics.append(_gauge("worker_health_status",
                          "Aggregate readiness severity (0 ok .. 4 failing)",
                          [_point(SEVERITY_VALUE[snap.status], now_ns)]))
    metrics.append(_gauge("worker_health_readiness_state",
                          "One-hot readiness state",
                          [_point(1 if snap.readiness is state else 0, now_ns,
                                  {"state": state.value})
                           for state in Readiness]))
    metrics.append(_gauge("worker_health_uptime_seconds", "Monitor uptime",
                          [_point(float(snap.uptime_s), now_ns)], unit="s"))
    metrics.append(_gauge("worker_health_boot_complete",
                          "1 once every critical check has been healthy at least once",
                          [_point(0 if snap.readiness is Readiness.STARTING else 1,
                                  now_ns)]))
    metrics.append(_gauge("worker_health_draining",
                          "1 once the process has been asked to stop",
                          [_point(1 if snap.draining else 0, now_ns)]))

    # -- per-check --------------------------------------------------------- #

    status_pts, severity_pts, latency_pts = [], [], []
    age_pts, transition_pts, interval_pts, error_pts = [], [], [], []

    for name, r in snap.results.items():
        spec = monitor.machine.spec(name)
        attrs = {"check": name, "critical": spec.critical, "evidence": r.evidence.value}
        status_pts.append(_point(1 if r.status in _UP else 0, now_ns, attrs))
        severity_pts.append(_point(SEVERITY_VALUE[r.status], now_ns, attrs))
        if r.latency_ms is not None:
            latency_pts.append(_point(round(r.latency_ms, 3), now_ns, attrs))
        if r.evidence_age_ms is not None:
            age_pts.append(_point(round(r.evidence_age_ms, 3), now_ns, attrs))
        transition_pts.append(_point(monitor.transitions(name), now_ns, attrs))
        interval_pts.append(_point(round(monitor.machine.next_interval(name), 3),
                                   now_ns, {"check": name}))
        if r.category is not None:
            error_pts.append(_point(1, now_ns,
                                    {"check": name, "category": r.category.value}))

    metrics.append(_gauge("worker_health_check_status",
                          "1 when the check is healthy, 0 otherwise", status_pts))
    metrics.append(_gauge("worker_health_check_severity",
                          "Per-check severity (0 ok .. 4 failing)", severity_pts))
    if latency_pts:
        metrics.append(_gauge("worker_health_check_latency_ms",
                              "Latency of the last evaluation", latency_pts, unit="ms"))
    if age_pts:
        metrics.append(_gauge("worker_health_check_evidence_age_ms",
                              "Age of the signal behind the verdict", age_pts, unit="ms"))
    metrics.append(_sum("worker_health_check_transitions_total",
                        "Status transitions", transition_pts))
    metrics.append(_gauge("worker_health_check_interval_seconds",
                          "Interval until this check runs again, backoff included",
                          interval_pts, unit="s"))
    if error_pts:
        metrics.append(_gauge("worker_health_check_error",
                              "1 for the error category a check currently reports",
                              error_pts))

    # -- processing --------------------------------------------------------- #

    received, succeeded, failed, in_flight = [], [], [], []
    lag, msg_age, ok_age = [], [], []
    for q, data in snap.processing.items():
        a = {"queue": q}
        received.append(_point(int(data.get("received", 0)), now_ns, a))
        succeeded.append(_point(int(data.get("succeeded", 0)), now_ns, a))
        failed.append(_point(int(data.get("failed", 0)), now_ns, a))
        in_flight.append(_point(int(data.get("in_flight", 0)), now_ns, a))
        if data.get("queue_lag") is not None:
            lag.append(_point(data["queue_lag"], now_ns, a))
        if data.get("last_message_age_s") is not None:
            msg_age.append(_point(data["last_message_age_s"], now_ns, a))
        if data.get("last_success_age_s") is not None:
            ok_age.append(_point(data["last_success_age_s"], now_ns, a))

    if received:
        metrics.append(_sum("worker_health_message_received_total",
                            "Messages received", received))
        metrics.append(_sum("worker_health_message_success_total",
                            "Messages processed successfully", succeeded))
        metrics.append(_sum("worker_health_message_failure_total",
                            "Messages that raised", failed))
        metrics.append(_gauge("worker_health_messages_in_flight",
                              "Messages currently being handled", in_flight))
    if lag:
        metrics.append(_gauge("worker_health_queue_lag",
                              "Messages waiting in the queue", lag))
    if msg_age:
        metrics.append(_gauge("worker_health_last_message_age_seconds",
                              "Age of the last received message", msg_age, unit="s"))
    if ok_age:
        metrics.append(_gauge("worker_health_last_success_age_seconds",
                              "Age of the last successful handle", ok_age, unit="s"))

    # -- timing and rolling windows ------------------------------------------ #

    for key, value in snap.timing.items():
        # The timing block also carries identity (`runner`), which is a
        # property of the process, not a measurement.  It rides on the
        # resource attributes below instead of becoming a metric named
        # worker_health_runner whose value cannot be a number.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        metrics.append(_gauge(f"worker_health_{key}", "Worker internal timing",
                              [_point(float(value), now_ns)]))

    windows = monitor.timings.export()
    window_pts, count_pts, handler_pts = [], [], []
    for key, summary in windows.items():
        for stat in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            window_pts.append(_point(summary[stat], now_ns,
                                     {"metric": key, "stat": stat}))
        count_pts.append(_point(int(summary["count"]), now_ns, {"metric": key}))
        # Handler latency is republished under its own name with a queue
        # attribute: it is the one window an application owner reads, and
        # making them filter on a metric-name string to find it is hostile.
        if key.startswith("worker.") and key.endswith(".handler_ms"):
            q = key[len("worker."):-len(".handler_ms")]
            for stat in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
                handler_pts.append(_point(summary[stat], now_ns,
                                          {"queue": q, "quantile": stat[:-3]}))

    if window_pts:
        metrics.append(_gauge("worker_health_window_ms",
                              "Rolling timing windows", window_pts, unit="ms"))
        metrics.append(_sum("worker_health_window_count",
                            "Samples in each rolling window", count_pts))
    if handler_pts:
        metrics.append(_gauge("worker_health_handler_duration_ms",
                              "Handler latency percentiles", handler_pts, unit="ms"))

    return {
        "resourceMetrics": [{
            "resource": {"attributes": _attrs({
                "service.name": snap.service,
                "service.instance.id": snap.instance,
                "service.version": snap.version,
                # The OTel semantic-convention name, so a backend's built-in
                # environment filters work without remapping.
                "deployment.environment.name": getattr(monitor, "environment", "") or None,
                "worker_health.runner": snap.timing.get("runner"),
            })},
            "scopeMetrics": [{
                "scope": {"name": "worker_health", "version": snap.version},
                "metrics": metrics,
            }],
        }]
    }


def build_logs(events: list, service: str, instance: str, version: str,
               environment: str = "") -> dict:
    """One OTLP ExportLogsServiceRequest for a batch of health events.

    Transitions, not samples: the emitter only produces a record when the
    worker's answer CHANGED, so this stays a handful of records a day per
    check rather than a firehose nobody reads.
    """
    records = []
    for event in events:
        body = dict(event)
        level = str(body.pop("level", "info"))
        name = str(body.pop("event", "health_event"))
        body.pop("timestamp", None)
        records.append({
            "timeUnixNano": str(int(time.time() * 1_000_000_000)),
            "severityText": level.upper(),
            "severityNumber": _SEVERITY_NUMBER.get(level, 9),
            "body": {"stringValue": name},
            "attributes": _attrs(body),
        })
    return {
        "resourceLogs": [{
            "resource": {"attributes": _attrs({
                "service.name": service,
                "service.instance.id": instance,
                "service.version": version,
                "deployment.environment.name": environment or None,
            })},
            "scopeLogs": [{
                "scope": {"name": "worker_health", "version": version},
                "logRecords": records,
            }],
        }]
    }


class OTLPExporter:
    """Background OTLP/HTTP pusher.

    Owns exactly one thread and one bounded queue.  Nothing it does can
    block, raise into, or slow down the worker.
    """

    def __init__(
        self,
        monitor,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        interval: float = 15.0,
        timeout: float = 5.0,
        max_queue: int = 1000,
        headers: dict | None = None,
        export_logs: bool = True,
    ) -> None:
        self._monitor = monitor
        self.endpoint = endpoint.rstrip("/")
        self.interval = max(float(interval), 1.0)
        self.timeout = float(timeout)
        self.headers = dict(headers or {})
        self.export_logs = export_logs

        self._queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self._pending_events: list = []
        self._events_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        # Counters, exposed on /health.  This is how an operator finds out
        # the collector is unreachable, since nothing here logs or raises.
        self.exported = 0
        self.failed = 0
        self.dropped = 0
        self.last_error: str | None = None
        self.last_export_at: float | None = None

    # -- lifecycle --------------------------------------------------------- #

    def start(self) -> "OTLPExporter":
        if self._thread is not None:
            return self
        if self.export_logs:
            self._monitor.on_event(self._collect_event)
        self._thread = threading.Thread(
            target=self._run, name="wh-otlp", daemon=True
        )
        self._thread.start()
        return self

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def status(self) -> dict:
        return {
            "endpoint": self.endpoint,
            "interval": self.interval,
            "exported": self.exported,
            "failed": self.failed,
            "dropped": self.dropped,
            "queued": self._queue.qsize(),
            "last_error": self.last_error,
        }

    # -- collection --------------------------------------------------------- #

    def _collect_event(self, payload: dict) -> None:
        """Event-emitter sink.  Runs on whatever thread emitted; must be fast."""
        with self._events_lock:
            # Bounded for the same reason the queue is: a worker flapping
            # between states must not accumulate records forever.
            if len(self._pending_events) >= 256:
                del self._pending_events[0]
            self._pending_events.append(payload)

    def _drain_events(self) -> list:
        with self._events_lock:
            events, self._pending_events = self._pending_events, []
        return events

    def _offer(self, path: str, payload: dict) -> None:
        """Enqueue, dropping the OLDEST item when full.

        Newest-wins is the right policy for state: during a collector
        outage the useful payload is the one describing the worker NOW, not
        the one describing it ten minutes ago.
        """
        item = (path, payload)
        try:
            self._queue.put_nowait(item)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
            self.dropped += 1
        except queue.Empty:
            pass
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            self.dropped += 1

    # -- the loop ------------------------------------------------------------ #

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._offer(METRICS_PATH, build_metrics(self._monitor))
                events = self._drain_events() if self.export_logs else []
                if events:
                    snap = self._monitor.snapshot()
                    self._offer(LOGS_PATH, build_logs(
                        events, snap.service, snap.instance, snap.version,
                        getattr(self._monitor, "environment", "")))
            except Exception as exc:      # noqa: BLE001
                # Building a payload must never kill the thread; a dead
                # exporter thread is a silent, permanent loss of telemetry.
                self.failed += 1
                self.last_error = type(exc).__name__

            self._flush()
            self._stop.wait(self.interval)

        self._flush()   # best effort on the way out

    def _flush(self) -> None:
        while True:
            try:
                path, payload = self._queue.get_nowait()
            except queue.Empty:
                return
            if not self._post(path, payload):
                # The link is down.  Stop draining: everything still queued
                # would fail the same way, and burning the whole queue
                # against a dead collector turns one outage into a stall.
                return

    def _post(self, path: str, payload: dict) -> bool:
        body = json.dumps(payload).encode()
        request = urllib.request.Request(
            self.endpoint + path, data=body, method="POST",
            headers={"Content-Type": "application/json", **self.headers},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if 200 <= response.status < 300:
                    self.exported += 1
                    self.last_export_at = time.monotonic()
                    self.last_error = None
                    return True
                self.failed += 1
                self.last_error = f"http_{response.status}"
                return False
        except urllib.error.HTTPError as exc:
            self.failed += 1
            self.last_error = f"http_{exc.code}"
            return False
        except Exception as exc:          # noqa: BLE001 - never raised onward
            self.failed += 1
            self.last_error = type(exc).__name__
            return False
