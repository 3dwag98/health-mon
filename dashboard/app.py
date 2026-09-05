"""Fleet aggregator: receives OTLP, polls what it can reach, streams SSE.

Two ways in, because they answer different questions:

* **OTLP push** (``POST /v1/metrics``, ``/v1/logs``) is how a worker that
  nothing can reach gets onto the board.  A supervised fleet has no stable
  scrape targets -- processes come and go on ports the supervisor chose, and
  plenty sit behind a NAT -- so workers are DISCOVERED here by pushing rather
  than enumerated in a config file.  This is the path that scales to fifty.
* **Polling** ``/health`` is kept for workers whose address is known, because
  that body carries what a metric stream cannot: readiness `reasons` in the
  operator's own words, per-check `detail`, and the probe settings behind
  each verdict.  Where both exist the richer polled body wins, and the pushed
  metrics keep the entry warm.

Workers stay headless and serve JSON.  This is the only component that
renders anything, which keeps the SDK's dependency surface at zero -- and it
is why this file is standard library only, OTLP decoding included.

SSE rather than WebSockets: the data flows one way, the browser reconnects
automatically with Last-Event-ID, and it survives proxies that mangle
upgrade headers.  Control actions, if any are ever added, are ordinary POSTs
-- a command is one request, not a conversation, and putting it on a socket
would cost the free reconnect on the read path.
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.0"))
PORT = int(os.getenv("PORT", "9000"))
HISTORY = int(os.getenv("HISTORY", "180"))
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# name=url pairs, comma separated
WORKERS = [
    tuple(part.split("=", 1))
    for part in os.getenv(
        "WORKERS",
        "billing=http://billing:8080,notify=http://notify:8080,"
        "reconcile=http://reconcile:8080",
    ).split(",")
    if "=" in part
]
LOADGEN_URL = os.getenv("LOADGEN_URL", "http://loadgen:8090")

# A pushed worker is forgotten this long after its last payload.  Long enough
# to ride out a collector hiccup, short enough that a decommissioned worker
# does not sit on the board forever claiming to be healthy.
OTLP_TTL = float(os.getenv("OTLP_TTL", "90"))

# Backlog WITH silence for this long is a stuck consumer.  Neither half means
# anything alone: a deep queue with a busy consumer is fine, and a silent
# consumer on an empty queue is fine.
STALE_AFTER = float(os.getenv("STALE_AFTER", "60"))

# How many workers must report the same check failing the same way before it
# is called a shared outage rather than a set of individual sick workers.
SHARED_OUTAGE_MIN = int(os.getenv("SHARED_OUTAGE_MIN", "2"))

# Severity ordinal -> the status word the rest of the dashboard speaks.
SEVERITY_STATUS = {0: "ok", 1: "starting", 2: "degraded", 3: "unknown", 4: "failing"}

# Categories that mean the worker did this to itself, mirrored from
# worker_health.core.model.WEDGED_CATEGORIES.  Copied rather than imported:
# the dashboard runs from its own image and must not depend on the SDK.
WEDGED = frozenset({
    "stalled", "not_consuming", "not_subscribed",
    "credit_exhausted", "poison_loop",
})


def _attrs(item) -> dict:
    """An OTLP KeyValue list, as a plain dict."""
    out = {}
    for kv in item.get("attributes") or ():
        value = kv.get("value") or {}
        for key in ("stringValue", "boolValue", "doubleValue", "intValue"):
            if key in value:
                raw = value[key]
                out[kv.get("key")] = (
                    float(raw) if key == "doubleValue"
                    else int(raw) if key == "intValue" else raw
                )
                break
    return out


def _points(metric):
    """Every data point of a metric, whatever kind, as (attributes, value)."""
    body = metric.get("gauge") or metric.get("sum") or {}
    for point in body.get("dataPoints") or ():
        if "asInt" in point:
            value = float(point["asInt"])
        elif "asDouble" in point:
            value = float(point["asDouble"])
        else:
            continue
        yield _attrs(point), value


def decode_metrics(payload: dict) -> list:
    """One OTLP ExportMetricsServiceRequest -> one view per resource.

    Deliberately tolerant: an unrecognised metric is skipped rather than
    fatal.  A dashboard that 500s because a worker shipped a new series is a
    dashboard that goes dark exactly when someone deployed something.
    """
    workers = []
    for resource in payload.get("resourceMetrics") or ():
        identity = _attrs(resource.get("resource") or {})
        service = str(identity.get("service.name") or "")
        instance = str(identity.get("service.instance.id") or "") or service
        if not service and not instance:
            continue

        checks: dict = {}
        processing: dict = {}
        timing: dict = {}
        view = {
            "service": service,
            "instance": instance,
            "environment": identity.get("deployment.environment.name") or None,
            "version": identity.get("service.version") or None,
            "checks": checks,
            "processing": processing,
            "timing": timing,
        }

        for scope in resource.get("scopeMetrics") or ():
            for metric in scope.get("metrics") or ():
                name = metric.get("name") or ""
                for attributes, value in _points(metric):
                    check = attributes.get("check")
                    queue = attributes.get("queue")

                    if name == "worker_health_ready":
                        view["ready"] = bool(value)
                    elif name == "worker_health_live":
                        view["live"] = bool(value)
                    elif name == "worker_health_status":
                        view["status"] = SEVERITY_STATUS.get(int(value), "unknown")
                    elif name == "worker_health_uptime_seconds":
                        view["uptime_s"] = value
                    elif name == "worker_health_check_severity" and check:
                        entry = checks.setdefault(check, {})
                        entry["internal_status"] = SEVERITY_STATUS.get(int(value), "unknown")
                        entry["critical"] = bool(attributes.get("critical"))
                        entry["evidence"] = attributes.get("evidence")
                    elif name == "worker_health_check_latency_ms" and check:
                        checks.setdefault(check, {})["latency_ms"] = value
                    elif name == "worker_health_check_error" and check and value:
                        checks.setdefault(check, {})["category"] = attributes.get("category")
                    elif name == "worker_health_queue_lag" and queue:
                        processing.setdefault(queue, {})["queue_lag"] = value
                    elif name == "worker_health_message_success_total" and queue:
                        processing.setdefault(queue, {})["succeeded"] = value
                    elif name == "worker_health_last_message_age_seconds" and queue:
                        processing.setdefault(queue, {})["last_message_age_s"] = value
                    elif name in ("worker_health_loop_lag_ms",
                                  "worker_health_worker_to_health_delta_ms"):
                        timing[name[len("worker_health_"):]] = value
        workers.append(view)
    return workers


def as_body(view: dict) -> dict:
    """An OTLP view in the shape /health would have returned.

    The same shape on purpose: every renderer, rollup and sparkline then
    works identically whether a worker was polled or pushed, instead of this
    file growing a second copy of everything.
    """
    body = {
        "status": view.get("status") or "unknown",
        "readiness": "ready" if view.get("ready", True) else "unready",
        "liveness": "alive" if view.get("live", True) else "unalive",
        "live": "ok" if view.get("live", True) else "failing",
        "service": view.get("service"),
        "instance": view.get("instance"),
        "version": view.get("version"),
        "uptime_s": view.get("uptime_s"),
        "checks": view.get("checks") or {},
        "processing": view.get("processing") or {},
        "timing": view.get("timing") or {},
    }
    if view.get("environment"):
        body["environment"] = view["environment"]
    reasons = [
        ("critical " if c.get("critical") else "") + f"check {name} is "
        + c["internal_status"]
        + (f" ({c['category']})" if c.get("category") else "")
        for name, c in sorted((view.get("checks") or {}).items())
        if c.get("internal_status") in ("failing", "degraded")
    ]
    if reasons:
        body["reasons"] = reasons
    return body


def shared_outages(entries, minimum: int = SHARED_OUTAGE_MIN) -> list:
    """Group broken checks by what is actually broken.

    Fifty workers all reporting `postgres failing (connection_refused)` is
    ONE database outage, and rendering it as fifty sick workers buries the
    only fact anyone can act on.  Grouping by (check, category) is what turns
    the wall of red back into a sentence.
    """
    groups: dict = {}
    for entry in entries:
        body = entry.get("body") or {}
        for name, check in (body.get("checks") or {}).items():
            if check.get("internal_status") not in ("failing", "degraded"):
                continue
            key = (name, check.get("category") or "unknown")
            group = groups.setdefault(key, {
                "check": name,
                "category": check.get("category") or "unknown",
                "status": "degraded",
                "critical": False,
                "workers": [],
            })
            group["workers"].append(entry["name"])
            group["critical"] = group["critical"] or bool(check.get("critical"))
            if check["internal_status"] == "failing":
                group["status"] = "failing"

    out = []
    for group in groups.values():
        group["workers"] = sorted(set(group["workers"]))
        group["count"] = len(group["workers"])
        group["shared"] = group["count"] >= minimum
        out.append(group)
    # Shared and critical first: that is the order someone reads under
    # pressure, and it is the opposite of alphabetical.
    out.sort(key=lambda g: (not g["shared"], not g["critical"],
                            g["status"] != "failing", -g["count"], g["check"]))
    return out


def stale_workers(entries, stale_after: float = STALE_AFTER) -> list:
    """Alive but not working: the failure with no process-level symptom.

    Two ways to see it, and both need the backlog half -- an idle worker on
    an empty queue is healthy forever, and saying otherwise is the false
    positive that teaches a team to ignore the board.
    """
    out = []
    for entry in entries:
        body = entry.get("body") or {}
        if not body:
            continue

        wedged = sorted({
            check.get("category") for check in (body.get("checks") or {}).values()
            if check.get("category") in WEDGED
        })
        # A handler failing on every message because the database is down
        # trips the poison-loop threshold in seconds. That is a correct
        # process in front of a broken dependency, and telling someone to
        # restart it is how a dependency outage becomes a crash loop. The
        # worker's own /live applies exactly this precedence; the board has
        # to agree with it or one of the two is lying.
        explained = any(
            check.get("internal_status") == "failing"
            and check.get("category") and check.get("category") not in WEDGED
            for check in (body.get("checks") or {}).values()
        )

        backlog = 0.0
        silence = None
        for queue in (body.get("processing") or {}).values():
            backlog = max(backlog, float(queue.get("queue_lag") or 0))
            age = queue.get("last_message_age_s")
            if age is not None:
                silence = age if silence is None else max(silence, age)
        silent_with_backlog = bool(
            backlog > 0 and silence is not None and silence > stale_after
        )

        if not wedged and not silent_with_backlog:
            continue
        out.append({
            "worker": entry["name"],
            "categories": wedged,
            "queue_lag": backlog,
            "silent_for_s": silence,
            "silent_with_backlog": silent_with_backlog,
            # The load-bearing distinction: a wedged worker is the one case a
            # restart repairs, and /live is already reporting it -- unless a
            # failing dependency explains the wedge, in which case it is not.
            "restart_would_help": bool(wedged) and not explained,
            "explained_by_dependency": explained,
        })
    return out


class Fleet:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latest: dict[str, dict] = {}
        # Probe settings per worker. Fetched separately and rarely: this is
        # configuration, not telemetry, and re-polling it every second would
        # be a lot of bytes to learn nothing.
        self._configs: dict[str, dict] = {}
        self._history: dict[str, deque] = {}
        self._transitions: deque = deque(maxlen=200)
        self._subscribers: list[queue.Queue] = []
        self._seq = 0

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=64)
        with self._lock:
            self._subscribers.append(q)
        return q

    def config_names(self) -> set:
        with self._lock:
            return set(self._configs)

    def unsubscribe(self, q) -> None:
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def _broadcast(self, event: dict) -> None:
        with self._lock:
            self._seq += 1
            event["seq"] = self._seq
            subs = list(self._subscribers)
        payload = json.dumps(event, default=str)
        for q in subs:
            try:
                q.put_nowait(payload)
            except queue.Full:
                # A slow browser must never stall the poller.  Drop for that
                # client only; SSE reconnect will resync it from /api/fleet.
                pass

    def update(self, name: str, url: str, body: dict | None, error: str | None) -> None:
        now = time.time()
        with self._lock:
            previous = self._latest.get(name, {})
            prev_status = (previous.get("body") or {}).get("status")

            entry = dict(previous)
            entry.update({"name": name, "url": url, "at": now, "poll_at": now,
                          "reachable": error is None, "error": error, "body": body})
            entry["source"] = "poll+otlp" if previous.get("otlp_at") else "poll"
            self._latest[name] = entry

            hist = self._history.setdefault(name, deque(maxlen=HISTORY))
            point = {"t": now}
            if body:
                point["status"] = body.get("status")
                timing = body.get("timing") or {}
                point["delta_ms"] = timing.get("worker_to_health_delta_ms")
                point["loop_lag_ms"] = timing.get("loop_lag_ms")
                for cname, c in (body.get("checks") or {}).items():
                    point[f"lat:{cname}"] = c.get("latency_ms")
                # Processing counters moved to their own top-level block when
                # the SDK gained per-queue tracking; the first queue is the
                # one this worker consumes.
                queues = list((body.get("processing") or {}).values())
                proc = queues[0] if queues else {}
                point["depth"] = proc.get("queue_lag")
                point["succeeded"] = proc.get("succeeded")
            hist.append(point)

            new_status = (body or {}).get("status") if body else "unreachable"
            changed = prev_status != new_status and prev_status is not None
            if changed:
                self._transitions.appendleft({
                    "t": now, "worker": name,
                    "from": prev_status, "to": new_status,
                    "categories": sorted({
                        c.get("category") for c in (body or {}).get("checks", {}).values()
                        if c.get("category")
                    }) if body else [],
                })

        self._broadcast({"type": "worker", "worker": entry})
        self._broadcast({"type": "rollup", **self.rollup()})
        if changed:
            with self._lock:
                latest_transition = self._transitions[0]
            self._broadcast({"type": "transition", "transition": latest_transition})

    def ingest_otlp(self, views: list) -> None:
        """Fold one pushed payload into the fleet.

        A pushed worker that nothing polls appears here for the first time,
        which is the whole point: the board should not need to be told the
        address of every process a supervisor happens to have started.
        """
        now = time.time()
        touched = []
        with self._lock:
            for view in views:
                name = self._identify(view)
                entry = self._latest.get(name)
                polled = bool(entry and entry.get("body") and entry.get("poll_at"))

                if entry is None:
                    entry = self._latest[name] = {"name": name, "url": "",
                                                  "reachable": True, "error": None,
                                                  "body": None}
                entry["otlp_at"] = now
                entry["at"] = now
                entry["source"] = "poll+otlp" if polled else "otlp"
                entry["environment"] = view.get("environment")
                # The polled body is richer -- reasons in words, per-check
                # detail, probe settings -- so it wins where it exists. The
                # push still keeps the entry warm and its identity current.
                if not polled:
                    entry["reachable"] = True
                    entry["error"] = None
                    entry["body"] = as_body(view)
                    self._record_point(name, entry["body"], now)
                touched.append(dict(entry))

        for entry in touched:
            self._broadcast({"type": "worker", "worker": entry})
        if touched:
            self._broadcast({"type": "rollup", **self.rollup()})

    def _identify(self, view: dict) -> str:
        """Which board entry this payload belongs to.

        Called under the lock.  A pushed `billing-1` and a polled `billing`
        are one worker, and showing them as two would double the fleet.
        """
        instance = str(view.get("instance") or "")
        service = str(view.get("service") or "")
        for name, entry in self._latest.items():
            if instance and (entry.get("body") or {}).get("instance") == instance:
                return name
        if service and service in self._latest:
            return service
        return instance or service

    def _record_point(self, name: str, body: dict, now: float) -> None:
        """Append one history sample.  Called under the lock."""
        hist = self._history.setdefault(name, deque(maxlen=HISTORY))
        point = {"t": now, "status": body.get("status")}
        timing = body.get("timing") or {}
        point["delta_ms"] = timing.get("worker_to_health_delta_ms")
        point["loop_lag_ms"] = timing.get("loop_lag_ms")
        for cname, check in (body.get("checks") or {}).items():
            point[f"lat:{cname}"] = check.get("latency_ms")
        queues = list((body.get("processing") or {}).values())
        proc = queues[0] if queues else {}
        point["depth"] = proc.get("queue_lag")
        point["succeeded"] = proc.get("succeeded")
        hist.append(point)

    def expire(self) -> None:
        """Drop pushed workers that have gone quiet.

        Only ever drops entries this dashboard learned about by push: a
        polled worker that stops answering is a REPORTED failure and must
        stay on the board saying so.
        """
        cutoff = time.time() - OTLP_TTL
        dropped = []
        with self._lock:
            for name, entry in list(self._latest.items()):
                if entry.get("source") == "otlp" and entry.get("otlp_at", 0) < cutoff:
                    del self._latest[name]
                    self._history.pop(name, None)
                    dropped.append(name)
        for name in dropped:
            self._broadcast({"type": "gone", "worker": name})

    def rollup(self) -> dict:
        """What is broken, grouped by the thing that is broken."""
        with self._lock:
            entries = list(self._latest.values())
        return {"outages": shared_outages(entries), "stale": stale_workers(entries)}

    def set_config(self, name: str, config: dict | None) -> None:
        if config is None:
            return
        with self._lock:
            changed = self._configs.get(name) != config
            self._configs[name] = config
        if changed:
            self._broadcast({"type": "config", "worker": name, "config": config})

    def snapshot(self) -> dict:
        with self._lock:
            entries = list(self._latest.values())
            body = {
                "workers": entries,
                "configs": dict(self._configs),
                "history": {k: list(v) for k, v in self._history.items()},
                "transitions": list(self._transitions),
                "seq": self._seq,
                "poll_interval": POLL_INTERVAL,
            }
        body["outages"] = shared_outages(entries)
        body["stale"] = stale_workers(entries)
        return body


FLEET = Fleet()


# How many health polls pass between configuration fetches. Settings change
# only on a deploy, so this is generous on purpose.
CONFIG_EVERY = int(os.getenv("CONFIG_EVERY", "60"))


def fetch(url: str, timeout: float = 2.5):
    """GET and parse JSON. Returns (body, error-name)."""
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=timeout) as r:
            return json.loads(r.read()), None
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read()), None
        except Exception:
            return None, f"HTTP {e.code}"
    except Exception as exc:
        return None, type(exc).__name__


def poll_loop() -> None:
    cycle = 0
    while True:
        started = time.monotonic()
        for name, url in WORKERS:
            body, error = fetch(url.rstrip("/") + "/health")
            FLEET.update(name, url, body, error)

            # Refetch settings on the first sight of a worker, on the slow
            # cadence, and whenever one comes back -- a worker that restarted
            # may have restarted onto a different configuration.
            if body is not None and (cycle % CONFIG_EVERY == 0
                                     or name not in FLEET.config_names()):
                config, _ = fetch(url.rstrip("/") + "/config")
                FLEET.set_config(name, config)
        cycle += 1
        time.sleep(max(0.05, POLL_INTERVAL - (time.monotonic() - started)))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        return

    def _send(self, code, body: bytes, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            try:
                with open(os.path.join(STATIC, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(404, b"missing index.html", "text/plain")
        elif path == "/api/fleet":
            self._send(200, json.dumps(FLEET.snapshot(), default=str).encode())
        elif path == "/api/stream":
            self._stream()
        elif path == "/api/rollup":
            self._send(200, json.dumps(FLEET.rollup(), default=str).encode())
        elif path == "/healthz":
            self._send(200, b'{"status":"ok"}')
        else:
            self._send(404, b'{"error":"not found"}')

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length <= 0:
            return None
        raw = self.rfile.read(length)
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            import gzip
            raw = gzip.decompress(raw)
        return json.loads(raw)

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/v1/metrics":
            # The OTLP receiver. Answers 200 with an empty partialSuccess,
            # which is what an exporter needs to consider the batch accepted;
            # a decode failure is 400 so the collector retries or drops
            # rather than silently believing it landed.
            try:
                FLEET.ingest_otlp(decode_metrics(self._read_json() or {}))
            except Exception as exc:
                self._send(400, json.dumps({"error": type(exc).__name__}).encode())
                return
            self._send(200, b'{"partialSuccess":{}}')
        elif path == "/v1/logs":
            # Accepted and dropped for now: transitions already reach the
            # board through the worker payloads. Answering 200 keeps the
            # collector from retrying a pipeline that is wired up correctly.
            self._send(200, b'{"partialSuccess":{}}')
        elif path == "/api/loadgen":
            n = int(self.headers.get("Content-Length", 0) or 0)
            payload = self.rfile.read(n)
            try:
                req = urllib.request.Request(
                    LOADGEN_URL.rstrip("/") + "/", data=payload,
                    headers={"Content-Type": "application/json"}, method="POST",
                )
                with urllib.request.urlopen(req, timeout=3) as r:
                    self._send(200, r.read())
            except Exception as exc:
                self._send(502, json.dumps({"error": type(exc).__name__}).encode())
        else:
            self._send(404, b'{"error":"not found"}')

    def _stream(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        q = FLEET.subscribe()
        try:
            initial = json.dumps(
                {"type": "snapshot", **FLEET.snapshot()}, default=str
            )
            self.wfile.write(f"event: message\ndata: {initial}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    payload = q.get(timeout=15)
                    frame = f"data: {payload}\n\n"
                except queue.Empty:
                    frame = ": keepalive\n\n"   # keeps proxies from timing out
                self.wfile.write(frame.encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            FLEET.unsubscribe(q)


def expire_loop() -> None:
    while True:
        time.sleep(min(15.0, max(1.0, OTLP_TTL / 3)))
        try:
            FLEET.expire()
        except Exception:
            pass


def main() -> int:
    threading.Thread(target=poll_loop, name="poller", daemon=True).start()
    threading.Thread(target=expire_loop, name="expiry", daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    print(f"dashboard on :{PORT}, polling {len(WORKERS)} workers "
          f"every {POLL_INTERVAL}s, receiving OTLP on /v1/metrics", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
