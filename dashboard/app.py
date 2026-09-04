"""Fleet aggregator: polls every worker, streams to browsers over SSE.

Workers stay headless and serve JSON.  This is the only component that
renders anything, which keeps the SDK's dependency surface at zero.

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


class Fleet:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latest: dict[str, dict] = {}
        self._history: dict[str, deque] = {}
        self._transitions: deque = deque(maxlen=200)
        self._subscribers: list[queue.Queue] = []
        self._seq = 0

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=64)
        with self._lock:
            self._subscribers.append(q)
        return q

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

            entry = {"name": name, "url": url, "at": now,
                     "reachable": error is None, "error": error, "body": body}
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
                proc = (body.get("checks") or {}).get("processing", {}).get("observed", {})
                point["depth"] = proc.get("queue_depth")
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
        if changed:
            with self._lock:
                latest_transition = self._transitions[0]
            self._broadcast({"type": "transition", "transition": latest_transition})

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "workers": list(self._latest.values()),
                "history": {k: list(v) for k, v in self._history.items()},
                "transitions": list(self._transitions),
                "seq": self._seq,
                "poll_interval": POLL_INTERVAL,
            }


FLEET = Fleet()


def poll_loop() -> None:
    while True:
        started = time.monotonic()
        for name, url in WORKERS:
            body, error = None, None
            try:
                req = urllib.request.Request(url.rstrip("/") + "/health")
                with urllib.request.urlopen(req, timeout=2.5) as r:
                    body = json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:
                    body = json.loads(e.read())
                except Exception:
                    error = f"HTTP {e.code}"
            except Exception as exc:
                error = type(exc).__name__
            FLEET.update(name, url, body, error)
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
        elif path == "/healthz":
            self._send(200, b'{"status":"ok"}')
        else:
            self._send(404, b'{"error":"not found"}')

    def do_POST(self):
        path = self.path.split("?")[0]
        if path == "/api/loadgen":
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


def main() -> int:
    threading.Thread(target=poll_loop, name="poller", daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    print(f"dashboard on :{PORT}, polling {len(WORKERS)} workers "
          f"every {POLL_INTERVAL}s", flush=True)
    srv.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
