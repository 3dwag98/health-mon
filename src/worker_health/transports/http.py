"""Standalone HTTP transport on its own thread.

Never the worker's loop: if the worker wedges, these endpoints must keep
answering -- that is the entire point of separating liveness from readiness.
Every handler serves a cached snapshot and performs no I/O.

Binding: the default is 0.0.0.0 because the common deployment is a
container whose port is published deliberately.  In a shared-host
deployment, bind to 127.0.0.1 (``health_host: 127.0.0.1``) and let the
supervisor or a sidecar reach it -- see docs/OPERATIONS.md.  Nothing here
is authenticated, so it must not be exposed to a public interface.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Paths served.  Anything else is a 404 with a hint, because the single
# most common integration mistake is asking for /healthz or /readyz.
ROUTES = ("/live", "/ready", "/health", "/config", "/metrics", "/events", "/")


def make_app(monitor):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):  # noqa: A003 - silence stdlib access log
            return

        def _send(self, code: int, payload: dict) -> None:
            body = json.dumps(payload, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/health+json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, code: int, text: str, content_type: str) -> None:
            body = text.encode()
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/live":
                # Deliberately the cheapest endpoint in the process: one
                # clock read, no snapshot, no lock.  A liveness probe that
                # contends on the same lock as everything else is a liveness
                # probe that fails when the process is merely busy.
                self._send(monitor.live_code(), {
                    "status": monitor.liveness().value,
                    "loop_lag_ms": monitor.loop_lag_ms(),
                    "service": monitor.service,
                    "instance": monitor.instance,
                })
            elif path == "/ready":
                # Deliberately lean: a readiness probe runs every couple of
                # seconds from a supervisor, and does not need the settings
                # or the timing windows to decide whether to route traffic.
                body = monitor.snapshot_dict(include_timings=False,
                                             include_config=False)
                self._send(monitor.ready_code(), body)
            elif path in ("/health", "/"):
                self._send(200, monitor.snapshot_dict(include_events=True))
            elif path == "/config":
                # What the verdicts are being made with: intervals, timeouts,
                # thresholds, criticality, and which clients are instrumented.
                # Redacted -- a probe's params can hold a DSN.
                self._send(200, monitor.describe_config())
            elif path == "/events":
                self._send(200, {"events": monitor.events.recent(50)})
            elif path == "/metrics":
                from ..telemetry.prometheus import render

                self._send_text(200, render(monitor), "text/plain; version=0.0.4")
            else:
                self._send(404, {"error": "not found", "routes": list(ROUTES)})

    return Handler


class HealthServer:
    def __init__(self, monitor, host: str = "0.0.0.0", port: int = 8080) -> None:
        self._server = ThreadingHTTPServer((host, port), make_app(monitor))
        self._server.daemon_threads = True
        self._thread: threading.Thread | None = None
        self.host = host
        self.port = self._server.server_address[1]

    def start(self) -> "HealthServer":
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="wh-http", daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass
