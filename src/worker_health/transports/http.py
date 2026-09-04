"""Standalone HTTP transport on its own thread.

Never the worker's loop: if the worker wedges, these endpoints must keep
answering -- that is the entire point of separating liveness from readiness.
Every handler serves a cached snapshot and performs no I/O.
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


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
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/live":
                status = monitor.live_status()
                self._send(monitor.live_code(), {
                    "status": status.value,
                    "loop_lag_ms": monitor.loop_lag_ms(),
                    "service": monitor.service,
                    "instance": monitor.instance,
                })
            elif path == "/ready":
                body = monitor.snapshot_dict(include_timings=False)
                self._send(monitor.ready_code(), body)
            elif path in ("/health", "/"):
                self._send(200, monitor.snapshot_dict())
            elif path == "/metrics":
                from ..telemetry.prometheus import render
                text = render(monitor).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(text)))
                self.end_headers()
                self.wfile.write(text)
            else:
                self._send(404, {"error": "not found"})

    return Handler


class HealthServer:
    def __init__(self, monitor, host: str = "0.0.0.0", port: int = 8080) -> None:
        self._server = ThreadingHTTPServer((host, port), make_app(monitor))
        self._server.daemon_threads = True
        self._thread: threading.Thread | None = None
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
