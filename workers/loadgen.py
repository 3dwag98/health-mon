"""Steady load onto billing.in, with a controllable rate.

POST /rate {"rate": 0} pauses it, which is how the demo produces a genuinely
idle queue -- the case that must never alert.
"""
from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pika

import settings as S
from pipeline import make_message

RATE = float(os.getenv("RATE", "8"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "8090"))
state = {"rate": RATE, "sent": 0, "burst": 0}


def control_server():
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            return

        def _json(self, code, payload):
            body = json.dumps(payload).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._json(200, dict(state))

        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0) or 0)
            try:
                payload = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                payload = {}
            if "rate" in payload:
                state["rate"] = max(0.0, float(payload["rate"]))
            if "burst" in payload:
                state["burst"] += int(payload["burst"])
            self._json(200, dict(state))

    srv = ThreadingHTTPServer(("0.0.0.0", CONTROL_PORT), H)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()


def main() -> int:
    control_server()
    params = pika.ConnectionParameters(
        host=S.MQ_HOST, port=S.MQ_PORT,
        credentials=pika.PlainCredentials(S.MQ_USER, S.MQ_PASSWORD),
        heartbeat=30, connection_attempts=10, retry_delay=3.0,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    for q in (S.IN_QUEUE, S.OUT_QUEUE, S.AUDIT_QUEUE):
        ch.queue_declare(queue=q, durable=True)

    print(f"loadgen: {state['rate']}/s -> {S.IN_QUEUE}, control on :{CONTROL_PORT}",
          flush=True)
    while True:
        burst = state["burst"]
        if burst:
            state["burst"] = 0
            for _ in range(burst):
                ch.basic_publish("", S.IN_QUEUE,
                                 json.dumps(make_message()).encode())
                state["sent"] += 1
            print(f"loadgen: burst of {burst}", flush=True)

        rate = state["rate"]
        if rate <= 0:
            conn.process_data_events(time_limit=0.5)
            continue
        ch.basic_publish("", S.IN_QUEUE, json.dumps(make_message()).encode())
        state["sent"] += 1
        conn.process_data_events(time_limit=min(1.0 / rate, 0.5))


if __name__ == "__main__":
    raise SystemExit(main())
