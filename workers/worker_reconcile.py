"""Reconciliation worker -- driven by the ASYNCIO monitor runner.

Note what is and is not asyncio here.  The runner choice is about how the
MONITOR schedules its checks, which is the pluggability point the SDK
actually owns; the consume loop is still blocking pika, exactly as in the
other workers.  That combination is deliberate: it proves the two runners
are interchangeable underneath the same policy core, and it is also the
realistic mixed-fleet case.

It also carries the custom-check example: `cache-drift` is a probe defined
in worker code with a decorator, registered alongside the ones the YAML
file declares.
"""
from __future__ import annotations

import json
import os
import signal
import sys

from sqlalchemy import text

from runtime import build, stop_consuming, supervise_consume

SERVICE = os.getenv("SERVICE", "reconcile")
QUEUE = os.getenv("IN_QUEUE", "billing.audit")


def main() -> int:
    ctx = build(SERVICE, runner="asyncio", queue=QUEUE, pool_size=3)
    log, monitor, conn = ctx["logger"], ctx["monitor"], ctx["connection"]
    tracker, engine, redis = ctx["tracker"], ctx["engine"], ctx["redis"]

    drift = {"count": 0, "checked": 0}

    @tracker.handler(queue=QUEUE)
    def process(body: bytes) -> None:
        msg = json.loads(body)
        account = msg["account_id"]

        with tracker.stage(QUEUE, "postgres_read"):
            with engine.connect() as c:
                actual = c.execute(
                    text("SELECT balance_cents FROM accounts WHERE id = :a"),
                    {"a": account},
                ).scalar()

        with tracker.stage(QUEUE, "redis_read"):
            cached = redis.get(f"billing:balance:{account}")

        drift["checked"] += 1
        if cached is not None and actual is not None and int(cached) != int(actual):
            drift["count"] += 1
            redis.setex(f"billing:balance:{account}", 300, int(actual))
            log.warning("cache drift corrected", extra={
                "service": SERVICE, "queue": QUEUE,
            })

    # A custom check, demonstrating the extensible check interface.  The
    # equivalent in YAML is `type: function` with an import path; this is
    # the in-code spelling, for a check that closes over local state.
    @monitor.check("cache-drift", critical=False, interval=5.0, ttl=30.0)
    def cache_drift():
        from worker_health import Status
        if drift["checked"] < 20:
            return Status.OK
        ratio = drift["count"] / max(1, drift["checked"])
        return Status.OK if ratio < 0.05 else Status.DEGRADED

    def on_message(ch, method, properties, body):
        try:
            process(body)
        except Exception as exc:  # noqa: BLE001
            log.error("reconcile failed", extra={
                "service": SERVICE, "queue": QUEUE, "category": type(exc).__name__,
            })
            ch.basic_nack(method.delivery_tag, requeue=False)
        else:
            ch.basic_ack(method.delivery_tag)

    def shutdown(signum, frame):
        stop_consuming(ctx)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        supervise_consume(ctx, QUEUE, on_message)
    finally:
        ctx["health"].stop(timeout=3)
        try:
            ctx["connection"].close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
