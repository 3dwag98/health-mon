"""Reconciliation worker -- driven by the ASYNCIO monitor runner.

Note what is and is not asyncio here.  The runner choice is about how the
MONITOR schedules its checks, which is the pluggability point the SDK
actually owns; the consume loop is still blocking pika, exactly as in the
other workers.  That combination is deliberate: it proves the two runners
are interchangeable underneath the same policy core, and it is also the
realistic mixed-fleet case.

The work itself verifies that the Redis balance cache agrees with Postgres,
so every message touches all three dependencies.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time

from sqlalchemy import text

import settings as S
from runtime import build

SERVICE = os.getenv("SERVICE", "reconcile")
QUEUE = os.getenv("IN_QUEUE", "billing.audit")


def main() -> int:
    ctx = build(SERVICE, runner="asyncio", queue=QUEUE, pool_size=3)
    log, monitor, broker = ctx["logger"], ctx["monitor"], ctx["broker"]
    conn, tracker, engine, redis = (
        ctx["connection"], ctx["tracker"], ctx["engine"], ctx["redis"]
    )

    channel = conn.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=S.PREFETCH)

    from worker_health import classify_postgres, classify_redis

    drift = {"count": 0, "checked": 0}

    @tracker.handler(queue=QUEUE)
    def process(body: bytes) -> None:
        msg = json.loads(body)
        account = msg["account_id"]

        with tracker.stage(QUEUE, "postgres_read"):
            with tracker.dependency("postgres", classify=classify_postgres):
                with engine.connect() as c:
                    actual = c.execute(
                        text("SELECT balance_cents FROM accounts WHERE id = :a"),
                        {"a": account},
                    ).scalar()

        with tracker.stage(QUEUE, "redis_read"):
            with tracker.dependency("redis", classify=classify_redis):
                cached = redis.get(f"billing:balance:{account}")

        drift["checked"] += 1
        if cached is not None and actual is not None and int(cached) != int(actual):
            drift["count"] += 1
            with tracker.dependency("redis", classify=classify_redis):
                redis.setex(f"billing:balance:{account}", 300, int(actual))
            log.warning("cache drift corrected", extra={
                "service": SERVICE, "queue": QUEUE,
            })

    # A custom check, demonstrating the extensible check interface.
    @monitor.check("cache-drift", critical=False, interval=5.0, ttl=30.0)
    def cache_drift():
        from worker_health import Status
        if drift["checked"] < 20:
            return Status.OK
        ratio = drift["count"] / max(1, drift["checked"])
        return Status.OK if ratio < 0.05 else Status.DEGRADED

    unacked = {"n": 0}

    def on_message(ch, method, properties, body):
        broker.update(last_delivery_at=time.monotonic())
        unacked["n"] += 1
        broker.update(unacked=unacked["n"])
        try:
            process(body)
        except Exception as exc:  # noqa: BLE001
            log.error("reconcile failed", extra={
                "service": SERVICE, "queue": QUEUE, "category": type(exc).__name__,
            })
            ch.basic_nack(method.delivery_tag, requeue=False)
        else:
            ch.basic_ack(method.delivery_tag)
        finally:
            unacked["n"] = max(0, unacked["n"] - 1)
            broker.update(unacked=unacked["n"])

    tag = channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    broker.update(consumer_tags=(tag,), channel_open=True, connection_open=True,
                  prefetch=S.PREFETCH)

    def shutdown(signum, frame):
        try:
            channel.stop_consuming()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        channel.start_consuming()
    finally:
        monitor.stop(timeout=3)
        ctx["server"].stop()
        try:
            conn.close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
