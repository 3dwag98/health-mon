"""Notification worker: consumes billing.out, writes to Postgres and Redis.

A second consumer on a different queue, so the fleet view has more than one
shape of worker in it.
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

SERVICE = os.getenv("SERVICE", "notify")
QUEUE = os.getenv("IN_QUEUE", "billing.out")

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    invoice_id   TEXT NOT NULL,
    balance_cents BIGINT NOT NULL,
    sent_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def main() -> int:
    ctx = build(SERVICE, runner="thread", queue=QUEUE, pool_size=3)
    log, monitor, broker = ctx["logger"], ctx["monitor"], ctx["broker"]
    conn, tracker, engine, redis = (
        ctx["connection"], ctx["tracker"], ctx["engine"], ctx["redis"]
    )

    with engine.begin() as c:
        c.execute(text(SCHEMA))

    channel = conn.channel()
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.basic_qos(prefetch_count=S.PREFETCH)

    from worker_health import classify_postgres, classify_redis

    @tracker.handler(queue=QUEUE)
    def process(body: bytes) -> None:
        msg = json.loads(body)
        with tracker.stage(QUEUE, "postgres_insert"):
            with tracker.dependency("postgres", classify=classify_postgres):
                with engine.begin() as c:
                    c.execute(
                        text("INSERT INTO notifications "
                             "(account_id, invoice_id, balance_cents) "
                             "VALUES (:a, :i, :b)"),
                        {"a": msg["account_id"], "i": msg["invoice_id"],
                         "b": msg["balance_cents"]},
                    )
        with tracker.stage(QUEUE, "redis_counter"):
            with tracker.dependency("redis", classify=classify_redis):
                redis.incr(f"notify:count:{msg['account_id']}")
                redis.setex("notify:last", 300, msg["invoice_id"])

    unacked = {"n": 0}

    def on_message(ch, method, properties, body):
        broker.update(last_delivery_at=time.monotonic())
        unacked["n"] += 1
        broker.update(unacked=unacked["n"])
        try:
            process(body)
        except Exception as exc:  # noqa: BLE001
            log.error("notification failed", extra={
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
