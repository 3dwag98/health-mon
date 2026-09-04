"""Billing worker: thread runner, blocking pika consume loop.

The shape most PM2-managed Python workers actually have.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time

import pika

import settings as S
from pipeline import BillingPipeline, LockNotAcquired, init_schema
from runtime import build

SERVICE = os.getenv("SERVICE", "billing")


def main() -> int:
    ctx = build(SERVICE, runner="thread", queue=S.IN_QUEUE, pool_size=5)
    log, monitor, broker = ctx["logger"], ctx["monitor"], ctx["broker"]
    conn, tracker = ctx["connection"], ctx["tracker"]

    init_schema(ctx["engine"])

    channel = conn.channel()
    channel.queue_declare(queue=S.IN_QUEUE, durable=True)
    channel.queue_declare(queue=S.OUT_QUEUE, durable=True)
    channel.queue_declare(queue=S.AUDIT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=S.PREFETCH)

    out_channel = conn.channel()

    def publish(body: bytes) -> None:
        # Fan out to both downstream queues so notify and reconcile each see
        # every invoice rather than competing for the same messages.
        for rk in (S.OUT_QUEUE, S.AUDIT_QUEUE):
            out_channel.basic_publish(
                exchange="", routing_key=rk, body=body,
                properties=pika.BasicProperties(delivery_mode=2),
            )

    pipeline = BillingPipeline(
        ctx["engine"], ctx["redis"], publish, tracker, S.IN_QUEUE, logger=log
    )

    # The one decorator.  Everything the processing check needs comes from it.
    @tracker.handler(queue=S.IN_QUEUE)
    def process(body: bytes) -> dict:
        return pipeline.handle(body)

    unacked = {"n": 0}

    def on_message(ch, method, properties, body):
        broker.update(last_delivery_at=time.monotonic())
        unacked["n"] += 1
        broker.update(unacked=unacked["n"])
        try:
            result = process(body)
        except LockNotAcquired:
            # Contended account: give it back rather than failing it.
            ch.basic_nack(method.delivery_tag, requeue=True)
        except Exception as exc:  # noqa: BLE001
            log.error("message failed", extra={
                "service": SERVICE, "queue": S.IN_QUEUE,
                "category": type(exc).__name__,
            })
            ch.basic_nack(method.delivery_tag, requeue=False)
        else:
            ch.basic_ack(method.delivery_tag)
            if result.get("result") == "applied":
                log.info("invoice applied", extra={
                    "service": SERVICE, "queue": S.IN_QUEUE,
                })
        finally:
            unacked["n"] = max(0, unacked["n"] - 1)
            broker.update(unacked=unacked["n"])

    tag = channel.basic_consume(queue=S.IN_QUEUE, on_message_callback=on_message)
    broker.update(consumer_tags=(tag,), channel_open=True, connection_open=True,
                  prefetch=S.PREFETCH)

    stopping = {"flag": False}

    def shutdown(signum, frame):
        if stopping["flag"]:
            return
        stopping["flag"] = True
        log.info("shutting down", extra={"service": SERVICE})
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
