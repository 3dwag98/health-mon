"""Billing worker: thread runner, blocking pika consume loop.

The shape most PM2-managed Python workers actually have, and the reference
for what integration costs: two calls in `runtime.build`, one decorator
here, and nothing else.  There is no health bookkeeping in the consume
callback -- deliveries, acks and prefetch are recorded by the instrumented
channel, and every query and Redis command by the driver instrumentation.
"""
from __future__ import annotations

import os
import signal
import sys

import pika

import settings as S
from pipeline import BillingPipeline, LockNotAcquired, init_schema
from runtime import build, stop_consuming, supervise_consume

SERVICE = os.getenv("SERVICE", "billing")


def main() -> int:
    ctx = build(SERVICE, runner="thread", queue=S.IN_QUEUE, pool_size=5)
    log, monitor = ctx["logger"], ctx["monitor"]
    tracker = ctx["tracker"]

    init_schema(ctx["engine"])

    # Rebuilt on every (re)connect, along with the consumer channel: a
    # publisher channel bound to a connection that has gone away is the
    # classic way a "recovered" consumer keeps failing every message.
    out = {"channel": None}

    def on_channel(channel) -> None:
        channel.queue_declare(queue=S.OUT_QUEUE, durable=True)
        channel.queue_declare(queue=S.AUDIT_QUEUE, durable=True)
        out["channel"] = ctx["connection"].channel()

    def publish(body: bytes) -> None:
        # Fan out to both downstream queues so notify and reconcile each see
        # every invoice rather than competing for the same messages.
        for rk in (S.OUT_QUEUE, S.AUDIT_QUEUE):
            out["channel"].basic_publish(
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

    def on_message(ch, method, properties, body):
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

    def shutdown(signum, frame):
        if ctx["_consume"]["stopping"]:
            return
        log.info("shutting down", extra={"service": SERVICE})
        stop_consuming(ctx)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        supervise_consume(ctx, S.IN_QUEUE, on_message, on_channel=on_channel)
    finally:
        ctx["health"].stop(timeout=3)
        try:
            ctx["connection"].close()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
