"""A Django worker command.  The health-aware part is one decorator.

Everything else -- the ORM queries in `process_payment`, the cache reads,
the broker connection -- is observed automatically by the instrumentation
that WorkerHealthConfig.ready() installed before this command ran.
"""
from __future__ import annotations

import json

import pika
from django.conf import settings
from django.core.management.base import BaseCommand

from billing.broker import state as broker_state
from billing.services import process_payment
from worker_health.instrument import instrument_pika_channel
from worker_health_django import get_monitor, get_tracker


class Command(BaseCommand):
    help = "Consumes billing events"

    def handle(self, *args, **options):
        tracker = get_tracker()
        monitor = get_monitor()

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(**settings.RABBITMQ)
        )
        channel = connection.channel()

        # Records deliveries, acks and prefetch into the broker state the
        # probe reads -- so the "idle queue vs stuck consumer" distinction
        # works without any bookkeeping in the callback below.
        instrument_pika_channel(channel, monitor, broker_state)
        channel.queue_declare(queue="billing.in", durable=True)
        channel.basic_qos(prefetch_count=10)

        @tracker.handler(queue="billing.in")
        def handle_message(body: dict):
            process_payment(body)

        def on_message(ch, method, properties, body):
            try:
                handle_message(json.loads(body))
            except Exception:
                ch.basic_nack(method.delivery_tag, requeue=False)
                raise
            ch.basic_ack(method.delivery_tag)

        channel.basic_consume(queue="billing.in", on_message_callback=on_message)
        try:
            channel.start_consuming()
        finally:
            connection.close()
