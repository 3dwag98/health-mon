"""A Django worker command.  The health-aware part is a base class and a
decorator.

    python manage.py consume_billing
    python manage.py consume_billing --health-port 8091

Everything else -- the ORM queries inside `process_payment`, the cache
reads, the broker connection -- is observed automatically, however deep in
the service layer it happens.

What subclassing ``WorkerHealthCommand`` buys over ``BaseCommand``:

  * ``self.tracker`` exists by the time ``handle()`` runs;
  * *this* command is wired for health and ``migrate``/``shell`` are not,
    without either being listed in settings;
  * ``--health-port`` works, and two workers on one host do not collide;
  * SIGTERM reports ``unready`` at once (503 on /ready, still 200 on /live)
    and lets the consumer finish the message in its hands.
"""
from __future__ import annotations

import json

import pika
from django.conf import settings

from billing.broker import state as broker_state
from billing.services import process_payment
from worker_health.instrument import instrument_pika_channel
from worker_health_django import WorkerHealthCommand


class Command(WorkerHealthCommand):
    help = "Consumes billing events"

    # The service label in metrics and logs.  Defaults to the command name.
    health_service = "billing-worker"
    health_queue = "billing.in"

    def handle(self, *args, **options):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(**settings.RABBITMQ)
        )
        self.channel = connection.channel()

        # Records deliveries, acks and prefetch into the broker state the
        # probe reads -- so the "idle queue vs stuck consumer" distinction
        # works without any bookkeeping in the callback below.
        instrument_pika_channel(self.channel, self.monitor, broker_state)
        self.channel.queue_declare(queue="billing.in", durable=True)
        self.channel.basic_qos(prefetch_count=10)

        @self.tracker.handler(queue="billing.in")
        def handle_message(body: dict):
            process_payment(body)

        def on_message(ch, method, properties, body):
            try:
                handle_message(json.loads(body))
            except Exception:
                ch.basic_nack(method.delivery_tag, requeue=False)
                raise
            ch.basic_ack(method.delivery_tag)

        self.channel.basic_consume(queue="billing.in", on_message_callback=on_message)
        try:
            self.channel.start_consuming()
        finally:
            connection.close()

    def on_shutdown(self, signum: int) -> None:
        """Break the consume loop so the `finally` above can close cleanly.

        Signal-handler rules apply: this is called from the handler, so it
        does one non-blocking thing.  ``stop_consuming`` lets pika finish
        delivering what it has already prefetched and then return from
        ``start_consuming``; killing the connection here would abandon those
        messages unacked instead.
        """
        connection = getattr(self.channel, "connection", None)
        if connection is not None:
            connection.add_callback_threadsafe(self.channel.stop_consuming)
