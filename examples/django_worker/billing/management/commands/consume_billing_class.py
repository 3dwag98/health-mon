"""The same worker, written as a class-based consumer.

A consumer that carries state -- a batch buffer, a counter, a client --
is naturally a class.  ``@tracker.handler`` applied to a class wraps
``__call__`` in place, so ``isinstance`` checks and anything holding a
reference to the class keep working; and an ``async def __call__`` is timed
across the await rather than being measured as "how long it took to create
a coroutine", which is what every inspect-based decorator gets wrong.
"""
from __future__ import annotations

import json

from billing.broker import consume_forever, state as broker_state
from billing.services import process_payment
from worker_health_django import WorkerHealthCommand


class Command(WorkerHealthCommand):
    help = "Consumes billing events with a class-based consumer"
    health_service = "billing-worker"
    health_queue = "billing.in"

    def handle(self, *args, **options):
        @self.tracker.handler(queue="billing.in")
        class BillingConsumer:
            def __init__(self) -> None:
                self.seen = 0

            def __call__(self, body: dict) -> None:
                self.seen += 1
                process_payment(body)

        consumer = BillingConsumer()
        # `self.stopping` is set by SIGTERM/SIGINT; a loop that watches it is
        # the difference between a clean drain and a lost message.
        for raw in consume_forever(broker_state, stop=self.stopping):
            consumer(json.loads(raw))

        self.stdout.write(f"consumer stopped after {consumer.seen} messages")
