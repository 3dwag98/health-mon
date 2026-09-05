"""The broker state the RabbitMQ probe reads.

Module-level because Django settings reference it by import path, and
because there is exactly one broker connection per worker process.
"""
from worker_health import BrokerState

state = BrokerState()


def consume_forever(state, stop):
    """Yield message bodies until ``stop`` is set.

    Stands in for whatever the project actually consumes from.  The shape
    that matters is the parameter: a consume loop that cannot be interrupted
    turns every deploy into a SIGKILL and every SIGKILL into a redelivery.
    """
    while not stop.is_set():
        for body in state.drain():          # your broker client here
            yield body
            if stop.is_set():
                return
        stop.wait(0.1)
