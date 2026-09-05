"""The broker state the RabbitMQ probe reads.

Module-level because Django settings reference it by import path, and
because there is exactly one broker connection per worker process.
"""
from worker_health import BrokerState

state = BrokerState()
