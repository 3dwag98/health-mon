"""Django integration for worker-health.

    INSTALLED_APPS = [..., "worker_health_django.apps.WorkerHealthConfig"]

    WORKER_HEALTH = {
        "ENABLED": True,
        "SERVICE": "billing-worker",
        "PORT": 8080,
        "DEFAULT_QUEUE": "billing.in",
        "BOOT_GRACE": 30,
        "PROBES": [
            {"type": "django_db", "name": "postgres", "critical": True,
             "interval": 15, "timeout": 2},
            {"type": "redis", "name": "redis-cache", "critical": False,
             "interval": 30, "timeout": 1,
             "params": {"client": "@redis_client"}},
        ],
    }

Then, in the worker command:

    from worker_health_django import get_tracker

    tracker = get_tracker()

    @tracker.handler(queue="billing.in")
    def handle_message(body: dict):
        process_payment(body)
"""
from .state import get_health, get_monitor, get_tracker, set_health_state

default_app_config = "worker_health_django.apps.WorkerHealthConfig"

__all__ = ["get_health", "get_monitor", "get_tracker", "set_health_state",
           "default_app_config"]
