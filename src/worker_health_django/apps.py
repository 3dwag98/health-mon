"""The Django app.  Add it to INSTALLED_APPS and configure WORKER_HEALTH.

``ready()`` is the only hook Django offers that runs after the app registry
is populated and before a management command executes, which is exactly the
window where instrumenting the ORM is both possible and still early enough
to catch every query.
"""
from __future__ import annotations

import logging

from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger("worker_health")


class WorkerHealthConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "worker_health_django"
    label = "worker_health"
    verbose_name = "Worker Health"

    def ready(self):
        config = getattr(settings, "WORKER_HEALTH", {}) or {}

        from .autowire import autowire, should_wire

        if not should_wire(config):
            return

        try:
            autowire(config)
        except Exception:
            # Health monitoring must never be the reason a worker fails to
            # start.  The traceback goes to the log; the worker runs.
            logger.exception("worker-health autowiring failed; continuing without it")
