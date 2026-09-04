"""The Django settings a worker needs.  Paste into your settings module.

Nothing else changes: no middleware, no URLs, no models.  The app's
``ready()`` hook does the wiring, and it declines to wire itself for
management commands that are not workers (migrate, collectstatic, shell and
friends) so a `manage.py migrate` in an init container does not try to bind
a health port.
"""

INSTALLED_APPS = [
    # ... your apps ...
    "worker_health_django.apps.WorkerHealthConfig",
]

WORKER_HEALTH = {
    "ENABLED": True,
    "SERVICE": "billing-worker",
    "PORT": 8080,
    # Bind to loopback when the health port is not published deliberately.
    # In a container with a published port, "0.0.0.0" is correct.
    "HOST": "127.0.0.1",
    "DEFAULT_QUEUE": "billing.in",
    "BOOT_GRACE": 30,

    # Only wire health for these management commands.  Without this the
    # default skip-list is used, which covers Django's own commands but
    # cannot know that `manage.py backfill_invoices` is not a worker.
    "COMMANDS": ["consume_billing"],

    # Which ORM connections to instrument, and what to call each in health
    # output: {alias: dependency name}.
    "DATABASES": {"default": "postgres"},
    "CACHE_ALIAS": "default",

    # Objects the probes reference as "@name".  Import paths, resolved at
    # ready() time -- a callable is called, a client is used as-is.
    "CONTEXT": {
        "broker_state": "billing.broker:state",
    },

    "PROBES": [
        {
            "type": "django_db",
            "name": "postgres",
            "critical": True,
            "interval": 15,
            "timeout": 2,
            "failure_threshold": 3,
            "success_threshold": 2,
            # With the ORM instrumented, real queries are the evidence and
            # this probe only runs after 60s of silence.
            "max_silence": 60,
        },
        {
            "type": "redis",
            "name": "redis-cache",
            "critical": False,
            "interval": 30,
            "timeout": 1,
            "params": {"client": "@redis_client"},
        },
        {
            "type": "rabbitmq",
            "name": "rabbitmq",
            "critical": True,
            "interval": 5,
            "timeout": 1,
            "params": {"broker_state": "@broker_state", "queue": "billing.in"},
        },
    ],
}
