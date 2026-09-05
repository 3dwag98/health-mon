"""The Django settings a worker needs.  Paste into your settings module.

Note the database probe type: `django_db`, not `postgres`.  The `postgres`
probe reads a SQLAlchemy engine's pool, which a Django project does not have;
`django_db` reads Django's own connection instead and reports the same
findings.  A `postgres` probe pointed at a Django connection would fall back
to its DSN probe and quietly lose every pool observation.


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
    "ENVIRONMENT": "production",
    "PORT": 8080,

    # Telemetry is pushed, not scraped: a supervised fleet has no stable
    # scrape targets. Leave the endpoint empty and export simply does not run.
    "OTEL_ENDPOINT": "http://otel-collector:4318",
    "OTEL_INTERVAL": 15.0,
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

            # A database answering in half a second is not down, and calling
            # it OK is how a worker keeps taking work it cannot finish. These
            # apply to real queries as well as to the probe.
            "latency_warn_ms": 150,
            "latency_critical_ms": 500,

            # Pool pressure is a finding about THIS APPLICATION -- the server
            # is fine -- so it goes to a different team than a database outage.
            "pool_warn_ratio": 0.80,
            "pool_critical_ratio": 0.95,
        },
        {
            "type": "redis",
            "name": "redis-cache",
            "critical": False,
            "interval": 30,
            "timeout": 1,
            "latency_warn_ms": 100,
            "params": {"client": "@redis_client"},
        },
        {
            "type": "rabbitmq",
            "name": "rabbitmq",
            "critical": True,
            "interval": 5,
            "timeout": 1,

            # Silence for this long is only a fault when there is a backlog to
            # take: an idle worker on a quiet queue is healthy forever, and
            # that discrimination is the whole point.
            "stale_after_seconds": 30,

            # A broker that is down does not need asking every five seconds.
            "backoff_multiplier": 2.0,
            "max_backoff_seconds": 60.0,

            "params": {"broker_state": "@broker_state", "queue": "billing.in"},
        },
    ],
}
