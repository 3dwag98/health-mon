"""Shared wiring for the sample workers.

This is the whole integration, and it is worth reading as the reference for
what a real worker has to do:

    build the clients it was going to build anyway
    hand them to setup_worker_health() as a context
    decorate the handler

Everything else -- instrumenting SQLAlchemy, redis-py and pika, installing
the probes named in the YAML, starting the health server, registering the
processing check -- happens inside that one call.  The worker files
themselves contain no health code beyond a decorator.
"""
from __future__ import annotations

import os

import pika
from sqlalchemy import create_engine

from worker_health import BrokerState, build_client, install_broker_probe, setup_worker_health
from worker_health.instrument import instrument_pika_channel

import settings as S

CONFIG_PATH = os.getenv(
    "WORKER_HEALTH_CONFIG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker-health.yaml"),
)


def build_engine(pool_size: int = 5):
    """Deliberately small pool.

    Small enough that exhaustion is reachable in a demo, which is the point:
    pool exhaustion must report as `pool_exhausted` (degraded) and never as
    `connection_refused` (outage), because those go to different teams.
    """
    return create_engine(
        S.pg_url(),
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=2,
        pool_pre_ping=False,
        connect_args={"application_name": f"worker-app-{S.SERVICE}"},
    )


def build_redis(db: int = 0):
    """Redis 7.2: protocol negotiation is a non-issue, timeouts are not.

    build_client() sets socket timeouts because redis-py's default of None
    means a black-holed connection hangs the probe forever.
    """
    return build_client(
        host=S.RD_HOST, port=S.RD_PORT, db=db, password=S.RD_PASSWORD,
        socket_timeout=1.5, socket_connect_timeout=1.5,
    )


def build_amqp():
    params = pika.ConnectionParameters(
        host=S.MQ_HOST, port=S.MQ_PORT,
        credentials=pika.PlainCredentials(S.MQ_USER, S.MQ_PASSWORD),
        heartbeat=15,
        blocked_connection_timeout=10,
        socket_timeout=5,
        connection_attempts=3,
        retry_delay=2.0,
    )
    return pika.BlockingConnection(params)


def build(service: str, *, runner: str = "thread", queue: str | None = None,
          pool_size: int = 5, engine=None, redis=None, connection=None,
          probes=(), **overrides):
    """Build the clients, then hand them to the SDK.

    Returns the same dict shape the workers used before, so nothing in a
    worker file has to know that the wiring moved.
    """
    queue = queue or S.IN_QUEUE

    engine = engine if engine is not None else build_engine(pool_size)
    redis = redis if redis is not None else build_redis()
    connection = connection if connection is not None else build_amqp()

    broker = BrokerState()
    broker.update(prefetch=S.PREFETCH)

    health = setup_worker_health(
        service=service,
        config_path=CONFIG_PATH,
        context={
            # Auto-instrumentation finds these by shape and records every
            # real query, command and broker event as OBSERVED evidence.
            "db_engine": engine,
            "redis_client": redis,
            "amqp_connection": connection,
            # Referenced by "@name" from the YAML.
            "broker_state": broker,
            "probe_dsn": S.pg_url(),
        },
        probes=list(probes),
        runner=runner,
        default_queue=queue,
        **overrides,
    )

    # The passive declare that yields queue depth and consumer count, driven
    # from the connection's OWN thread via call_later.  No second connection,
    # no cross-thread access to a BlockingConnection, and if the worker's
    # loop stops turning the state goes stale -- which is itself the signal.
    install_broker_probe(connection, broker, queue, interval=2.0,
                         logger=health.logger)

    health.logger.info("worker health started", extra={
        "service": service, "instance": health.monitor.instance, "queue": queue,
    })
    return {
        "health": health,
        "monitor": health.monitor, "server": health.server, "logger": health.logger,
        "engine": engine, "redis": redis, "connection": connection,
        "processing": health.processing, "broker": broker,
        "tracker": health.tracker, "queue": queue,
        "instance": health.monitor.instance,
    }


def consume_channel(ctx: dict, queue: str, *, prefetch: int | None = None):
    """Open the consumer channel with delivery/ack tracking already attached.

    ``instrument_pika_channel`` is what removes the hand-written
    ``broker.update(last_delivery_at=...)`` and the unacked counter from
    every consume callback -- and with them the classic bug where one path
    through the callback forgets to decrement and the worker eventually
    reports credit exhaustion while perfectly healthy.
    """
    channel = ctx["connection"].channel()
    instrument_pika_channel(channel, ctx["monitor"], ctx["broker"])
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=prefetch if prefetch is not None else S.PREFETCH)
    return channel
