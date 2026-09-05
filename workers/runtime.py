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
import random
import time

import pika
from sqlalchemy import create_engine

from worker_health import (BrokerState, build_client, classify_amqp,
                           install_broker_probe, setup_worker_health)
from worker_health.instrument import instrument_pika_channel, instrument_pika_connection

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
            # The broker connection is deliberately absent: it is opened
            # AFTER health is serving, by the consume loop's own retry, so a
            # broker that is down at boot produces a worker that reports the
            # outage instead of a process that cannot start.  The connection
            # is instrumented as it is opened, in reconnect_amqp().
            **({"amqp_connection": connection} if connection is not None else {}),
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
    #
    # Installed here only when a connection was handed in; otherwise
    # supervise_consume installs it on the connection it opens.
    if connection is not None:
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
        "service": service,
        "instance": health.monitor.instance,
        # Owned by supervise_consume: the channel currently being consumed
        # on, and the flag a signal handler sets to stop for good.
        "_consume": {"channel": None, "stopping": False},
    }


# The ways a blocking pika connection reports that the broker went away.
AMQP_FAULTS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.AMQPChannelError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ConnectionClosedByBroker,
    OSError,
)


def stop_consuming(ctx: dict) -> None:
    """Ask the consume loop to finish.  Safe from a signal handler."""
    ctx["_consume"]["stopping"] = True
    channel = ctx["_consume"]["channel"]
    if channel is not None:
        try:
            channel.stop_consuming()
        except Exception:
            pass


def reconnect_amqp(ctx: dict, queue: str) -> None:
    """Replace a dead broker connection and re-arm everything bound to it."""
    try:
        old = ctx.get("connection")
        if old is not None and old.is_open:
            old.close()
    except Exception:
        pass

    connection = build_amqp()
    ctx["connection"] = connection
    broker = ctx["broker"]
    instrument_pika_connection(connection, ctx["monitor"], broker)
    broker.update(connection_open=True, blocked=False, probe_error=None,
                  reconnects=broker.read()["reconnects"] + 1)
    # The old probe's call_later chain died with the old connection.
    install_broker_probe(connection, broker, queue, interval=2.0,
                         logger=ctx["logger"])


def supervise_consume(ctx: dict, queue: str, on_message, *, on_channel=None,
                      backoff_initial: float = 1.0, backoff_max: float = 30.0) -> None:
    """Consume until asked to stop, surviving broker outages.

    This is the reference implementation of the rule the library states
    everywhere else: a worker REPORTS a dependency failure, it does not die
    of one.  A consumer that exits when the broker restarts takes its own
    health endpoint down with it -- exactly when someone needs to read it --
    and under a supervisor it becomes a crash loop that turns one broker
    blip into a fleet-wide outage.

    ``on_channel`` is called with each newly opened channel, for the queue
    declarations and publisher channels a worker has to rebuild alongside it.
    """
    log, broker = ctx["logger"], ctx["broker"]
    state = ctx["_consume"]
    delay = backoff_initial

    while not state["stopping"]:
        try:
            if ctx["connection"] is None or not ctx["connection"].is_open:
                reconnect_amqp(ctx, queue)
            channel = consume_channel(ctx, queue)
            state["channel"] = channel
            if on_channel is not None:
                on_channel(channel)
            channel.basic_consume(queue=queue, on_message_callback=on_message)
            # Reset only after a channel is actually open and consuming, so a
            # connection that dies during setup keeps escalating its backoff.
            delay = backoff_initial
            channel.start_consuming()
            return                      # stop_consuming() returned: clean exit
        except AMQP_FAULTS as exc:
            if state["stopping"]:
                return
            state["channel"] = None
            broker.update(connection_open=False, channel_open=False,
                          channel_state="closed", consumer_state="stopped",
                          probe_error=classify_amqp(exc))
            log.warning("broker connection lost; retrying", extra={
                "service": ctx["service"], "queue": queue,
                "category": classify_amqp(exc).value,
            })
            # Jittered so a fleet that lost the same broker does not
            # reconnect in a synchronised thundering herd.
            time.sleep(max(0.0, delay * (1.0 + random.uniform(-0.2, 0.2))))
            delay = min(delay * 2.0, backoff_max)


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
