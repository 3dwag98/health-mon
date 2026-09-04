"""Shared wiring: build a monitor with all four checks and serve it.

This is what the integration guide reduces to -- about forty lines to give
any worker full dependency, processing and liveness reporting.
"""
from __future__ import annotations

import logging
import os

import pika
from sqlalchemy import create_engine

from worker_health import (
    BrokerState,
    HealthMonitor,
    HealthServer,
    PostgresCheck,
    ProcessingCheck,
    ProcessingState,
    RabbitMQCheck,
    RedisCheck,
    RestartPolicy,
    Tracker,
    build_client,
    install_broker_probe,
)
from worker_health.telemetry.logs import configure, log_transition

import settings as S


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
    # protocol=2 is mandatory against Redis 5.0.6 -- redis-py 6+ negotiates
    # RESP3 with HELLO, which 5.0.6 does not implement, and every command
    # then fails.  build_client() pins it.
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
          extra_checks=(), broker_critical: bool = True):
    logger = configure(S.LOG_LEVEL)
    queue = queue or S.IN_QUEUE
    instance = S.INSTANCE or f"{service}-{os.getpid()}"

    monitor = HealthMonitor(
        service=service, version="0.1.0", instance=instance,
        runner=runner, tick=0.2, logger=logger,
    )
    monitor.on_transition(log_transition(logger, service, instance))

    engine = engine if engine is not None else build_engine(pool_size)
    redis = redis if redis is not None else build_redis()
    connection = connection if connection is not None else build_amqp()

    processing = ProcessingState()
    broker = BrokerState()
    broker.update(prefetch=S.PREFETCH)
    tracker = Tracker(monitor, processing, default_queue=queue)

    monitor.register(
        PostgresCheck(app_engine=engine, probe_dsn=S.pg_url(), name="postgres"),
        name="postgres", critical=True,
        interval=3.0, timeout=2.0, ttl=15.0, max_silence=6.0,
        failure_threshold=2, success_threshold=2,
    )
    monitor.register(
        RedisCheck(redis, name="redis"),
        name="redis", critical=False,
        interval=3.0, timeout=2.0, ttl=15.0, max_silence=6.0,
        failure_threshold=2, success_threshold=2,
    )
    monitor.register(
        RabbitMQCheck(broker, queue=queue, name="rabbitmq",
                      backlog_threshold=int(os.getenv("BACKLOG_THRESHOLD", "500")),
                      stale_after=12.0),
        name="rabbitmq", critical=broker_critical,
        interval=2.0, timeout=2.0, ttl=20.0,
        failure_threshold=2, success_threshold=2,
    )
    monitor.register(
        ProcessingCheck(processing, broker_state=broker,
                        max_idle=float(os.getenv("MAX_IDLE", "45")),
                        max_since_success=float(os.getenv("MAX_SINCE_SUCCESS", "90"))),
        name="processing", critical=False,
        interval=2.0, timeout=2.0, ttl=20.0,
        failure_threshold=2, success_threshold=1,
    )
    for check, kwargs in extra_checks:
        monitor.register(check, **kwargs)

    if S.RESTART_ENABLED:
        monitor.set_restart_policy(RestartPolicy(
            enabled=True,
            after_cycles=S.RESTART_AFTER_CYCLES,
            min_uptime=S.RESTART_MIN_UPTIME,
            logger=logger,
        ))

    install_broker_probe(connection, broker, queue, interval=2.0, logger=logger)

    server = HealthServer(monitor, port=S.HEALTH_PORT).start()
    monitor.start(boot_grace=float(os.getenv("BOOT_GRACE", "20")))

    logger.info("worker health started", extra={
        "service": service, "instance": instance, "queue": queue,
    })
    return {
        "monitor": monitor, "server": server, "logger": logger,
        "engine": engine, "redis": redis, "connection": connection,
        "processing": processing, "broker": broker, "tracker": tracker,
        "queue": queue, "instance": instance,
    }
