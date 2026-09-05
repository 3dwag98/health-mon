"""FastAPI lifespan wiring.

    app = FastAPI(lifespan=health_lifespan(
        settings=HealthSettings(),
        context=lambda: {"db_engine": engine, "redis_client": redis},
        consumers=[BillingConsumer],
    ))

The lifespan owns three things a worker otherwise wires by hand: the
monitor, the consumer tasks, and their shutdown.  Shutdown is the part that
usually goes wrong -- a consumer task that is cancelled but never awaited
produces "Task exception was never retrieved" on exit and, worse, can leave
a broker connection half-closed.  This awaits every task it started.

``runner="asyncio"`` is the default here for a reason: on an event loop,
the loop-lag probe measures the thing that actually breaks an async worker
-- a coroutine blocking the loop -- while a thread runner would measure
scheduler starvation and miss it entirely.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Awaitable, Callable, Iterable, Mapping

from worker_health import WorkerHealth, setup_worker_health

from .settings import HealthSettings, probe_specs, to_config

ContextFactory = Callable[[], Mapping[str, Any]] | Mapping[str, Any] | None
ConsumerFactory = Callable[[Any], Any]


def build_health(
    settings: HealthSettings | None = None,
    *,
    context: ContextFactory = None,
    config_path: str | None = None,
    probes: Iterable[Mapping[str, Any]] | None = None,
    default_probes: bool = True,
    factory=None,
    **overrides: Any,
) -> WorkerHealth:
    """Build and start the monitor.  Usable outside a lifespan too."""
    settings = settings or HealthSettings()
    resolved_context = dict(context() if callable(context) else (context or {}))

    config = to_config(settings)
    for key, value in overrides.items():
        if hasattr(config, key) and value is not None:
            setattr(config, key, value)

    specs: list[Mapping[str, Any]] = []
    if config_path:
        from worker_health.config import load_config

        file_config = load_config(config_path)
        config.probes = list(file_config.probes)
    elif default_probes:
        # Only the probes whose objects are actually present.  A worker
        # without RabbitMQ should not get a permanently failing broker check
        # because the default list mentioned one.
        for spec in probe_specs(settings, queue=config.default_queue):
            required = [
                value[1:] for value in spec.get("params", {}).values()
                if isinstance(value, str) and value.startswith("@")
            ]
            if all(key in resolved_context for key in required):
                specs.append(spec)

    specs.extend(probes or ())
    return setup_worker_health(
        config=config, context=resolved_context, probes=list(specs), factory=factory,
    )


def health_lifespan(
    settings: HealthSettings | None = None,
    *,
    context: ContextFactory = None,
    config_path: str | None = None,
    probes: Iterable[Mapping[str, Any]] | None = None,
    consumers: Iterable[ConsumerFactory] = (),
    on_startup: Callable[[WorkerHealth], Awaitable[None]] | None = None,
    **overrides: Any,
):
    """Build the ``lifespan`` callable FastAPI expects.

    ``consumers`` are factories taking the tracker and returning an object
    with an async ``run()``.  Each is started as a task and cancelled --
    then awaited -- on shutdown.
    """

    @contextlib.asynccontextmanager
    async def lifespan(app):
        health = build_health(
            settings, context=context, config_path=config_path,
            probes=probes, **overrides,
        )

        app.state.health = health
        app.state.monitor = health.monitor
        app.state.tracker = health.tracker

        if on_startup is not None:
            await on_startup(health)

        tasks: list[asyncio.Task] = []
        for make in consumers:
            consumer = make(health.tracker)
            tasks.append(asyncio.create_task(
                consumer.run(), name=f"worker-health-consumer-{len(tasks)}"
            ))

        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                # Awaiting each cancelled task is what turns "Task exception
                # was never retrieved" at exit into a clean shutdown.
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            health.stop()

    return lifespan


def attach_health(app, health: WorkerHealth) -> WorkerHealth:
    """Publish an already-built bundle on an app that wires its own lifespan."""
    app.state.health = health
    app.state.monitor = health.monitor
    app.state.tracker = health.tracker
    return health
