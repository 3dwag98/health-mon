"""FastAPI integration for worker-health.

    from worker_health_fastapi import HealthSettings, health_lifespan

    app = FastAPI(lifespan=health_lifespan(
        settings=HealthSettings(),
        context=lambda: {
            "db_engine": engine,
            "redis_client": redis_client,
            "broker_state": broker_state,
        },
        consumers=[BillingConsumer],
    ))

``routes.router`` is optional and imports FastAPI; everything else here
works without it, so a worker with no HTTP surface of its own can still use
the lifespan.
"""
from .deps import get_health, get_monitor, get_tracker
from .settings import HealthSettings, probe_specs, to_config
from .wiring import attach_health, build_health, health_lifespan

__all__ = [
    "HealthSettings", "health_lifespan", "build_health", "attach_health",
    "get_health", "get_monitor", "get_tracker", "probe_specs", "to_config",
]
