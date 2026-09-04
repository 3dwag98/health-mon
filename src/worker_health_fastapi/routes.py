"""Optional in-app health routes.

The SDK already serves /live, /ready, /health and /metrics on its OWN
thread and its own port, and that remains the authoritative surface: it
keeps answering when the event loop is wedged, which is precisely when a
route defined here would stop responding.

These exist for the deployments where a second port is not available -- a
platform that only routes one -- and they are mounted under /internal to
make the distinction visible.  Read docs/OPERATIONS.md before relying on
them as the only liveness signal.
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse

router = APIRouter(prefix="/internal", tags=["health"])


def _monitor(request: Request):
    monitor = getattr(request.app.state, "monitor", None)
    if monitor is None:
        raise RuntimeError(
            "worker-health is not attached to this app. Use health_lifespan() "
            "or attach_health(app, health)."
        )
    return monitor


@router.get("/live")
async def live(request: Request):
    monitor = _monitor(request)
    return JSONResponse(
        content={
            "status": monitor.liveness().value,
            "loop_lag_ms": monitor.loop_lag_ms(),
            "service": monitor.service,
            "instance": monitor.instance,
        },
        status_code=monitor.live_code(),
    )


@router.get("/ready")
async def ready(request: Request):
    monitor = _monitor(request)
    return JSONResponse(
        content=monitor.snapshot_dict(include_timings=False),
        status_code=monitor.ready_code(),
    )


@router.get("/health")
async def health(request: Request):
    return JSONResponse(content=_monitor(request).snapshot_dict(include_events=True))


@router.get("/metrics")
async def metrics(request: Request):
    from worker_health.telemetry.prometheus import render

    return PlainTextResponse(
        render(_monitor(request)), media_type="text/plain; version=0.0.4"
    )
