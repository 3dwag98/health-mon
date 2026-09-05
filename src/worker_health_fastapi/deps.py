"""FastAPI dependencies.

    @app.get("/queue-depth")
    async def depth(monitor = Depends(get_monitor)):
        return monitor.snapshot_dict()["processing"]
"""
from __future__ import annotations


def get_health(request):
    return getattr(request.app.state, "health", None)


def get_monitor(request):
    return getattr(request.app.state, "monitor", None)


def get_tracker(request):
    return getattr(request.app.state, "tracker", None)
