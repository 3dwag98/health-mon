"""Optional in-app health views, for deployments that route one port.

The SDK's own HTTP server remains the authoritative surface: it runs on its
own thread, so it keeps answering when the worker's loop is wedged -- which
is the one moment a liveness answer matters. These views are served by
Django, on Django's worker, and stop answering under exactly the conditions
they exist to report.

They are worth having anyway, because a Django *worker* usually has no HTTP
server at all and a platform that can only reach one port needs somewhere
to point. Mount them in a web process next to the worker, or in the worker
itself when it does serve HTTP.

Every view is read-only and does no I/O: it serves the same cached snapshot
the SDK's own endpoints do, so a health scrape cannot become load. There is
no authentication here -- see docs/OPERATIONS.md, and do not route these
publicly.
"""
from __future__ import annotations

import json

from django.http import HttpResponse, JsonResponse

from .state import get_monitor

# The one honest failure mode: health was never wired in this process.
# Reported as 503 rather than 200, because a probe that answers "I do not
# know" with a 200 is worse than one that fails.
_NOT_WIRED = {
    "status": "unknown",
    "detail": "worker-health is not wired in this process. Check "
              "WORKER_HEALTH['ENABLED'] and the COMMANDS allow-list.",
}


def _monitor_or_503():
    monitor = get_monitor()
    if monitor is None:
        return None, JsonResponse(_NOT_WIRED, status=503)
    return monitor, None


def live(request):
    """Loop responsiveness only. Never 503s because a dependency is down."""
    monitor, error = _monitor_or_503()
    if error:
        return error
    return JsonResponse(
        {
            "status": monitor.liveness().value,
            "loop_lag_ms": monitor.loop_lag_ms(),
            "service": monitor.service,
            "instance": monitor.instance,
        },
        status=monitor.live_code(),
    )


def ready(request):
    """Full readiness: 503 on `starting` or `unready`, 200 on `degraded`."""
    monitor, error = _monitor_or_503()
    if error:
        return error
    return JsonResponse(
        monitor.snapshot_dict(include_timings=False, include_config=False),
        status=monitor.ready_code(),
    )


def health(request):
    monitor, error = _monitor_or_503()
    if error:
        return error
    return JsonResponse(monitor.snapshot_dict(include_events=True))


def config(request):
    """The settings behind the verdicts. Redacted at the source."""
    monitor, error = _monitor_or_503()
    if error:
        return error
    return JsonResponse(monitor.describe_config())


def events(request):
    monitor, error = _monitor_or_503()
    if error:
        return error
    return JsonResponse({"events": monitor.events.recent(50)})


def metrics(request):
    monitor, error = _monitor_or_503()
    if error:
        return HttpResponse(
            json.dumps(_NOT_WIRED), status=503, content_type="application/json")

    from worker_health.telemetry.prometheus import render

    return HttpResponse(render(monitor),
                        content_type="text/plain; version=0.0.4")
