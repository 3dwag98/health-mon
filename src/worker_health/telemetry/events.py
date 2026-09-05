"""Structured health events.

The rule from the brief, restated: log transitions, not samples.  A worker
that logs every successful probe produces ~5,760 lines a day per check and
teaches its team to filter the whole stream out; a worker that logs only
the moments where its answer CHANGED produces a handful, and every one of
them is worth reading.

Every event is a flat JSON object with a stable `event` key, so a log
pipeline can route on it without parsing prose.  Fields are drawn from
closed vocabularies (check names are registered, categories are an enum);
free text goes through ``security.safe_detail`` on the way out.
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Any

from ..core.model import ErrorCategory, Liveness, Readiness, Status
from ..security import safe_detail


class Event(str, Enum):
    """The catalogue.  Adding one here is what makes it routable."""

    WORKER_STARTED = "worker_started"
    # Emitted once by setup_worker_health with the wiring summary: which
    # probes were installed and which clients are being observed.  The one
    # line to look at when evidence says `probed` and you expected
    # `observed`.
    WORKER_HEALTH_CONFIGURED = "worker_health_configured"
    WORKER_STOPPED = "worker_stopped"
    BOOT_GRACE_STARTED = "boot_grace_started"
    BOOT_GRACE_COMPLETED = "boot_grace_completed"
    HEALTH_TRANSITION = "health_transition"
    DEPENDENCY_RECOVERED = "dependency_recovered"
    PROCESSING_STALE_DETECTED = "processing_stale_detected"
    QUEUE_LAG_THRESHOLD_CROSSED = "queue_lag_threshold_crossed"
    PROBE_TIMEOUT = "probe_timeout"
    PROBE_ERROR = "probe_error"
    LOCAL_FAULT_DETECTED = "local_fault_detected"
    READINESS_CHANGED = "readiness_changed"
    LIVENESS_CHANGED = "liveness_changed"
    CHECK_REGISTERED = "check_registered"
    RESTART_REQUESTED = "restart_requested"


# Which categories mean "the worker did this to itself".  A local fault is
# the only kind a restart can plausibly repair, so it is called out as its
# own event -- see policy.restart for why that distinction is load-bearing.
LOCAL_FAULT_CATEGORIES = frozenset({
    ErrorCategory.STALLED,
    ErrorCategory.NOT_CONSUMING,
    ErrorCategory.NOT_SUBSCRIBED,
    ErrorCategory.CREDIT_EXHAUSTED,
    ErrorCategory.POISON_LOOP,
    ErrorCategory.INTERNAL,
})

# Categories that get their own event because they need their own alert.
CATEGORY_EVENTS = {
    ErrorCategory.STALLED: Event.PROCESSING_STALE_DETECTED,
    ErrorCategory.NOT_CONSUMING: Event.PROCESSING_STALE_DETECTED,
    ErrorCategory.BACKLOG: Event.QUEUE_LAG_THRESHOLD_CROSSED,
    ErrorCategory.TIMEOUT: Event.PROBE_TIMEOUT,
}

_LEVEL_FOR_STATUS = {
    Status.OK: "info",
    Status.DISABLED: "info",
    Status.STARTING: "info",
    Status.DEGRADED: "warning",
    Status.UNKNOWN: "warning",
    Status.FAILING: "error",
}


class EventEmitter:
    """Turns state changes into log records and fan-out callbacks.

    Holds no state of its own beyond the identity labels, so it is safe to
    call from the scheduler thread, a runner task, or the HTTP thread.
    """

    def __init__(self, logger=None, *, service: str = "", instance: str = "") -> None:
        self.logger = logger
        self.service = service
        self.instance = instance
        self._listeners: list = []
        self._recent: list[dict] = []
        self._max_recent = 100

    def subscribe(self, fn) -> None:
        """Fan an event out to a caller-supplied sink (a queue, a webhook)."""
        self._listeners.append(fn)

    def recent(self, limit: int = 50) -> list[dict]:
        """The last few events, for the /health body and the dashboard.

        Bounded, because this list lives in the process forever.
        """
        return list(self._recent[-limit:])

    def emit(self, event: Event | str, *, level: str = "info", **fields: Any) -> dict:
        name = event.value if isinstance(event, Event) else str(event)
        payload: dict[str, Any] = {
            "timestamp": _iso(),
            "level": level,
            "event": name,
            "service": self.service,
            "instance": self.instance,
        }
        for key, value in fields.items():
            if value is None:
                continue
            payload[key] = _wire(key, value)

        self._recent.append(payload)
        if len(self._recent) > self._max_recent:
            del self._recent[: len(self._recent) - self._max_recent]

        if self.logger is not None:
            log = getattr(self.logger, level, None) or self.logger.info
            # `extra` keys land as top-level fields in JsonFormatter; the
            # message is the event name so a plain-text tail is still legible.
            log(name, extra={"event_fields": payload})

        for fn in self._listeners:
            try:
                fn(payload)
            except Exception:      # a broken sink must never break health
                pass
        return payload

    # -- the specific events ------------------------------------------- #

    def transition(self, name, previous: Status, current: Status, result,
                   *, critical: bool) -> None:
        level = _LEVEL_FOR_STATUS.get(current, "info")
        category = result.category.value if result.category else None
        self.emit(
            Event.HEALTH_TRANSITION,
            level=level,
            check=name,
            previous_status=previous.value,
            current_status=current.value,
            critical=critical,
            category=category,
            evidence=result.evidence.value,
            latency_ms=result.latency_ms,
            evidence_age_ms=result.evidence_age_ms,
            detail=result.detail,
        )

        if current is Status.OK and previous in (Status.FAILING, Status.DEGRADED):
            self.emit(
                Event.DEPENDENCY_RECOVERED,
                check=name, critical=critical,
                previous_status=previous.value,
                evidence=result.evidence.value,
                latency_ms=result.latency_ms,
            )
            return

        if current is not Status.FAILING or result.category is None:
            return

        specific = CATEGORY_EVENTS.get(result.category)
        if specific is not None:
            self.emit(specific, level="error", check=name,
                      category=result.category.value,
                      critical=critical, detail=result.detail,
                      **_observed_subset(result))
        if result.category in LOCAL_FAULT_CATEGORIES:
            self.emit(Event.LOCAL_FAULT_DETECTED, level="error", check=name,
                      category=result.category.value, critical=critical,
                      detail=result.detail)

    def readiness_changed(self, previous: Readiness, current: Readiness,
                          reasons: tuple[str, ...]) -> None:
        level = "info" if current in (Readiness.READY, Readiness.STARTING) else "warning"
        if current is Readiness.UNREADY:
            level = "error"
        self.emit(Event.READINESS_CHANGED, level=level,
                  previous_status=previous.value, current_status=current.value,
                  reasons=list(reasons))

    def liveness_changed(self, previous: Liveness, current: Liveness,
                         loop_lag_ms: float) -> None:
        self.emit(
            Event.LIVENESS_CHANGED,
            level="error" if current is Liveness.UNALIVE else "info",
            previous_status=previous.value, current_status=current.value,
            loop_lag_ms=loop_lag_ms,
        )

    def probe_error(self, check: str, category: ErrorCategory, detail: str | None = None,
                    *, timeout: bool = False) -> None:
        self.emit(
            Event.PROBE_TIMEOUT if timeout else Event.PROBE_ERROR,
            level="warning", check=check, category=category.value, detail=detail,
        )


def _observed_subset(result) -> dict:
    """The few observed fields worth putting on an event.

    Deliberately a whitelist: `observed` is open-ended, and an event with
    thirty fields is one nobody reads.
    """
    keep = ("queue", "queue_depth", "unacked", "prefetch", "received",
            "succeeded", "failed", "idle_seconds", "consecutive_failures")
    return {k: v for k, v in (result.observed or {}).items() if k in keep}


def _wire(key: str, value: Any) -> Any:
    if key in ("detail", "reason") and isinstance(value, str):
        return safe_detail(value)
    if key == "reasons" and isinstance(value, (list, tuple)):
        return [safe_detail(str(v)) for v in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, float):
        return round(value, 3)
    return value


def _iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
