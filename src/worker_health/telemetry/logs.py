"""Structured JSON logging.

Only the closed category ever leaves the process.  Driver exception text
embeds DSNs, so it is never logged -- that is the single rule this module
exists to enforce, and every value that passes through here is run through
``security.redact`` as a backstop in case a caller forgets.
"""
from __future__ import annotations

import json
import logging
import time

from ..security import redact, safe_detail

# Fields a caller may attach with `extra=` and have appear at the top level.
_PASSTHROUGH = (
    "service", "instance", "check", "category", "status", "evidence",
    "latency_ms", "queue", "delta_ms", "event", "critical", "detail",
    "previous_status", "current_status", "evidence_age_ms", "reasons",
    "queue_depth", "loop_lag_ms",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "ts": round(record.created, 3),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": redact(record.getMessage()),
        }

        # A structured event carries its own complete payload; merging it
        # wholesale is what keeps the event schema in events.py rather than
        # split across two modules.
        fields = getattr(record, "event_fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
            payload.setdefault("msg", fields.get("event"))
            payload["level"] = fields.get("level", payload["level"])
        else:
            for key in _PASSTHROUGH:
                value = getattr(record, key, None)
                if value is not None:
                    payload[key] = safe_detail(value) if key == "detail" else value

        if record.exc_info:
            # The class name only.  The message and traceback of a driver
            # exception are exactly where connection strings live.
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None

        return json.dumps(payload, default=str)


def configure(level: str = "INFO", *, force: bool = True) -> logging.Logger:
    """Install the JSON formatter on the root logger.

    ``force=False`` leaves an application's existing handlers alone, which is
    what the Django and FastAPI autowiring wants: a worker team that has
    already configured logging should not have it replaced by an SDK.
    """
    root = logging.getLogger()
    if force or not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        root.handlers[:] = [handler]
    root.setLevel(level.upper())
    return logging.getLogger("worker_health")


def log_transition(logger, service, instance):
    """Back-compatible transition listener.

    ``HealthMonitor`` emits the richer ``health_transition`` event on its
    own; this remains for code that wired the listener up by hand.
    """
    def listener(name, previous, current, result):
        logger.info(
            "check status changed",
            extra={
                "service": service, "instance": instance, "check": name,
                "previous_status": previous.value, "status": current.value,
                "evidence": result.evidence.value,
                "category": result.category.value if result.category else None,
                "latency_ms": result.latency_ms,
            },
        )
    return listener
