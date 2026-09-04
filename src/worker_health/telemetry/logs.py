"""Structured JSON logging.

Only the closed category ever leaves the process.  Driver exception text
embeds DSNs, so it is never logged -- that is the single rule this module
exists to enforce.
"""
from __future__ import annotations

import json
import logging
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": round(time.time(), 3),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key in ("service", "instance", "check", "category", "status",
                    "evidence", "latency_ms", "queue", "delta_ms"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload, default=str)


def configure(level: str = "INFO") -> logging.Logger:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    return logging.getLogger("worker_health")


def log_transition(logger, service, instance):
    def listener(name, previous, current, result):
        logger.info(
            "check status changed",
            extra={
                "service": service, "instance": instance, "check": name,
                "status": current.value, "evidence": result.evidence.value,
                "category": result.category.value if result.category else None,
                "latency_ms": result.latency_ms,
            },
        )
    return listener
