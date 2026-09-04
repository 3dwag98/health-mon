"""Environment wiring shared by every sample worker."""
from __future__ import annotations

import os

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "app")
PG_PASSWORD = os.getenv("PG_PASSWORD", "canary-pg-8f3ad91c")
PG_DB = os.getenv("PG_DB", "app")

MQ_HOST = os.getenv("MQ_HOST", "rabbitmq")
MQ_PORT = int(os.getenv("MQ_PORT", "5672"))
MQ_USER = os.getenv("MQ_USER", "app")
MQ_PASSWORD = os.getenv("MQ_PASSWORD", "canary-mq-4b7ce02d")

RD_HOST = os.getenv("RD_HOST", "redis")
RD_PORT = int(os.getenv("RD_PORT", "6379"))
RD_PASSWORD = os.getenv("RD_PASSWORD", "canary-rd-1a9fe63b")

SERVICE = os.getenv("SERVICE", "worker")
INSTANCE = os.getenv("HEALTH_INSTANCE", "")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

IN_QUEUE = os.getenv("IN_QUEUE", "billing.in")
OUT_QUEUE = os.getenv("OUT_QUEUE", "billing.out")
AUDIT_QUEUE = os.getenv("AUDIT_QUEUE", "billing.audit")
PREFETCH = int(os.getenv("PREFETCH", "10"))

RESTART_ENABLED = os.getenv("RESTART_ENABLED", "false").lower() == "true"
RESTART_AFTER_CYCLES = int(os.getenv("RESTART_AFTER_CYCLES", "5"))
RESTART_MIN_UPTIME = float(os.getenv("RESTART_MIN_UPTIME", "120"))


def pg_url(user: str | None = None, password: str | None = None) -> str:
    u = user or PG_USER
    p = password or PG_PASSWORD
    return f"postgresql+psycopg://{u}:{p}@{PG_HOST}:{PG_PORT}/{PG_DB}"
