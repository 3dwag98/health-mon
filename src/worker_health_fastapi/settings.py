"""FastAPI-flavoured settings.

Uses ``pydantic-settings`` when the application already has it (most
FastAPI projects do), and falls back to a plain dataclass reading the same
``HEALTH_`` environment variables when it does not.  Both produce the same
``HealthConfig``, so nothing downstream can tell which path was taken --
and worker-health does not force pydantic on a project that has no other
use for it.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Any

from worker_health.config import ENV_PREFIX, HealthConfig

try:                                    # pragma: no cover - import shape only
    from pydantic_settings import BaseSettings, SettingsConfigDict

    _HAS_PYDANTIC = True
except Exception:                       # pragma: no cover
    BaseSettings = object               # type: ignore[assignment]
    SettingsConfigDict = dict           # type: ignore[assignment]
    _HAS_PYDANTIC = False


@dataclass
class _Defaults:
    service: str = "fastapi-worker"
    instance: str = ""
    health_host: str = "0.0.0.0"
    health_port: int = 8080
    default_queue: str = "default"
    boot_grace: float = 30.0
    runner: str = "asyncio"
    log_level: str = "INFO"

    # Per-dependency knobs, mirroring the plan's settings block.  They are a
    # convenience over a full probe list: a worker with the standard three
    # dependencies can tune them here and never write YAML.
    db_interval: float = 15.0
    db_timeout: float = 2.0
    db_critical: bool = True

    redis_interval: float = 30.0
    redis_timeout: float = 1.0
    redis_critical: bool = False

    rabbit_interval: float = 5.0
    rabbit_timeout: float = 1.0
    rabbit_critical: bool = True


if _HAS_PYDANTIC:

    class HealthSettings(BaseSettings):     # type: ignore[misc]
        """``HEALTH_*`` environment variables, validated by pydantic."""

        model_config = SettingsConfigDict(env_prefix=ENV_PREFIX, extra="ignore")

        service: str = _Defaults.service
        instance: str = _Defaults.instance
        health_host: str = _Defaults.health_host
        health_port: int = _Defaults.health_port
        default_queue: str = _Defaults.default_queue
        boot_grace: float = _Defaults.boot_grace
        runner: str = _Defaults.runner
        log_level: str = _Defaults.log_level

        db_interval: float = _Defaults.db_interval
        db_timeout: float = _Defaults.db_timeout
        db_critical: bool = _Defaults.db_critical

        redis_interval: float = _Defaults.redis_interval
        redis_timeout: float = _Defaults.redis_timeout
        redis_critical: bool = _Defaults.redis_critical

        rabbit_interval: float = _Defaults.rabbit_interval
        rabbit_timeout: float = _Defaults.rabbit_timeout
        rabbit_critical: bool = _Defaults.rabbit_critical

        def as_dict(self) -> dict[str, Any]:
            return dict(self.model_dump())

else:

    @dataclass
    class HealthSettings(_Defaults):        # type: ignore[no-redef]
        """The same settings, read straight from the environment."""

        def __post_init__(self) -> None:
            for field in fields(self):
                raw = os.getenv(f"{ENV_PREFIX}{field.name.upper()}")
                if raw is None:
                    continue
                setattr(self, field.name, _coerce(field.type, raw))

        def as_dict(self) -> dict[str, Any]:
            return {f.name: getattr(self, f.name) for f in fields(self)}


def to_config(settings: "HealthSettings") -> HealthConfig:
    """Project the settings onto the SDK's config object."""
    data = settings.as_dict()
    config = HealthConfig()
    for key, value in data.items():
        if hasattr(config, key):
            setattr(config, key, value)
    return config


def probe_specs(settings: "HealthSettings", *, queue: str | None = None) -> list[dict]:
    """The standard three probes, built from the per-dependency knobs.

    Returned as raw dicts so a caller can edit, extend or drop any of them
    before handing them to the factory -- which is the point of exposing it
    rather than installing them directly.
    """
    data = settings.as_dict()
    return [
        {
            "type": "postgres", "name": "postgres",
            "critical": data["db_critical"],
            "interval": data["db_interval"], "timeout": data["db_timeout"],
            "params": {"engine": "@db_engine"},
        },
        {
            "type": "redis", "name": "redis",
            "critical": data["redis_critical"],
            "interval": data["redis_interval"], "timeout": data["redis_timeout"],
            "params": {"client": "@redis_client"},
        },
        {
            "type": "rabbitmq", "name": "rabbitmq",
            "critical": data["rabbit_critical"],
            "interval": data["rabbit_interval"], "timeout": data["rabbit_timeout"],
            "params": {
                "broker_state": "@broker_state",
                "queue": queue or data["default_queue"],
            },
        },
    ]


def _coerce(annotation, value: str):
    annotation = str(annotation)
    text = value.strip()
    if "bool" in annotation:
        return text.lower() in ("1", "true", "yes", "on")
    if "int" in annotation:
        return int(float(text))
    if "float" in annotation:
        return float(text)
    return text
