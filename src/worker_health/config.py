"""Configuration: one shape, three sources.

A worker-health config can come from a YAML file, a Python mapping (Django
settings), or the environment.  All three land in the same ``HealthConfig``
so the wiring code downstream has exactly one thing to read, and so a
setting means the same thing wherever it was written.

Precedence, highest first:

    explicit keyword arguments to setup_worker_health()
    environment variables            (HEALTH_*)
    the config file / mapping
    the defaults here

Environment above file is deliberate: the file is baked into an image, the
environment is what an operator can change at 3am without a rebuild.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping

from .probes.spec import ProbeConfigError, ProbeSpec

ENV_PREFIX = "HEALTH_"

# The top-level key inside a config file.  Its presence lets worker-health
# settings live inside a larger application config file rather than
# demanding one of their own.
ROOT_KEY = "worker_health"


@dataclass(slots=True)
class HealthConfig:
    service: str = "worker"
    instance: str = ""
    version: str = "0.0.0"

    # transport
    health_host: str = "0.0.0.0"
    health_port: int = 8080
    serve_http: bool = True

    # engine
    runner: str = "thread"            # "thread" or "asyncio"
    tick: float = 0.2
    boot_grace: float = 30.0
    max_workers: int = 8
    loop_lag_threshold_ms: float = 2000.0

    # processing
    default_queue: str = "default"
    processing_check: bool = True
    max_idle: float = 60.0
    max_since_success: float = 120.0
    poison_threshold: int = 10

    # behaviour
    log_level: str = "INFO"
    configure_logging: bool = True
    instrument: bool = True
    strict_probes: bool = True
    load_plugins: bool = True

    probes: list[ProbeSpec] = field(default_factory=list)
    restart: dict[str, Any] = field(default_factory=dict)

    # -- construction ---------------------------------------------------- #

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> "HealthConfig":
        """Accept both the SDK's snake_case and Django's UPPER_CASE spellings."""
        if not data:
            return cls()
        body = data.get(ROOT_KEY, data) if isinstance(data, Mapping) else data
        normalised = {str(k).lower(): v for k, v in body.items()}

        fields = {f for f in cls.__dataclass_fields__}          # noqa: SLF001
        kwargs: dict[str, Any] = {}
        for key, value in normalised.items():
            target = _ALIASES.get(key, key)
            if target == "probes":
                continue
            if target in fields:
                kwargs[target] = value

        config = cls(**kwargs)
        config.probes = _parse_probes(normalised.get("probes"))
        config.restart = dict(normalised.get("restart") or {})
        return config

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None,
                 base: "HealthConfig | None" = None) -> "HealthConfig":
        """Overlay ``HEALTH_*`` variables onto ``base``.

        Probes are not configurable from the environment: a list of nested
        mappings squeezed into environment variables is a format nobody can
        read, and a config file or Django setting is available in every
        deployment that could set the variables anyway.
        """
        env = os.environ if env is None else env
        config = replace(base) if base is not None else cls()
        if base is not None:
            config.probes = list(base.probes)
            config.restart = dict(base.restart)

        for name, value in env.items():
            if not name.startswith(ENV_PREFIX):
                continue
            key = _ALIASES.get(name[len(ENV_PREFIX):].lower(), name[len(ENV_PREFIX):].lower())
            spec = cls.__dataclass_fields__.get(key)             # noqa: SLF001
            if spec is None or key in ("probes", "restart"):
                continue
            try:
                setattr(config, key, _coerce(spec.type, value))
            except (TypeError, ValueError):
                continue           # a malformed override keeps the default
        return config

    # -- reads ------------------------------------------------------------ #

    def probe(self, name: str) -> ProbeSpec | None:
        for spec in self.probes:
            if spec.name == name:
                return spec
        return None

    def redacted(self) -> dict[str, Any]:
        """A log-safe dict of the whole configuration."""
        from .security import redact_mapping

        body = {
            key: getattr(self, key)
            for key in self.__dataclass_fields__                # noqa: SLF001
            if key not in ("probes", "restart")
        }
        body["probes"] = [spec.redacted() for spec in self.probes]
        body["restart"] = redact_mapping(self.restart)
        return redact_mapping(body)


# Spellings people actually use, mapped onto the field names.
_ALIASES = {
    "enabled": "serve_http",
    "port": "health_port",
    "host": "health_host",
    "bind": "health_host",
    "queue": "default_queue",
    "grace": "boot_grace",
    "loglevel": "log_level",
    "service_name": "service",
    "instance_id": "instance",
}


def _parse_probes(raw) -> list[ProbeSpec]:
    if not raw:
        return []
    if isinstance(raw, Mapping):
        # Mapping form: {name: {type: ..., ...}}.  Convenient in Django
        # settings, where a dict of named blocks reads better than a list.
        return [
            ProbeSpec.from_raw({**dict(body or {}), "name": name})
            for name, body in raw.items()
        ]
    return [r if isinstance(r, ProbeSpec) else ProbeSpec.from_raw(r) for r in raw]


def _coerce(annotation, value: str):
    text = str(value).strip()
    annotation = str(annotation)
    if "bool" in annotation:
        return text.lower() in ("1", "true", "yes", "on")
    if "int" in annotation:
        return int(float(text))
    if "float" in annotation:
        return float(text)
    return text


def load_config(path: str | Path | None = None, *, env: Mapping[str, str] | None = None,
                required: bool = False) -> HealthConfig:
    """Load a config file, then overlay the environment.

    ``path`` may be YAML or JSON, decided by extension and then by content
    -- a file named ``.conf`` that happens to hold JSON still loads.
    """
    data: Mapping[str, Any] | None = None
    if path:
        file = Path(path)
        if not file.exists():
            if required:
                raise ProbeConfigError(f"config file not found: {file}")
        else:
            data = _load_file(file)

    config = HealthConfig.from_mapping(data)
    return HealthConfig.from_env(env, base=config)


# ${VAR} and ${VAR:-default}, expanded before parsing.
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def expand_env(text: str, env: Mapping[str, str] | None = None) -> str:
    """Substitute environment references in a config file.

    One config file then serves a whole fleet: queue names, hosts and
    thresholds differ per worker and come from the environment, while the
    probe SHAPE stays in version control where it can be reviewed.

    A reference with no value and no default expands to empty rather than
    raising, so a probe that does not apply to this worker degrades to an
    obvious misconfiguration in the health output instead of stopping the
    process at import time.
    """
    source = os.environ if env is None else env

    def replace_ref(match: re.Match) -> str:
        name, default = match.group(1), match.group(2)
        return source.get(name, default if default is not None else "")

    return _ENV_REF.sub(replace_ref, text)


def _load_file(file: Path) -> Mapping[str, Any]:
    text = expand_env(file.read_text(encoding="utf-8"))
    if file.suffix.lower() == ".json" or text.lstrip().startswith("{"):
        return json.loads(text)

    from ._yaml import safe_load

    data = safe_load(text)
    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise ProbeConfigError(
            f"{file}: expected a mapping at the top level, got {type(data).__name__}"
        )
    return data
