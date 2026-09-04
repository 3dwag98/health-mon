"""ProbeSpec: one declarative probe definition, validated once.

Validation happens here, at wiring time, rather than at check time.  A
typo in `interval` should stop the worker from starting with a clear
message naming the probe -- not silently schedule a check every zero
seconds and be discovered when the database falls over.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping

# A param value of "@name" is a reference into the runtime context: the
# live engine, client or state object the YAML file cannot contain.
REFERENCE_PREFIX = "@"


class ProbeConfigError(ValueError):
    """Raised for a probe definition that cannot be built.

    Carries the probe name in the message because a config file with twelve
    probes and an unnamed error is a puzzle, not a diagnostic.
    """


@dataclass(slots=True)
class ProbeSpec:
    type: str
    name: str
    critical: bool = True
    enabled: bool = True
    interval: float = 15.0
    timeout: float = 2.0
    failure_threshold: int = 3
    success_threshold: int = 2
    max_silence: float = 60.0
    # None means "derive from interval": a check is stale once it has missed
    # roughly two evaluations, which is the right default at every interval
    # and is wrong as a fixed number at most of them.
    ttl: float | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.type:
            raise ProbeConfigError("probe is missing a `type`")
        if not self.name:
            self.name = self.type
        for numeric in ("interval", "timeout", "max_silence"):
            value = float(getattr(self, numeric))
            if value <= 0:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {numeric} must be greater than zero"
                )
            setattr(self, numeric, value)
        for count in ("failure_threshold", "success_threshold"):
            value = int(getattr(self, count))
            if value < 1:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {count} must be at least 1"
                )
            setattr(self, count, value)
        if self.timeout >= self.interval:
            # Not fatal, but it means a slow check is always in flight when
            # the next one is due; the runner will skip rather than pile up.
            pass
        self.ttl = float(self.ttl) if self.ttl is not None else \
            round(self.interval * 2 + self.timeout, 3)

    # -- construction ---------------------------------------------------- #

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any]) -> "ProbeSpec":
        if not isinstance(raw, Mapping):
            raise ProbeConfigError(f"probe definition must be a mapping, got {type(raw).__name__}")
        data = dict(raw)
        known = {f for f in cls.__dataclass_fields__}          # noqa: SLF001
        params = dict(data.pop("params", {}) or {})
        unknown = set(data) - known
        if unknown:
            # Anything not a spec field is treated as a param, so a short
            # form (`url: ...` instead of `params: {url: ...}`) works, but
            # a misspelled `intervall` still shows up in the error below.
            for key in sorted(unknown):
                params[key] = data.pop(key)
        try:
            spec = cls(**{k: v for k, v in data.items() if k in known})
        except TypeError as exc:
            # A missing `type`, or a field given a value of the wrong shape.
            # Raised as a config error naming the probe, because a raw
            # TypeError from a dataclass constructor tells a reader nothing
            # about which line of their YAML is wrong.
            label = data.get("name") or data.get("type") or "<unnamed>"
            raise ProbeConfigError(
                f"probe {label!r} could not be built from its definition: {exc}"
            ) from exc
        spec.params = params
        return spec

    def with_params(self, params: Mapping[str, Any]) -> "ProbeSpec":
        return replace(self, params=dict(params))

    # -- context references ---------------------------------------------- #

    def resolved_params(self, context: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Swap ``"@name"`` values for the live objects in ``context``.

        ``"@@literal"`` escapes to a literal ``"@literal"``, because an
        HTTP header value or a Redis key pattern may legitimately start
        with an at-sign.
        """
        context = context or {}
        out: dict[str, Any] = {}
        for key, value in self.params.items():
            out[key] = self._resolve(key, value, context)
        return out

    def _resolve(self, key: str, value: Any, context: Mapping[str, Any]) -> Any:
        if isinstance(value, str) and value.startswith(REFERENCE_PREFIX):
            if value.startswith(REFERENCE_PREFIX * 2):
                return value[1:]
            ref = value[1:]
            if ref not in context:
                available = ", ".join(sorted(context)) or "nothing"
                raise ProbeConfigError(
                    f"probe {self.name!r}: param {key!r} references {value!r}, "
                    f"but the context provides {available}"
                )
            return context[ref]
        if isinstance(value, dict):
            return {k: self._resolve(f"{key}.{k}", v, context) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve(f"{key}[{i}]", v, context) for i, v in enumerate(value)]
        return value

    # -- projection onto the monitor's registration kwargs ---------------- #

    def registration_kwargs(self) -> dict[str, Any]:
        return {
            "critical": self.critical,
            "enabled": self.enabled,
            "interval": self.interval,
            "timeout": self.timeout,
            "ttl": self.ttl,
            "failure_threshold": self.failure_threshold,
            "success_threshold": self.success_threshold,
            "max_silence": self.max_silence,
        }

    def redacted(self) -> dict[str, Any]:
        """A log-safe view.  Params can hold a DSN, so they go through redaction."""
        from ..security import redact_mapping

        return {
            "type": self.type, "name": self.name, "critical": self.critical,
            "enabled": self.enabled, "interval": self.interval,
            "timeout": self.timeout, "params": redact_mapping(self.params),
        }
