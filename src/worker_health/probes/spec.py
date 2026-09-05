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

# Accepted alternative spellings for spec fields.
_FIELD_ALIASES = {
    "max_backoff_seconds": "backoff_max",
    "max_backoff": "backoff_max",
    "backoff_initial_seconds": "backoff_initial",
    "stale_after": "stale_after_seconds",
    "latency_warning_ms": "latency_warn_ms",
}


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
    # None means "derive from interval", for the same reason ttl does: a
    # fixed 2s default is longer than any interval under two seconds, so a
    # config that sets only a short interval would otherwise inherit a
    # timeout it never chose and overlap every evaluation.
    timeout: float | None = None
    failure_threshold: int = 3
    success_threshold: int = 2
    max_silence: float = 60.0
    # None means "derive from interval": a check is stale once it has missed
    # roughly two evaluations, which is the right default at every interval
    # and is wrong as a fixed number at most of them.
    ttl: float | None = None

    # Backoff while the check is failing.  The engine has always supported
    # this (core.machine.CheckSpec); until now there was no way to reach it
    # from a config file, so every probe in the fleet backed off on the same
    # hardcoded 5s/2x/60s curve whatever its dependency cost to ask.
    backoff_initial: float = 5.0
    backoff_max: float = 60.0
    backoff_multiplier: float = 2.0
    backoff_jitter: float = 0.1

    # Latency thresholds.  A dependency that answers in 900ms is not down,
    # and calling it OK is how a worker keeps taking work it cannot finish
    # in time.  None means "do not judge latency", which stays the default
    # because a threshold guessed by a library is a false alarm generator.
    latency_warn_ms: float | None = None
    latency_critical_ms: float | None = None

    # Connection-pool pressure, as a fraction of capacity.  None leaves the
    # check's own default in place.  Pool exhaustion is a finding about the
    # APPLICATION, not the server, and it goes to a different team -- which
    # is why it is worth being able to catch before it is total.
    pool_warn_ratio: float | None = None
    pool_critical_ratio: float | None = None

    # How long the worker may go without taking work before it is stale.
    # Only ever a fault when there is a backlog to take: an idle worker on a
    # quiet queue is healthy forever, and that discrimination is the whole
    # point.  Maps onto whichever knob the check type calls it.
    stale_after_seconds: float | None = None

    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.type:
            raise ProbeConfigError("probe is missing a `type`")
        if not self.name:
            self.name = self.type
        for numeric in ("interval", "max_silence"):
            value = float(getattr(self, numeric))
            if value <= 0:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {numeric} must be greater than zero"
                )
            setattr(self, numeric, value)

        explicit_timeout = self.timeout is not None
        if not explicit_timeout:
            # Half the interval, capped at the 2s that suited the default
            # 15s cadence.  Always strictly inside the interval, so a probe
            # that only tunes `interval` can never configure an overlap.
            self.timeout = round(min(2.0, self.interval / 2.0), 3)
        self.timeout = float(self.timeout)
        if self.timeout <= 0:
            raise ProbeConfigError(
                f"probe {self.name!r}: timeout must be greater than zero"
            )
        for count in ("failure_threshold", "success_threshold"):
            value = int(getattr(self, count))
            if value < 1:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {count} must be at least 1"
                )
            setattr(self, count, value)
        for numeric in ("backoff_initial", "backoff_max", "backoff_jitter"):
            value = float(getattr(self, numeric))
            if value < 0:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {numeric} cannot be negative"
                )
            setattr(self, numeric, value)
        self.backoff_multiplier = float(self.backoff_multiplier)
        if self.backoff_multiplier < 1.0:
            # Below 1.0 the interval SHRINKS on every consecutive failure,
            # which is precisely the retry storm backoff exists to prevent.
            raise ProbeConfigError(
                f"probe {self.name!r}: backoff_multiplier must be at least 1.0, "
                f"or a failing dependency gets asked more often, not less"
            )
        if self.backoff_max < self.backoff_initial:
            raise ProbeConfigError(
                f"probe {self.name!r}: backoff_max ({self.backoff_max}) is below "
                f"backoff_initial ({self.backoff_initial})"
            )

        for optional in ("latency_warn_ms", "latency_critical_ms",
                         "stale_after_seconds"):
            value = getattr(self, optional)
            if value is None:
                continue
            value = float(value)
            if value <= 0:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {optional} must be greater than zero"
                )
            setattr(self, optional, value)

        for ratio in ("pool_warn_ratio", "pool_critical_ratio"):
            value = getattr(self, ratio)
            if value is None:
                continue
            value = float(value)
            if not 0.0 < value <= 1.0:
                raise ProbeConfigError(
                    f"probe {self.name!r}: {ratio} must be a fraction of pool "
                    f"capacity between 0 and 1, got {value}"
                )
            setattr(self, ratio, value)
        if (self.pool_warn_ratio is not None
                and self.pool_critical_ratio is not None
                and self.pool_critical_ratio < self.pool_warn_ratio):
            raise ProbeConfigError(
                f"probe {self.name!r}: pool_critical_ratio "
                f"({self.pool_critical_ratio}) is below pool_warn_ratio "
                f"({self.pool_warn_ratio}), so the warning could never fire"
            )
        if (self.latency_warn_ms is not None
                and self.latency_critical_ms is not None
                and self.latency_critical_ms < self.latency_warn_ms):
            raise ProbeConfigError(
                f"probe {self.name!r}: latency_critical_ms "
                f"({self.latency_critical_ms}) is below latency_warn_ms "
                f"({self.latency_warn_ms}), so the warning could never fire"
            )

        if explicit_timeout and self.timeout >= self.interval:
            # A check whose timeout is not shorter than its interval is
            # always still in flight when the next evaluation is due.  The
            # runner skips rather than piling up, so the practical effect is
            # a check that silently runs at half the configured rate -- or
            # less.  That is a misconfiguration worth stopping for, because
            # it is invisible in every output the worker produces.
            raise ProbeConfigError(
                f"probe {self.name!r}: timeout ({self.timeout}s) must be less "
                f"than interval ({self.interval}s), or evaluations overlap and "
                f"the check quietly runs slower than configured"
            )
        self.ttl = float(self.ttl) if self.ttl is not None else \
            round(self.interval * 2 + self.timeout, 3)

    # -- construction ---------------------------------------------------- #

    @classmethod
    def from_raw(cls, raw: Mapping[str, Any]) -> "ProbeSpec":
        if not isinstance(raw, Mapping):
            raise ProbeConfigError(f"probe definition must be a mapping, got {type(raw).__name__}")
        data = dict(raw)
        # Spellings that mean the same field.  Config written against one
        # naming convention should not silently become a param that no
        # builder reads -- which is how a threshold set in good faith ends
        # up doing nothing at all.
        for alias, target in _FIELD_ALIASES.items():
            if alias in data and target not in data:
                data[target] = data.pop(alias)
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
            "backoff_initial": self.backoff_initial,
            "backoff_max": self.backoff_max,
            "backoff_multiplier": self.backoff_multiplier,
            "backoff_jitter": self.backoff_jitter,
        }

    def redacted(self) -> dict[str, Any]:
        """A log-safe view.  Params can hold a DSN, so they go through redaction."""
        from ..security import redact_mapping

        body = {
            "type": self.type, "name": self.name, "critical": self.critical,
            "enabled": self.enabled, "interval": self.interval,
            "timeout": self.timeout,
            "backoff_initial": self.backoff_initial,
            "backoff_max": self.backoff_max,
            "backoff_multiplier": self.backoff_multiplier,
            "params": redact_mapping(self.params),
        }
        # Only present when set, so /config shows a latency bar exactly when
        # one is actually being enforced.
        if self.latency_warn_ms is not None:
            body["latency_warn_ms"] = self.latency_warn_ms
        if self.latency_critical_ms is not None:
            body["latency_critical_ms"] = self.latency_critical_ms
        for key in ("pool_warn_ratio", "pool_critical_ratio", "stale_after_seconds"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body
