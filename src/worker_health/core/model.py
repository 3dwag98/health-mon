"""Status vocabulary, error taxonomy, and the immutable result records.

Nothing in this module performs I/O or imports a driver.  It is the only
vocabulary the rest of the package is allowed to speak in.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class Status(Enum):
    """Internal status.

    Deliberately not an ``IntEnum``.  Ordering these values by their
    definition order would imply ``UNKNOWN`` is worse than ``FAILING``, which
    is backwards -- see ``SEVERITY``.
    """

    OK = "ok"
    DEGRADED = "degraded"
    FAILING = "failing"
    UNKNOWN = "unknown"
    STARTING = "starting"


class Evidence(str, Enum):
    """How a verdict was reached.

    A green light backed by a synthetic probe is a materially weaker claim
    than one backed by three hundred successful messages in the last minute.
    Every result carries this so the two never look identical.
    """

    OBSERVED = "observed"          # from the worker's own real traffic
    INTROSPECTED = "introspected"  # local connection/pool state, zero I/O
    PROBED = "probed"              # synthetic request, only when silent
    NONE = "none"                  # no evidence at all yet


# Explicit because it is not obvious.  UNKNOWN means "no measurement", which
# is less severe than a confirmed failure: treating it as worse produces a
# false outage on every deploy and every slow check.
SEVERITY: Mapping[Status, int] = {
    Status.OK: 0,
    Status.STARTING: 1,
    Status.DEGRADED: 2,
    Status.UNKNOWN: 2,
    Status.FAILING: 3,
}

# Wire vocabulary, following the shape of the (expired) IETF health-check
# draft because it is what the ecosystem recognises.  A convention, not a
# standard we claim conformance to.
WIRE: Mapping[Status, str] = {
    Status.OK: "pass",
    Status.STARTING: "warn",
    Status.DEGRADED: "warn",
    Status.UNKNOWN: "warn",
    Status.FAILING: "fail",
}

# Liveness and readiness need SEPARATE maps.  One shared map that returns 503
# for STARTING kills every worker mid-boot; one that returns 503 for FAILING
# restarts the whole fleet when a shared database goes down.  Liveness answers
# exactly one question: is this process's loop responsive.
LIVE_CODE: Mapping[Status, int] = {
    Status.OK: 200,
    Status.STARTING: 200,
    Status.DEGRADED: 200,
    Status.UNKNOWN: 200,
    Status.FAILING: 200,
}

READY_CODE: Mapping[Status, int] = {
    Status.OK: 200,
    Status.DEGRADED: 200,
    Status.UNKNOWN: 200,
    Status.STARTING: 503,
    Status.FAILING: 503,
}


class ErrorCategory(str, Enum):
    """Closed enum.

    Safe as a metric label precisely because it is closed, and the only
    failure detail permitted to leave the process -- driver exception text
    embeds DSNs, so it never reaches a log line or a response body.
    """

    # transport
    TIMEOUT = "timeout"
    CONNECTION_REFUSED = "connection_refused"
    CONNECTION_LOST = "connection_lost"
    PROTOCOL_ERROR = "protocol_error"
    # authorization
    AUTH_FAILED = "auth_failed"
    # resource state
    RESOURCE_MISSING = "resource_missing"
    RESOURCE_LOCKED = "resource_locked"
    CONFIG_DRIFT = "config_drift"
    DEPENDENCY_VERSION = "dependency_version"
    # database
    POOL_EXHAUSTED = "pool_exhausted"
    READ_ONLY = "read_only"
    # broker
    HEARTBEAT_TIMEOUT = "heartbeat_timeout"
    BROKER_ALARM = "broker_alarm"
    BROKER_SHUTDOWN = "broker_shutdown"
    NO_CONSUMERS = "no_consumers"
    NOT_SUBSCRIBED = "not_subscribed"
    NOT_CONSUMING = "not_consuming"
    CREDIT_EXHAUSTED = "credit_exhausted"
    BACKLOG = "backlog"
    # cache
    MEMORY_PRESSURE = "memory_pressure"
    LOADING = "loading"
    ROLE_CHANGED = "role_changed"
    # processing
    STALLED = "stalled"
    POISON_LOOP = "poison_loop"
    # internal
    STALE = "stale"
    INTERNAL = "internal"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One observation of one dependency.

    ``checked_at`` is monotonic and every piece of arithmetic uses it.
    ``wall_clock`` is epoch and exists only so a human can read the JSON.
    A single NTP correction would otherwise make every check look stale at
    once.
    """

    name: str
    status: Status
    checked_at: float
    wall_clock: float
    evidence: Evidence = Evidence.NONE
    latency_ms: float | None = None
    category: ErrorCategory | None = None
    # Age of the underlying signal at the moment the verdict was formed.
    # For OBSERVED results this is how long ago the worker last did the
    # thing we are inferring health from.
    evidence_age_ms: float | None = None
    detail: str | None = None
    # Primitives only.  No exception objects, no connection references --
    # that is how a __repr__ containing a DSN reaches a log line.
    observed: Mapping[str, int | float | str | bool] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Snapshot:
    status: Status
    live_status: Status
    results: Mapping[str, CheckResult]
    built_at: float
    wall_clock: float
    service: str
    instance: str
    version: str
    uptime_s: float
    timing: Mapping[str, float | int] = field(default_factory=dict)

    def check(self, name: str) -> CheckResult:
        return self.results[name]

    @property
    def ready_code(self) -> int:
        return READY_CODE[self.status]

    @property
    def live_code(self) -> int:
        return LIVE_CODE[self.live_status]
