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
    # Registered but switched off by configuration.  Kept in the snapshot so
    # an operator can see the check exists and is not merely missing, and
    # excluded from every aggregate.
    DISABLED = "disabled"


class Readiness(str, Enum):
    """The answer /ready gives, in the vocabulary operators use.

    Separate from ``Status`` on purpose.  ``Status`` describes ONE
    dependency; this describes the WORKER, and the two are not the same
    sentence -- a failing non-critical dependency is ``failing`` as a status
    and ``degraded`` as a readiness.
    """

    READY = "ready"
    DEGRADED = "degraded"
    UNREADY = "unready"
    STARTING = "starting"


class Liveness(str, Enum):
    """The answer /live gives.  Two values, because it asks one question."""

    ALIVE = "alive"
    UNALIVE = "unalive"


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
    Status.DISABLED: 0,
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
    Status.DISABLED: "pass",
    Status.STARTING: "warn",
    Status.DEGRADED: "warn",
    Status.UNKNOWN: "warn",
    Status.FAILING: "fail",
}

# Liveness and readiness need SEPARATE maps.  One shared map that returns 503
# for STARTING kills every worker mid-boot; one that returns 503 for FAILING
# restarts the whole fleet when a shared database goes down.  Liveness answers
# exactly one question: is this process's loop responsive.
#
# LIVE_CODE is the projection of a DEPENDENCY status onto /live, and every
# entry is 200 by design: no dependency verdict, of any severity, may ever
# make a process look dead.  The liveness verdict itself is projected through
# LIVENESS_CODE below, where a wedged loop does return 503.
LIVE_CODE: Mapping[Status, int] = {
    Status.OK: 200,
    Status.DISABLED: 200,
    Status.STARTING: 200,
    Status.DEGRADED: 200,
    Status.UNKNOWN: 200,
    Status.FAILING: 200,
}

READY_CODE: Mapping[Status, int] = {
    Status.OK: 200,
    Status.DISABLED: 200,
    Status.DEGRADED: 200,
    Status.UNKNOWN: 200,
    Status.STARTING: 503,
    Status.FAILING: 503,
}


# The projection from the aggregate dependency status onto the worker-level
# vocabulary.  One place, so /ready, /health, the metrics and the dashboard
# cannot disagree about what "degraded" means.
READINESS_FROM_STATUS: Mapping[Status, "Readiness"] = {
    Status.OK: Readiness.READY,
    Status.DISABLED: Readiness.READY,
    Status.STARTING: Readiness.STARTING,
    Status.DEGRADED: Readiness.DEGRADED,
    Status.UNKNOWN: Readiness.DEGRADED,
    Status.FAILING: Readiness.UNREADY,
}

READINESS_CODE: Mapping["Readiness", int] = {
    Readiness.READY: 200,
    # Degraded is still serving.  Returning 503 here would pull a worker out
    # of rotation for a non-critical cache, which is how a cache outage
    # becomes a total outage.
    Readiness.DEGRADED: 200,
    Readiness.STARTING: 503,
    Readiness.UNREADY: 503,
}

LIVENESS_CODE: Mapping["Liveness", int] = {
    Liveness.ALIVE: 200,
    Liveness.UNALIVE: 503,
}


class ErrorCategory(str, Enum):
    """Closed enum.

    Safe as a metric label precisely because it is closed, and the only
    failure detail permitted to leave the process -- driver exception text
    embeds DSNs, so it never reaches a log line or a response body.
    """

    # transport
    TIMEOUT = "timeout"
    # Answered, but too slowly to be useful.  Distinct from TIMEOUT on
    # purpose: a dependency that replies in 900ms is up, and the operator
    # response ("why is it slow") is nothing like the response to a
    # dependency that never replied at all.
    SLOW = "slow"
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


# The categories that mean THIS PROCESS is wedged: it is running, its loop
# may even be turning, and it will sit here forever without intervention.
# A restart is the actual remedy, so these -- and only these -- are allowed
# to make /live fail.
#
# Deliberately NARROWER than policy.restart.SELF_FAULTS, which also lists
# CONNECTION_LOST.  A lost connection is usually the dependency restarting,
# the worker reconnects on its own, and killing it changes nothing except
# the amount of in-flight work destroyed.  A supervisor watching /live must
# never be handed that.
WEDGED_CATEGORIES = frozenset({
    ErrorCategory.STALLED,          # receiving work, completing none
    ErrorCategory.NOT_CONSUMING,    # backlog present, taking nothing from it
    ErrorCategory.NOT_SUBSCRIBED,   # connection open, subscription gone
    ErrorCategory.CREDIT_EXHAUSTED, # every prefetch slot held by an unacked message
    ErrorCategory.POISON_LOOP,      # same message failing over and over
})


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
    # Why readiness is what it is, in the operator's words.  Populated by
    # aggregate.readiness(); empty when the worker is plainly ready.
    reasons: tuple[str, ...] = ()
    processing: Mapping[str, Mapping[str, float | int | str | None]] = field(
        default_factory=dict
    )
    # Set once the process has been asked to stop.  A draining worker is
    # still ALIVE -- killing it mid-message is the thing draining exists to
    # avoid -- but it must stop being handed new work immediately.
    draining: bool = False

    def check(self, name: str) -> CheckResult:
        return self.results[name]

    @property
    def liveness(self) -> Liveness:
        return Liveness.ALIVE if self.live_status is Status.OK else Liveness.UNALIVE

    @property
    def readiness(self) -> Readiness:
        # Draining outranks the dependency verdict: the worker may be
        # perfectly healthy and is still about to exit.
        if self.draining:
            return Readiness.UNREADY
        # A process whose loop is not turning cannot process work, whatever
        # its dependencies say.
        if self.liveness is Liveness.UNALIVE:
            return Readiness.UNREADY
        return READINESS_FROM_STATUS[self.status]

    @property
    def ready_code(self) -> int:
        return READINESS_CODE[self.readiness]

    @property
    def live_code(self) -> int:
        return LIVENESS_CODE[self.liveness]
