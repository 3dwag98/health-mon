"""Snapshot assembly: criticality, boot grace, and the live/ready split."""
from __future__ import annotations

from .clock import Clock
from .machine import StateMachine
from .model import SEVERITY, Liveness, Readiness, Status


def aggregate(
    machine: StateMachine,
    clock: Clock,
    boot_deadline: float | None,
) -> Status:
    """Readiness verdict across every registered check."""
    specs = machine.enabled_specs()
    if not specs:
        return Status.OK

    effective = {s.name: machine.effective(s.name) for s in specs}

    # The boot grace ends on the first successful evaluation of all critical
    # checks OR on a deadline, whichever comes first -- not on a fixed sleep,
    # which is either too short on a slow morning or too long always.
    if boot_deadline is not None and clock.monotonic() < boot_deadline:
        criticals = [effective[s.name] for s in specs if s.critical]
        if criticals and not all(v is Status.OK for v in criticals):
            return Status.STARTING

    worst = Status.OK
    for spec in specs:
        v = effective[spec.name]
        if v is Status.DISABLED:
            continue            # switched off; it has no opinion
        if v is Status.STARTING:
            v = Status.DEGRADED
        if v is Status.FAILING and not spec.critical:
            v = Status.DEGRADED   # a non-critical failure never fails the whole
        if v is Status.UNKNOWN:
            # No measurement is not an outage -- but a CRITICAL dependency
            # that has never answered is not something to route work to
            # either.  Cold start, a probe that has timed out on every
            # attempt, a check whose last result aged past its TTL: in all
            # three the honest answer is "not ready", not "probably fine".
            # Boot grace already covers the legitimate startup window above.
            v = Status.FAILING if spec.critical else Status.DEGRADED
        if SEVERITY[v] > SEVERITY[worst]:
            worst = v
    return worst


def boot_complete(machine: StateMachine) -> bool:
    """True once every critical check has been observed OK at least once."""
    for spec in machine.enabled_specs():
        if spec.critical and machine.state(spec.name).last_ok_at is None:
            return False
    return True


def readiness(status: Status, liveness: Liveness) -> Readiness:
    """Project the aggregate onto the worker-level vocabulary.

    A wedged loop outranks everything: a process that cannot turn its loop
    cannot process work, however healthy its dependencies look.
    """
    from .model import READINESS_FROM_STATUS

    if liveness is Liveness.UNALIVE:
        return Readiness.UNREADY
    return READINESS_FROM_STATUS[status]


def reasons(machine: StateMachine, liveness: Liveness) -> tuple[str, ...]:
    """Why readiness is what it is, in the operator's words.

    Named checks and closed categories only -- never a driver message, which
    is where DSNs live.  This is what turns a 503 into an actionable page
    instead of a puzzle.
    """
    out: list[str] = []
    if liveness is Liveness.UNALIVE:
        out.append("event loop lag above threshold")

    for spec in machine.enabled_specs():
        state = machine.effective(spec.name)
        if state is Status.FAILING:
            category = _category(machine, spec.name)
            kind = "critical" if spec.critical else "non-critical"
            out.append(f"{kind} check {spec.name} is failing ({category})")
        elif state is Status.DEGRADED:
            out.append(f"check {spec.name} is degraded ({_category(machine, spec.name)})")
        elif state is Status.UNKNOWN:
            kind = "critical" if spec.critical else "non-critical"
            if machine.state(spec.name).last_result is None:
                out.append(f"{kind} check {spec.name} has not completed its "
                           "first evaluation")
            else:
                out.append(f"{kind} check {spec.name} has no current "
                           "measurement (result older than its ttl)")
    return tuple(out)


def _category(machine: StateMachine, name: str) -> str:
    result = machine.state(name).last_result
    if result is None or result.category is None:
        return "unknown"
    return result.category.value
