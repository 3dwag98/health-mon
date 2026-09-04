"""Snapshot assembly: criticality, boot grace, and the live/ready split."""
from __future__ import annotations

from .clock import Clock
from .machine import StateMachine
from .model import SEVERITY, Status


def aggregate(
    machine: StateMachine,
    clock: Clock,
    boot_deadline: float | None,
) -> Status:
    """Readiness verdict across every registered check."""
    specs = machine.specs
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
        if v is Status.STARTING:
            v = Status.DEGRADED
        if v is Status.FAILING and not spec.critical:
            v = Status.DEGRADED   # a non-critical failure never fails the whole
        if v is Status.UNKNOWN:
            v = Status.DEGRADED   # no measurement is not an outage
        if SEVERITY[v] > SEVERITY[worst]:
            worst = v
    return worst


def boot_complete(machine: StateMachine) -> bool:
    """True once every critical check has been observed OK at least once."""
    for spec in machine.specs:
        if spec.critical and machine.state(spec.name).last_ok_at is None:
            return False
    return True
