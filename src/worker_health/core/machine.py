"""Per-check state machine: thresholds, breaker, backoff, jitter.

Pure and synchronous.  Both runners drive this same object, so scheduling
policy cannot fork between them -- the runners own only the mechanics of
"call this thing with a timeout".
"""
from __future__ import annotations

import random
from dataclasses import dataclass

from .clock import Clock
from .model import CheckResult, Status


@dataclass(slots=True)
class CheckSpec:
    name: str
    critical: bool = True
    interval: float = 10.0
    timeout: float = 3.0
    ttl: float = 30.0
    failure_threshold: int = 3
    success_threshold: int = 2
    breaker_threshold: int = 5
    max_backoff: float = 120.0
    jitter: float = 0.2
    # How long a check may rely on passively observed evidence before it
    # falls back to a synthetic probe.
    max_silence: float = 30.0


@dataclass(slots=True)
class CheckState:
    effective: Status = Status.UNKNOWN
    consecutive_fail: int = 0
    consecutive_ok: int = 0
    last_result: CheckResult | None = None
    last_ok_at: float | None = None
    backoff_step: int = 0
    transitions: int = 0
    next_due: float = 0.0
    # Monotonic timestamp of the transition into the current effective state.
    entered_at: float = 0.0


class StateMachine:
    def __init__(
        self,
        specs,
        clock: Clock,
        rng: random.Random | None = None,
    ) -> None:
        self._specs = {s.name: s for s in specs}
        self._states = {s.name: CheckState() for s in specs}
        self._clock = clock
        self._rng = rng or random.Random(0)

    # -- registration -------------------------------------------------- #

    def add(self, spec: CheckSpec) -> None:
        self._specs[spec.name] = spec
        self._states.setdefault(spec.name, CheckState())

    @property
    def specs(self):
        return tuple(self._specs.values())

    def spec(self, name: str) -> CheckSpec:
        return self._specs[name]

    # -- transitions ---------------------------------------------------- #

    def apply(self, name: str, result: CheckResult) -> None:
        spec, st = self._specs[name], self._states[name]
        st.last_result = result
        previous = st.effective

        if result.status is Status.FAILING:
            st.consecutive_fail += 1
            st.consecutive_ok = 0
            if st.consecutive_fail >= spec.failure_threshold:
                st.effective = Status.FAILING

        elif result.status is Status.OK:
            st.consecutive_ok += 1
            st.consecutive_fail = 0
            st.backoff_step = 0
            st.last_ok_at = result.checked_at
            # Recovery out of a BAD state is confirmed, not assumed: the
            # spec this replaces let DEGRADED -> OK happen on a single
            # sample, which left degraded checks with no hysteresis at all.
            #
            # UNKNOWN is not a bad state, it is the absence of one, so the
            # first ever measurement promotes immediately -- there is nothing
            # to confirm recovery from, and requiring confirmation here would
            # double every worker's time to ready.
            recovering_from_bad = previous in (Status.FAILING, Status.DEGRADED)
            if not recovering_from_bad or st.consecutive_ok >= spec.success_threshold:
                st.effective = Status.OK

        elif result.status is Status.DEGRADED:
            # Already thresholded upstream from a measured value.
            # Thresholding a threshold only adds lag.
            st.consecutive_fail = 0
            st.consecutive_ok = 0
            st.effective = Status.DEGRADED

        else:
            st.consecutive_ok = 0
            st.effective = Status.UNKNOWN

        if st.effective is not previous:
            st.transitions += 1
            st.entered_at = result.checked_at

        # The breaker changes probe FREQUENCY, never reported status.  One
        # that reported UNKNOWN while open would hide the outage it exists
        # to survive.
        if st.consecutive_fail >= spec.breaker_threshold:
            st.backoff_step = min(st.backoff_step + 1, 12)

        st.next_due = self._schedule(spec, st)

    def _schedule(self, spec: CheckSpec, st: CheckState) -> float:
        base = spec.interval
        if st.backoff_step:
            base = min(spec.interval * (2 ** st.backoff_step), spec.max_backoff)
        # Not an optimisation.  Forty workers started by one command have
        # their ticks aligned to within milliseconds; un-jittered, every
        # dependency sees a synchronised burst forever.
        factor = 1.0 + self._rng.uniform(-spec.jitter, spec.jitter)
        return self._clock.monotonic() + base * factor

    def mark_unknown(self, name: str) -> None:
        spec, st = self._specs[name], self._states[name]
        if st.effective is not Status.UNKNOWN:
            st.transitions += 1
            st.entered_at = self._clock.monotonic()
        st.effective = Status.UNKNOWN
        st.consecutive_ok = 0
        # Reschedule, or a check marked unknown can stop being scheduled.
        st.next_due = self._schedule(spec, st)

    # -- reads ---------------------------------------------------------- #

    def effective(self, name: str) -> Status:
        """TTL is applied at READ time.

        This is what makes a wedged scheduler visible: nothing is writing
        results, so every check ages into UNKNOWN on its own.
        """
        spec, st = self._specs[name], self._states[name]
        if st.last_result is None:
            return Status.UNKNOWN
        if self._clock.monotonic() - st.last_result.checked_at > spec.ttl:
            return Status.UNKNOWN
        return st.effective

    def due(self, name: str) -> bool:
        return self._clock.monotonic() >= self._states[name].next_due

    def state(self, name: str) -> CheckState:
        return self._states[name]

    def transitions(self, name: str) -> int:
        return self._states[name].transitions
