"""Prometheus exposition.

Every label value is drawn from a closed set -- check names are registered,
categories are an enum, evidence and status are enums.  A free-text error
string as a label is how a Prometheus instance dies, so nothing here can
produce one.

Two families exist for each verdict on purpose:

* a **binary** series (``worker_health_ready``, ``worker_health_check_status``)
  where 1 is good and 0 is not.  Alerts are written against these, because
  ``== 0`` is unambiguous and survives someone adding a status value later.
* a **severity** series (``worker_health_status``,
  ``worker_health_check_severity``) carrying the full 0-4 ordinal, for
  dashboards that want to colour degraded differently from failing.

Getting one series to do both jobs is what makes an alert say "less than 2"
and nobody remember why.
"""
from __future__ import annotations

from ..core.model import Readiness, Status

SEVERITY_VALUE = {
    Status.OK: 0, Status.DISABLED: 0, Status.STARTING: 1,
    Status.DEGRADED: 2, Status.UNKNOWN: 3, Status.FAILING: 4,
}

READINESS_VALUE = {
    Readiness.READY: 0, Readiness.STARTING: 1,
    Readiness.DEGRADED: 2, Readiness.UNREADY: 3,
}

# Statuses that count as "up" in the binary series.  DISABLED is up because
# a check switched off deliberately must not page anyone.
_UP = (Status.OK, Status.DISABLED)


def _esc(v) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def render(monitor) -> str:
    snap = monitor.snapshot()
    base = f'service="{_esc(snap.service)}",instance="{_esc(snap.instance)}"'
    out: list[str] = []
    add = out.append

    def metric(name: str, help_text: str, kind: str = "gauge") -> None:
        add(f"# HELP {name} {help_text}")
        add(f"# TYPE {name} {kind}")

    # -- worker verdicts -------------------------------------------------- #

    metric("worker_health_ready", "1 when /ready would return 200")
    add(f"worker_health_ready{{{base}}} "
        f"{1 if snap.readiness in (Readiness.READY, Readiness.DEGRADED) else 0}")

    metric("worker_health_live", "1 when /live would return 200")
    add(f"worker_health_live{{{base}}} {1 if snap.live_code == 200 else 0}")

    metric("worker_health_status", "Aggregate readiness severity (0 ok .. 4 failing)")
    add(f"worker_health_status{{{base}}} {SEVERITY_VALUE[snap.status]}")

    metric("worker_health_readiness_state",
           "One-hot readiness state (ready, degraded, unready, starting)")
    for state in Readiness:
        add(f'worker_health_readiness_state{{{base},state="{state.value}"}} '
            f'{1 if snap.readiness is state else 0}')

    metric("worker_health_uptime_seconds", "Monitor uptime")
    add(f"worker_health_uptime_seconds{{{base}}} {snap.uptime_s}")

    metric("worker_health_boot_complete",
           "1 once every critical check has been observed healthy at least once")
    add(f"worker_health_boot_complete{{{base}}} "
        f"{0 if snap.readiness is Readiness.STARTING else 1}")

    # -- per-check -------------------------------------------------------- #

    metric("worker_health_check_status", "1 when the check is healthy, 0 otherwise")
    metric("worker_health_check_severity", "Per-check severity (0 ok .. 4 failing)")
    metric("worker_health_check_latency_ms", "Latency of the last evaluation")
    metric("worker_health_check_evidence_age_ms",
           "Age of the signal behind the verdict at the moment it was formed")
    metric("worker_health_check_transitions_total", "Status transitions", "counter")
    metric("worker_health_check_interval_seconds",
           "Interval until this check runs again, including failure backoff")
    metric("worker_health_check_error",
           "1 for the error category currently reported by a check")

    for name, r in snap.results.items():
        spec = monitor.machine.spec(name)
        labels = (f'{base},check="{_esc(name)}",'
                  f'critical="{str(spec.critical).lower()}",'
                  f'evidence="{r.evidence.value}"')
        add(f"worker_health_check_status{{{labels}}} {1 if r.status in _UP else 0}")
        add(f"worker_health_check_severity{{{labels}}} {SEVERITY_VALUE[r.status]}")
        if r.latency_ms is not None:
            add(f"worker_health_check_latency_ms{{{labels}}} {round(r.latency_ms, 3)}")
        if r.evidence_age_ms is not None:
            add(f"worker_health_check_evidence_age_ms{{{labels}}} "
                f"{round(r.evidence_age_ms, 3)}")
        add(f"worker_health_check_transitions_total{{{labels}}} "
            f"{monitor.transitions(name)}")
        add(f'worker_health_check_interval_seconds{{{base},check="{_esc(name)}"}} '
            f"{round(monitor.machine.next_interval(name), 3)}")
        if r.category is not None:
            add(f'worker_health_check_error{{{base},check="{_esc(name)}",'
                f'category="{r.category.value}"}} 1')

    # -- processing ------------------------------------------------------- #

    metric("worker_health_message_received_total", "Messages received", "counter")
    metric("worker_health_message_success_total", "Messages processed successfully", "counter")
    metric("worker_health_message_failure_total", "Messages that raised", "counter")
    metric("worker_health_messages_in_flight", "Messages currently being handled")
    metric("worker_health_queue_lag", "Messages waiting in the queue")
    metric("worker_health_last_message_age_seconds", "Age of the last received message")
    metric("worker_health_last_success_age_seconds", "Age of the last successful handle")

    for queue, data in snap.processing.items():
        q = f'{base},queue="{_esc(queue)}"'
        add(f"worker_health_message_received_total{{{q}}} {data.get('received', 0)}")
        add(f"worker_health_message_success_total{{{q}}} {data.get('succeeded', 0)}")
        add(f"worker_health_message_failure_total{{{q}}} {data.get('failed', 0)}")
        add(f"worker_health_messages_in_flight{{{q}}} {data.get('in_flight', 0)}")
        if data.get("queue_lag") is not None:
            add(f"worker_health_queue_lag{{{q}}} {data['queue_lag']}")
        for key, name in (("last_message_age_s", "worker_health_last_message_age_seconds"),
                          ("last_success_age_s", "worker_health_last_success_age_seconds")):
            if data.get(key) is not None:
                add(f"{name}{{{q}}} {data[key]}")

    # -- handler latency, per queue --------------------------------------- #

    windows = monitor.timings.export()
    metric("worker_health_handler_duration_ms", "Handler latency percentiles")
    for key, summary in windows.items():
        if not key.startswith("worker.") or not key.endswith(".handler_ms"):
            continue
        queue = key[len("worker."):-len(".handler_ms")]
        for stat in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            add(f'worker_health_handler_duration_ms{{{base},queue="{_esc(queue)}",'
                f'quantile="{stat[:-3]}"}} {summary[stat]}')

    # -- worker internals -------------------------------------------------- #

    t = snap.timing
    for key, name, help_text in (
        ("loop_lag_ms", "worker_health_loop_lag_ms",
         "How far the health loop slipped from its own cadence"),
        ("runner_tick_delay_ms", "worker_health_runner_tick_delay_ms",
         "Lateness of the last runner tick"),
        ("snapshot_build_ms", "worker_health_snapshot_build_ms",
         "Time to assemble a snapshot"),
        ("worker_to_health_delta_ms", "worker_health_worker_to_health_delta_ms",
         "Age of the worker's own signal when health last looked at it"),
        ("worker_last_activity_age_ms", "worker_health_last_activity_age_ms",
         "Time since the worker last did real work"),
        ("health_eval_age_ms", "worker_health_eval_age_ms",
         "Age of the newest check evaluation"),
        ("health_oldest_eval_age_ms", "worker_health_oldest_eval_age_ms",
         "Age of the oldest check evaluation"),
    ):
        if key in t:
            metric(name, help_text)
            add(f"{name}{{{base}}} {t[key]}")

    # -- rolling windows ---------------------------------------------------- #

    metric("worker_health_window_ms", "Rolling timing windows")
    metric("worker_health_window_count", "Samples in each rolling window", "counter")
    for key, summary in windows.items():
        for stat in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            add(f'worker_health_window_ms{{{base},metric="{_esc(key)}",'
                f'stat="{stat}"}} {summary[stat]}')
        add(f'worker_health_window_count{{{base},metric="{_esc(key)}"}} '
            f'{summary["count"]}')

    return "\n".join(out) + "\n"
