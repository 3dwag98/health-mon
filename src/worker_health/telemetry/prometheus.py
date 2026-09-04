"""Prometheus exposition.

Every label value is drawn from a closed set -- check names are registered,
categories are an enum.  A free-text error string as a label is how a
Prometheus instance dies.
"""
from __future__ import annotations

from ..core.model import SEVERITY, ErrorCategory, Status

_STATUS_VALUE = {
    Status.OK: 0, Status.STARTING: 1, Status.DEGRADED: 2,
    Status.UNKNOWN: 3, Status.FAILING: 4,
}


def _esc(v: str) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def render(monitor) -> str:
    snap = monitor.snapshot()
    svc, inst = _esc(snap.service), _esc(snap.instance)
    base = f'service="{svc}",instance="{inst}"'
    out: list[str] = []
    add = out.append

    add("# HELP worker_health_status Aggregate readiness (0 ok .. 4 failing)")
    add("# TYPE worker_health_status gauge")
    add(f"worker_health_status{{{base}}} {_STATUS_VALUE[snap.status]}")

    add("# HELP worker_health_live Liveness (0 ok, 4 failing)")
    add("# TYPE worker_health_live gauge")
    add(f"worker_health_live{{{base}}} {_STATUS_VALUE[snap.live_status]}")

    add("# HELP worker_health_uptime_seconds Monitor uptime")
    add("# TYPE worker_health_uptime_seconds gauge")
    add(f"worker_health_uptime_seconds{{{base}}} {snap.uptime_s}")

    add("# HELP worker_health_check_status Per-check status")
    add("# TYPE worker_health_check_status gauge")
    add("# HELP worker_health_check_latency_ms Last check latency")
    add("# TYPE worker_health_check_latency_ms gauge")
    add("# HELP worker_health_check_evidence_age_ms Age of the signal behind the verdict")
    add("# TYPE worker_health_check_evidence_age_ms gauge")
    add("# HELP worker_health_check_transitions_total Status transitions")
    add("# TYPE worker_health_check_transitions_total counter")

    for name, r in snap.results.items():
        lbl = f'{base},check="{_esc(name)}",evidence="{r.evidence.value}"'
        add(f"worker_health_check_status{{{lbl}}} {_STATUS_VALUE[r.status]}")
        if r.latency_ms is not None:
            add(f"worker_health_check_latency_ms{{{lbl}}} {r.latency_ms}")
        if r.evidence_age_ms is not None:
            add(f"worker_health_check_evidence_age_ms{{{lbl}}} {r.evidence_age_ms}")
        add(f"worker_health_check_transitions_total{{{lbl}}} "
            f"{monitor.transitions(name)}")
        if r.category is not None:
            cat = f'{base},check="{_esc(name)}",category="{r.category.value}"'
            add(f"worker_health_check_error{{{cat}}} 1")

    t = snap.timing
    add("# HELP worker_health_timing_ms Timing relationships")
    add("# TYPE worker_health_timing_ms gauge")
    for key in ("loop_lag_ms", "worker_last_activity_age_ms", "health_eval_age_ms",
                "worker_to_health_delta_ms", "snapshot_build_ms"):
        if key in t:
            add(f'worker_health_timing_ms{{{base},kind="{key}"}} {t[key]}')

    add("# HELP worker_health_window_ms Rolling timing windows")
    add("# TYPE worker_health_window_ms gauge")
    for key, summary in monitor.timings.export().items():
        for stat in ("p50_ms", "p95_ms", "p99_ms", "max_ms"):
            add(f'worker_health_window_ms{{{base},metric="{_esc(key)}",'
                f'stat="{stat}"}} {summary[stat]}')
        add(f'worker_health_window_count{{{base},metric="{_esc(key)}"}} '
            f'{summary["count"]}')

    return "\n".join(out) + "\n"
