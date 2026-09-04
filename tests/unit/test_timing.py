"""Timing windows and the worker-to-health delta."""
from __future__ import annotations

import pytest

from worker_health.core.timing import Timings, Window

pytestmark = pytest.mark.unit


def test_window_percentiles():
    w = Window(maxlen=100)
    for v in range(1, 101):
        w.add(float(v))
    s = w.summary()
    assert s["count"] == 100
    assert s["p50_ms"] == pytest.approx(50, abs=2)
    assert s["p99_ms"] == pytest.approx(99, abs=2)
    assert s["max_ms"] == 100.0


def test_window_is_bounded():
    w = Window(maxlen=10)
    for v in range(100):
        w.add(float(v))
    assert w.summary()["count"] == 100       # total is cumulative
    assert w.summary()["max_ms"] == 99.0     # but only the tail is retained
    assert w.last == 99.0


def test_empty_window_does_not_divide_by_zero():
    assert Window().summary()["p99_ms"] == 0.0


def test_timings_are_isolated_per_key():
    t = Timings()
    t.observe("a", 1.0)
    t.observe("b", 100.0)
    assert t.summary("a")["max_ms"] == 1.0
    assert t.summary("b")["max_ms"] == 100.0
    assert t.summary("missing") is None


def test_export_covers_every_key():
    t = Timings()
    for k in ("x", "y", "z"):
        t.observe(k, 5.0)
    assert set(t.export()) == {"x", "y", "z"}
