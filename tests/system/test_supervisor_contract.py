"""L5: the contract a process supervisor is wired against.

PM2 itself is not exercised here, and deliberately so — nothing in this
package imports, shells out to, or depends on it, and a test that installed
Node to prove a URL returns 503 would be testing PM2, not this. What matters
is the contract a supervisor config can only *encode*:

    /live  503  -> this process is wedged. Restart it.
    /ready 503  -> a dependency is down. Do NOT restart it.

Getting that backwards is the failure this package exists to prevent: it
converts one database outage into a fleet of crash-looping workers hammering
a database that is already in trouble. So the tests below drive a real worker
over real HTTP, and the last one asserts the shipped PM2 example still points
at the endpoint the others just proved out.
"""
from __future__ import annotations

import json
import pathlib
import re
import time
import urllib.error
import urllib.request

import pytest

from worker_health import ErrorCategory, Status, setup_worker_health
from worker_health.core.model import CheckResult, Evidence

pytestmark = pytest.mark.system


def get(url, timeout=5.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def wait_for(predicate, timeout=15.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    raise AssertionError(f"{what} not met within {timeout}s")


class Worker:
    """A real worker: health server on a real port, one controllable check."""

    def __init__(self, **overrides):
        self.verdict = {"value": Status.OK}
        self.health = setup_worker_health(
            service="supervised",
            config={"worker_health": {
                "health_host": "127.0.0.1", "health_port": 0, "boot_grace": 0,
                "log_level": "WARNING", "processing_check": False,
                "loop_lag_threshold_ms": 300.0,
                **overrides,
            }},
            probes=[{"type": "function", "name": "dependency", "critical": True,
                     "interval": 0.1, "timeout": 0.05, "failure_threshold": 1,
                     "success_threshold": 1, "ttl": 1e9,
                     "params": {"fn": self._check}}],
        )
        self.base = f"http://127.0.0.1:{self.health.server.port}"

    def _check(self):
        value = self.verdict["value"]
        if isinstance(value, ErrorCategory):
            # A full CheckResult, so the test can choose the CATEGORY --
            # which is the only thing that decides restart-or-not.
            return CheckResult(
                name="dependency", status=Status.FAILING,
                checked_at=self.health.monitor.clock.monotonic(), wall_clock=0.0,
                evidence=Evidence.OBSERVED, category=value,
                observed={"queue_depth": 1000, "idle_seconds": 90},
            )
        return value

    def live(self):
        return get(self.base + "/live")

    def ready(self):
        return get(self.base + "/ready")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.health.stop(timeout=2)


# -- the two signals ---------------------------------------------------------- #

def test_a_healthy_worker_answers_both_endpoints_200():
    with Worker() as w:
        wait_for(lambda: w.ready()[0] == 200, what="worker to become ready")
        assert w.live()[0] == 200


def test_a_dependency_outage_moves_ready_and_leaves_live_alone():
    """The signal PM2 must ignore. Restarting this process does not bring the
    database back."""
    with Worker() as w:
        wait_for(lambda: w.ready()[0] == 200, what="worker to become ready")

        w.verdict["value"] = ErrorCategory.CONNECTION_REFUSED
        wait_for(lambda: w.ready()[0] == 503, what="readiness to follow the dependency")

        code, body = w.live()
        assert code == 200, "a dependency outage must never restart the worker"
        assert body["status"] == "alive"

        # And it recovers on its own once the dependency comes back.
        w.verdict["value"] = Status.OK
        wait_for(lambda: w.ready()[0] == 200, what="readiness to recover")
        assert w.live()[0] == 200


def test_a_backlog_nobody_is_consuming_moves_live():
    """The signal PM2 restarts on. The process is running, the loop may even
    be turning, and it will sit on that backlog forever."""
    with Worker() as w:
        wait_for(lambda: w.ready()[0] == 200, what="worker to become ready")

        w.verdict["value"] = ErrorCategory.NOT_CONSUMING
        wait_for(lambda: w.live()[0] == 503, what="liveness to report the wedge")

        code, body = w.live()
        assert body["status"] == "unalive"
        # A supervisor that just killed a process deserves to know why
        # without making a second request.
        assert any("not_consuming" in r for r in body["reasons"])

        w.verdict["value"] = Status.OK
        wait_for(lambda: w.live()[0] == 200, what="liveness to recover")


def test_a_wedged_loop_moves_live():
    """The original liveness signal: nothing is driving the health loop, so
    nothing in this process is turning over either."""
    with Worker() as w:
        wait_for(lambda: w.ready()[0] == 200, what="worker to become ready")

        # Stop the scheduler while leaving the HTTP thread serving -- which
        # is the entire reason the transport runs on its own thread.
        w.health.monitor.stop(timeout=2)

        wait_for(lambda: w.live()[0] == 503, what="liveness to notice the loop stopped")
        assert any("behind its cadence" in r for r in w.live()[1]["reasons"])


def test_liveness_answers_while_the_scheduler_is_stopped():
    """If /live only answered when the worker was healthy it would be
    useless: the case it exists for is exactly the case it must survive."""
    with Worker() as w:
        w.health.monitor.stop(timeout=2)
        code, body = w.live()
        assert code in (200, 503)
        assert body["service"] == "supervised"


# -- the documented config must match the tested behaviour -------------------- #

def test_the_shipped_pm2_example_points_at_live_and_not_ready():
    """Stops the reference config drifting from the contract above. A PM2
    file that health-checks /ready is the crash-loop this package is about,
    and it would be a documentation bug with production consequences."""
    config = (pathlib.Path(__file__).resolve().parents[2]
              / "docs" / "ecosystem.config.js").read_text(encoding="utf-8")

    # Drop whole-line comments only: the file explains /ready at length and
    # must, but a blanket `//.*` would also eat the `//` in the URL itself.
    code = re.sub(r"^[ 	]*//.*$", "", config, flags=re.MULTILINE)
    urls = re.findall(r"health_check_url:\s*\"([^\"]+)\"", code)
    assert urls, "the PM2 example no longer declares a health_check_url"
    for url in urls:
        assert url.endswith("/live"), url
