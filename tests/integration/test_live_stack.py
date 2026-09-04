"""L1-L4: runs inside the tests container, against the live compose stack.

    docker compose --profile test run --rm tests
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.integration

WORKERS = {
    "billing": os.getenv("BILLING_URL", "http://billing:8080"),
    "notify": os.getenv("NOTIFY_URL", "http://notify:8080"),
    "reconcile": os.getenv("RECONCILE_URL", "http://reconcile:8080"),
}
TOXI = os.getenv("TOXIPROXY_URL", "http://toxiproxy:8474")

# Distinctive per-dependency passwords. Any of these appearing in a body, a
# log line or a metric is a credential leak.
CANARIES = ("canary-pg-8f3ad91c", "canary-mq-4b7ce02d", "canary-rd-1a9fe63b")


def get(url, timeout=5.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def health(name):
    code, body = get(WORKERS[name] + "/health")
    return code, json.loads(body)


def toxi(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(TOXI + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def wait_for(predicate, timeout=60.0, interval=1.0, what="condition"):
    """Poll to a deadline. Never a bare sleep in an assertion -- a test that
    passes because of a well-chosen sleep fails on a loaded runner."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
            last = value
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"{what} not met within {timeout}s (last={last!r})")


@pytest.fixture(scope="module", autouse=True)
def _ready():
    wait_for(lambda: all(health(n)[1].get("status") in ("ok", "degraded")
                         for n in WORKERS),
             timeout=120, what="every worker reachable")
    yield
    toxi("POST", "/reset")


@pytest.fixture(autouse=True)
def _clean_network():
    toxi("POST", "/reset")
    yield
    toxi("POST", "/reset")


# -- contract ---------------------------------------------------------------- #

@pytest.mark.parametrize("name", sorted(WORKERS))
def test_worker_reports_all_four_checks(name):
    code, body = health(name)
    assert code == 200
    assert set(body["checks"]) >= {"postgres", "redis", "rabbitmq", "processing"}
    assert body["status"] in {"ok", "degraded", "failing", "starting", "unknown"}


@pytest.mark.parametrize("name", sorted(WORKERS))
def test_every_check_declares_its_evidence(name):
    """A probe-backed green must never look like a traffic-backed green."""
    _, body = health(name)
    for cname, c in body["checks"].items():
        assert c["evidence"] in {"observed", "introspected", "probed", "none"}, cname


@pytest.mark.parametrize("name", sorted(WORKERS))
def test_liveness_is_independent_of_dependencies(name):
    code, body = get(WORKERS[name] + "/live")
    assert code == 200
    assert json.loads(body)["status"] == "ok"


def test_dependency_health_comes_from_real_traffic_under_load():
    """With messages flowing, postgres and redis need no synthetic probe."""
    def observed():
        _, b = health("billing")
        return all(b["checks"][c]["evidence"] == "observed"
                   for c in ("postgres", "redis", "processing"))
    wait_for(observed, timeout=60, what="checks standing on real traffic")


def test_broker_check_is_introspective():
    """Gathered on the worker's own connection, by its own thread."""
    _, b = health("billing")
    assert b["checks"]["rabbitmq"]["evidence"] == "introspected"


# -- timing ------------------------------------------------------------------ #

@pytest.mark.parametrize("name", sorted(WORKERS))
def test_timing_block_is_populated(name):
    _, body = health(name)
    t = body["timing"]
    assert t["runner"] in ("thread", "asyncio")
    assert t["snapshot_build_ms"] < 50.0
    assert "loop_lag_ms" in t


def test_snapshot_read_path_does_no_io():
    """/ready must serve a cached snapshot. If it probed inline it would be
    slow exactly when everything else is already on fire."""
    samples = []
    for _ in range(40):
        t0 = time.perf_counter()
        get(WORKERS["billing"] + "/ready")
        samples.append((time.perf_counter() - t0) * 1000)
    samples.sort()
    p95 = samples[int(len(samples) * 0.95) - 1]
    assert p95 < 250.0, f"/ready p95 {p95:.1f}ms"


def test_worker_to_health_delta_is_reported_under_load():
    def has_delta():
        _, b = health("billing")
        return b["timing"].get("worker_to_health_delta_ms")
    delta = wait_for(has_delta, timeout=60, what="delta metric present")
    assert delta >= 0


# -- guardrails -------------------------------------------------------------- #

@pytest.mark.parametrize("name", sorted(WORKERS))
def test_no_credentials_in_any_response(name):
    for path in ("/health", "/ready", "/live", "/metrics"):
        _, raw = get(WORKERS[name] + path)
        text = raw.decode("utf-8", "replace")
        for canary in CANARIES:
            assert canary not in text, f"{canary[:12]}... leaked via {name}{path}"


@pytest.mark.parametrize("name", sorted(WORKERS))
def test_metric_labels_are_bounded(name):
    """A free-text error string as a label is how a Prometheus instance dies."""
    from worker_health.core.model import ErrorCategory
    _, raw = get(WORKERS[name] + "/metrics")
    allowed = {c.value for c in ErrorCategory}
    for line in raw.decode().splitlines():
        if line.startswith("worker_health_check_error{") and 'category="' in line:
            cat = line.split('category="', 1)[1].split('"', 1)[0]
            assert cat in allowed, cat


def test_adapters_call_no_mutating_methods():
    """Static proof of the read-only invariant. Catches a basic_publish added
    during a refactor, which review will eventually miss."""
    import ast
    import pathlib
    forbidden = {"basic_publish", "basic_get", "basic_ack", "basic_nack",
                 "queue_delete", "queue_purge", "exchange_declare",
                 "flushdb", "flushall", "commit"}
    root = pathlib.Path(__file__).resolve().parents[2] / "src/worker_health/checks"
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text())
        called = {n.func.attr for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not (called & forbidden), f"{path.name}: {called & forbidden}"


def test_aliveness_test_endpoint_is_never_referenced():
    """Verified on RabbitMQ 3.13: it declares a queue, publishes and consumes.
    That violates the non-destructive guardrail outright.

    Checks string literals that are actually used in code -- docstrings are
    excluded, since documenting WHY the endpoint is banned is not a use of it.
    """
    import ast
    import pathlib

    def docstring_nodes(tree):
        out = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.ClassDef,
                                 ast.FunctionDef, ast.AsyncFunctionDef)):
                body = getattr(node, "body", None)
                if body and isinstance(body[0], ast.Expr) and \
                        isinstance(body[0].value, ast.Constant) and \
                        isinstance(body[0].value.value, str):
                    out.add(id(body[0].value))
        return out

    root = pathlib.Path(__file__).resolve().parents[2] / "src"
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        skip = docstring_nodes(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                    and id(node) not in skip:
                assert "aliveness-test" not in node.value, f"{path}:{node.lineno}"


# -- fault injection --------------------------------------------------------- #

@pytest.mark.faults
@pytest.mark.slow
def test_postgres_blackhole_is_detected_then_recovers():
    """The highest-value injection: packets dropped, socket never closes.
    Any adapter without an explicit socket timeout hangs here forever."""
    toxi("POST", "/proxies/postgres/toxics", {
        "name": "blackhole", "type": "timeout", "stream": "downstream",
        "toxicity": 1.0, "attributes": {"timeout": 0},
    })
    try:
        result = wait_for(
            lambda: health("billing")[1]["checks"]["postgres"]["internal_status"]
            == "failing",
            timeout=90, what="postgres reported failing")
        assert result
        _, body = health("billing")
        assert body["checks"]["postgres"]["category"] in ("timeout", "connection_lost")
        assert body["status"] == "failing"          # postgres is critical
    finally:
        toxi("POST", "/reset")

    wait_for(lambda: health("billing")[1]["checks"]["postgres"]["internal_status"] == "ok",
             timeout=90, what="postgres recovered")


@pytest.mark.faults
@pytest.mark.slow
def test_redis_failure_degrades_but_does_not_fail_the_worker():
    """Redis is registered non-critical, so its loss must not read as an
    outage -- 503 on a cache blip is how consumers learn to ignore you."""
    toxi("POST", "/proxies/redis-cache", {"enabled": False})
    try:
        wait_for(
            lambda: health("billing")[1]["checks"]["redis"]["internal_status"]
            in ("failing", "degraded"),
            timeout=90, what="redis reported unhealthy")
        code, body = get(WORKERS["billing"] + "/ready")
        assert body is not None
        assert code == 200, "a non-critical dependency must not 503 readiness"
    finally:
        toxi("POST", "/proxies/redis-cache", {"enabled": True})

    wait_for(lambda: health("billing")[1]["checks"]["redis"]["internal_status"] == "ok",
             timeout=90, what="redis recovered")
