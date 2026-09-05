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
OTEL = os.getenv("OTEL_COLLECTOR_URL", "http://otel-collector:8888")
LOADGEN = os.getenv("LOADGEN_URL", "http://loadgen:8090")
DASHBOARD = os.getenv("DASHBOARD_URL", "http://dashboard:9000")

# Distinctive per-dependency passwords. Any of these appearing in a body, a
# log line or a metric is a credential leak.
CANARIES = ("canary-pg-8f3ad91c", "canary-mq-4b7ce02d", "canary-rd-1a9fe63b")


def get(url, timeout=5.0, retries=2):
    """Fetch, retrying a dropped connection.

    Toxiproxy resets every proxied connection when a toxic is added or
    removed, and a worker restarting its broker link can reset an in-flight
    request. That is the environment being tested, not a failure of it, so
    the harness absorbs it rather than turning it into a red test.
    """
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except OSError:
            if attempt == retries:
                raise
            time.sleep(0.5)


def health(name):
    code, body = get(WORKERS[name] + "/health")
    return code, json.loads(body)


def set_load(rate):
    """Drive the load generator's rate.

    Some assertions are only meaningful on an idle worker: this demo does its
    database work on the same thread pika runs its I/O loop on, so a slow
    database starves the broker probe and every check goes stale at once.
    That coupling is real and worth knowing about, but it is not what the
    test below is about.
    """
    data = json.dumps({"rate": rate}).encode()
    req = urllib.request.Request(LOADGEN + "/", data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return r.status


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
    # /live speaks the two-value liveness vocabulary, not the check-status
    # one: it answers "is this process turning over", not "is it healthy".
    assert json.loads(body)["status"] == "alive"


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
    for path in ("/health", "/ready", "/live", "/config", "/events"):
        _, raw = get(WORKERS[name] + path)
        text = raw.decode("utf-8", "replace")
        for canary in CANARIES:
            assert canary not in text, f"{canary[:12]}... leaked via {name}{path}"


@pytest.mark.parametrize("name", sorted(WORKERS))
def test_reported_categories_come_from_the_closed_vocabulary(name):
    """A free-text error string reaching an attribute is how a backend dies."""
    from worker_health.core.model import ErrorCategory
    allowed = {c.value for c in ErrorCategory}
    _, body = health(name)
    for check, entry in body["checks"].items():
        if "category" in entry:
            assert entry["category"] in allowed, f"{check}: {entry['category']}"


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

def restore_baseline():
    """Put the fleet back to steady state and wait until it says so.

    Fault tests share one live stack, so each has to both leave it clean AND
    refuse to start until it is. Asserting against a fleet still recovering
    from the previous test is how a suite becomes order-dependent and then
    becomes ignored.
    """
    toxi("POST", "/reset")
    set_load(25)
    for name in sorted(WORKERS):
        wait_for(lambda n=name: health(n)[1]["status"] == "ok",
                 timeout=150, what=f"{name} back to healthy")
        wait_for(lambda n=name: get(WORKERS[n] + "/live")[0] == 200,
                 timeout=60, what=f"{name} back to live")


@pytest.fixture
def clean_fleet():
    restore_baseline()
    yield
    restore_baseline()


@pytest.mark.faults
@pytest.mark.slow
def test_postgres_blackhole_is_detected_then_recovers(clean_fleet):
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
def test_redis_failure_degrades_but_does_not_fail_the_worker(clean_fleet):
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


@pytest.mark.faults
@pytest.mark.slow
def test_rabbitmq_refused_fails_readiness_but_never_liveness(clean_fleet):
    """The split PM2 depends on. The broker being gone makes this worker
    unable to accept work (503 on /ready), and restarting it would not bring
    the broker back -- so /live must stay 200 and the supervisor must stay
    out of it."""
    toxi("POST", "/proxies/rabbitmq", {"enabled": False})
    try:
        wait_for(
            lambda: health("billing")[1]["checks"]["rabbitmq"]["internal_status"]
            in ("failing", "degraded"),
            timeout=90, what="rabbitmq reported unhealthy")

        _, body = health("billing")
        category = body["checks"]["rabbitmq"].get("category")
        # `stalled` is the transient before pika notices the socket is
        # gone; `connection_lost` is where it settles. Both are honest
        # answers to "can this worker use the broker".
        assert category in ("connection_refused", "connection_lost",
                            "not_subscribed", "timeout", "stale",
                            "stalled"), category

        # The process must still be here to say so.  A consumer that exits
        # when the broker restarts takes its own health endpoint down with
        # it, and under a supervisor turns one blip into a crash loop.
        assert body["uptime_s"] > 0

        # rabbitmq is critical, so readiness follows it down...
        wait_for(lambda: get(WORKERS["billing"] + "/ready")[0] == 503,
                 timeout=60, what="readiness 503")
        # ...and liveness does not.
        assert get(WORKERS["billing"] + "/live")[0] == 200
    finally:
        toxi("POST", "/proxies/rabbitmq", {"enabled": True})

    wait_for(lambda: health("billing")[1]["checks"]["rabbitmq"]["internal_status"] == "ok",
             timeout=120, what="rabbitmq recovered")


@pytest.mark.faults
@pytest.mark.slow
def test_a_latency_spike_degrades_without_taking_the_worker_out_of_rotation(clean_fleet):
    """A dependency answering slowly is up. It is worth a warning and it is
    not worth a 503 -- removing every worker from rotation because the
    database got slow is how a slowdown becomes an outage.

    Run against an idle worker on purpose. Under load this demo would ALSO
    report the broker stale, because it does its database work on pika's own
    I/O thread; that is a property of this worker's shape, not of the
    latency verdict being tested here.
    """
    set_load(0)
    try:
        wait_for(lambda: health("billing")[1]["checks"]["processing"]
                 ["observed"].get("in_flight") == 0,
                 timeout=60, what="the worker to drain")
        toxi("POST", "/proxies/postgres/toxics", {
            "name": "slow", "type": "latency", "stream": "downstream",
            "toxicity": 1.0, "attributes": {"latency": 150, "jitter": 0},
        })

        wait_for(
            lambda: health("billing")[1]["checks"]["postgres"]["internal_status"]
            == "degraded",
            timeout=90, what="postgres reported degraded")

        _, body = health("billing")
        entry = body["checks"]["postgres"]
        assert entry["category"] == "slow"
        assert entry["latency_ms"] >= entry["observed"]["latency_threshold_ms"]

        # The whole point: degraded is not unready.
        assert body["readiness"] == "degraded"
        assert get(WORKERS["billing"] + "/ready")[0] == 200
        assert get(WORKERS["billing"] + "/live")[0] == 200
        # And no other check was dragged down with it.
        assert body["checks"]["rabbitmq"]["internal_status"] == "ok"
    finally:
        toxi("POST", "/reset")
        set_load(25)

    wait_for(lambda: health("billing")[1]["checks"]["postgres"]["internal_status"] == "ok",
             timeout=90, what="postgres recovered")


# -- telemetry --------------------------------------------------------------- #

@pytest.mark.parametrize("name", sorted(WORKERS))
def test_every_worker_pushes_to_the_collector(name):
    """Push, not scrape. The counters live on /health because the exporter is
    silent by design -- a collector nobody can reach has to be visible
    somewhere, and a log line per failed export is its own incident."""
    export = wait_for(lambda: health(name)[1].get("export"),
                      timeout=60, what=f"{name} to report an exporter")
    assert export["endpoint"]
    wait_for(lambda: health(name)[1]["export"]["exported"] > 0,
             timeout=60, what=f"{name} to complete an export")

    _, body = health(name)
    export = body["export"]
    assert export["failed"] == 0, export["last_error"]
    # A bounded queue that is dropping means the collector cannot keep up.
    assert export["dropped"] == 0


def test_the_collector_accepts_the_payloads_it_is_sent():
    """Proves the payloads are VALID OTLP, not merely that bytes were sent:
    a hand-written encoding that a collector rejects would otherwise look
    identical to one that works."""
    def accepted():
        code, raw = get(OTEL + "/metrics", timeout=5.0)
        if code != 200:
            return None
        total = 0
        for line in raw.decode("utf-8", "replace").splitlines():
            if line.startswith("otelcol_receiver_accepted_metric_points"):
                total += float(line.rsplit(" ", 1)[-1])
        return total if total > 0 else None

    assert wait_for(accepted, timeout=90, what="the collector to accept metric points")

    code, raw = get(OTEL + "/metrics", timeout=5.0)
    refused = 0.0
    for line in raw.decode("utf-8", "replace").splitlines():
        if line.startswith("otelcol_receiver_refused_metric_points"):
            refused += float(line.rsplit(" ", 1)[-1])
    assert refused == 0.0, "the collector refused points the workers sent"


@pytest.mark.faults
@pytest.mark.slow
def test_a_redis_latency_spike_degrades_but_keeps_the_worker_ready(clean_fleet):
    """Redis is registered non-critical and slow is not down. Neither half of
    that may take the worker out of rotation -- a 503 on a slow cache is how
    a cache slowdown becomes a total outage."""
    set_load(0)
    try:
        wait_for(lambda: health("billing")[1]["checks"]["processing"]
                 ["observed"].get("in_flight") == 0,
                 timeout=60, what="the worker to drain")
        toxi("POST", "/proxies/redis-cache/toxics", {
            "name": "slow", "type": "latency", "stream": "downstream",
            "toxicity": 1.0, "attributes": {"latency": 400, "jitter": 0},
        })

        wait_for(
            lambda: health("billing")[1]["checks"]["redis"]["internal_status"]
            == "degraded",
            timeout=90, what="redis reported degraded")

        _, body = health("billing")
        entry = body["checks"]["redis"]
        assert entry["category"] == "slow"
        assert entry["latency_ms"] >= entry["observed"]["latency_threshold_ms"]

        assert get(WORKERS["billing"] + "/ready")[0] == 200
        assert get(WORKERS["billing"] + "/live")[0] == 200
    finally:
        toxi("POST", "/reset")
        set_load(25)

    wait_for(lambda: health("billing")[1]["checks"]["redis"]["internal_status"] == "ok",
             timeout=90, what="redis recovered")


# -- the fleet dashboard ------------------------------------------------------ #

def fleet():
    code, raw = get(DASHBOARD + "/api/fleet")
    assert code == 200, code
    return json.loads(raw)


def test_the_dashboard_receives_the_workers_own_otlp_push():
    """Workers push to the collector; the collector fans out here. Nothing in
    a worker knows the dashboard exists, which is what lets the board scale
    past the workers someone remembered to list in a config file."""
    body = wait_for(
        lambda: fleet() if any("otlp" in (w.get("source") or "")
                               for w in fleet()["workers"]) else None,
        timeout=120, what="a worker to arrive over OTLP")

    pushed = [w for w in body["workers"] if "otlp" in (w.get("source") or "")]
    assert pushed, body["workers"]
    for worker in pushed:
        # Identity survives the wire, including which deployment this is.
        assert worker["environment"] == "demo", worker


def test_a_pushed_worker_does_not_double_the_fleet():
    """A pushed `billing-1` and a polled `billing` are one worker."""
    names = [w["name"] for w in fleet()["workers"]]
    assert sorted(names) == sorted(WORKERS), names


@pytest.mark.faults
@pytest.mark.slow
def test_the_dashboard_reports_one_shared_outage_not_three_sick_workers(clean_fleet):
    """The view the plan is really asking for. Fifty workers reporting the
    same database down is ONE database outage, and rendering it as fifty sick
    workers buries the only fact anyone can act on."""
    toxi("POST", "/proxies/postgres", {"enabled": False})
    try:
        # Wait for the whole fleet to have reported, not merely for the
        # group to exist: workers notice an outage at their own cadence, and
        # asserting on the first two to arrive is a race.
        outage = wait_for(
            lambda: next(
                (o for o in fleet()["outages"]
                 if o["check"] == "postgres" and o["count"] == len(WORKERS)),
                None),
            timeout=120, what="every worker to report the outage")

        assert outage["shared"] is True
        assert sorted(outage["workers"]) == sorted(WORKERS)
        assert outage["status"] == "failing"
        assert outage["critical"] is True

        # And every worker stays live: the whole point of grouping it as a
        # shared dependency is that restarting them would not help.
        for name in WORKERS:
            assert get(WORKERS[name] + "/live")[0] == 200, name
    finally:
        toxi("POST", "/reset")

    wait_for(lambda: not fleet()["outages"], timeout=120,
             what="the outage to clear")
