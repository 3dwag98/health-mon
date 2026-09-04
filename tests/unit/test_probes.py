"""L0: the probe factory -- specs, references, registration, isolation."""
from __future__ import annotations

import pytest

from worker_health import HealthMonitor, Status
from worker_health.probes import ProbeConfigError, ProbeFactory, ProbeSpec, default_factory

pytestmark = pytest.mark.unit


def _monitor():
    return HealthMonitor("test", instance="test-1")


# -- ProbeSpec --------------------------------------------------------------- #

def test_short_form_params_are_accepted():
    """`url: ...` at the top level means the same as `params: {url: ...}`.

    Config files in the wild are written both ways, and rejecting one of
    them produces a confusing error a long way from the mistake.
    """
    spec = ProbeSpec.from_raw({"type": "http", "name": "vendor",
                               "url": "https://example.test/ping"})
    assert spec.params["url"] == "https://example.test/ping"


def test_ttl_defaults_from_the_interval():
    """A fixed TTL is wrong at most intervals; two missed evaluations is not."""
    spec = ProbeSpec.from_raw({"type": "disk", "name": "d", "interval": 10, "timeout": 2})
    assert spec.ttl == 22.0


def test_invalid_numbers_are_rejected_at_boot_with_the_probe_named():
    with pytest.raises(ProbeConfigError) as exc:
        ProbeSpec.from_raw({"type": "disk", "name": "worker-disk", "interval": 0})
    assert "worker-disk" in str(exc.value)
    assert "interval" in str(exc.value)

    with pytest.raises(ProbeConfigError):
        ProbeSpec.from_raw({"type": "disk", "name": "d", "failure_threshold": 0})


def test_a_probe_with_no_type_is_rejected():
    with pytest.raises(ProbeConfigError):
        ProbeSpec.from_raw({"name": "nameless"})


# -- context references ------------------------------------------------------ #

def test_at_references_resolve_from_the_context():
    engine = object()
    spec = ProbeSpec.from_raw({"type": "postgres", "name": "pg",
                               "params": {"engine": "@db_engine", "pool_warn_ratio": 0.8}})
    resolved = spec.resolved_params({"db_engine": engine})
    assert resolved["engine"] is engine
    assert resolved["pool_warn_ratio"] == 0.8


def test_a_missing_reference_names_what_is_available():
    """The error has to say what the context DID have, or a typo in a
    twelve-probe config is a scavenger hunt."""
    spec = ProbeSpec.from_raw({"type": "redis", "name": "cache",
                               "params": {"client": "@redis_clint"}})
    with pytest.raises(ProbeConfigError) as exc:
        spec.resolved_params({"redis_client": object(), "db_engine": object()})
    message = str(exc.value)
    assert "cache" in message and "redis_clint" in message
    assert "db_engine" in message and "redis_client" in message


def test_double_at_escapes_to_a_literal():
    """A header value or a Redis key pattern may legitimately start with @."""
    spec = ProbeSpec.from_raw({"type": "http", "name": "h",
                               "params": {"url": "https://x.test", "token": "@@literal"}})
    assert spec.resolved_params({})["token"] == "@literal"


def test_references_resolve_inside_nested_structures():
    marker = object()
    spec = ProbeSpec.from_raw({"type": "function", "name": "f",
                               "params": {"nested": {"a": ["@thing"]}}})
    assert spec.resolved_params({"thing": marker})["nested"]["a"][0] is marker


# -- factory ----------------------------------------------------------------- #

def test_every_documented_builtin_type_is_registered():
    registered = set(default_factory().types())
    documented = {
        "postgres", "sqlalchemy", "django_db", "redis", "rabbitmq", "kafka",
        "http", "tcp", "dns", "disk", "file_age", "function", "processing",
    }
    assert documented <= registered


def test_default_factory_is_not_shared_between_callers():
    """A module-level singleton would leak one worker's custom types into
    another's factory in the same process, and make tests order-dependent."""
    first = default_factory()

    @first.probe_type("only-here")
    def build(spec, context):
        raise AssertionError("never built")

    assert "only-here" in first.types()
    assert "only-here" not in default_factory().types()


def test_unknown_type_lists_the_registered_ones():
    factory = ProbeFactory()
    factory.register_type("known", lambda spec, ctx: None)
    with pytest.raises(ProbeConfigError) as exc:
        factory.create(ProbeSpec.from_raw({"type": "typo", "name": "x"}), {})
    assert "known" in str(exc.value)


def test_the_config_name_wins_over_the_builders_name():
    """Metric labels and alerts are written against the configured name, so
    a builder that names its check something else must not win."""
    factory = default_factory()
    check = factory.create(
        ProbeSpec.from_raw({"type": "disk", "name": "data-volume",
                            "params": {"path": "/"}}), {})
    assert check.name == "data-volume"


def test_custom_probe_types_need_no_sdk_change():
    factory = default_factory()
    built = {}

    class VendorCheck:
        name = "vendor"

        def evaluate(self, ctx):
            raise AssertionError("not evaluated in this test")

        def close(self):
            pass

    @factory.probe_type("vendor-api")
    def build_vendor(spec, context):
        built["url"] = spec.params["url"]
        built["timeout"] = spec.timeout
        return VendorCheck()

    monitor = _monitor()
    factory.install_from_config(monitor, [
        {"type": "vendor-api", "name": "vendor-api", "critical": False,
         "interval": 30, "timeout": 2, "params": {"url": "https://vendor.test/ping"}},
    ], {})

    assert built == {"url": "https://vendor.test/ping", "timeout": 2.0}
    assert monitor.machine.spec("vendor-api").critical is False


def test_registration_kwargs_reach_the_state_machine():
    monitor = _monitor()
    default_factory().install_from_config(monitor, [
        {"type": "disk", "name": "d", "critical": True, "interval": 20,
         "timeout": 3, "failure_threshold": 5, "success_threshold": 4,
         "max_silence": 45, "params": {"path": "/"}},
    ], {})
    spec = monitor.machine.spec("d")
    assert (spec.critical, spec.interval, spec.timeout) == (True, 20.0, 3.0)
    assert (spec.failure_threshold, spec.success_threshold) == (5, 4)
    assert spec.max_silence == 45.0


# -- failure isolation -------------------------------------------------------- #

def test_strict_mode_stops_the_worker_at_boot():
    """A malformed probe is a config bug: better found with a human watching."""
    monitor = _monitor()
    with pytest.raises(ProbeConfigError):
        default_factory().install_from_config(
            monitor, [{"type": "rabbitmq", "name": "mq", "params": {"queue": "q"}}],
            {}, strict=True,
        )


def test_non_strict_mode_reports_instead_of_refusing_to_start():
    """One bad config line must not take a hundred workers offline -- but it
    must not be invisible either."""
    monitor = _monitor()
    installed = default_factory().install_from_config(
        monitor, [{"type": "rabbitmq", "name": "mq", "params": {"queue": "q"}}],
        {}, strict=False,
    )
    assert installed == []
    assert "mq" in monitor.checks           # present, and permanently failing
    assert monitor.checks["mq"].evaluate(_ctx()).status is Status.FAILING


def test_disabled_probes_are_registered_but_never_scheduled():
    monitor = _monitor()
    default_factory().install_from_config(monitor, [
        {"type": "disk", "name": "d", "enabled": False, "params": {"path": "/"}},
    ], {})
    assert monitor.machine.effective("d") is Status.DISABLED
    assert monitor.machine.due("d") is False
    assert [s.name for s in monitor.machine.enabled_specs()] == []


def test_a_disabled_check_does_not_affect_readiness():
    from worker_health.core.aggregate import aggregate

    monitor = _monitor()
    default_factory().install_from_config(monitor, [
        {"type": "disk", "name": "d", "critical": True, "enabled": False,
         "params": {"path": "/"}},
    ], {})
    assert aggregate(monitor.machine, monitor.clock, None) is Status.OK


# -- built-in builders that need no driver ----------------------------------- #

def test_http_probe_refuses_a_mutating_method():
    """A health check that POSTs is a health check that can create an order.
    Enforced in code so a YAML typo fails at wiring time, not at 3am."""
    with pytest.raises(ValueError):
        default_factory().create(ProbeSpec.from_raw({
            "type": "http", "name": "h",
            "params": {"url": "https://x.test", "method": "POST"}}), {})


def test_function_probe_accepts_a_context_callable():
    factory = default_factory()
    check = factory.create(
        ProbeSpec.from_raw({"type": "function", "name": "vendor",
                            "params": {"fn": "@probe_fn"}}),
        {"probe_fn": lambda: True},
    )
    assert check.evaluate(_ctx()).status is Status.OK


def test_function_probe_rejects_a_non_callable():
    with pytest.raises(ProbeConfigError):
        default_factory().create(
            ProbeSpec.from_raw({"type": "function", "name": "f",
                                "params": {"fn": "@thing"}}),
            {"thing": "not callable"},
        )


def test_disk_probe_degrades_below_the_threshold():
    check = default_factory().create(ProbeSpec.from_raw({
        "type": "disk", "name": "d",
        "params": {"path": "/", "min_free_gb": 10 ** 9}}), {})
    result = check.evaluate(_ctx())
    assert result.status is Status.DEGRADED
    assert result.observed["free_gb"] >= 0


def test_probe_params_are_redacted_for_logging():
    spec = ProbeSpec.from_raw({"type": "postgres", "name": "pg",
                               "params": {"dsn": "postgres://u:hunter2@db:5432/app"}})
    assert "hunter2" not in str(spec.redacted())
    assert "db:5432" in str(spec.redacted())


def _ctx():
    from worker_health.checks.base import CheckContext, TrafficLog

    return CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                        traffic=TrafficLog())
