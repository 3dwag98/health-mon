"""L0: configuration -- the YAML subset, env overlay, precedence, redaction."""
from __future__ import annotations

import json

import pytest

from worker_health.config import HealthConfig, expand_env, load_config
from worker_health.probes import ProbeConfigError

pytestmark = pytest.mark.unit


SAMPLE = """
worker_health:
  service: billing-worker
  instance: billing-1
  health_port: 8080
  boot_grace: 30
  default_queue: billing.in

  probes:
    - type: postgres
      name: postgres
      critical: true
      interval: 15
      timeout: 2
      params:
        engine: "@db_engine"

    - type: redis
      name: redis-cache
      critical: false
      interval: 30
      params:
        client: "@redis_client"

    - type: http
      name: vendor-api       # a trailing comment
      critical: false
      params:
        url: https://api.vendor.com/ping
        expect_status: 200
        headers: {}
"""


def _load_with_fallback(text):
    """Parse with the built-in loader, never PyYAML.

    The fallback is what runs on a worker that did not install PyYAML, so it
    is the path that has to be tested -- exercising it only when the
    dependency happens to be absent means it is tested nowhere.
    """
    source = open("src/worker_health/_yaml.py").read().replace(
        "import yaml            # type: ignore", "raise ImportError"
    )
    namespace: dict = {}
    exec(compile(source, "yaml_fallback", "exec"), namespace)
    return namespace["safe_load"](text)


# -- the YAML subset --------------------------------------------------------- #

def test_the_builtin_loader_matches_pyyaml_on_a_real_config():
    yaml = pytest.importorskip("yaml")
    assert _load_with_fallback(SAMPLE) == yaml.safe_load(SAMPLE)


def test_the_builtin_loader_handles_scalars_comments_and_empties():
    parsed = _load_with_fallback(
        "a: 1\n"
        "b: 2.5\n"
        "c: true\n"
        "d: null\n"
        "e: \"quoted: value\"   # comment\n"
        "f: {}\n"
        "g: []\n"
        "h: plain string\n"
    )
    assert parsed == {"a": 1, "b": 2.5, "c": True, "d": None,
                      "e": "quoted: value", "f": {}, "g": [], "h": "plain string"}


def test_a_url_value_is_not_split_on_its_colon():
    assert _load_with_fallback("url: https://api.vendor.com/ping")["url"] == \
        "https://api.vendor.com/ping"


def test_unsupported_yaml_is_refused_rather_than_mis_parsed():
    """Anchors and block scalars must not be silently guessed at: a config
    that parses to the wrong thing is worse than one that fails to load."""
    from worker_health._yaml import YamlSubsetError

    source = open("src/worker_health/_yaml.py").read().replace(
        "import yaml            # type: ignore", "raise ImportError")
    namespace: dict = {}
    exec(compile(source, "yaml_fallback", "exec"), namespace)

    with pytest.raises(namespace["YamlSubsetError"]):
        namespace["safe_load"]("base: &anchor\n  a: 1\nother:\n  <<: *anchor\n")
    assert YamlSubsetError is not None


# -- env substitution -------------------------------------------------------- #

def test_env_substitution_with_defaults():
    env = {"IN_QUEUE": "billing.in"}
    assert expand_env("q: ${IN_QUEUE}", env) == "q: billing.in"
    assert expand_env("d: ${MISSING:-500}", env) == "d: 500"
    assert expand_env("e: ${MISSING}", env) == "e: "


def test_env_substitution_lets_one_file_serve_a_fleet(tmp_path, monkeypatch):
    monkeypatch.setenv("SERVICE", "notify-worker")
    monkeypatch.setenv("IN_QUEUE", "billing.out")
    path = tmp_path / "worker-health.yaml"
    path.write_text(
        "worker_health:\n"
        "  service: ${SERVICE:-worker}\n"
        "  default_queue: ${IN_QUEUE:-default}\n"
        "  boot_grace: ${BOOT_GRACE:-25}\n"
    )
    config = load_config(path)
    assert (config.service, config.default_queue, config.boot_grace) == \
        ("notify-worker", "billing.out", 25.0)


# -- HealthConfig ------------------------------------------------------------ #

def test_probes_survive_the_round_trip_from_a_file(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(SAMPLE)
    config = load_config(path)
    assert [p.name for p in config.probes] == ["postgres", "redis-cache", "vendor-api"]
    assert config.probe("postgres").critical is True
    assert config.probe("redis-cache").critical is False
    assert config.probe("vendor-api").params["url"] == "https://api.vendor.com/ping"


def test_json_is_accepted_as_well_as_yaml(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"worker_health": {
        "service": "x", "probes": [{"type": "disk", "name": "d",
                                    "params": {"path": "/"}}]}}))
    config = load_config(path)
    assert config.service == "x" and config.probes[0].type == "disk"


def test_django_upper_case_settings_are_understood():
    config = HealthConfig.from_mapping({
        "ENABLED": True, "SERVICE": "billing-worker", "PORT": 9100,
        "DEFAULT_QUEUE": "billing.in", "BOOT_GRACE": 30,
        "PROBES": [{"type": "django_db", "name": "postgres", "critical": True}],
    })
    assert config.service == "billing-worker"
    assert config.health_port == 9100
    assert config.default_queue == "billing.in"
    assert [p.type for p in config.probes] == ["django_db"]


def test_probes_may_be_a_mapping_of_named_blocks():
    config = HealthConfig.from_mapping({
        "probes": {"postgres": {"type": "django_db", "critical": True},
                   "cache": {"type": "redis", "params": {"client": "@c"}}}})
    assert sorted(p.name for p in config.probes) == ["cache", "postgres"]


def test_the_environment_wins_over_the_file(monkeypatch, tmp_path):
    """The file is baked into an image; the environment is what an operator
    can change at 3am without a rebuild."""
    path = tmp_path / "c.yaml"
    path.write_text("worker_health:\n  service: from-file\n  health_port: 8080\n")
    monkeypatch.setenv("HEALTH_PORT", "9999")
    config = load_config(path)
    assert config.health_port == 9999
    assert config.service == "from-file"


def test_an_unparseable_override_keeps_the_default(monkeypatch):
    monkeypatch.setenv("HEALTH_PORT", "not-a-number")
    assert HealthConfig.from_env().health_port == 8080


def test_a_missing_config_file_is_not_fatal_by_default():
    assert load_config("/nonexistent/worker-health.yaml").service == "worker"
    with pytest.raises(ProbeConfigError):
        load_config("/nonexistent/worker-health.yaml", required=True)


def test_the_redacted_config_carries_no_secrets():
    config = HealthConfig.from_mapping({
        "service": "billing",
        "probes": [{"type": "postgres", "name": "pg",
                    "params": {"dsn": "postgres://app:hunter2@db:5432/app"}}],
        "restart": {"enabled": True, "token": "abc123"},
    })
    rendered = json.dumps(config.redacted())
    assert "hunter2" not in rendered and "abc123" not in rendered
    assert "db:5432" in rendered            # the useful half survives
