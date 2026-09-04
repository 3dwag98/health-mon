"""The built-in probe types.

Every builder is a plain function of ``(spec, context)``, which is the same
contract a third-party plugin implements -- so nothing here has privileges
a user's own probe does not.  That is deliberate: the built-ins are the
worked examples for the extension point.

Driver imports happen inside the builders, so registering all of these
costs nothing at import time and a worker without SQLAlchemy can still list
the postgres type without being able to build it.
"""
from __future__ import annotations

from typing import Any, Mapping

from .spec import ProbeConfigError, ProbeSpec


def register_all(factory) -> None:
    factory.register_type("postgres", build_postgres)
    factory.register_type("sqlalchemy", build_postgres)      # alias
    factory.register_type("django_db", build_django_db)
    factory.register_type("redis", build_redis)
    factory.register_type("rabbitmq", build_rabbitmq)
    factory.register_type("kafka", build_kafka)
    factory.register_type("http", build_http)
    factory.register_type("tcp", build_tcp)
    factory.register_type("dns", build_dns)
    factory.register_type("disk", build_disk)
    factory.register_type("file_age", build_file_age)
    factory.register_type("function", build_function)
    factory.register_type("processing", build_processing)


# -- helpers ------------------------------------------------------------- #

def _require(spec: ProbeSpec, key: str) -> Any:
    if key not in spec.params or spec.params[key] is None:
        raise ProbeConfigError(
            f"probe {spec.name!r} (type {spec.type!r}) requires param {key!r}"
        )
    return spec.params[key]


def _first(spec: ProbeSpec, *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = spec.params.get(key)
        if value is not None:
            return value
    return default


def _dependency(spec: ProbeSpec, fallback: str) -> str:
    """Which traffic-log entry this check reads its observed evidence from.

    Defaults to the check's own name, so a config that names a probe
    `redis-cache` and instruments a client as `redis-cache` lines up with no
    extra setting.  ``dependency: ""`` opts out of observed evidence.
    """
    value = spec.params.get("dependency")
    if value is None:
        return spec.name or fallback
    return str(value)


def _import_target(path: str):
    """Resolve ``"package.module:attribute"`` to the object.

    Only used for params that explicitly name an import path; nothing in
    the config is imported implicitly.
    """
    module_name, _, attribute = str(path).partition(":")
    if not module_name or not attribute:
        raise ProbeConfigError(
            f"{path!r} is not an import path; expected 'module:attribute'"
        )
    import importlib

    module = importlib.import_module(module_name)
    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ProbeConfigError(f"{module_name!r} has no attribute {attribute!r}") from exc


# -- dependency probes ---------------------------------------------------- #

def build_postgres(spec: ProbeSpec, context: Mapping[str, Any]):
    """SQLAlchemy-backed Postgres check.

    ``engine`` is the application's own engine, and passing it is what
    makes pool exhaustion visible -- a probe that only opens its own
    connection can never see that the app has run out of slots.
    """
    from ..checks.postgres import PostgresCheck

    engine = _first(spec, "engine", "app_engine", "db_engine")
    dsn = _first(spec, "probe_dsn", "dsn", "url")
    if engine is None and not dsn:
        raise ProbeConfigError(
            f"probe {spec.name!r}: needs `engine` (a live SQLAlchemy engine, "
            f"usually '@db_engine') or `dsn` for the fallback probe"
        )
    return PostgresCheck(
        app_engine=engine,
        probe_dsn=dsn,
        name=spec.name,
        dependency=_dependency(spec, "postgres"),
        pool_warn_ratio=float(_first(spec, "pool_warn_ratio", default=0.9)),
    )


def build_django_db(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.django_db import DjangoDbCheck

    return DjangoDbCheck(
        alias=str(_first(spec, "alias", "connection", default="default")),
        name=spec.name,
        dependency=_dependency(spec, "postgres"),
    )


def build_redis(spec: ProbeSpec, context: Mapping[str, Any]):
    """Redis check against a supplied client, or one built from a URL.

    Passing the worker's own client is strongly preferred: a check on a
    separate client tells you the server is up, not that the worker's
    connection is.
    """
    from ..checks.redis_ import RedisCheck, build_client

    client = _first(spec, "client", "redis", "redis_client")
    if client is None:
        url = _first(spec, "url", "dsn")
        if url:
            client = build_client(url, socket_timeout=spec.timeout,
                                  socket_connect_timeout=spec.timeout)
        elif spec.params.get("host"):
            client = build_client(
                host=str(spec.params["host"]),
                port=int(_first(spec, "port", default=6379)),
                db=int(_first(spec, "db", default=0)),
                password=_first(spec, "password"),
                socket_timeout=spec.timeout,
                socket_connect_timeout=spec.timeout,
            )
        else:
            raise ProbeConfigError(
                f"probe {spec.name!r}: needs `client` (usually '@redis_client'), "
                f"`url`, or `host`"
            )
    return RedisCheck(
        client,
        name=spec.name,
        dependency=_dependency(spec, "redis"),
        label=str(_first(spec, "label", default="")),
        memory_warn_ratio=float(_first(spec, "memory_warn_ratio", default=0.9)),
    )


def build_rabbitmq(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.rabbitmq import RabbitMQCheck

    state = _first(spec, "broker_state", "state")
    if state is None:
        raise ProbeConfigError(
            f"probe {spec.name!r}: needs `broker_state` (usually '@broker_state'). "
            f"The RabbitMQ check reads the worker's own connection state rather "
            f"than opening one of its own."
        )
    return RabbitMQCheck(
        state,
        queue=str(_require(spec, "queue")),
        name=spec.name,
        dependency=_dependency(spec, "rabbitmq"),
        backlog_threshold=int(_first(spec, "backlog_threshold", "max_depth", default=1000)),
        stale_after=float(_first(spec, "stale_after", default=20.0)),
    )


def build_kafka(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.kafka import KafkaCheck, KafkaConsumerState

    state = _first(spec, "state", "consumer_state", "kafka_state")
    if state is None:
        state = KafkaConsumerState(
            group=str(_first(spec, "group", default="")),
            topics=tuple(_first(spec, "topics", default=()) or ()),
        )
    lag_fn = _first(spec, "lag_fn")
    if isinstance(lag_fn, str):
        lag_fn = _import_target(lag_fn)
    return KafkaCheck(
        state,
        name=spec.name,
        dependency=_dependency(spec, "kafka"),
        max_lag=int(_first(spec, "max_lag", default=10_000)),
        stale_after=float(_first(spec, "stale_after", default=30.0)),
        max_rebalance=float(_first(spec, "max_rebalance", default=60.0)),
        lag_fn=lag_fn,
    )


# -- reachability probes -------------------------------------------------- #

def build_http(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.network import HttpProbe

    expect = _first(spec, "expect_status", "expect", default=200)
    if isinstance(expect, list):
        expect = tuple(int(v) for v in expect)
    return HttpProbe(
        url=str(_require(spec, "url")),
        name=spec.name,
        dependency=_dependency(spec, ""),
        expect_status=expect if isinstance(expect, tuple) else int(expect),
        method=str(_first(spec, "method", default="GET")),
        timeout=spec.timeout,
        headers=_first(spec, "headers", default=None),
        slow_ms=_first(spec, "slow_ms"),
    )


def build_tcp(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.network import TcpProbe

    return TcpProbe(
        host=str(_require(spec, "host")),
        port=int(_require(spec, "port")),
        name=spec.name,
        dependency=_dependency(spec, ""),
        timeout=spec.timeout,
    )


def build_dns(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.network import DnsProbe

    return DnsProbe(
        host=str(_first(spec, "host", "name") or _require(spec, "host")),
        name=spec.name,
        dependency=_dependency(spec, ""),
        family=str(_first(spec, "family", default="any")),
        min_records=int(_first(spec, "min_records", default=1)),
    )


# -- local resource probes ------------------------------------------------ #

def build_disk(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.system import DiskSpaceProbe

    return DiskSpaceProbe(
        path=str(_first(spec, "path", default="/")),
        name=spec.name,
        dependency=_dependency(spec, ""),
        min_free_gb=float(_first(spec, "min_free_gb", default=5.0)),
        min_free_ratio=_first(spec, "min_free_ratio"),
        fail_free_gb=_first(spec, "fail_free_gb"),
    )


def build_file_age(spec: ProbeSpec, context: Mapping[str, Any]):
    from ..checks.system import FileAgeProbe

    return FileAgeProbe(
        path=str(_require(spec, "path")),
        name=spec.name,
        dependency=_dependency(spec, ""),
        max_age_s=float(_first(spec, "max_age_s", "max_age", default=300.0)),
        min_size_bytes=int(_first(spec, "min_size_bytes", default=0)),
        missing_is_failure=bool(_first(spec, "missing_is_failure", default=True)),
    )


# -- escape hatches ------------------------------------------------------- #

def build_function(spec: ProbeSpec, context: Mapping[str, Any]):
    """Any Python callable, from the context or an import path.

    The callable may return a bool, a Status, or a full CheckResult; an
    exception is caught, classified as `internal`, and isolated to this
    check.
    """
    from ..checks.custom import CustomCheck

    target = _first(spec, "fn", "callable", "target")
    if target is None:
        raise ProbeConfigError(
            f"probe {spec.name!r}: needs `fn`, either '@some_context_key' or "
            f"'module:attribute'"
        )
    if isinstance(target, str):
        target = _import_target(target)
    if not callable(target):
        raise ProbeConfigError(f"probe {spec.name!r}: `fn` is not callable")
    return CustomCheck(target, name=spec.name)


def build_processing(spec: ProbeSpec, context: Mapping[str, Any]):
    """The processing check, so it can be tuned from the same config file."""
    from ..checks.processing import ProcessingCheck, ProcessingState

    state = _first(spec, "state", "processing_state") or context.get("processing_state")
    if state is None:
        state = ProcessingState()
    return ProcessingCheck(
        state,
        name=spec.name,
        broker_state=_first(spec, "broker_state") or context.get("broker_state"),
        max_idle=float(_first(spec, "max_idle", default=60.0)),
        max_since_success=float(_first(spec, "max_since_success", default=120.0)),
        poison_threshold=int(_first(spec, "poison_threshold", default=10)),
    )
