"""``setup_worker_health()`` -- the whole integration, in one call.

    health = setup_worker_health(
        service="billing-worker",
        config_path="worker-health.yaml",
        context={"db_engine": engine, "redis_client": redis, "broker_state": broker},
    )

    @health.tracker.handler(queue="billing.in")
    def handle(message): ...

Everything the brief asks for follows from those two blocks: /live, /ready,
/health, /metrics, structured logs, dependency checks with real-traffic
evidence, processing health, custom probes and dashboard metrics.

What this function is careful NOT to do is fail the worker.  A missing
config file, an un-instrumentable client, a probe type that needs a driver
which is not installed -- none of those stop a worker from starting, and
each is visible in the health output rather than as a traceback at boot.
The exception is a probe definition that is outright malformed while
``strict_probes`` is on: that is a config bug, it is caught at boot with a
human watching, and it is better found there than at 3am.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .checks.processing import ProcessingCheck, ProcessingState
from .config import HealthConfig, load_config
from .monitor import HealthMonitor
from .policy.restart import RestartPolicy
from .probes import ProbeFactory, ProbeSpec, default_factory
from .telemetry.events import Event
from .telemetry.logs import configure
from .track import Tracker
from .transports import registry
from .telemetry.otel import OTLPExporter
from .transports.http import HealthServer


@dataclass
class WorkerHealth:
    """Everything the wiring produced, for a worker that wants the pieces."""

    monitor: HealthMonitor
    tracker: Tracker
    config: HealthConfig
    factory: ProbeFactory
    processing: ProcessingState
    server: HealthServer | None = None
    logger: Any = None
    instrumented: dict[str, str] = field(default_factory=dict)
    exporter: OTLPExporter | None = None

    # The two calls a worker makes most often, forwarded so it does not have
    # to reach through .monitor for them.
    @property
    def handler(self):
        return self.tracker.handler

    def snapshot(self) -> dict:
        return self.monitor.snapshot_dict()

    def stop(self, timeout: float = 5.0) -> None:
        self.monitor.stop(timeout=timeout)
        # Stopped before the server so a final flush can still describe a
        # worker that is on its way out.
        if self.exporter is not None:
            self.exporter.stop()
        if self.server is not None:
            self.server.stop()
        registry.unregister(getattr(self.monitor, "run_record", None))

    def begin_shutdown(self, reason: str = "shutting down") -> None:
        """Report unready now; keep serving /live while work drains."""
        self.monitor.begin_shutdown(reason)

    @property
    def port(self) -> int | None:
        return self.server.port if self.server is not None else None

    def __enter__(self) -> "WorkerHealth":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()


def setup_worker_health(
    service: str | None = None,
    *,
    config_path: str | Path | None = None,
    config: HealthConfig | Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
    factory: ProbeFactory | None = None,
    probes: list | None = None,
    start: bool = True,
    logger=None,
    **overrides: Any,
) -> WorkerHealth:
    """Build, wire and start a worker's health monitoring.

    ``context`` is the bridge between declarative config and live objects:
    its keys are what ``"@name"`` references in the config resolve to, and
    its values are what auto-instrumentation is applied to.
    """
    config = _resolve_config(service, config_path, config, overrides)
    context = dict(context or {})

    logger = logger or configure(config.log_level, force=config.configure_logging)

    monitor = HealthMonitor(
        service=config.service,
        version=config.version,
        environment=config.environment,
        instance=config.instance or None,
        runner=config.runner,
        tick=config.tick,
        max_workers=config.max_workers,
        loop_lag_threshold_ms=config.loop_lag_threshold_ms,
        live_on_self_fault=config.live_on_self_fault,
        logger=logger,
    )

    processing = context.get("processing_state") or ProcessingState()
    context.setdefault("processing_state", processing)
    context.setdefault("monitor", monitor)

    tracker = Tracker(
        monitor, processing,
        default_queue=config.default_queue,
        broker_state=context.get("broker_state"),
    )
    context.setdefault("tracker", tracker)

    # 1. Instrument first, so any probe built below that shares a client is
    #    already being observed by the time it runs.
    instrumented: dict[str, str] = {}
    if config.instrument:
        from .instrument import autowire_context

        instrumented = autowire_context(monitor, context)

    # 2. Probes.
    factory = factory or default_factory()
    if config.load_plugins:
        factory.load_plugins()
    specs = list(config.probes)
    for extra in probes or ():
        specs.append(extra if isinstance(extra, ProbeSpec) else ProbeSpec.from_raw(extra))
    factory.install_from_config(monitor, specs, context, strict=config.strict_probes)

    # 3. The processing check, unless the config already declared one.
    if config.processing_check and not any(s.type == "processing" for s in specs):
        monitor.register(
            ProcessingCheck(
                processing,
                name="processing",
                broker_state=context.get("broker_state"),
                max_idle=config.max_idle,
                max_since_success=config.max_since_success,
                poison_threshold=config.poison_threshold,
            ),
            name="processing",
            # Non-critical by default.  Processing health is the most
            # valuable signal here and also the most situational: a worker
            # on a queue that is quiet by design must not 503 for being
            # quiet, so the default reports it without pulling the worker
            # out of rotation.  Set critical: true in config to change that.
            critical=False,
            interval=5.0, timeout=2.0, ttl=30.0,
            failure_threshold=2, success_threshold=1,
        )

    # 4. Controlled restart, off unless configured.
    if config.restart.get("enabled"):
        monitor.set_restart_policy(_build_restart_policy(config.restart, logger))

    # 5. Transport.
    server = None
    if config.serve_http:
        try:
            server = HealthServer.bind(
                monitor, host=config.health_host, port=config.health_port,
                search=config.health_port_search,
            ).start()
            # Publish where we actually landed, so `worker-health` and
            # `manage.py worker_health` in another shell can find this
            # process without being told a port.
            monitor.run_record = registry.register(
                service=config.service, instance=monitor.instance,
                host=config.health_host, port=server.port,
                version=config.version, command=" ".join(sys.argv[:2]),
            )
        except OSError as exc:
            # The port is taken -- most often a second copy of the same
            # worker on one host, or a Django autoreloader.  The monitor is
            # still useful (metrics, logs, the CLI), so this is reported and
            # survived rather than raised.
            logger.warning(
                "health server could not bind port %s; continuing without HTTP "
                "endpoints (metrics, logs and the CLI still work)",
                config.health_port,
                extra={"service": config.service, "category": type(exc).__name__},
            )

    # Reporting only: /config answers "what settings is this running on",
    # which is the question a dashboard cannot otherwise answer.
    monitor.attach_config(
        config,
        source=str(config_path) if config_path else os.getenv("WORKER_HEALTH_CONFIG"),
        instrumented=instrumented,
    )

    # 6. OTLP export, off unless an endpoint was configured.
    exporter = None
    if config.otel_endpoint:
        exporter = OTLPExporter(
            monitor,
            endpoint=config.otel_endpoint,
            interval=config.otel_interval,
            timeout=config.otel_timeout,
            max_queue=config.otel_max_queue,
            export_logs=config.otel_logs,
        )
        monitor.exporter = exporter
        if start:
            exporter.start()

    if start:
        monitor.start(boot_grace=config.boot_grace)

    monitor.events.emit(
        Event.WORKER_HEALTH_CONFIGURED,
        port=server.port if server else None,
        probes=[s.name for s in specs],
        instrumented=sorted(instrumented.values()),
        queue=config.default_queue,
        otel_endpoint=config.otel_endpoint or None,
    )

    return WorkerHealth(
        monitor=monitor, tracker=tracker, config=config, factory=factory,
        processing=processing, server=server, logger=logger,
        instrumented=instrumented, exporter=exporter,
    )


def _resolve_config(service, config_path, config, overrides) -> HealthConfig:
    """File, mapping or nothing -- then environment, then explicit arguments."""
    if isinstance(config, HealthConfig):
        resolved = config
    elif config is not None:
        resolved = HealthConfig.from_mapping(config)
        resolved = HealthConfig.from_env(base=resolved)
    else:
        path = config_path or os.getenv("WORKER_HEALTH_CONFIG")
        resolved = load_config(path)

    if service:
        resolved.service = service
    for key, value in overrides.items():
        if value is None:
            continue
        if not hasattr(resolved, key):
            raise TypeError(f"setup_worker_health() got an unexpected argument {key!r}")
        setattr(resolved, key, value)
    return resolved


def _build_restart_policy(settings: Mapping[str, Any], logger) -> RestartPolicy:
    from .core.model import ErrorCategory
    from .policy.restart import SELF_FAULTS

    triggers = settings.get("triggers")
    if triggers:
        resolved = set()
        for name in triggers:
            try:
                resolved.add(ErrorCategory(str(name)))
            except ValueError:
                continue
        triggers = resolved or SELF_FAULTS
    else:
        triggers = SELF_FAULTS

    return RestartPolicy(
        enabled=True,
        triggers=triggers,
        after_cycles=int(settings.get("after_cycles", 5)),
        min_uptime=float(settings.get("min_uptime", 120.0)),
        cooldown=float(settings.get("cooldown", 600.0)),
        max_per_hour=int(settings.get("max_per_hour", 3)),
        drain_timeout=float(settings.get("drain_timeout", 30.0)),
        exit_code=int(settings.get("exit_code", 70)),
        logger=logger,
    )
