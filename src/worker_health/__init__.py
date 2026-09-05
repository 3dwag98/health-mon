"""worker-health: operational health for message-driven Python workers.

Observation first.  Health is derived from the worker's OWN connections and
its real traffic wherever possible; a synthetic probe is a labelled fallback
for when the worker has been silent, never the primary signal.

The whole integration, for a worker team:

    from worker_health import setup_worker_health

    health = setup_worker_health(
        service="billing-worker",
        config_path="worker-health.yaml",
        context={"db_engine": engine, "redis_client": redis,
                 "broker_state": broker_state},
    )

    @health.tracker.handler(queue="billing.in")
    def handle(message: dict):
        process_payment(message)
"""
from .checks.base import CheckContext, TrafficLog
from .checks.custom import CustomCheck
from .checks.django_db import DjangoDbCheck
from .checks.kafka import KafkaCheck, KafkaConsumerState, classify_kafka
from .checks.network import DnsProbe, HttpProbe, TcpProbe
from .checks.postgres import PostgresCheck, classify_postgres
from .checks.processing import ProcessingCheck, ProcessingState
from .checks.rabbitmq import BrokerState, RabbitMQCheck, classify_amqp, install_broker_probe
from .checks.redis_ import RedisCheck, build_client, classify_redis
from .checks.system import DiskSpaceProbe, FileAgeProbe
from .config import HealthConfig, load_config
from .core.clock import FakeClock, MonotonicClock
from .core.machine import CheckSpec, StateMachine
from .core.model import (
    LIVE_CODE,
    LIVENESS_CODE,
    READINESS_CODE,
    READINESS_FROM_STATUS,
    READY_CODE,
    SEVERITY,
    WIRE,
    CheckResult,
    ErrorCategory,
    Evidence,
    Liveness,
    Readiness,
    Snapshot,
    Status,
)
from .instrument import is_health_probe_active, probe_scope
from .monitor import HealthMonitor
from .policy.restart import DEPENDENCY_FAULTS, SELF_FAULTS, RestartPolicy
from .probes import ProbeConfigError, ProbeFactory, ProbeSpec, default_factory
from .security import redact, redact_dsn
from .setup import WorkerHealth, setup_worker_health
from .telemetry.events import Event, EventEmitter
from .track import Tracker
from .transports.http import HealthServer

__version__ = "0.2.0"

__all__ = [
    # the one-call entry point
    "setup_worker_health", "WorkerHealth",
    # core objects
    "HealthMonitor", "HealthServer", "Tracker", "RestartPolicy",
    "HealthConfig", "load_config",
    # probes and the factory
    "ProbeFactory", "ProbeSpec", "ProbeConfigError", "default_factory",
    # checks
    "PostgresCheck", "RedisCheck", "RabbitMQCheck", "ProcessingCheck",
    "CustomCheck", "DjangoDbCheck", "KafkaCheck", "HttpProbe", "TcpProbe",
    "DnsProbe", "DiskSpaceProbe", "FileAgeProbe",
    # state carried by the worker
    "ProcessingState", "BrokerState", "KafkaConsumerState", "TrafficLog",
    "CheckContext", "install_broker_probe", "build_client",
    # classification
    "classify_postgres", "classify_redis", "classify_amqp", "classify_kafka",
    # vocabulary
    "Status", "Readiness", "Liveness", "Evidence", "ErrorCategory",
    "CheckResult", "Snapshot", "CheckSpec", "StateMachine",
    "MonotonicClock", "FakeClock",
    "SEVERITY", "WIRE", "LIVE_CODE", "READY_CODE", "READINESS_CODE",
    "LIVENESS_CODE", "READINESS_FROM_STATUS",
    "SELF_FAULTS", "DEPENDENCY_FAULTS",
    # telemetry and safety
    "Event", "EventEmitter", "redact", "redact_dsn",
    "is_health_probe_active", "probe_scope",
    "__version__",
]
