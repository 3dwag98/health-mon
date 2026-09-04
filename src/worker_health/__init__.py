"""worker-health: operational health for message-driven Python workers.

Observation first.  Health is derived from the worker's OWN connections and
its real traffic wherever possible; a synthetic probe is a labelled fallback
for when the worker has been silent, never the primary signal.
"""
from .checks.base import CheckContext, TrafficLog
from .checks.custom import CustomCheck
from .checks.postgres import PostgresCheck, classify_postgres
from .checks.processing import ProcessingCheck, ProcessingState
from .checks.rabbitmq import BrokerState, RabbitMQCheck, classify_amqp, install_broker_probe
from .checks.redis_ import RedisCheck, build_client, classify_redis
from .core.clock import FakeClock, MonotonicClock
from .core.machine import CheckSpec, StateMachine
from .core.model import (
    LIVE_CODE,
    READY_CODE,
    SEVERITY,
    WIRE,
    CheckResult,
    ErrorCategory,
    Evidence,
    Snapshot,
    Status,
)
from .monitor import HealthMonitor
from .policy.restart import DEPENDENCY_FAULTS, SELF_FAULTS, RestartPolicy
from .track import Tracker
from .transports.http import HealthServer

__version__ = "0.1.0"

__all__ = [
    "HealthMonitor", "HealthServer", "Tracker", "RestartPolicy",
    "PostgresCheck", "RedisCheck", "RabbitMQCheck", "ProcessingCheck", "CustomCheck",
    "ProcessingState", "BrokerState", "TrafficLog", "CheckContext",
    "install_broker_probe", "build_client",
    "classify_postgres", "classify_redis", "classify_amqp",
    "Status", "Evidence", "ErrorCategory", "CheckResult", "Snapshot",
    "CheckSpec", "StateMachine", "MonotonicClock", "FakeClock",
    "SEVERITY", "WIRE", "LIVE_CODE", "READY_CODE",
    "SELF_FAULTS", "DEPENDENCY_FAULTS", "__version__",
]
