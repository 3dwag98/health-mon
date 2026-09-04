"""L0: auto-instrumentation, with fake drivers.

Fakes rather than real clients on purpose: this tier must pass with the
docker daemon stopped, and what is being tested is the SDK's bookkeeping,
not redis-py's.  The shapes below match the real methods that get patched.
"""
from __future__ import annotations

import pytest

from worker_health import BrokerState, ErrorCategory, HealthMonitor, Status
from worker_health.checks.base import CheckContext
from worker_health.instrument import is_health_probe_active, probe_scope
from worker_health.instrument.pika_ import instrument_pika_channel, instrument_pika_connection
from worker_health.instrument.recorder import TrafficRecorder, recorder_for
from worker_health.instrument.redis_ import instrument_redis_sync, uninstrument_redis

pytestmark = pytest.mark.unit


def _monitor():
    return HealthMonitor("test", instance="test-1")


# -- probe suppression ------------------------------------------------------- #

def test_probe_scope_is_off_by_default_and_restores_itself():
    assert is_health_probe_active() is False
    with probe_scope():
        assert is_health_probe_active() is True
    assert is_health_probe_active() is False


def test_traffic_from_a_probe_is_not_recorded():
    """The core of the evidence model.  If a probe's own SELECT 1 counted as
    traffic, a silent worker would report `observed` forever on the strength
    of nothing but its own health checks."""
    monitor = _monitor()
    recorder = TrafficRecorder(monitor, "postgres")

    with probe_scope():
        recorder.success(1.0)
        recorder.failure(RuntimeError("boom"))

    assert monitor.traffic.get("postgres") is None

    recorder.success(2.0)
    assert monitor.traffic.get("postgres").successes == 1


def test_the_check_ladder_suppresses_its_own_probe():
    from worker_health.checks.base import BaseCheck

    monitor = _monitor()
    seen = {}

    class Probe(BaseCheck):
        name = "dep"
        dependency = "dep"

        def probe(self, ctx):
            seen["active"] = is_health_probe_active()
            import time
            return self.ok(ctx, time.perf_counter())

    ctx = CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                       traffic=monitor.traffic)
    Probe().evaluate(ctx)
    assert seen["active"] is True


def test_the_recorder_classifies_by_dependency_name():
    monitor = _monitor()
    recorder_for(monitor, "postgres").failure(Exception("connection refused"))
    recorder_for(monitor, "redis-cache").failure(Exception("NOAUTH required"))

    assert monitor.traffic.get("postgres").last_category is ErrorCategory.CONNECTION_REFUSED
    assert monitor.traffic.get("redis-cache").last_category is ErrorCategory.AUTH_FAILED


def test_latency_reaches_the_timing_windows_as_well_as_the_traffic_log():
    monitor = _monitor()
    TrafficRecorder(monitor, "postgres").success(4.0)
    summary = monitor.timings.summary("dependency.postgres.duration_ms")
    assert summary["count"] == 1 and summary["max_ms"] == 4.0


# -- redis-py ---------------------------------------------------------------- #

class FakeRedis:
    """The one method every redis-py call funnels through."""

    def __init__(self, fail=None):
        self.connection_pool = type("Pool", (), {"max_connections": 10})()
        self.calls = 0
        self._fail = fail

    def execute_command(self, *args, **kwargs):
        self.calls += 1
        if self._fail is not None:
            raise self._fail
        return "OK"


@pytest.fixture(autouse=True)
def _restore_fake_redis():
    yield
    uninstrument_redis(FakeRedis)


def test_redis_commands_become_observed_traffic():
    monitor = _monitor()
    client = FakeRedis()
    instrument_redis_sync(client, monitor, "redis")

    client.execute_command("GET", "k")
    client.execute_command("SET", "k", "v")

    record = monitor.traffic.get("redis")
    assert record.successes == 2 and record.failures == 0
    assert client.calls == 2                    # the real call still happened


def test_a_failed_command_is_classified_and_re_raised():
    monitor = _monitor()
    client = FakeRedis(fail=TimeoutError("Timeout reading from socket"))
    instrument_redis_sync(client, monitor, "redis")

    with pytest.raises(TimeoutError):
        client.execute_command("GET", "k")

    record = monitor.traffic.get("redis")
    assert record.failures == 1
    assert record.last_category is ErrorCategory.TIMEOUT


def test_two_clients_report_under_their_own_names():
    """The patch is on the class; the target is read off the instance, so a
    cache client and a lock client stay distinguishable."""
    monitor = _monitor()
    cache, locks = FakeRedis(), FakeRedis()
    instrument_redis_sync(cache, monitor, "redis-cache")
    instrument_redis_sync(locks, monitor, "redis-locks")

    cache.execute_command("GET", "k")
    locks.execute_command("SET", "lock", "1")

    assert monitor.traffic.get("redis-cache").successes == 1
    assert monitor.traffic.get("redis-locks").successes == 1


def test_an_unregistered_client_is_passed_straight_through():
    monitor = _monitor()
    registered, other = FakeRedis(), FakeRedis()
    instrument_redis_sync(registered, monitor, "redis")

    assert other.execute_command("PING") == "OK"
    assert monitor.traffic.get("redis") is None


def test_instrumenting_twice_does_not_double_count():
    """Nested wrappers would also make the unpatch un-doable."""
    monitor = _monitor()
    client = FakeRedis()
    instrument_redis_sync(client, monitor, "redis")
    instrument_redis_sync(client, monitor, "redis")

    client.execute_command("GET", "k")
    assert monitor.traffic.get("redis").successes == 1


# -- pika -------------------------------------------------------------------- #

class FakeChannel:
    def __init__(self):
        self.consumed = None
        self.acked = 0

    def basic_consume(self, queue, on_message_callback=None, **kwargs):
        self.consumed = on_message_callback
        return "ctag-1"

    def basic_qos(self, prefetch_count=None, **kwargs):
        return None

    def basic_ack(self, delivery_tag, **kwargs):
        self.acked += 1

    def basic_nack(self, delivery_tag, **kwargs):
        return None

    def basic_publish(self, *args, **kwargs):
        return None


class FakeConnection:
    is_open = True

    def __init__(self):
        self.closed_cb = None
        self.blocked_cb = None
        self.unblocked_cb = None

    def add_on_connection_closed_callback(self, cb):
        self.closed_cb = cb

    def add_on_connection_blocked_callback(self, cb):
        self.blocked_cb = cb

    def add_on_connection_unblocked_callback(self, cb):
        self.unblocked_cb = cb


def test_delivery_and_ack_bookkeeping_needs_no_handler_code():
    """The hand-written version of this is where the classic bug lives: one
    path through the callback forgets to decrement, unacked drifts up past
    prefetch, and the worker reports credit exhaustion while perfectly fine."""
    monitor = _monitor()
    state = BrokerState()
    channel = FakeChannel()
    instrument_pika_channel(channel, monitor, state)

    channel.basic_qos(prefetch_count=10)
    handled = []
    channel.basic_consume("billing.in", on_message_callback=lambda *a: handled.append(a))

    assert state.read()["prefetch"] == 10
    assert state.read()["consumer_tags"] == ("ctag-1",)
    assert state.read()["consumer_state"] == "consuming"

    channel.consumed(channel, object(), None, b"{}")
    assert len(handled) == 1
    assert state.read()["unacked"] == 1
    assert state.read()["last_delivery_at"] is not None

    channel.basic_ack(1)
    assert state.read()["unacked"] == 0
    assert state.read()["last_ack_at"] is not None
    assert channel.acked == 1


def test_a_nack_settles_the_message_too():
    monitor = _monitor()
    state = BrokerState()
    channel = FakeChannel()
    instrument_pika_channel(channel, monitor, state)
    channel.basic_consume("q", on_message_callback=lambda *a: None)

    channel.consumed(channel, object(), None, b"{}")
    channel.basic_nack(1)
    assert state.read()["unacked"] == 0


def test_a_blocked_connection_is_its_own_category():
    """The broker is up and has told us to stop publishing. Reporting that as
    `connection_lost` sends someone to look at the network instead of at the
    broker's disk alarm."""
    monitor = _monitor()
    state = BrokerState()
    connection = FakeConnection()
    instrument_pika_connection(connection, monitor, state, "rabbitmq")

    connection.blocked_cb(connection, None)
    assert state.read()["blocked"] is True
    assert monitor.traffic.get("rabbitmq").last_category is ErrorCategory.BROKER_ALARM

    connection.unblocked_cb(connection, None)
    assert state.read()["blocked"] is False


def test_a_blocked_broker_makes_the_check_fail_with_the_alarm_category():
    from worker_health import RabbitMQCheck

    state = BrokerState()
    state.update(connection_open=True, channel_open=True, blocked=True,
                 consumer_tags=("t",), last_probe_at=100.0, queue_depth=0)
    ctx = CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                       traffic=_monitor().traffic)
    result = RabbitMQCheck(state, queue="billing.in").evaluate(ctx)
    assert result.status is Status.FAILING
    assert result.category is ErrorCategory.BROKER_ALARM


def test_a_closed_connection_is_recorded_as_a_failure():
    monitor = _monitor()
    state = BrokerState()
    connection = FakeConnection()
    instrument_pika_connection(connection, monitor, state, "rabbitmq")

    connection.closed_cb(connection, "ConnectionClosedByBroker: shutdown")
    assert state.read()["connection_open"] is False
    assert monitor.traffic.get("rabbitmq").failures == 1


class FakeBlockingConnection:
    """pika's BlockingConnection shape: no closed callback, by design.

    It raises on a closed connection instead of calling back, so detecting
    a pika connection by that one method misses the most common worker shape
    entirely -- which is what this test exists to prevent regressing.
    """

    is_open = True

    def __init__(self):
        self.blocked_cb = None
        self.unblocked_cb = None

    def add_on_connection_blocked_callback(self, cb):
        self.blocked_cb = cb

    def add_on_connection_unblocked_callback(self, cb):
        self.unblocked_cb = cb


def test_a_blocking_connection_is_detected_and_instrumented():
    from worker_health.instrument import autowire_context

    monitor = _monitor()
    state = BrokerState()
    connection = FakeBlockingConnection()

    wired = autowire_context(monitor, {"amqp_connection": connection,
                                       "broker_state": state})

    assert wired == {"amqp_connection": "rabbitmq"}
    assert connection.blocked_cb is not None
    assert state.read()["connection_open"] is True

    connection.blocked_cb(connection, None)
    assert monitor.traffic.get("rabbitmq").last_category is ErrorCategory.BROKER_ALARM


def test_autowiring_survives_a_client_it_cannot_instrument():
    """A driver version that moved the patched method must not stop a boot."""
    from worker_health.instrument import autowire_context

    class Odd:
        connection_pool = None
        execute_command = None

    assert autowire_context(_monitor(), {"weird": Odd(), "nothing": None}) == {}
