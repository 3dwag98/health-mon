"""RabbitMQ (pika) auto-instrumentation.

A broker connection is not like a database pool: nothing "returns an error"
when it dies.  The socket closes, or the broker raises a resource alarm and
simply stops delivering, and the consumer sits there looking busy.  So the
signals here are lifecycle callbacks and delivery bookkeeping rather than
call outcomes.

What this removes from worker code is the hand-written pair every pika
consumer otherwise grows:

    broker.update(last_delivery_at=time.monotonic())
    unacked += 1                       # ... and the matching decrement,
                                       #     in a finally, on every path

Missing one of those decrements is a real and common bug: unacked drifts
upward, eventually crosses prefetch, and the health check declares credit
exhaustion on a worker that is perfectly fine.  Wrapping ``basic_consume``
and the ack/nack methods keeps the two sides of that count together.
"""
from __future__ import annotations

import functools
import time

from ..checks.rabbitmq import classify_amqp
from ..core.model import ErrorCategory
from .recorder import TrafficRecorder


def instrument_pika_connection(connection, monitor, state=None,
                               dependency_name: str = "rabbitmq"):
    """Attach the connection lifecycle callbacks.

    ``blocked`` deserves its own category: the broker is up, the connection
    is open, and it has told us to stop publishing because it is out of
    memory or disk.  Reporting that as `connection_lost` sends someone to
    look at the network instead of at the broker's disk alarm.
    """
    recorder = TrafficRecorder(monitor, dependency_name, classify_amqp)

    def on_closed(_conn, reason=None):
        if state is not None:
            state.update(connection_open=False, channel_state="closed")
        recorder.failure(category=_closed_category(reason))

    def on_blocked(_conn, _method=None):
        if state is not None:
            state.update(blocked=True)
        recorder.failure(category=ErrorCategory.BROKER_ALARM)

    def on_unblocked(_conn, _method=None):
        if state is not None:
            state.update(blocked=False)
        # An unblock is a recovery, and recording it as a success is what
        # lets the traffic-backed evidence return to OK without waiting for
        # the next probe.
        recorder.success(0.0)

    # Registered individually because the connection classes differ:
    # BlockingConnection has the blocked/unblocked pair and no closed
    # callback (it raises instead); SelectConnection has all three.  A
    # missing one is normal, not an error.
    for register, callback in (
        ("add_on_connection_closed_callback", on_closed),
        ("add_on_connection_blocked_callback", on_blocked),
        ("add_on_connection_unblocked_callback", on_unblocked),
    ):
        hook = getattr(connection, register, None)
        if hook is None:
            continue
        try:
            hook(callback)
        except Exception:
            pass

    if state is not None:
        state.update(connection_open=bool(getattr(connection, "is_open", True)))
    return connection


def instrument_pika_channel(channel, monitor, state, *,
                            dependency_name: str = "rabbitmq"):
    """Track deliveries, acks and prefetch without touching handler code.

    Wraps four methods on the channel INSTANCE (not the class), so a probe
    channel on the same connection is unaffected and keeps issuing its
    passive declares without being counted as consumption.
    """
    if getattr(channel, "_worker_health_instrumented", False):
        return channel

    recorder = TrafficRecorder(monitor, dependency_name, classify_amqp)
    counters = {"unacked": 0}

    original_consume = channel.basic_consume
    original_qos = channel.basic_qos
    original_ack = channel.basic_ack
    original_nack = getattr(channel, "basic_nack", None)
    original_reject = getattr(channel, "basic_reject", None)
    original_publish = channel.basic_publish

    def _delivered() -> None:
        counters["unacked"] += 1
        state.update(last_delivery_at=time.monotonic(), unacked=counters["unacked"],
                     channel_state="open")
        recorder.success(0.0)

    def _settled() -> None:
        counters["unacked"] = max(0, counters["unacked"] - 1)
        state.update(last_ack_at=time.monotonic(), unacked=counters["unacked"])

    @functools.wraps(original_consume)
    def basic_consume(queue, on_message_callback=None, *args, **kwargs):
        wrapped = on_message_callback

        if on_message_callback is not None:
            @functools.wraps(on_message_callback)
            def wrapped(ch, method, properties, body):      # noqa: F811
                _delivered()
                return on_message_callback(ch, method, properties, body)

        tag = original_consume(queue, wrapped, *args, **kwargs)
        existing = state.read()["consumer_tags"]
        state.update(
            consumer_tags=tuple(existing) + (tag,),
            consumer_state="consuming",
            channel_state="open",
        )
        return tag

    @functools.wraps(original_qos)
    def basic_qos(*args, **kwargs):
        out = original_qos(*args, **kwargs)
        prefetch = kwargs.get("prefetch_count")
        if prefetch is None and args:
            prefetch = args[0]
        if prefetch is not None:
            # The health check compares unacked against prefetch to detect
            # credit exhaustion; reading it from the call is the only way to
            # be sure the two numbers refer to the same QoS setting.
            state.update(prefetch=int(prefetch))
        return out

    @functools.wraps(original_ack)
    def basic_ack(*args, **kwargs):
        out = original_ack(*args, **kwargs)
        _settled()
        return out

    channel.basic_consume = basic_consume
    channel.basic_qos = basic_qos
    channel.basic_ack = basic_ack

    if original_nack is not None:
        @functools.wraps(original_nack)
        def basic_nack(*args, **kwargs):
            out = original_nack(*args, **kwargs)
            _settled()
            return out
        channel.basic_nack = basic_nack

    if original_reject is not None:
        @functools.wraps(original_reject)
        def basic_reject(*args, **kwargs):
            out = original_reject(*args, **kwargs)
            _settled()
            return out
        channel.basic_reject = basic_reject

    @functools.wraps(original_publish)
    def basic_publish(*args, **kwargs):
        started = time.perf_counter()
        try:
            out = original_publish(*args, **kwargs)
        except Exception as exc:
            recorder.failure(exc)
            raise
        recorder.success((time.perf_counter() - started) * 1000.0)
        return out

    channel.basic_publish = basic_publish
    channel._worker_health_instrumented = True
    state.update(channel_state="open")
    return channel


def _closed_category(reason) -> ErrorCategory:
    if reason is None:
        return ErrorCategory.CONNECTION_LOST
    return classify_amqp(reason if isinstance(reason, BaseException) else Exception(str(reason)))
