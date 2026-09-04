"""RabbitMQ check.

Two verified facts shape this adapter.

1. A passive declare against a missing queue closes the channel it was
   issued on, while leaving the connection open.  A probe sharing the
   consumer's channel therefore stops the worker consuming -- the health
   check becomes the outage.  So the probe gets its own channel.

2. ``pika.BlockingConnection`` is not thread-safe, so a monitor thread
   cannot open that channel itself.  Instead the probe is driven by
   ``connection.call_later`` on the connection's OWN thread: no new
   connection, no cross-thread access, and the worker's real connection is
   what gets observed.

The useful consequence: if the worker's loop wedges, ``call_later`` stops
firing, the broker state goes stale, and the check reports it.  A dedicated
monitor connection would have kept reporting a cheerful green.

``/api/aliveness-test`` is deliberately never used.  Verified on RabbitMQ
3.13: it declares a queue, publishes a message and consumes it, which
violates the non-destructive guardrail outright.  It is a no-op only from
4.0 onward.
"""
from __future__ import annotations

import threading
import time

from ..core.model import ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


class BrokerState:
    """Everything known about the worker's own broker connection.

    Written exclusively from the connection's own thread; read from the
    monitor thread.  Python attribute assignment is atomic and every field
    is a scalar, so a lock is needed only for the compound read.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.connection_open = False
        self.channel_open = False
        self.consumer_tags: tuple[str, ...] = ()
        self.queue_depth: int | None = None
        self.consumer_count: int | None = None
        self.unacked = 0
        self.prefetch = 0
        self.last_probe_at: float | None = None
        self.last_delivery_at: float | None = None
        self.probe_error: ErrorCategory | None = None
        self.probe_channel_reopens = 0
        self.reconnects = 0

    def read(self) -> dict:
        with self._lock:
            return {
                "connection_open": self.connection_open,
                "channel_open": self.channel_open,
                "consumer_tags": self.consumer_tags,
                "queue_depth": self.queue_depth,
                "consumer_count": self.consumer_count,
                "unacked": self.unacked,
                "prefetch": self.prefetch,
                "last_probe_at": self.last_probe_at,
                "last_delivery_at": self.last_delivery_at,
                "probe_error": self.probe_error,
                "probe_channel_reopens": self.probe_channel_reopens,
                "reconnects": self.reconnects,
            }

    def update(self, **fields) -> None:
        with self._lock:
            for k, v in fields.items():
                setattr(self, k, v)


class RabbitMQCheck(BaseCheck):
    def __init__(
        self,
        state: BrokerState,
        *,
        queue: str,
        name: str = "rabbitmq",
        dependency: str = "rabbitmq",
        backlog_threshold: int = 1000,
        stale_after: float = 20.0,
    ) -> None:
        self.name = name
        self.dependency = dependency
        self.queue = queue
        self._state = state
        self._backlog_threshold = backlog_threshold
        self._stale_after = stale_after

    def evaluate(self, ctx: CheckContext):
        """Entirely introspective.

        Every signal here was gathered by the worker's own connection on its
        own thread.  There is no synthetic path and no fallback probe -- if
        the worker is not talking to the broker, that IS the finding.
        """
        started = time.perf_counter()
        s = self._state.read()

        observed = {
            "queue": self.queue,
            "connection_open": s["connection_open"],
            "channel_open": s["channel_open"],
            "consumers": len(s["consumer_tags"]),
            "unacked": s["unacked"],
            "prefetch": s["prefetch"],
        }
        if s["queue_depth"] is not None:
            observed["queue_depth"] = s["queue_depth"]
        if s["consumer_count"] is not None:
            observed["queue_consumers"] = s["consumer_count"]

        age_ms = None
        if s["last_probe_at"] is not None:
            age_ms = round((ctx.now - s["last_probe_at"]) * 1000.0, 3)

        if not s["connection_open"]:
            return self.fail(
                ctx, ErrorCategory.CONNECTION_LOST, started,
                evidence=Evidence.INTROSPECTED,
                detail="worker's broker connection is closed", **observed,
            )

        if s["probe_error"] is not None:
            return self.fail(
                ctx, s["probe_error"], started, evidence=Evidence.INTROSPECTED,
                detail="broker probe failed on the worker's connection", **observed,
            )

        # Nothing has updated the state in a while.  With call_later driving
        # the probe from the connection's own thread, that means the loop is
        # not turning -- the worker is wedged.
        if s["last_probe_at"] is None:
            return self._starting(ctx, started, observed, age_ms)
        if (ctx.now - s["last_probe_at"]) > self._stale_after:
            return self.fail(
                ctx, ErrorCategory.STALLED, started, evidence=Evidence.INTROSPECTED,
                detail="broker state is stale; the worker loop is not turning",
                **observed,
            )

        if not s["consumer_tags"]:
            return self.fail(
                ctx, ErrorCategory.NOT_SUBSCRIBED, started,
                evidence=Evidence.INTROSPECTED,
                detail="connection is open but nothing is subscribed", **observed,
            )

        depth = s["queue_depth"]

        # Credit exhaustion: every prefetch slot is held by an unacked
        # message, so the broker will send nothing more.  From inside the
        # process this is indistinguishable from an idle queue unless you
        # compare against QoS -- which is the whole reason prefetch is here.
        if s["prefetch"] and s["unacked"] >= s["prefetch"] and (depth or 0) > 0:
            return self.fail(
                ctx, ErrorCategory.CREDIT_EXHAUSTED, started,
                evidence=Evidence.INTROSPECTED,
                detail="all prefetch credit is held by unacked messages",
                **observed,
            )

        if depth is not None and depth > self._backlog_threshold:
            return self.degraded(
                ctx, ErrorCategory.BACKLOG, started, evidence=Evidence.INTROSPECTED,
                detail="queue depth above threshold", **observed,
            )

        # The discrimination this package exists for.  Depth 0 with no
        # deliveries is a QUIET queue and must never alert -- a false alert
        # here is what teaches a team to ignore the system.  Depth > 0 with
        # no deliveries is a STUCK consumer.
        if depth is not None and depth > 0 and s["last_delivery_at"] is not None:
            since_delivery = ctx.now - s["last_delivery_at"]
            if since_delivery > self._stale_after:
                observed["seconds_since_delivery"] = round(since_delivery, 2)
                return self.fail(
                    ctx, ErrorCategory.NOT_CONSUMING, started,
                    evidence=Evidence.INTROSPECTED,
                    detail="messages are queued but none are being delivered",
                    **observed,
                )

        return CheckResultOK(self, ctx, started, age_ms, observed)

    def _starting(self, ctx, started, observed, age_ms):
        from ..core.model import CheckResult
        return CheckResult(
            name=self.name, status=Status.UNKNOWN, checked_at=ctx.now,
            wall_clock=ctx.wall, evidence=Evidence.INTROSPECTED,
            latency_ms=round((time.perf_counter() - started) * 1000, 3),
            evidence_age_ms=age_ms, detail="no broker observation yet",
            observed=observed,
        )


def CheckResultOK(check, ctx, started, age_ms, observed):
    from ..core.model import CheckResult
    return CheckResult(
        name=check.name, status=Status.OK, checked_at=ctx.now, wall_clock=ctx.wall,
        evidence=Evidence.INTROSPECTED,
        latency_ms=round((time.perf_counter() - started) * 1000, 3),
        evidence_age_ms=age_ms, observed=observed,
    )


def classify_amqp(exc: BaseException) -> ErrorCategory:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    code = getattr(exc, "reply_code", None)

    if code == 404 or "no queue" in text or "not_found" in text:
        return ErrorCategory.RESOURCE_MISSING
    if code in (403, 530) or "access refused" in text or "acces_refused" in text:
        return ErrorCategory.AUTH_FAILED
    if "probableauthenticationerror" in name or "login was refused" in text:
        return ErrorCategory.AUTH_FAILED
    if "connectionclosedbybroker" in name or "shutdown" in text:
        return ErrorCategory.BROKER_SHUTDOWN
    if "heartbeat" in text:
        return ErrorCategory.HEARTBEAT_TIMEOUT
    if "timeout" in name or "timed out" in text:
        return ErrorCategory.TIMEOUT
    if "connection refused" in text or "econnrefused" in text:
        return ErrorCategory.CONNECTION_REFUSED
    if "resource_locked" in text or "exclusive" in text:
        return ErrorCategory.RESOURCE_LOCKED
    if "channelclosed" in name or "connectionclosed" in name:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.UNKNOWN


def install_broker_probe(connection, state: BrokerState, queue: str,
                         interval: float = 2.0, logger=None):
    """Drive the broker probe from the connection's OWN thread.

    ``call_later`` re-arms itself, so the probe rides the worker's existing
    I/O loop: no new connection, no cross-thread access to a
    BlockingConnection, and no risk of the health check interfering with
    consumption.  When the loop stops turning, the probe stops updating and
    the staleness is itself the signal.
    """
    holder: dict = {"channel": None}

    def _ensure_channel():
        ch = holder["channel"]
        if ch is not None and ch.is_open:
            return ch
        ch = connection.channel()
        holder["channel"] = ch
        state.update(probe_channel_reopens=state.read()["probe_channel_reopens"] + 1)
        return ch

    def _tick():
        try:
            if not connection.is_open:
                state.update(connection_open=False)
                return
            ch = _ensure_channel()
            # Passive declare returns depth and consumer count over plain
            # AMQP -- no management plugin required.
            res = ch.queue_declare(queue=queue, passive=True)
            state.update(
                connection_open=True,
                queue_depth=res.method.message_count,
                consumer_count=res.method.consumer_count,
                last_probe_at=time.monotonic(),
                probe_error=None,
            )
        except Exception as exc:  # noqa: BLE001
            holder["channel"] = None   # a 404 closed it; never reuse
            state.update(
                probe_error=classify_amqp(exc),
                last_probe_at=time.monotonic(),
                connection_open=bool(getattr(connection, "is_open", False)),
            )
            if logger is not None:
                logger.warning("broker probe failed", extra={"category":
                               classify_amqp(exc).value})
        finally:
            try:
                connection.call_later(interval, _tick)
            except Exception:
                pass

    connection.call_later(interval, _tick)
    return holder
