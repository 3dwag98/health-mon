"""Kafka consumer health.

Modelled on the RabbitMQ adapter for the same reason: the interesting
failures are not "can I reach a broker" but "is THIS consumer still in the
group and still being given work".  A Kafka consumer that has been fenced
out of its group, or is stuck in a rebalance, or has had its partitions
revoked, keeps its TCP connections open and looks perfectly healthy to any
connectivity probe.

Client libraries differ (confluent-kafka, kafka-python, aiokafka) and none
of them is thread-safe, so this check never touches the consumer object
from the monitor thread.  The consumer's own loop writes into
``KafkaConsumerState``; the check reads it.  If the loop stops turning, the
state goes stale, and that staleness IS the finding -- the same property
that makes the RabbitMQ adapter able to see a wedged worker.
"""
from __future__ import annotations

import threading
import time

from ..core.model import CheckResult, ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


class KafkaConsumerState:
    """What the consumer's own loop reports about itself.

    Every setter is cheap and lock-free enough to call on the hot path:
    ``record_poll`` on every poll, ``record_delivery`` on every batch.
    """

    def __init__(self, group: str = "", topics: tuple[str, ...] = ()) -> None:
        self._lock = threading.Lock()
        self.group = group
        self.topics = tuple(topics)
        self.assignment: tuple[str, ...] = ()
        self.last_poll_at: float | None = None
        self.last_delivery_at: float | None = None
        self.last_commit_at: float | None = None
        self.rebalancing = False
        self.paused = False
        self.closed = False
        self.lag: int | None = None
        self.member_id: str = ""
        self.errors = 0
        self.last_category: ErrorCategory | None = None

    # -- written by the consumer loop ----------------------------------- #

    def record_poll(self, *, empty: bool = False) -> None:
        with self._lock:
            self.last_poll_at = time.monotonic()
            if not empty:
                self.last_delivery_at = self.last_poll_at

    def record_delivery(self, count: int = 1) -> None:
        if count <= 0:
            return
        with self._lock:
            self.last_delivery_at = time.monotonic()

    def record_commit(self) -> None:
        with self._lock:
            self.last_commit_at = time.monotonic()

    def record_error(self, category: ErrorCategory) -> None:
        with self._lock:
            self.errors += 1
            self.last_category = category

    def update(self, **fields) -> None:
        with self._lock:
            for key, value in fields.items():
                setattr(self, key, value)

    def read(self) -> dict:
        with self._lock:
            return {
                "group": self.group,
                "topics": self.topics,
                "assignment": self.assignment,
                "last_poll_at": self.last_poll_at,
                "last_delivery_at": self.last_delivery_at,
                "last_commit_at": self.last_commit_at,
                "rebalancing": self.rebalancing,
                "paused": self.paused,
                "closed": self.closed,
                "lag": self.lag,
                "member_id": self.member_id,
                "errors": self.errors,
                "last_category": self.last_category,
            }


class KafkaCheck(BaseCheck):
    """Entirely introspective, like the RabbitMQ adapter.

    ``lag_fn`` is optional and, when supplied, is called on the check's own
    thread -- so a caller who has a thread-safe way to read lag (an admin
    client, a metrics endpoint, cached watermarks) can plug it in without
    this module having to guess which client library is in use.
    """

    def __init__(
        self,
        state: KafkaConsumerState,
        *,
        name: str = "kafka",
        dependency: str = "kafka",
        max_lag: int = 10_000,
        stale_after: float = 30.0,
        max_rebalance: float = 60.0,
        lag_fn=None,
    ) -> None:
        self.name = name
        self.dependency = dependency
        self._state = state
        self._max_lag = int(max_lag)
        self._stale_after = float(stale_after)
        self._max_rebalance = float(max_rebalance)
        self._lag_fn = lag_fn
        self._rebalance_since: float | None = None

    def evaluate(self, ctx: CheckContext) -> CheckResult:
        started = time.perf_counter()
        s = self._state.read()

        lag = s["lag"]
        if self._lag_fn is not None:
            try:
                lag = self._lag_fn()
            except Exception:
                lag = s["lag"]      # a lag reader that fails is not an outage

        observed = {
            "group": s["group"],
            "partitions": len(s["assignment"]),
            "rebalancing": s["rebalancing"],
            "paused": s["paused"],
        }
        if lag is not None:
            observed["lag"] = int(lag)
        if s["topics"]:
            observed["topics"] = ",".join(s["topics"])

        age_ms = None
        if s["last_poll_at"] is not None:
            age_ms = round((ctx.now - s["last_poll_at"]) * 1000.0, 3)

        if s["closed"]:
            return self.fail(ctx, ErrorCategory.CONNECTION_LOST, started,
                             evidence=Evidence.INTROSPECTED,
                             detail="consumer is closed", **observed)

        if s["last_poll_at"] is None:
            return CheckResult(
                name=self.name, status=Status.UNKNOWN, checked_at=ctx.now,
                wall_clock=ctx.wall, evidence=Evidence.INTROSPECTED,
                latency_ms=_ms(started), evidence_age_ms=None,
                detail="no poll observed yet", observed=observed,
            )

        # The loop stopped turning.  Nothing else below can be trusted,
        # because every number in the state is as old as this.
        if (ctx.now - s["last_poll_at"]) > self._stale_after:
            return self.fail(ctx, ErrorCategory.STALLED, started,
                             evidence=Evidence.INTROSPECTED,
                             detail="consumer has not polled recently", **observed)

        # A rebalance is normal and brief.  One that never ends means the
        # member cannot rejoin -- usually a session timeout shorter than the
        # handler, which no connectivity check can see.
        if s["rebalancing"]:
            self._rebalance_since = self._rebalance_since or ctx.now
            if (ctx.now - self._rebalance_since) > self._max_rebalance:
                return self.fail(ctx, ErrorCategory.NOT_SUBSCRIBED, started,
                                 evidence=Evidence.INTROSPECTED,
                                 detail="stuck in rebalance", **observed)
            return self.degraded(ctx, ErrorCategory.NOT_SUBSCRIBED, started,
                                 evidence=Evidence.INTROSPECTED,
                                 detail="rebalancing", **observed)
        self._rebalance_since = None

        if not s["assignment"]:
            return self.fail(ctx, ErrorCategory.NOT_SUBSCRIBED, started,
                             evidence=Evidence.INTROSPECTED,
                             detail="consumer holds no partition assignment",
                             **observed)

        if s["paused"]:
            return self.degraded(ctx, ErrorCategory.CREDIT_EXHAUSTED, started,
                                 evidence=Evidence.INTROSPECTED,
                                 detail="consumer is paused", **observed)

        if lag is not None and int(lag) > self._max_lag:
            # Lag alone is degraded, never failing: a healthy consumer
            # catching up after a deploy has lag, and paging for it is how a
            # team learns to ignore the alert.
            return self.degraded(ctx, ErrorCategory.BACKLOG, started,
                                 evidence=Evidence.INTROSPECTED,
                                 detail="consumer lag above threshold", **observed)

        # Lag with no deliveries is the stuck-consumer signature; lag of
        # zero with no deliveries is just a quiet topic.
        if (lag or 0) > 0 and s["last_delivery_at"] is not None:
            since = ctx.now - s["last_delivery_at"]
            if since > self._stale_after:
                observed["seconds_since_delivery"] = round(since, 2)
                return self.fail(ctx, ErrorCategory.NOT_CONSUMING, started,
                                 evidence=Evidence.INTROSPECTED,
                                 detail="lag present but nothing delivered recently",
                                 **observed)

        return CheckResult(
            name=self.name, status=Status.OK, checked_at=ctx.now,
            wall_clock=ctx.wall, evidence=Evidence.INTROSPECTED,
            latency_ms=_ms(started), evidence_age_ms=age_ms, observed=observed,
        )


def classify_kafka(exc: BaseException) -> ErrorCategory:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    if "timed out" in text or "timeout" in name:
        return ErrorCategory.TIMEOUT
    if "authentication" in text or "sasl" in text or "authorization" in text:
        return ErrorCategory.AUTH_FAILED
    if "unknown topic" in text or "unknown_topic" in text:
        return ErrorCategory.RESOURCE_MISSING
    if "rebalance" in text or "not subscribed" in text:
        return ErrorCategory.NOT_SUBSCRIBED
    if "coordinator" in text or "broker transport failure" in text:
        return ErrorCategory.CONNECTION_LOST
    if "refused" in text:
        return ErrorCategory.CONNECTION_REFUSED
    return ErrorCategory.UNKNOWN


def _ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 3)
