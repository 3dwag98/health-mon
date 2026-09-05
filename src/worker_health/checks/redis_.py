"""Redis check, pinned for Redis 7.2.

7.2 speaks RESP3 and implements HELLO, so the protocol negotiation that
older servers choked on is a non-issue here: redis-py may use either
protocol and every command works.  ``build_client`` therefore leaves the
protocol at the client's default and spends its attention on the setting
that actually breaks health checks -- socket timeouts.

A client with ``socket_timeout=None`` (redis-py's default) hangs forever on
a black-holed connection.  That is the failure mode this package exists to
survive, and a probe that hangs is worse than no probe: it holds a pool
slot, it never reports, and the check ages into `unknown` instead of the
`timeout` it actually was.  So the helper always sets one.

``DEPENDENCY_VERSION`` remains in the taxonomy: a caller-supplied client
pointed at an older server, or one with a protocol mismatch, is reported as
a version problem rather than `connection_refused`, because the remediation
is a client setting and not a network to investigate.
"""
from __future__ import annotations

import time

from ..core.model import ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


def build_client(url: str = "", *, host="localhost", port=6379, db=0,
                 password=None, socket_timeout=1.0, socket_connect_timeout=1.0,
                 protocol: int | None = None, **kwargs):
    """Construct a redis client with health-check-safe defaults.

    Never leave socket timeouts as None: without them a black-holed
    connection hangs the probe forever.  ``retry_on_timeout=False`` for the
    same reason -- a health probe must report a timeout, not silently retry
    past it and report the second attempt's result as if it were the first.

    ``protocol`` is optional.  Pass 2 when talking to a server older than
    6.0, which has no HELLO command.
    """
    import redis as redis_lib

    options = dict(
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        retry_on_timeout=False,
        # Client-side keepalive pings would show up in the traffic log as
        # the worker's own activity, which is exactly the false evidence
        # this package is built to avoid.
        health_check_interval=0,
    )
    if protocol is not None:
        options["protocol"] = protocol
    options.update(kwargs)

    if url:
        return redis_lib.Redis.from_url(url, **options)
    return redis_lib.Redis(host=host, port=port, db=db, password=password, **options)


class RedisCheck(BaseCheck):
    def __init__(
        self,
        client,
        *,
        name: str = "redis",
        dependency: str = "redis",
        label: str = "",
        memory_warn_ratio: float = 0.9,
    ) -> None:
        self.name = name
        self.dependency = dependency
        self._client = client
        self._label = label
        self._memory_warn_ratio = memory_warn_ratio

    def introspect(self, ctx: CheckContext):
        """Pool counters only.  No command is sent."""
        started = time.perf_counter()
        try:
            pool = self._client.connection_pool
            created = len(getattr(pool, "_created_connections", []) or []) or \
                getattr(pool, "_created_connections", 0)
            available = len(getattr(pool, "_available_connections", []) or [])
            in_use = len(getattr(pool, "_in_use_connections", []) or [])
        except Exception:
            return None
        observed = {"pool_in_use": in_use, "pool_available": available}
        if isinstance(created, int):
            observed["pool_created"] = created
        max_conn = getattr(self._client.connection_pool, "max_connections", None)
        if isinstance(max_conn, int) and max_conn > 0 and in_use >= max_conn:
            return self.degraded(
                ctx, ErrorCategory.POOL_EXHAUSTED, started,
                detail="redis client pool has no free connections", **observed,
            )
        return self.ok(ctx, started, evidence=Evidence.INTROSPECTED, **observed)

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        self._client.ping()
        info = self._client.info("memory")
        rep = self._client.info("replication")

        used = int(info.get("used_memory", 0))
        maxmem = int(info.get("maxmemory", 0) or 0)
        role = str(rep.get("role", "unknown"))

        observed = {"used_memory_bytes": used, "role": role}
        if self._label:
            observed["label"] = self._label
        if maxmem:
            ratio = used / maxmem
            observed["maxmemory_bytes"] = maxmem
            observed["memory_ratio"] = round(ratio, 4)
            if ratio >= self._memory_warn_ratio:
                # Approaching maxmemory means evictions, and on a worker
                # that uses Redis for idempotency keys or locks an eviction
                # is a correctness problem, not a performance one.
                return self.degraded(
                    ctx, ErrorCategory.MEMORY_PRESSURE, started,
                    evidence=Evidence.PROBED,
                    detail="used_memory is close to maxmemory", **observed,
                )
        return self.ok(ctx, started, **observed)

    def classify(self, exc: BaseException) -> ErrorCategory:
        return classify_redis(exc)


def classify_redis(exc: BaseException) -> ErrorCategory:
    name = type(exc).__name__.lower()
    text = str(exc).lower()

    # A protocol or server-version mismatch.  Its own category: the fix is a
    # client setting, not a network or a server problem.  (The classic case
    # is redis-py negotiating RESP3 against a server older than 6.0, which
    # has no HELLO -- 7.2 has it, but a client can still be pointed at an
    # old sidecar or an emulator that does not.)
    if "unknown command" in text and "hello" in text:
        return ErrorCategory.DEPENDENCY_VERSION
    if "unknown command" in text or "wrong number of arguments" in text:
        return ErrorCategory.DEPENDENCY_VERSION

    if "timeout" in name or "timed out" in text:
        return ErrorCategory.TIMEOUT
    if "connection refused" in text or "connect call failed" in text:
        return ErrorCategory.CONNECTION_REFUSED
    if "noauth" in text or "wrongpass" in text or "authentication" in text:
        return ErrorCategory.AUTH_FAILED
    if "loading" in text:
        return ErrorCategory.LOADING
    if "readonly" in text or "read only replica" in text:
        return ErrorCategory.ROLE_CHANGED
    if "misconf" in text:
        # Redis refusing writes because its own RDB save failed.  Reads
        # still work, so this is degradation with a very specific cause.
        return ErrorCategory.CONFIG_DRIFT
    if "oom" in text or "maxmemory" in text:
        return ErrorCategory.MEMORY_PRESSURE
    if "connectionerror" in name or "connection closed" in text or "connection lost" in text:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.UNKNOWN
