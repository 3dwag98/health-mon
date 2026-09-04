"""Redis check, pinned for Redis 5.0.6.

Verified against redis:5.0.6-alpine: redis-py 6+ negotiates RESP3 by sending
HELLO 3 on connect, and 5.0.6 has no HELLO command, so the handshake fails
and EVERY subsequent call raises -- PING included.  ``protocol=2`` fixes it.

A caller-supplied client that has this wrong is detected and reported as
DEPENDENCY_VERSION rather than CONNECTION_REFUSED, because the remediation
is completely different and a wrong category sends someone to stare at a
perfectly healthy server.
"""
from __future__ import annotations

import time

from ..core.model import ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


def build_client(url: str = "", *, host="localhost", port=6379, db=0,
                 password=None, socket_timeout=1.0, socket_connect_timeout=1.0):
    """Construct a redis client that actually works against Redis 5.

    ``protocol=2`` is mandatory and is why this helper exists.  Never leave
    socket timeouts as None: without them a black-holed connection hangs the
    probe forever, which is precisely the failure this package must survive.
    """
    import redis as redis_lib

    kwargs = dict(
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        retry_on_timeout=False,
        health_check_interval=0,
    )
    # protocol= only exists on redis-py >= 5; older clients are RESP2 anyway.
    try:
        from redis import __version__ as _v
        if int(str(_v).split(".")[0]) >= 5:
            kwargs["protocol"] = 2
    except Exception:
        pass

    if url:
        return redis_lib.Redis.from_url(url, **kwargs)
    return redis_lib.Redis(host=host, port=port, db=db, password=password, **kwargs)


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

    # The Redis 5 / RESP3 mismatch.  Its own category: the fix is a client
    # setting, not a network or a server problem.
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
    if "oom" in text or "maxmemory" in text:
        return ErrorCategory.MEMORY_PRESSURE
    if "connectionerror" in name or "connection closed" in text or "connection lost" in text:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.UNKNOWN
