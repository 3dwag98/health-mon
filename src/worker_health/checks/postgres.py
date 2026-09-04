"""Postgres check.

Pool state is read directly off the application's own engine -- which is
both cheaper and more certain than the probe-that-avoids-the-pool trick,
because ``checkedout() == size() + overflow()`` IS pool exhaustion, with no
query issued and no ambiguity about whether the server is at fault.

The fallback probe uses a separate NullPool engine so it can never compete
for application slots, and can never itself cause the exhaustion it is
supposed to detect.
"""
from __future__ import annotations

import time

from ..core.model import ErrorCategory, Evidence, Status
from .base import BaseCheck, CheckContext


class PostgresCheck(BaseCheck):
    def __init__(
        self,
        app_engine=None,
        probe_dsn: str | None = None,
        *,
        name: str = "postgres",
        dependency: str = "postgres",
        pool_warn_ratio: float = 0.9,
    ) -> None:
        self.name = name
        self.dependency = dependency
        self._app_engine = app_engine
        self._probe_dsn = probe_dsn
        self._pool_warn_ratio = pool_warn_ratio
        self._probe_engine = None

    # -- introspection: zero I/O ---------------------------------------- #

    def introspect(self, ctx: CheckContext):
        if self._app_engine is None:
            return None
        started = time.perf_counter()
        try:
            pool = self._app_engine.pool
            checked_out = pool.checkedout()
            size = pool.size()
            overflow = max(pool.overflow(), 0)
            capacity = size + overflow
        except Exception:
            return None

        observed = {
            "pool_checked_out": checked_out,
            "pool_size": size,
            "pool_overflow": overflow,
            "pool_capacity": capacity,
        }
        if capacity > 0 and checked_out >= capacity:
            # The application cannot get a connection.  The SERVER is very
            # probably fine -- reporting connection_refused here is what
            # sends on-call to the wrong team.
            return self.degraded(
                ctx, ErrorCategory.POOL_EXHAUSTED, started,
                detail="application pool has no free slots", **observed,
            )
        if capacity > 0 and checked_out >= capacity * self._pool_warn_ratio:
            observed["pool_pressure"] = True
        return self.ok(ctx, started, evidence=Evidence.INTROSPECTED, **observed)

    # -- probe: only when traffic is stale ------------------------------ #

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        engine = self._ensure_probe_engine()
        if engine is None:
            return self.fail(
                ctx, ErrorCategory.CONFIG_DRIFT, started,
                detail="no probe DSN configured",
            )
        from sqlalchemy import text

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            ro = conn.execute(text("SHOW transaction_read_only")).scalar()

        if str(ro).lower() in ("on", "true"):
            # A failover promoted us onto a read-only replica.  The worker
            # can still read; it cannot write.  Degraded, not down.
            return self.degraded(
                ctx, ErrorCategory.READ_ONLY, started, evidence=Evidence.PROBED,
                detail="server is in read-only mode", read_only=True,
            )
        return self.ok(ctx, started, read_only=False)

    def _ensure_probe_engine(self):
        if self._probe_engine is not None:
            return self._probe_engine
        if not self._probe_dsn:
            return None
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        self._probe_engine = create_engine(
            self._probe_dsn,
            poolclass=NullPool,
            connect_args={
                "application_name": "worker-health-probe",
                "connect_timeout": 2,
            },
        )
        return self._probe_engine

    # -- classification --------------------------------------------------- #

    def classify(self, exc: BaseException) -> ErrorCategory:
        return classify_postgres(exc)

    def close(self) -> None:
        if self._probe_engine is not None:
            try:
                self._probe_engine.dispose()
            except Exception:
                pass


def classify_postgres(exc: BaseException) -> ErrorCategory:
    """Map a driver exception onto the closed taxonomy.

    Matching is on exception TYPE and SQLSTATE where available, and on a
    lowercase substring otherwise.  The message itself never escapes this
    function -- psycopg embeds the DSN in several of them.
    """
    name = type(exc).__name__.lower()
    text = str(exc).lower()

    sqlstate = getattr(exc, "sqlstate", None) or getattr(
        getattr(exc, "orig", None), "sqlstate", None
    )
    if sqlstate:
        if sqlstate.startswith("28"):      # invalid authorization
            return ErrorCategory.AUTH_FAILED
        if sqlstate == "57014":            # query canceled
            return ErrorCategory.TIMEOUT
        if sqlstate.startswith("53"):      # insufficient resources
            return ErrorCategory.POOL_EXHAUSTED
        if sqlstate == "25006":            # read-only sql transaction
            return ErrorCategory.READ_ONLY
        if sqlstate.startswith("08"):      # connection exception
            return ErrorCategory.CONNECTION_LOST

    if "timeout" in name or "timeout" in text or "timed out" in text:
        return ErrorCategory.TIMEOUT
    if "password" in text or "authentication" in text or "role" in text and "does not exist" in text:
        return ErrorCategory.AUTH_FAILED
    if "connection refused" in text or "could not connect" in text:
        return ErrorCategory.CONNECTION_REFUSED
    if "read-only" in text or "read only" in text:
        return ErrorCategory.READ_ONLY
    if "too many clients" in text:
        return ErrorCategory.POOL_EXHAUSTED
    if "server closed" in text or "connection is closed" in text or "eof" in text:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.UNKNOWN
