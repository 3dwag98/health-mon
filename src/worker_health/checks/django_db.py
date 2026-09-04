"""Django ORM database check.

Django is imported inside the methods, never at module import, so the SDK
stays importable in a worker that has never heard of Django.

The probe deliberately does NOT call ``connection.is_usable()``.  That
method issues a query on the *application's* connection and, on failure,
marks it broken -- a health check that can invalidate the worker's own
connection is a health check that causes incidents.  This opens its own
cursor, runs ``SELECT 1``, and closes the connection it opened.
"""
from __future__ import annotations

import time

from ..core.model import ErrorCategory, Evidence
from .base import BaseCheck, CheckContext
from .postgres import classify_postgres


class DjangoDbCheck(BaseCheck):
    def __init__(self, *, alias: str = "default", name: str = "postgres",
                 dependency: str = "postgres") -> None:
        self.name = name
        self.dependency = dependency
        self.alias = alias

    # -- introspection: zero I/O ---------------------------------------- #

    def introspect(self, ctx: CheckContext):
        started = time.perf_counter()
        try:
            from django.db import connections

            conn = connections[self.alias]
        except Exception:
            return None

        observed = {
            "alias": self.alias,
            "vendor": getattr(conn, "vendor", "unknown"),
            "connected": conn.connection is not None,
            "in_atomic_block": bool(getattr(conn, "in_atomic_block", False)),
        }

        # A connection that needs rollback will fail every subsequent query
        # with the same opaque error until something resets it.  Reporting
        # it as a database outage is the classic misdiagnosis: the server is
        # fine, this worker's transaction is poisoned.
        if getattr(conn, "needs_rollback", False):
            return self.degraded(
                ctx, ErrorCategory.RESOURCE_LOCKED, started,
                detail="connection is marked for rollback", **observed,
            )
        return self.ok(ctx, started, evidence=Evidence.INTROSPECTED, **observed)

    # -- probe: only when traffic is stale ------------------------------ #

    def probe(self, ctx: CheckContext):
        from django.db import connections

        started = time.perf_counter()
        # A fresh alias-scoped connection object per thread.  Django's
        # connection handler is thread-local, so this never shares the
        # request/handler connection.
        conn = connections.create_connection(self.alias) \
            if hasattr(connections, "create_connection") else connections[self.alias]
        opened = conn.connection is None
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                read_only = _read_only(conn, cursor)
        finally:
            if opened:
                try:
                    conn.close()
                except Exception:
                    pass

        observed = {"alias": self.alias, "vendor": getattr(conn, "vendor", "unknown")}
        if read_only:
            # A failover promoted this worker onto a replica.  It can read;
            # it cannot write.  Degraded, not down.
            return self.degraded(ctx, ErrorCategory.READ_ONLY, started,
                                 evidence=Evidence.PROBED,
                                 detail="server is in read-only mode",
                                 read_only=True, **observed)
        return self.ok(ctx, started, read_only=False, **observed)

    def classify(self, exc: BaseException) -> ErrorCategory:
        return classify_postgres(exc)


def _read_only(conn, cursor) -> bool:
    if getattr(conn, "vendor", "") != "postgresql":
        return False
    try:
        cursor.execute("SHOW transaction_read_only")
        row = cursor.fetchone()
    except Exception:
        return False
    return bool(row) and str(row[0]).lower() in ("on", "true")
