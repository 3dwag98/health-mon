"""SQLAlchemy auto-instrumentation.

Uses SQLAlchemy's own event system rather than wrapping the session or the
engine.  Three consequences worth the choice:

* every query is covered, including ones issued by the ORM's flush, by a
  library, or by code the worker team never sees;
* nothing in business code changes -- no context manager per query, which
  is the requirement that started this;
* the hooks are attached to ONE engine, so two engines in the same process
  can report as two different dependencies.

The timings come off ``conn.info``, which is per-connection, so overlapping
queries on different connections cannot swap their start times.
"""
from __future__ import annotations

import time

from ..checks.postgres import classify_postgres
from .recorder import TrafficRecorder

_KEY = "worker_health_query_start"


def instrument_sqlalchemy(engine, monitor, dependency_name: str = "postgres"):
    """Record every real query against ``engine`` as observed traffic.

    Returns the engine so it can be used inline.  Idempotent per engine:
    calling it twice does not double-count.
    """
    target = getattr(engine, "sync_engine", engine)   # AsyncEngine -> sync core
    if getattr(target, "_worker_health_instrumented", False):
        return engine

    from sqlalchemy import event

    recorder = TrafficRecorder(monitor, dependency_name, classify_postgres)

    @event.listens_for(target, "before_cursor_execute")
    def _before(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault(_KEY, []).append(time.perf_counter())

    @event.listens_for(target, "after_cursor_execute")
    def _after(conn, cursor, statement, parameters, context, executemany):
        started = conn.info.get(_KEY)
        if not started:
            return
        recorder.success((time.perf_counter() - started.pop(-1)) * 1000.0)

    @event.listens_for(target, "handle_error")
    def _error(exception_context):
        # Pop the start time too, or a failed statement leaves an orphan
        # entry and every later query on that connection reports the wrong
        # duration.
        conn = getattr(exception_context, "connection", None)
        started = getattr(conn, "info", {}).get(_KEY) if conn is not None else None
        if started:
            started.pop(-1)
        recorder.failure(exception_context.original_exception)

    target._worker_health_instrumented = True
    return engine


def uninstrument_sqlalchemy(engine) -> None:
    """Remove the flag so a test fixture can re-instrument a fresh monitor."""
    target = getattr(engine, "sync_engine", engine)
    if hasattr(target, "_worker_health_instrumented"):
        delattr(target, "_worker_health_instrumented")
