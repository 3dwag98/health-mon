"""L0: the Django integration, against a real Django with a sqlite database.

Django is a dev dependency, not a runtime one -- these skip cleanly when it
is absent, and the SDK stays importable either way. sqlite because the point
is the instrumentation path, not the driver: `execute_wrappers` is engine
agnostic and a real database of any kind exercises it honestly.
"""
from __future__ import annotations

import pytest

from worker_health import ErrorCategory, HealthMonitor, Status
from worker_health.checks.base import CheckContext
from worker_health.instrument.context import probe_scope

django = pytest.importorskip("django")

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module", autouse=True)
def _django_setup():
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            DEBUG=False,
            DATABASES={
                "default": {"ENGINE": "django.db.backends.sqlite3",
                            "NAME": ":memory:"},
                "replica": {"ENGINE": "django.db.backends.sqlite3",
                            "NAME": ":memory:"},
            },
            INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
            USE_TZ=True,
            LOGGING_CONFIG=None,
        )
        django.setup()
    yield


@pytest.fixture(autouse=True)
def _clean_instrumentation():
    from worker_health.instrument.django_ import uninstrument_django_db

    uninstrument_django_db()
    yield
    uninstrument_django_db()


def _monitor():
    return HealthMonitor("django-worker", instance="dj-1")


def _query(alias="default", sql="SELECT 1"):
    from django.db import connections

    with connections[alias].cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()


# -- instrumentation ---------------------------------------------------------- #

def test_orm_queries_become_observed_traffic():
    """Via `connection.execute_wrappers`, Django's own documented hook --
    not a monkeypatch of CursorWrapper."""
    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor, dependency_name="postgres", alias="default")

    assert _query() == (1,)
    _query()

    record = monitor.traffic.get("postgres")
    assert record.successes == 2 and record.failures == 0
    assert record.last_latency_ms is not None


def test_the_wrapper_is_installed_on_django_own_list():
    """If this stops being true, Django changed its instrumentation API and
    the failure should be here rather than silently in production."""
    from django.db import connections

    from worker_health.instrument.django_ import _QueryWrapper, instrument_django_db

    _query()                       # ensure the connection exists first
    instrument_django_db(_monitor())
    installed = connections["default"].execute_wrappers
    assert any(isinstance(w, _QueryWrapper) for w in installed)


def test_executemany_is_covered_by_the_same_wrapper():
    """The old monkeypatch needed a second patch for executemany; the
    documented hook carries a `many` flag instead."""
    from django.db import connections

    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor)

    with connections["default"].cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS wh_many (v integer)")
        cursor.executemany("INSERT INTO wh_many VALUES (%s)", [(1,), (2,), (3,)])

    # One execute plus one executemany, both recorded.
    assert monitor.traffic.get("postgres").successes >= 2


def test_a_failing_query_is_classified_and_re_raised():
    from django.db import utils

    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor)

    with pytest.raises(utils.DatabaseError):
        _query(sql="SELECT * FROM a_table_that_does_not_exist")

    record = monitor.traffic.get("postgres")
    assert record.failures == 1
    assert record.last_category is not None


def test_two_aliases_report_as_two_dependencies():
    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor, dependency_name="postgres", alias="default")
    instrument_django_db(monitor, dependency_name="replica-db", alias="replica")

    _query("default")
    _query("replica")

    assert monitor.traffic.get("postgres").successes == 1
    assert monitor.traffic.get("replica-db").successes == 1


def test_an_unregistered_alias_is_passed_straight_through():
    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor, dependency_name="postgres", alias="default")

    _query("replica")                       # registered nowhere
    assert monitor.traffic.get("replica") is None
    assert monitor.traffic.get("postgres") is None


def test_health_probe_queries_are_not_counted_as_traffic():
    """The whole evidence model rests on this: a probe's own SELECT 1 must
    never make a silent worker look busy."""
    from worker_health.instrument.django_ import instrument_django_db

    monitor = _monitor()
    instrument_django_db(monitor)

    with probe_scope():
        _query()

    assert monitor.traffic.get("postgres") is None

    _query()
    assert monitor.traffic.get("postgres").successes == 1


def test_instrumenting_twice_installs_one_wrapper():
    from django.db import connections

    from worker_health.instrument.django_ import _QueryWrapper, instrument_django_db

    _query()
    monitor = _monitor()
    instrument_django_db(monitor)
    instrument_django_db(monitor)

    wrappers = connections["default"].execute_wrappers
    assert sum(isinstance(w, _QueryWrapper) for w in wrappers) == 1

    _query()
    assert monitor.traffic.get("postgres").successes == 1


def test_uninstrument_leaves_django_as_it_found_it():
    from django.db import connections

    from worker_health.instrument.django_ import (
        _QueryWrapper,
        instrument_django_db,
        uninstrument_django_db,
    )

    _query()
    instrument_django_db(_monitor())
    uninstrument_django_db()

    wrappers = connections["default"].execute_wrappers
    assert not any(isinstance(w, _QueryWrapper) for w in wrappers)


# -- the django_db check ------------------------------------------------------ #

def test_the_django_db_check_probes_read_only():
    """Deliberately unlike django-health-check, whose database backend
    creates a row, updates it and deletes it on every single check."""
    from worker_health.checks.django_db import DjangoDbCheck

    check = DjangoDbCheck(alias="default", name="db")
    ctx = CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                       traffic=_monitor().traffic)
    result = check.evaluate(ctx)
    assert result.status is Status.OK
    assert result.observed["vendor"] == "sqlite"


# -- django-health-check interop ---------------------------------------------- #

class _Error(Exception):
    """Stands in for health_check.exceptions.ServiceUnavailable."""

    def __init__(self, message="Service unavailable"):
        super().__init__(message)
        self.message = message


class HealthyBackend:
    critical_service = True

    def __init__(self):
        self.errors = []

    def run_check(self):
        return None

    @classmethod
    def identifier(cls):
        return cls.__name__


class FailingBackend(HealthyBackend):
    critical_service = False

    def run_check(self):
        self.errors.append(_Error("Service unavailable: connection refused"))


class RaisingBackend(HealthyBackend):
    def run_check(self):
        raise RuntimeError("backend exploded")


def _adapter_result(backend_class):
    from worker_health_django.compat import DjangoHealthCheckAdapter

    ctx = CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                       traffic=_monitor().traffic)
    return DjangoHealthCheckAdapter(backend_class).evaluate(ctx)


def test_an_existing_health_check_backend_runs_unchanged():
    result = _adapter_result(HealthyBackend)
    assert result.status is Status.OK
    assert result.observed["backend"] == "HealthyBackend"


def test_a_backend_error_becomes_a_classified_failure():
    result = _adapter_result(FailingBackend)
    assert result.status is Status.FAILING
    assert result.category is ErrorCategory.CONNECTION_REFUSED


def test_a_backend_that_raises_is_isolated():
    """django-health-check backends are third-party code; one that raises
    must report, not take the monitor down."""
    result = _adapter_result(RaisingBackend)
    assert result.status is Status.FAILING
    assert result.detail == "health check backend raised"


def test_errors_do_not_leak_between_runs():
    """The backend accumulates into `self.errors`, so a shared instance
    would report a failure from ten minutes ago forever."""
    from worker_health_django.compat import DjangoHealthCheckAdapter

    adapter = DjangoHealthCheckAdapter(FailingBackend)
    ctx = CheckContext(now=100.0, wall=0.0, deadline=101.0, max_silence=10.0,
                       traffic=_monitor().traffic)
    first = adapter.evaluate(ctx)
    second = adapter.evaluate(ctx)
    assert first.observed["error_count"] == second.observed["error_count"] == 1


def test_critical_service_maps_onto_criticality():
    from worker_health_django.compat import DjangoHealthCheckAdapter

    assert DjangoHealthCheckAdapter(HealthyBackend).critical is True
    # django-health-check has no degraded state; worker-health gives a
    # non-critical backend one instead of a 500.
    assert DjangoHealthCheckAdapter(FailingBackend).critical is False


def test_discovery_is_silent_when_the_library_is_absent():
    from worker_health_django.compat import discover_backends

    assert discover_backends() == []
