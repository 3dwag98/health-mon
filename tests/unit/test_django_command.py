"""L0: the manage.py worker path, against a real Django.

The subject here is not the checks -- those have their own tests -- but the
four things that only go wrong when health lives inside a management
command: which process wires, which port it takes, what a SIGTERM does, and
whether a second shell can see any of it.
"""
from __future__ import annotations

import os
import signal
import socket
import threading

import pytest

django = pytest.importorskip("django")

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module", autouse=True)
def _django_setup():
    from django.conf import settings

    if not settings.configured:
        settings.configure(
            DEBUG=False,
            DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3",
                                   "NAME": ":memory:"}},
            INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth"],
            USE_TZ=True,
            LOGGING_CONFIG=None,
        )
        django.setup()
    yield


@pytest.fixture(autouse=True)
def _isolated_runtime(tmp_path, monkeypatch):
    """Keep the run registry out of the developer's real runtime directory."""
    monkeypatch.setenv("WORKER_HEALTH_RUNTIME_DIR", str(tmp_path / "run"))
    from worker_health_django import state

    state.reset()
    yield
    state.reset()


def _command(**attrs):
    from worker_health_django import WorkerHealthCommand

    namespace = {"health_settings": {"ENABLED": True, "PROBES": []},
                 "health_port": 0, "install_signal_handlers": False}
    namespace.update(attrs)
    return type("Command", (WorkerHealthCommand,), namespace)


def _run(command_cls, argv=("worker",), **options):
    command = command_cls()
    parser = command.create_parser("manage.py", "worker")
    parsed = vars(parser.parse_args(list(argv[1:])))
    parsed.update(options)
    parsed.setdefault("verbosity", 0)
    command.execute(**parsed)
    return command


# -- wiring ------------------------------------------------------------------ #

def test_the_command_gives_handle_a_live_tracker():
    """Django constructs the command, so nothing can be passed in; if this
    is not already set by the time handle() runs there is no way to get it."""
    seen = {}

    class Command(_command()):
        def handle(self, *args, **options):
            seen["tracker"] = self.tracker
            seen["port"] = self.health.port

            @self.tracker.handler(queue="q")
            def work(n):
                return n * 2

            assert work(2) == 4

    _run(Command)
    assert seen["tracker"] is not None
    assert isinstance(seen["port"], int) and seen["port"] > 0


def test_no_health_runs_the_worker_with_monitoring_off():
    """Health being switched off must not change whether business code runs."""
    ran = []

    class Command(_command()):
        def handle(self, *args, **options):
            ran.append(self.tracker)

    command = _run(Command, argv=("worker", "--no-health"))
    assert ran == [None] and command.health is None


def test_a_wiring_failure_does_not_stop_the_worker():
    ran = []

    class Command(_command()):
        def handle(self, *args, **options):
            ran.append(True)

        def _settings(self):
            raise RuntimeError("settings are broken")

    _run(Command)
    assert ran == [True]


def test_the_health_state_is_reachable_from_module_state_while_running():
    from worker_health_django import get_tracker

    class Command(_command()):
        def handle(self, *args, **options):
            assert get_tracker() is self.tracker

    _run(Command)


# -- port selection ---------------------------------------------------------- #

def test_a_per_command_ports_map_wins_over_the_base_port():
    """Keyed by the name the command is invoked by, which is the name an
    operator already has in front of them in the PM2 config."""
    command = _command(health_port=None)()
    assert command._port({}, {"PORT": 8080,
                              "PORTS": {command._name(): 9123}}) == 9123


def test_a_command_knows_the_name_it_is_invoked_by():
    cls = _command()
    cls.__module__ = "billing.management.commands.billing_worker"
    assert cls()._name() == "billing_worker"


def test_the_supervisor_instance_ordinal_lays_a_cluster_out_contiguously(monkeypatch):
    """Four copies of one command under PM2 cannot all have 8080."""
    monkeypatch.setenv("NODE_APP_INSTANCE", "3")
    command = _command(health_port=None)()
    assert command._port({}, {"PORT": 8080}) == 8083


def test_an_explicit_flag_beats_everything(monkeypatch):
    monkeypatch.setenv("NODE_APP_INSTANCE", "3")
    command = _command(health_port=9000)()
    assert command._port({"health_port": 9999}, {"PORT": 8080}) == 9999


def test_a_busy_port_is_searched_past_rather_than_fatal():
    """Two workers started from one shell is the normal case, and the second
    one failing to boot because of its health port would be absurd."""
    holder = socket.socket()
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", 0))
    taken = holder.getsockname()[1]
    holder.listen(1)
    try:
        class Command(_command(health_port=taken, health_port_search=5)):
            health_settings = {"ENABLED": True, "PROBES": [], "HOST": "127.0.0.1"}

            def handle(self, *args, **options):
                pass

        command = _run(Command)
        assert command.health.port != taken
        assert taken < command.health.port <= taken + 5
    finally:
        holder.close()


# -- shutdown ----------------------------------------------------------------- #

def test_a_stop_signal_takes_the_worker_out_of_rotation_but_keeps_it_alive():
    """The seconds between SIGTERM and SIGKILL are for finishing the message
    in hand.  A liveness probe that 503s during them turns an orderly
    shutdown into a kill."""
    from worker_health.core.model import Liveness, Readiness

    observed = {}

    class Command(_command(install_signal_handlers=True)):
        def handle(self, *args, **options):
            os.kill(os.getpid(), signal.SIGTERM)
            assert self.stopping.wait(2.0), "the signal never arrived"
            observed["readiness"] = self.monitor.readiness()
            observed["liveness"] = self.monitor.liveness()
            observed["ready_code"] = self.monitor.ready_code()
            observed["live_code"] = self.monitor.live_code()
            observed["reasons"] = self.monitor.readiness_reasons()

    _run(Command)
    assert observed["readiness"] is Readiness.UNREADY
    assert observed["liveness"] is Liveness.ALIVE
    assert observed["ready_code"] == 503
    assert observed["live_code"] == 200
    assert any("SIGTERM" in r for r in observed["reasons"])


def test_sleep_returns_early_on_shutdown():
    """A polling worker that uses time.sleep adds its whole poll interval to
    every deployment."""
    import time

    class Command(_command(install_signal_handlers=False)):
        def handle(self, *args, **options):
            threading.Timer(0.05, self.stopping.set).start()
            started = time.perf_counter()
            assert self.sleep(5.0) is False
            assert time.perf_counter() - started < 2.0
            assert self.should_stop

    _run(Command)


def test_signal_handlers_are_restored_afterwards():
    original = signal.getsignal(signal.SIGTERM)

    class Command(_command(install_signal_handlers=True)):
        def handle(self, *args, **options):
            assert signal.getsignal(signal.SIGTERM) is not original

    _run(Command)
    assert signal.getsignal(signal.SIGTERM) is original


# -- discovery from another process -------------------------------------------- #

def test_a_running_worker_is_discoverable_from_the_host_registry():
    """`manage.py worker_health` is a NEW process; module state is empty in
    it by definition, so a worker has to leave a trail on the host."""
    from worker_health.transports import registry

    class Command(_command()):
        def handle(self, *args, **options):
            entries = registry.entries()
            assert [e for e in entries if e["pid"] == os.getpid()]
            entry = entries[0]
            assert entry["port"] == self.health.port
            assert entry["url"].startswith("http://")

    _run(Command)
    # ...and the trail is cleaned up on the way out.
    assert registry.entries() == []


def test_a_dead_process_record_is_pruned_by_the_next_reader():
    from worker_health.transports import registry

    path = registry.register(service="ghost", instance="ghost-1",
                             host="127.0.0.1", port=8099)
    assert path is not None
    # A pid that cannot exist: the record outlived a SIGKILL.
    text = path.read_text().replace(f'"pid": {os.getpid()}', '"pid": 2147483646')
    path.write_text(text)
    assert registry.entries() == []
    assert not path.exists()


# -- which process wires -------------------------------------------------------- #

def test_a_worker_health_command_is_left_alone_by_settings_autowiring(monkeypatch):
    """Both wiring paths firing means two monitors and two ports, and the
    command's own --health-port silently losing the race."""
    from worker_health_django import autowire

    monkeypatch.setattr(autowire, "command_wires_itself", lambda name: name == "billing")
    config = {"ENABLED": True}
    assert autowire.should_wire(config, ["manage.py", "billing"]) is False
    assert autowire.should_wire(config, ["manage.py", "other_worker"]) is True


@pytest.mark.parametrize("command", ["migrate", "shell", "collectstatic",
                                     "runserver", "test", "help", "worker_health"])
def test_short_lived_entry_points_never_start_a_health_server(command):
    from worker_health_django.autowire import should_wire

    assert should_wire({"ENABLED": True}, ["manage.py", command]) is False


def test_the_autoreloader_parent_does_not_bind_the_port(monkeypatch):
    """Django re-executes under the reloader with RUN_MAIN=true in the
    child; wiring in both means the child -- the one doing the work -- loses
    the race for the port."""
    from worker_health_django.autowire import should_wire

    argv = ["manage.py", "billing_worker", "--reload"]
    monkeypatch.delenv("RUN_MAIN", raising=False)
    assert should_wire({"ENABLED": True}, argv) is False
    monkeypatch.setenv("RUN_MAIN", "true")
    assert should_wire({"ENABLED": True}, argv) is True


def test_an_explicit_command_list_is_honoured():
    from worker_health_django.autowire import should_wire

    config = {"ENABLED": True, "COMMANDS": ["billing_worker"]}
    assert should_wire(config, ["manage.py", "billing_worker"]) is True
    assert should_wire(config, ["manage.py", "import_csv"]) is False


def test_disabled_means_disabled():
    from worker_health_django.autowire import should_wire

    assert should_wire({"ENABLED": False}, ["manage.py", "billing_worker"]) is False
