"""``WorkerHealthCommand`` -- health for a worker that runs as ``manage.py``.

A long-running Django worker is almost always a management command:

    python manage.py billing_worker

which PM2 or systemd keeps alive.  That shape has four problems the plain
Django ``BaseCommand`` does not solve, and this class solves all four:

1. **Which process is a worker?**  ``AppConfig.ready()`` runs for *every*
   entry point -- ``migrate``, ``shell``, ``collectstatic``, a pytest run,
   the autoreloader's parent -- so settings-driven autowiring has to guess
   from ``sys.argv`` which of them wants a health server.  Subclassing this
   command is not a guess: it is a declaration, and ``ready()`` steps aside
   for it.

2. **Which port?**  A host running a billing worker and a notifier cannot
   give both 8080.  The port comes from ``--health-port``, a per-command
   ``PORTS`` map, or a base port plus the supervisor's own instance ordinal,
   and a collision searches upward rather than killing the worker.

3. **Shutdown.**  PM2 sends SIGINT, Kubernetes sends SIGTERM, and both then
   wait before SIGKILL.  Those seconds are for finishing the message in
   hand -- so the command reports ``unready`` immediately (503 on /ready,
   still 200 on /live, so a liveness probe does not escalate an orderly
   shutdown into a kill) and sets an event the worker's loop can watch.

4. **Getting the tracker into the handler.**  Django constructs the command,
   so nothing can be passed in.  ``self.tracker`` is here by the time
   ``handle()`` runs.

Usage:

    from worker_health_django import WorkerHealthCommand

    class Command(WorkerHealthCommand):
        health_service = "billing-worker"
        health_queue = "billing.in"

        def handle(self, *args, **options):
            @self.tracker.handler(queue="billing.in")
            def process(message):
                Invoice.objects.create(...)      # observed automatically

            for message in consume(stop=self.stopping):
                process(message)

Everything else -- probes, instrumentation, metrics, logs -- comes from
``settings.WORKER_HEALTH`` exactly as it does for the settings-driven path.
"""
from __future__ import annotations

import logging
import os
import signal
import threading

from django.conf import settings
from django.core.management.base import BaseCommand

from .state import get_health, reset, set_health_state

logger = logging.getLogger("worker_health")

# What a supervisor calls its slot.  PM2 sets NODE_APP_INSTANCE in cluster
# mode and pm_id always; systemd templates set the instance name.
_INSTANCE_VARS = ("HEALTH_INSTANCE_INDEX", "NODE_APP_INSTANCE", "PM2_INSTANCE_ID", "pm_id")


class WorkerHealthCommand(BaseCommand):
    """A ``BaseCommand`` that is also a monitored worker."""

    # -- what a subclass sets ---------------------------------------------- #
    health_service: str | None = None      # defaults to the command's name
    health_queue: str | None = None        # default queue label for @handler
    health_port: int | None = None         # overridden by --health-port
    health_probes: list | None = None      # overrides settings PROBES
    health_settings: dict | None = None    # merged over settings.WORKER_HEALTH
    health_enabled: bool = True
    #: Ports to try above the chosen one before giving up.  Non-zero because
    #: a manage.py worker shares a host by default; a container that must
    #: have exactly its published port sets 0.
    health_port_search: int = 20
    #: How long stop() waits for the scheduler to finish its current pass.
    drain_timeout: float = 10.0
    install_signal_handlers: bool = True

    # -- what a subclass reads --------------------------------------------- #
    health = None
    tracker = None
    monitor = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        #: Set when a shutdown signal arrives.  A consume loop should watch
        #: this instead of looping forever: ``while not self.stopping.is_set()``
        #: or ``self.sleep(1.0)``.
        self.stopping = threading.Event()
        self._owns_health = False
        self._previous_signals: dict = {}

    # -- arguments --------------------------------------------------------- #

    def create_parser(self, prog_name, subcommand, **kwargs):
        # create_parser rather than add_arguments: a subclass that overrides
        # add_arguments without calling super() is a normal thing to write,
        # and it must not silently lose these flags.
        parser = super().create_parser(prog_name, subcommand, **kwargs)
        group = parser.add_argument_group("worker health")
        group.add_argument("--health-port", type=int, default=None,
                           help="port for /live, /ready, /health, /metrics")
        group.add_argument("--health-host", default=None,
                           help="bind address (default from settings)")
        group.add_argument("--health-service", default=None,
                           help="service label in metrics and logs")
        group.add_argument("--health-queue", default=None,
                           help="default queue label for @tracker.handler")
        group.add_argument("--no-health", action="store_true",
                           help="run the worker with monitoring switched off")
        return parser

    # -- lifecycle ---------------------------------------------------------- #

    def execute(self, *args, **options):
        """Wrap the command's own run in a health lifetime.

        ``execute`` and not ``handle``, so a subclass writes the ``handle``
        it would have written anyway.
        """
        if options.get("no_health") or not self.health_enabled:
            logger.info("worker-health disabled for this run")
            return super().execute(*args, **options)

        self._start_health(options)
        if self.install_signal_handlers:
            self._install_signals()
        try:
            return super().execute(*args, **options)
        except KeyboardInterrupt:
            # Ctrl-C and PM2's SIGINT arrive here once the loop unwinds.
            # An orderly stop is not a crash, and must not look like one.
            self.stdout.write("interrupted; shutting down")
            return None
        finally:
            self._restore_signals()
            self._stop_health()

    # -- wiring -------------------------------------------------------------- #

    def _start_health(self, options: dict) -> None:
        existing = get_health()
        if existing is not None:
            # settings.WORKER_HEALTH named this command in COMMANDS, so
            # AppConfig.ready() already wired and bound.  Adopt it rather
            # than starting a second monitor on a second port.
            self._adopt(existing, options)
            return

        try:
            health, config = self._build(options)
        except Exception:
            # Everything from here to a bound port -- reading settings,
            # importing a CONTEXT path, resolving a cache backend, binding
            # -- can fail on a misconfiguration, and none of it is a reason
            # for the worker not to run.  The traceback goes to the log.
            logger.exception("worker-health failed to start; running without it")
            return

        from .autowire import instrument_django

        instrument_django(health.monitor, self._settings())
        set_health_state(health)
        self._bind(health)
        self._owns_health = True

        port = health.port
        self.stdout.write(self.style.SUCCESS(
            f"worker-health: {config.service} on "
            + (f"http://{config.health_host}:{port}" if port else "no HTTP port")
        ))
        if port is not None and port != config.health_port:
            # Said out loud, because a supervisor was told a different one.
            self.stdout.write(self.style.WARNING(
                f"  port {config.health_port} was busy; bound {port} instead"
            ))

    def _build(self, options: dict):
        from worker_health import setup_worker_health

        from .autowire import build_config, build_context

        config_dict = self._settings()
        config = build_config(config_dict)
        config.service = self._service(options, config_dict)
        config.health_port = self._port(options, config_dict)
        config.health_port_search = self.health_port_search
        config.health_host = (options.get("health_host")
                              or config_dict.get("HOST") or config.health_host)
        config.default_queue = (options.get("health_queue") or self.health_queue
                                or config.default_queue)
        if self.health_probes is not None:
            from worker_health.config import coerce_probes

            config.probes = coerce_probes(self.health_probes)

        return setup_worker_health(config=config,
                                   context=build_context(config_dict)), config

    def _adopt(self, health, options: dict) -> None:
        self._bind(health)
        self._owns_health = False
        if options.get("health_port"):
            self.stderr.write(
                "--health-port ignored: this command is listed in "
                "WORKER_HEALTH['COMMANDS'], so the server was already bound "
                "during app startup. Remove it from COMMANDS to let the "
                "command own its port."
            )

    def _bind(self, health) -> None:
        self.health = health
        self.monitor = getattr(health, "monitor", None)
        self.tracker = getattr(health, "tracker", None)

    def _stop_health(self) -> None:
        if self.health is None or not self._owns_health:
            return
        try:
            self.health.stop(timeout=self.drain_timeout)
        except Exception:
            logger.exception("worker-health did not stop cleanly")
        reset()

    # -- settings resolution -------------------------------------------------- #

    def _settings(self) -> dict:
        config = dict(getattr(settings, "WORKER_HEALTH", {}) or {})
        config.update(self.health_settings or {})
        # A command that subclasses this one is a worker by construction;
        # ENABLED gates the settings-driven path, not this one.
        config.setdefault("ENABLED", True)
        return config

    def _service(self, options: dict, config: dict) -> str:
        return (options.get("health_service") or self.health_service
                or config.get("SERVICE") or self._name())

    def _name(self) -> str:
        """The name this command is invoked by: ``manage.py <name>``.

        Which is the module basename under ``management/commands/`` -- the
        only naming rule Django enforces, and the one an operator has in
        front of them when they write a PORTS map or a PM2 config.
        """
        module = type(self).__module__
        marker = "management.commands."
        if marker in module:
            return module.split(marker, 1)[1].split(".")[0]
        return module.rsplit(".", 1)[-1]

    def _port(self, options: dict, config: dict) -> int:
        if options.get("health_port"):
            return int(options["health_port"])
        if self.health_port is not None:
            return int(self.health_port)

        ports = config.get("PORTS") or {}
        named = ports.get(self._name()) or ports.get(self._service(options, config))
        if named:
            return int(named)

        base = int(config.get("PORT", 8080) or 8080)
        return base + self._instance_offset()

    def _instance_offset(self) -> int:
        """PM2 cluster slot, so four copies of one command lay out 8080-8083."""
        for var in _INSTANCE_VARS:
            value = os.getenv(var, "").strip()
            if value.isdigit():
                return int(value)
        return 0

    # -- signals ---------------------------------------------------------------- #

    def _install_signals(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                self._previous_signals[sig] = signal.signal(sig, self._on_signal)
            except (ValueError, OSError):
                # Not the main thread, or a platform without the signal.
                pass

    def _restore_signals(self) -> None:
        for sig, previous in self._previous_signals.items():
            try:
                signal.signal(sig, previous)
            except (ValueError, OSError):
                pass
        self._previous_signals.clear()

    def _on_signal(self, signum, frame):
        if self.stopping.is_set():
            # Second signal: the operator has stopped asking politely.
            self._restore_signals()
            os.kill(os.getpid(), signum)
            return
        self.stopping.set()
        name = signal.Signals(signum).name
        if self.monitor is not None:
            # Out of rotation now, alive until the loop finishes: this is
            # the whole reason readiness and liveness are separate.
            self.monitor.begin_shutdown(f"{name} received, draining")
        self.stdout.write(f"{name} received; draining "
                          f"(send it again to stop immediately)")
        self.on_shutdown(signum)

    # -- hooks for the subclass ---------------------------------------------------- #

    def on_shutdown(self, signum: int) -> None:
        """Called once, from the signal handler, when a stop is requested.

        Signal-handler rules apply: set a flag, close a socket, cancel a
        consumer -- do not block here.  ``self.stopping`` is already set.
        """

    def sleep(self, seconds: float) -> bool:
        """Sleep, but wake immediately on shutdown.  True if still running.

        A polling worker that uses ``time.sleep`` adds its whole poll
        interval to every deployment.
        """
        return not self.stopping.wait(seconds)

    @property
    def should_stop(self) -> bool:
        return self.stopping.is_set()
