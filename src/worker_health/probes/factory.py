"""The probe factory: pluggable, declarative, discoverable.

Three ways to add a probe type, in increasing order of independence:

1. ``factory.register_type("s3", builder)`` -- in the worker's own wiring.
2. ``@factory.probe_type("s3")`` -- the same thing, spelled as a decorator.
3. an entry point in another distribution's ``pyproject.toml`` --
   ``factory.load_plugins()`` finds it with no import from this package.

(3) is what makes the SDK reusable across a firm: a platform team ships
``acme-health-probes`` with the probes for their internal services, and
every worker gets them by installing it.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping

from .spec import ProbeConfigError, ProbeSpec

ProbeBuilder = Callable[[ProbeSpec, dict], Any]

ENTRY_POINT_GROUP = "worker_health.probes"


class ProbeFactory:
    def __init__(self) -> None:
        self._builders: dict[str, ProbeBuilder] = {}
        self._plugins_loaded = False

    # -- registration ---------------------------------------------------- #

    def register_type(self, type_name: str, builder: ProbeBuilder) -> "ProbeFactory":
        if not callable(builder):
            raise ProbeConfigError(f"builder for {type_name!r} is not callable")
        self._builders[type_name] = builder
        return self

    def probe_type(self, type_name: str):
        """Decorator form of :meth:`register_type`."""
        def deco(builder: ProbeBuilder) -> ProbeBuilder:
            self.register_type(type_name, builder)
            return builder
        return deco

    def types(self) -> tuple[str, ...]:
        return tuple(sorted(self._builders))

    def has(self, type_name: str) -> bool:
        return type_name in self._builders

    # -- construction ---------------------------------------------------- #

    def create(self, spec: ProbeSpec, context: Mapping[str, Any] | None = None):
        """Build one check object from a spec.  No registration, no side effects."""
        builder = self._builders.get(spec.type)
        if builder is None:
            known = ", ".join(self.types()) or "none"
            raise ProbeConfigError(
                f"probe {spec.name!r}: unknown type {spec.type!r}. Registered types: {known}"
            )
        # Copy the whole spec rather than re-listing its fields: a
        # field-by-field rebuild silently drops every field added later,
        # and the builder then sees defaults the operator never wrote.
        resolved = spec.with_params(spec.resolved_params(context or {}))
        _promote_thresholds(resolved)
        check = builder(resolved, dict(context or {}))
        if check is None:
            raise ProbeConfigError(
                f"probe {spec.name!r}: builder for type {spec.type!r} returned None"
            )
        # The spec's name wins over whatever the builder called it: the
        # config file is the source of truth for identity, and metric labels
        # and alerts are written against that name.
        check.name = spec.name
        # Latency policy is uniform across every check type, so it is set
        # here rather than threaded through each builder -- which also means
        # a third-party probe gets it without doing anything.
        if spec.latency_warn_ms is not None:
            check.latency_warn_ms = spec.latency_warn_ms
        if spec.latency_critical_ms is not None:
            check.latency_critical_ms = spec.latency_critical_ms
        return check

    def install(self, monitor, spec: ProbeSpec, context: Mapping[str, Any] | None = None):
        """Build and register one probe on ``monitor``."""
        check = self.create(spec, context)
        monitor.register(check, name=spec.name, **spec.registration_kwargs())
        return check

    def install_from_config(self, monitor, raw_specs: Iterable[Any],
                            context: Mapping[str, Any] | None = None,
                            *, strict: bool = True) -> list:
        """Install a whole list of probe definitions.

        ``strict`` is the interesting knob.  On (the default) a bad probe
        stops the worker at boot, where a human is watching.  Off, the probe
        is skipped and registered as a permanently failing check, which is
        what a fleet rollout wants: one bad config line should not take a
        hundred workers offline, but it must not be invisible either.
        """
        installed = []
        for raw in raw_specs or ():
            # Parsing is inside the guard, not before it.  Most of the ways
            # a probe definition can be wrong -- a timeout that does not fit
            # its interval, a backoff ceiling under its floor -- are caught
            # while building the spec, and leaving that outside meant
            # `strict: false` protected against almost nothing.
            try:
                spec = raw if isinstance(raw, ProbeSpec) else ProbeSpec.from_raw(raw)
            except ProbeConfigError:
                if strict:
                    raise
                _install_unparsable(monitor, raw)
                continue
            try:
                installed.append(self.install(monitor, spec, context))
            except ProbeConfigError:
                if strict:
                    raise
                _install_broken(monitor, spec)
        return installed

    # -- plugin discovery ------------------------------------------------ #

    def load_plugins(self, group: str = ENTRY_POINT_GROUP, *, reload: bool = False) -> tuple[str, ...]:
        """Register every probe type advertised by an installed distribution.

        A plugin that fails to import is skipped rather than fatal: a
        broken third-party probe must not stop a worker from reporting
        health, which is the one thing that still works when everything
        else is broken.
        """
        if self._plugins_loaded and not reload:
            return ()
        self._plugins_loaded = True

        loaded: list[str] = []
        try:
            from importlib.metadata import entry_points
        except Exception:
            return ()

        try:
            found = entry_points(group=group)
        except TypeError:                       # importlib.metadata < 3.10 shape
            found = entry_points().get(group, [])
        except Exception:
            return ()

        for entry in found:
            try:
                builder = entry.load()
            except Exception:
                continue
            if callable(builder):
                self.register_type(entry.name, builder)
                loaded.append(entry.name)
        return tuple(loaded)


# Spec-level thresholds, and the param name each check type reads them
# under.  Declaring a threshold once at the top of a probe definition and
# having every check type understand it is the point; without this the
# operator has to know which builder spells it which way.
_THRESHOLD_PARAMS = {
    "pool_warn_ratio": ("pool_warn_ratio",),
    "pool_critical_ratio": ("pool_critical_ratio",),
    "stale_after_seconds": ("stale_after", "max_idle"),
}


def _promote_thresholds(spec: ProbeSpec) -> None:
    """Copy spec-level thresholds down into the params builders read.

    An explicit param always wins: someone who wrote the low-level spelling
    meant it, and silently overriding it would be worse than either.
    """
    for field, param_names in _THRESHOLD_PARAMS.items():
        value = getattr(spec, field, None)
        if value is None:
            continue
        for param in param_names:
            spec.params.setdefault(param, value)


def _install_broken(monitor, spec: ProbeSpec) -> None:
    """Register a placeholder that reports the misconfiguration.

    Better than a missing check: `config_drift` on a named check points at
    the config file, while a check that silently does not exist points at
    nothing.
    """
    from ..checks.custom import CustomCheck
    from ..core.model import Status

    def report() -> Status:
        return Status.FAILING

    check = CustomCheck(report, name=spec.name)
    monitor.register(check, name=spec.name, **spec.registration_kwargs())


def _install_unparsable(monitor, raw) -> None:
    """Placeholder for a definition too broken to become a spec at all.

    There is no spec to take settings from, so the check is registered on
    defaults under whatever name the definition claimed.  The point is the
    same as above: a named failing check sends someone to the config file.
    """
    from ..checks.custom import CustomCheck
    from ..core.model import Status

    name = "unnamed-probe"
    if isinstance(raw, Mapping):
        name = str(raw.get("name") or raw.get("type") or name)

    def report() -> Status:
        return Status.FAILING

    monitor.register(CustomCheck(report, name=name), name=name, critical=False)


def default_factory(*, load_plugins: bool = False) -> ProbeFactory:
    """A factory with every built-in type registered.

    A fresh instance each call.  Custom types registered by one worker must
    not leak into another's factory in the same process -- which is exactly
    what a module-level singleton would do, and what makes test isolation
    impossible.
    """
    from . import builtin

    factory = ProbeFactory()
    builtin.register_all(factory)
    if load_plugins:
        factory.load_plugins()
    return factory
