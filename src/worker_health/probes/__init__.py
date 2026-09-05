"""Declarative, pluggable probes.

    from worker_health.probes import ProbeSpec, default_factory

    factory = default_factory()
    factory.load_plugins()
    factory.install_from_config(monitor, config.probes, context)

The factory is the seam between configuration (a YAML file, Django
settings, a dict) and live objects (an engine, a client, a broker state).
Configuration names things; the context supplies them; ``"@name"`` is how
the two meet.
"""
from __future__ import annotations

from .builtin import register_all
from .factory import ENTRY_POINT_GROUP, ProbeBuilder, ProbeFactory, default_factory
from .spec import REFERENCE_PREFIX, ProbeConfigError, ProbeSpec

__all__ = [
    "ProbeSpec", "ProbeFactory", "ProbeBuilder", "ProbeConfigError",
    "default_factory", "register_all", "ENTRY_POINT_GROUP", "REFERENCE_PREFIX",
]
