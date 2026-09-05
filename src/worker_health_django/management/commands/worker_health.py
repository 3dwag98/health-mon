"""``manage.py worker_health`` -- inspect the workers running on this host.

The subtlety that makes this command worth its own file: it is a *new*
process.  The monitor lives in the ``manage.py billing_worker`` process that
is still running somewhere else, so reading it out of module state -- which
is what the first version of this command did -- returns nothing, always,
for the exact use it was written for.

So it looks in three places, in order:

1. this process, for the case where a command inspects its own health;
2. the host's run registry, written by every worker that binds a port;
3. ``--url``, for a worker in another container or on another host.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request

from django.core.management.base import BaseCommand

from worker_health.transports import registry
from worker_health_django.state import get_monitor


class Command(BaseCommand):
    help = "Show worker-health status for the workers running on this host"

    def add_arguments(self, parser):
        parser.add_argument("--url", help="health base URL of one worker")
        parser.add_argument("--service", help="pick a registered worker by service name")
        parser.add_argument("--list", action="store_true",
                            help="list registered workers and exit")
        parser.add_argument("--json", action="store_true", help="full snapshot as JSON")
        parser.add_argument("--metrics", action="store_true",
                            help="Prometheus exposition instead of JSON")
        parser.add_argument("--timeout", type=float, default=2.0)

    def handle(self, *args, **options):
        if options["list"]:
            return self._list()

        snapshots = self._collect(options)
        if not snapshots:
            self.stderr.write(
                "No worker-health process found.\n"
                "  - a worker must be running (manage.py <your worker command>)\n"
                "  - it must have bound a health port (see the line it prints at startup)\n"
                "  - or pass --url http://host:port"
            )
            return

        for label, snapshot in snapshots:
            self._render(label, snapshot, options)

    # -- sources ------------------------------------------------------------ #

    def _collect(self, options) -> list[tuple[str, dict]]:
        if options["url"]:
            body = self._fetch(options["url"], options)
            return [(options["url"], body)] if body else []

        monitor = get_monitor()
        if monitor is not None:
            # Wired in THIS process: a command inspecting itself.
            if options["metrics"]:
                from worker_health.telemetry.prometheus import render

                self.stdout.write(render(monitor))
                return []
            return [("this process", monitor.snapshot_dict())]

        out = []
        for entry in registry.entries():
            if options["service"] and entry.get("service") != options["service"]:
                continue
            body = self._fetch(entry["url"], options)
            if body:
                out.append((f"{entry['service']} ({entry['url']}, pid {entry['pid']})", body))
        return out

    def _fetch(self, base: str, options) -> dict | None:
        path = "/metrics" if options["metrics"] else "/health"
        url = base.rstrip("/") + path
        try:
            with urllib.request.urlopen(url, timeout=options["timeout"]) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raw = exc.read()          # /ready answers 503 with a full body
        except OSError as exc:
            self.stderr.write(f"{base}: unreachable ({type(exc).__name__})")
            return None

        if options["metrics"]:
            self.stdout.write(raw.decode("utf-8", "replace"))
            return None
        try:
            return json.loads(raw or b"{}")
        except ValueError:
            self.stderr.write(f"{base}: response was not JSON")
            return None

    def _list(self) -> None:
        entries = registry.entries()
        if not entries:
            self.stdout.write("no worker-health processes registered on this host")
            return
        self.stdout.write(f"{'SERVICE':<24} {'INSTANCE':<24} {'PID':>7}  URL")
        for entry in entries:
            self.stdout.write(
                f"{entry.get('service', ''):<24} {entry.get('instance', ''):<24} "
                f"{entry.get('pid', 0):>7}  {entry.get('url', '')}"
            )

    # -- rendering ----------------------------------------------------------- #

    def _render(self, label: str, snapshot: dict, options) -> None:
        if options["json"]:
            self.stdout.write(json.dumps(snapshot, indent=2, default=str))
            return

        readiness = snapshot.get("readiness", "unknown")
        style = self.style.SUCCESS if readiness == "ready" else (
            self.style.WARNING if readiness == "degraded" else self.style.ERROR)
        self.stdout.write(style(
            f"{label}: readiness={readiness} liveness={snapshot.get('liveness')}"
            + ("  [draining]" if snapshot.get("draining") else "")
        ))

        for name, check in (snapshot.get("checks") or {}).items():
            flag = "critical" if check.get("critical") else "non-critical"
            detail = f" -- {check['detail']}" if check.get("detail") else ""
            self.stdout.write(
                f"  {name:<18} {check.get('internal_status', ''):<9} "
                f"{check.get('evidence', ''):<13} {flag}{detail}"
            )
        for queue, block in (snapshot.get("processing") or {}).items():
            self.stdout.write(
                f"  queue {queue:<12} received={block.get('received')} "
                f"succeeded={block.get('succeeded')} failed={block.get('failed')} "
                f"depth={block.get('queue_depth')}"
            )
        for reason in snapshot.get("reasons", []):
            self.stdout.write(f"  ! {reason}")
