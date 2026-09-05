"""``manage.py worker_health`` -- inspect health from the same process.

Useful when the HTTP port is not reachable (a locked-down host, a container
without a published port) and as the readable form of what /health returns.
"""
from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from worker_health_django.state import get_monitor


class Command(BaseCommand):
    help = "Print the worker-health snapshot for this process"

    def add_arguments(self, parser):
        parser.add_argument("--json", action="store_true", help="full snapshot as JSON")
        parser.add_argument("--metrics", action="store_true",
                            help="Prometheus exposition instead of JSON")

    def handle(self, *args, **options):
        monitor = get_monitor()
        if monitor is None:
            self.stderr.write(
                "worker-health is not wired in this process. Check that "
                "WORKER_HEALTH['ENABLED'] is True and that this command is not "
                "in the skip list (see worker_health_django.autowire)."
            )
            return

        if options["metrics"]:
            from worker_health.telemetry.prometheus import render

            self.stdout.write(render(monitor))
            return

        snapshot = monitor.snapshot_dict()
        if options["json"]:
            self.stdout.write(json.dumps(snapshot, indent=2, default=str))
            return

        self.stdout.write(
            f"{snapshot['service']} ({snapshot['instance']}): "
            f"readiness={snapshot['readiness']} liveness={snapshot['liveness']}"
        )
        for name, check in snapshot["checks"].items():
            flag = "critical" if check["critical"] else "non-critical"
            detail = f" -- {check['detail']}" if check.get("detail") else ""
            self.stdout.write(
                f"  {name:<16} {check['internal_status']:<9} "
                f"{check['evidence']:<13} {flag}{detail}"
            )
        for reason in snapshot.get("reasons", []):
            self.stdout.write(f"  ! {reason}")
