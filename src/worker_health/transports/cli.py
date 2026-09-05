"""CLI probe: exit 0 when ready, 1 when not.

For a shell check, a PM2 healthcheck, or a container HEALTHCHECK line.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="worker-health")
    ap.add_argument("--url", default=None,
                    help="worker to probe (default: the only one on this host)")
    ap.add_argument("--service", default=None,
                    help="pick a registered worker by service name")
    ap.add_argument("--live", action="store_true", help="probe liveness instead of readiness")
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--json", action="store_true", help="print the response body")
    ap.add_argument("--list", action="store_true",
                    help="list the workers running on this host and exit")
    args = ap.parse_args(argv)

    if args.list:
        return _list()

    url = args.url or _discover(args.service)
    if url is None:
        print("no worker-health process found on this host; pass --url",
              file=sys.stderr)
        return 1
    args.url = url

    path = "/live" if args.live else "/ready"
    try:
        with urllib.request.urlopen(args.url.rstrip("/") + path, timeout=args.timeout) as r:
            body = json.loads(r.read() or b"{}")
            code = r.status
    except urllib.error.HTTPError as e:
        body = json.loads(e.read() or b"{}")
        code = e.code
    except Exception as exc:
        print(f"unreachable: {type(exc).__name__}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(body, indent=2))
    else:
        print(f"{body.get('status', 'unknown')} (HTTP {code})")
    return 0 if code == 200 else 1


def _discover(service: str | None) -> str | None:
    """Find a running worker without being told where it is.

    Every worker that binds a health port publishes host, port and pid to a
    per-user run directory, so the common case -- one worker on this box --
    needs no arguments at all, and a fleet needs only a service name.
    """
    from . import registry

    entries = [e for e in registry.entries()
               if service is None or e.get("service") == service]
    if len(entries) == 1:
        return entries[0].get("url")
    if not entries:
        # Nothing registered: fall back to the historical default rather
        # than failing, since a worker on an older version still answers.
        return None if service else "http://127.0.0.1:8080"
    print(f"{len(entries)} workers are running; choose one with --service:",
          file=sys.stderr)
    for entry in entries:
        print(f"  {entry.get('service')}  {entry.get('url')}", file=sys.stderr)
    return None


def _list() -> int:
    from . import registry

    entries = registry.entries()
    if not entries:
        print("no worker-health processes registered on this host")
        return 1
    print(f"{'SERVICE':<24} {'INSTANCE':<24} {'PID':>7}  URL")
    for entry in entries:
        print(f"{entry.get('service', ''):<24} {entry.get('instance', ''):<24} "
              f"{entry.get('pid', ''):>7}  {entry.get('url', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
