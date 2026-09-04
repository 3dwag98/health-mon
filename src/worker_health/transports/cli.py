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
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--live", action="store_true", help="probe liveness instead of readiness")
    ap.add_argument("--timeout", type=float, default=2.0)
    ap.add_argument("--json", action="store_true", help="print the response body")
    args = ap.parse_args(argv)

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


if __name__ == "__main__":
    raise SystemExit(main())
