"""A host-local index of the worker processes running right now.

The problem this solves is specific and it shows up the first time anyone
tries to operate a fleet of ``manage.py`` workers:

    $ python manage.py worker_health
    worker-health is not wired in this process.

Of course it isn't.  ``manage.py worker_health`` is a *new* process; the
monitor lives in the worker process that is still running somewhere else.
An operator asking "how is this box doing" has no way to know that the
billing worker landed on 8081 and the notifier on 8082 -- least of all when
ports were assigned by search after a collision.

So every worker that starts an HTTP server drops a small JSON file here, and
removes it on the way out.  Entries whose process is gone are pruned when
the directory is read, which is what makes a crash safe: the next reader
cleans up after it.

Nothing secret is written -- service, instance, pid, host, port -- and the
directory is created 0700 under the invoking user, so a shared /tmp does not
turn this into a way to read another user's layout.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

ENV_DIR = "WORKER_HEALTH_RUNTIME_DIR"


def runtime_dir() -> Path:
    explicit = os.getenv(ENV_DIR)
    if explicit:
        return Path(explicit)
    xdg = os.getenv("XDG_RUNTIME_DIR")
    if xdg:
        return Path(xdg) / "worker-health"
    # Per-uid rather than a shared /tmp/worker-health: a directory another
    # user can create first is a directory another user controls.
    return Path("/tmp") / f"worker-health-{os.getuid()}"


def register(*, service: str, instance: str, host: str, port: int,
             version: str = "", command: str = "") -> Path | None:
    """Record this process.  Returns the file written, or None."""
    entry = {
        "service": service,
        "instance": instance,
        "pid": os.getpid(),
        "host": host,
        "port": port,
        "url": _url(host, port),
        "version": version,
        "command": command,
        "started": time.time(),
    }
    try:
        directory = runtime_dir()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = directory / f"{_safe(service)}-{os.getpid()}.json"
        # Write-then-rename so a reader never sees half a record.
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(entry), encoding="utf-8")
        tmp.replace(path)
        return path
    except OSError:
        # A read-only filesystem, a locked-down container, an unwritable
        # TMPDIR.  None of those is a reason for a worker not to start.
        return None


def unregister(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def entries() -> list[dict]:
    """Every live worker on this host, oldest first.

    Dead processes are pruned as a side effect: a worker that was SIGKILLed
    never got to clean up, and the reader is the only one left who can.
    """
    directory = runtime_dir()
    out: list[dict] = []
    try:
        files = sorted(directory.glob("*.json"))
    except OSError:
        return out

    for file in files:
        try:
            entry = json.loads(file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        pid = entry.get("pid")
        if not isinstance(pid, int) or not _alive(pid):
            try:
                file.unlink(missing_ok=True)
            except OSError:
                pass
            continue
        out.append(entry)
    out.sort(key=lambda e: e.get("started", 0))
    return out


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Someone else's process holds the pid: it exists, it is just not
        # ours to signal.  Treating that as dead would delete a live record.
        return True
    except OSError:
        return True
    return True


def _url(host: str, port: int) -> str:
    # 0.0.0.0 is what the server bound, not an address a client can call.
    reachable = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
    if ":" in reachable and not reachable.startswith("["):
        reachable = f"[{reachable}]"
    return f"http://{reachable}:{port}"


def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "-" for c in name) or "worker"
