"""Local resource probes: disk space and file freshness.

These answer a question no dependency probe can: the worker's own host is
running out of something.  A full disk is the classic silent killer of a
message worker -- the broker keeps delivering, the handler keeps accepting,
and every write fails at the last step.
"""
from __future__ import annotations

import os
import shutil
import time

from ..core.model import ErrorCategory, Evidence
from .base import BaseCheck, CheckContext

_GB = 1024 ** 3


class DiskSpaceProbe(BaseCheck):
    """Free space on one path.

    Two thresholds because the useful signal is not the same everywhere: an
    absolute floor for a small volume, a ratio for a large one.  Whichever
    is configured is applied; if both are, either can degrade the check.
    """

    def __init__(self, path: str, *, name: str = "disk", dependency: str = "",
                 min_free_gb: float = 5.0, min_free_ratio: float | None = None,
                 fail_free_gb: float | None = None) -> None:
        self.name = name
        self.dependency = dependency
        self.path = path
        self.min_free_gb = float(min_free_gb)
        self.min_free_ratio = min_free_ratio
        # Below this, it is not "getting tight", it is over.
        self.fail_free_gb = fail_free_gb

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        try:
            usage = shutil.disk_usage(self.path)
        except FileNotFoundError:
            return self.fail(ctx, ErrorCategory.RESOURCE_MISSING, started,
                             detail="path does not exist", path=self.path)
        except OSError:
            return self.fail(ctx, ErrorCategory.INTERNAL, started,
                             detail="could not stat path", path=self.path)

        free_gb = usage.free / _GB
        ratio = usage.free / usage.total if usage.total else 0.0
        observed = {
            "path": self.path,
            "free_gb": round(free_gb, 2),
            "total_gb": round(usage.total / _GB, 2),
            "free_ratio": round(ratio, 4),
        }

        if self.fail_free_gb is not None and free_gb < self.fail_free_gb:
            return self.fail(ctx, ErrorCategory.RESOURCE_LOCKED, started,
                             detail="free space below the hard floor", **observed)
        low_absolute = free_gb < self.min_free_gb
        low_ratio = self.min_free_ratio is not None and ratio < float(self.min_free_ratio)
        if low_absolute or low_ratio:
            return self.degraded(ctx, ErrorCategory.RESOURCE_LOCKED, started,
                                 evidence=Evidence.PROBED,
                                 detail="free space below threshold", **observed)
        return self.ok(ctx, started, evidence=Evidence.PROBED, **observed)


class FileAgeProbe(BaseCheck):
    """Freshness of a file the worker (or something it depends on) writes.

    The standard use is a heartbeat or an export that a downstream job
    consumes: the file existing proves nothing, its mtime proves the
    pipeline still runs.
    """

    def __init__(self, path: str, *, name: str = "file_age", dependency: str = "",
                 max_age_s: float = 300.0, min_size_bytes: int = 0,
                 missing_is_failure: bool = True) -> None:
        self.name = name
        self.dependency = dependency
        self.path = path
        self.max_age_s = float(max_age_s)
        self.min_size_bytes = int(min_size_bytes)
        self.missing_is_failure = missing_is_failure

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            if not self.missing_is_failure:
                return self.ok(ctx, started, evidence=Evidence.PROBED,
                               path=self.path, present=False)
            return self.fail(ctx, ErrorCategory.RESOURCE_MISSING, started,
                             detail="file does not exist", path=self.path)
        except OSError:
            return self.fail(ctx, ErrorCategory.INTERNAL, started,
                             detail="could not stat file", path=self.path)

        # Wall clock, not monotonic: an mtime is an epoch timestamp and can
        # only be compared against one.  The staleness window is minutes, so
        # a clock correction is noise rather than a false alarm.
        age = max(0.0, time.time() - stat.st_mtime)
        observed = {"path": self.path, "age_s": round(age, 2),
                    "size_bytes": stat.st_size}

        if stat.st_size < self.min_size_bytes:
            return self.fail(ctx, ErrorCategory.STALE, started,
                             detail="file is smaller than expected", **observed)
        if age > self.max_age_s:
            return self.fail(ctx, ErrorCategory.STALE, started,
                             detail="file has not been updated recently", **observed)
        return self.ok(ctx, started, evidence=Evidence.PROBED, **observed)
