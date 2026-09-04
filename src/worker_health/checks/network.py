"""Network reachability probes: HTTP, TCP, DNS.

All three are read-only by construction.  The HTTP probe refuses any method
that is not GET, HEAD or OPTIONS -- a health check that POSTs is a health
check that can create an order, and the guardrail says probes never mutate
external state.  It is enforced in code rather than documented, because a
YAML file that says `method: POST` should fail at wiring time, not at 3am.
"""
from __future__ import annotations

import socket
import time

from ..core.model import ErrorCategory, Evidence, Status
from ..security import endpoint
from .base import BaseCheck, CheckContext

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class HttpProbe(BaseCheck):
    """External API health over plain urllib -- no dependency added.

    Only the status code and the latency are looked at.  The body is read
    and discarded up to a small bound: a vendor that returns 200 with a
    50MB error page should not be able to allocate the worker to death.
    """

    def __init__(
        self,
        url: str,
        *,
        name: str = "http",
        dependency: str = "",
        expect_status: int | tuple[int, ...] = 200,
        method: str = "GET",
        timeout: float = 2.0,
        headers: dict | None = None,
        slow_ms: float | None = None,
        max_bytes: int = 65536,
    ) -> None:
        method = method.upper()
        if method not in SAFE_METHODS:
            raise ValueError(
                f"http probe method must be one of {sorted(SAFE_METHODS)}, got {method!r}"
            )
        self.name = name
        self.dependency = dependency
        self.url = url
        self.method = method
        self.timeout = timeout
        self.headers = dict(headers or {})
        self.slow_ms = slow_ms
        self.max_bytes = max_bytes
        self.expect = (
            (expect_status,) if isinstance(expect_status, int) else tuple(expect_status)
        )

    def probe(self, ctx: CheckContext):
        import urllib.error
        import urllib.request

        started = time.perf_counter()
        request = urllib.request.Request(self.url, method=self.method,
                                         headers=self.headers)
        # The URL can carry a token in a query string; only the host ever
        # reaches an observed field or a log line.
        host = endpoint(self.url) or "unknown"
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response.read(self.max_bytes)
                code = response.status
        except urllib.error.HTTPError as exc:
            code = exc.code
        except Exception as exc:  # noqa: BLE001
            return self.fail(ctx, self.classify(exc), started,
                             detail="request failed", host=host)

        latency_ms = (time.perf_counter() - started) * 1000.0
        observed = {"http_status": code, "host": host}

        if code not in self.expect:
            return self.fail(ctx, _http_category(code), started,
                             detail=f"unexpected status {code}", **observed)
        if self.slow_ms is not None and latency_ms > self.slow_ms:
            return self.degraded(ctx, ErrorCategory.TIMEOUT, started,
                                 evidence=Evidence.PROBED,
                                 detail="responded above the slow threshold",
                                 **observed)
        return self.ok(ctx, started, **observed)

    def classify(self, exc: BaseException) -> ErrorCategory:
        text = str(exc).lower()
        name = type(exc).__name__.lower()
        if "timed out" in text or "timeout" in name:
            return ErrorCategory.TIMEOUT
        if "refused" in text:
            return ErrorCategory.CONNECTION_REFUSED
        if "name or service not known" in text or "nodename nor servname" in text:
            return ErrorCategory.RESOURCE_MISSING
        if "certificate" in text or "ssl" in text:
            return ErrorCategory.PROTOCOL_ERROR
        return ErrorCategory.CONNECTION_LOST


def _http_category(code: int) -> ErrorCategory:
    if code in (401, 403):
        return ErrorCategory.AUTH_FAILED
    if code == 404:
        return ErrorCategory.RESOURCE_MISSING
    if code == 408 or code == 504:
        return ErrorCategory.TIMEOUT
    if code == 429:
        return ErrorCategory.CREDIT_EXHAUSTED
    if 500 <= code < 600:
        return ErrorCategory.CONNECTION_LOST
    return ErrorCategory.PROTOCOL_ERROR


class TcpProbe(BaseCheck):
    """Connectivity only: open a socket, close it, report the handshake time.

    Useful for a dependency with no cheap application-level ping, and as the
    thing that distinguishes "the port is closed" from "the service accepted
    the connection and then said nothing" -- which are different outages and
    which a single latency number cannot tell apart.
    """

    def __init__(self, host: str, port: int, *, name: str = "tcp",
                 dependency: str = "", timeout: float = 2.0) -> None:
        self.name = name
        self.dependency = dependency
        self.host = host
        self.port = int(port)
        self.timeout = timeout

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        observed = {"host": self.host, "port": self.port}
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout):
                pass
        except socket.timeout:
            return self.fail(ctx, ErrorCategory.TIMEOUT, started,
                             detail="connect timed out", **observed)
        except ConnectionRefusedError:
            return self.fail(ctx, ErrorCategory.CONNECTION_REFUSED, started,
                             detail="connection refused", **observed)
        except socket.gaierror:
            return self.fail(ctx, ErrorCategory.RESOURCE_MISSING, started,
                             detail="host did not resolve", **observed)
        except OSError:
            return self.fail(ctx, ErrorCategory.CONNECTION_LOST, started,
                             detail="connect failed", **observed)
        return self.ok(ctx, started, **observed)


class DnsProbe(BaseCheck):
    """Resolution health.

    Its own check because DNS failure has a signature every other probe
    misreports: everything fails at once, all with connection errors, and
    the dependency teams all get paged for a problem none of them own.
    """

    def __init__(self, host: str, *, name: str = "dns", dependency: str = "",
                 family: str = "any", min_records: int = 1) -> None:
        self.name = name
        self.dependency = dependency
        self.host = host
        self.min_records = int(min_records)
        self._family = {
            "any": socket.AF_UNSPEC, "ipv4": socket.AF_INET, "ipv6": socket.AF_INET6,
        }.get(str(family).lower(), socket.AF_UNSPEC)

    def probe(self, ctx: CheckContext):
        started = time.perf_counter()
        try:
            records = socket.getaddrinfo(self.host, None, self._family,
                                         socket.SOCK_STREAM)
        except socket.gaierror as exc:
            return self.fail(ctx, ErrorCategory.RESOURCE_MISSING, started,
                             detail="name did not resolve", host=self.host,
                             error=type(exc).__name__)
        except Exception:  # noqa: BLE001
            return self.fail(ctx, ErrorCategory.UNKNOWN, started,
                             detail="resolver error", host=self.host)

        count = len({r[4][0] for r in records})
        observed = {"host": self.host, "records": count}
        if count < self.min_records:
            return self.degraded(ctx, ErrorCategory.RESOURCE_MISSING, started,
                                 evidence=Evidence.PROBED,
                                 detail="fewer records than expected", **observed)
        return self.ok(ctx, started, **observed)
