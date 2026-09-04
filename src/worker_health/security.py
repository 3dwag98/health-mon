"""Redaction.

One rule, enforced in one place: nothing that could be a credential leaves
the process.  Not in a response body, not in a log line, not in a metric
label, not in a `detail` string.

This matters more than it looks.  Driver exceptions are the leak: psycopg
puts the whole DSN in several of its connection errors, pika puts the
username in an authentication failure, redis-py echoes the command it was
running -- and a health system's entire job is to report exactly those
exceptions.  The package's primary defence is structural (a closed
``ErrorCategory`` enum is what gets reported, never the exception text);
this module is the second line, for the places where a human-readable
string genuinely has to be carried.
"""
from __future__ import annotations

import re
from typing import Any, Mapping

MASK = "***"

# scheme://user:password@host -> the password only.
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^:/@\s]+):(?P<secret>[^@\s]*)@")

# key=value and key: value forms, for DSN fragments and config echoes.
_SECRET_KEYS = (
    "password", "passwd", "pwd", "secret", "token", "api_key", "apikey",
    "access_key", "secret_key", "private_key", "authorization", "auth",
    "credentials", "sas", "session_token", "client_secret",
)
_KEY_VALUE = re.compile(
    r"(?i)\b(?P<key>" + "|".join(_SECRET_KEYS) + r")\b(?P<sep>\s*[=:]\s*)(?P<value>\"[^\"]*\"|'[^']*'|[^\s,;&)]+)"
)

# A JWT is three base64url segments.  Recognisable on sight, so redact it
# wherever it appears rather than only under a known key.
_JWT = re.compile(r"\beyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}\b")

_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._\-+/=]{6,}")

# Keys whose VALUE is a secret, for mapping redaction.
_SECRET_KEY_HINTS = tuple(_SECRET_KEYS) + ("dsn", "url", "uri", "connection_string", "conn_str")


def redact(value: str | None) -> str | None:
    """Scrub a free-text string of anything credential-shaped.

    Idempotent, and safe to call on text that contains no secret at all.
    """
    if not value:
        return value
    out = _URL_CREDENTIALS.sub(lambda m: f"{m.group('scheme')}{m.group('user')}:{MASK}@", value)
    out = _KEY_VALUE.sub(lambda m: f"{m.group('key')}{m.group('sep')}{MASK}", out)
    out = _JWT.sub(MASK, out)
    out = _BEARER.sub(lambda m: f"{m.group(1)} {MASK}", out)
    return out


def redact_dsn(dsn: str | None) -> str | None:
    """``postgres://user:password@host/db`` -> ``postgres://user:***@host/db``.

    The user, host, port and database survive because they are what an
    operator needs to see to know WHICH database is in trouble.
    """
    return redact(dsn)


def endpoint(dsn: str | None) -> str | None:
    """Reduce a DSN to ``host:port`` -- no user, no password, no path.

    This is the form safe to put in a metric label or an ``observed`` field:
    bounded cardinality and no credential material at all.
    """
    if not dsn:
        return None
    try:
        from urllib.parse import urlsplit

        parts = urlsplit(dsn)
        if parts.hostname:
            return f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname
    except Exception:
        pass
    return None


def redact_mapping(data: Mapping[str, Any]) -> dict[str, Any]:
    """Redact a config-shaped mapping, recursively.

    Used when configuration is echoed into a log line or the ``/health``
    body: a probe's params can hold a DSN, and a config dump is the classic
    way a password reaches a log aggregator.
    """
    out: dict[str, Any] = {}
    for key, value in data.items():
        lowered = str(key).lower()
        if isinstance(value, Mapping):
            out[key] = redact_mapping(value)
        elif isinstance(value, (list, tuple)):
            out[key] = [
                redact_mapping(v) if isinstance(v, Mapping)
                else (redact(v) if isinstance(v, str) else v)
                for v in value
            ]
        elif any(hint in lowered for hint in _SECRET_KEY_HINTS):
            if isinstance(value, str) and "://" in value:
                out[key] = redact_dsn(value)      # keep host/db, lose the secret
            elif value is None:
                out[key] = None
            else:
                out[key] = MASK
        elif isinstance(value, str):
            out[key] = redact(value)
        else:
            out[key] = value
    return out


def safe_detail(text: str | None, limit: int = 200) -> str | None:
    """Redact and bound a `detail` string.

    Bounded because a detail field is rendered in a dashboard cell and
    shipped in every snapshot; an unbounded driver traceback in there is a
    denial-of-service on the reader's attention as well as on the log bill.
    """
    cleaned = redact(text)
    if cleaned is None:
        return None
    cleaned = " ".join(cleaned.split())
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 1] + "…"
