"""Centralized secret redaction (PRD 022).

One scrub chokepoint for every observability surface: any credential parsed at
the tool boundary (or decrypted off a job row) is registered here, and every
log line, error message, job-trail entry, and provenance record is passed
through :func:`redact`/:func:`redact_obj` before it is emitted or persisted —
so a per-user EDL token can never leak through logging or the durable trail.

Log coverage uses ``logging.setLogRecordFactory``: the factory is the single
point every ``LogRecord`` from every logger passes through *before* any
handler or capture hook sees it, so redaction cannot be bypassed by a handler
added later (including pytest's ``caplog``). The factory is installed lazily,
the first time a secret is registered, and is a no-op passthrough while no
secrets are registered.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict

__all__ = ["redact", "redact_obj", "register_secret", "clear_secrets"]

#: What a registered secret is replaced with, everywhere.
REDACTED = "[redacted]"

#: Registered secrets are bounded (oldest evicted) so a long-lived process
#: serving many identities never grows this set without bound.
_MAX_SECRETS = 256

#: Secrets shorter than this are never registered: replacing very short
#: strings would mangle unrelated text far more often than it protects.
_MIN_SECRET_LENGTH = 8

_secrets: OrderedDict[str, None] = OrderedDict()
_lock = threading.Lock()
_factory_installed = False


def register_secret(value: str | None) -> None:
    """Register ``value`` for redaction on every surface. Idempotent, bounded."""
    if not value or len(value) < _MIN_SECRET_LENGTH:
        return
    with _lock:
        _secrets.pop(value, None)
        _secrets[value] = None
        while len(_secrets) > _MAX_SECRETS:
            _secrets.popitem(last=False)
    _install_log_redaction()


def clear_secrets() -> None:
    """Drop every registered secret (test helper)."""
    with _lock:
        _secrets.clear()


def redact(text: str) -> str:
    """Return ``text`` with every registered secret replaced by ``[redacted]``."""
    if not text or not _secrets:
        return text
    with _lock:
        secrets = list(_secrets)
    for secret in secrets:
        if secret in text:
            text = text.replace(secret, REDACTED)
    return text


def redact_obj(obj: object) -> object:
    """Recursively redact every string inside a JSON-ish structure.

    Applied to provenance event details and job-trail payloads before they are
    persisted; non-string leaves pass through untouched.
    """
    if not _secrets:
        return obj
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        return {key: redact_obj(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        redacted = [redact_obj(item) for item in obj]
        return type(obj)(redacted) if isinstance(obj, tuple) else redacted
    return obj


def _install_log_redaction() -> None:
    """Install the redacting ``LogRecord`` factory once (idempotent)."""
    global _factory_installed
    with _lock:
        if _factory_installed:
            return
        _factory_installed = True
    previous = logging.getLogRecordFactory()

    def factory(*args: object, **kwargs: object) -> logging.LogRecord:
        record = previous(*args, **kwargs)
        if _secrets:
            # Format eagerly so args holding a secret can't survive redaction.
            record.msg = redact(record.getMessage())
            record.args = None
        return record

    logging.setLogRecordFactory(factory)
