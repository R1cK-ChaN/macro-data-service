"""Secret-redaction helpers (issue #102).

Provider error messages — typically wrapped urllib3 / requests exceptions —
include the full request URL in their text. When that URL carries an
``api_key=…`` or similar token in the query string, the secret leaks into
``shadow.log`` and ``daily_digest.jsonl`` the moment we serialize the error.

:func:`redact_secrets` strips the value of every known secret query
parameter, replacing it with ``***``. The match is anchored on the
parameter name to avoid mangling unrelated text.

:class:`SecretRedactingFilter` is a logging filter wrapping the same call
so the file handler scrubs records regardless of which call site
formatted them.
"""

from __future__ import annotations

import logging
import re
import traceback

# Parameter names whose values must never reach disk. The list is
# deliberately specific: matching ``key=`` would clobber unrelated values
# like ``key=stored`` in human-readable log lines.
_SECRET_PARAM_NAMES = (
    "api_key",
    "apikey",
    "api_token",
    "access_token",
    "auth_token",
    "authtoken",
    "authentication",
    "token",
    "password",
    "passwd",
    "client_secret",
)

_SECRET_QUERY_RE = re.compile(
    r"(?i)\b(" + "|".join(_SECRET_PARAM_NAMES) + r")=([^&\s\"'<>\\]+)",
)

_BEARER_RE = re.compile(r"(?i)(bearer\s+)([A-Za-z0-9\-_\.=]+)")

_REDACTED = "***"


def redact_secrets(text: str | None) -> str:
    """Replace secret values inside ``text`` with ``***``.

    Returns ``""`` for ``None`` to keep callers tolerant of missing
    error fields. Idempotent — already-redacted strings pass through
    unchanged because ``***`` does not match the value alphabet.
    """
    if not text:
        return text or ""
    redacted = _SECRET_QUERY_RE.sub(lambda m: f"{m.group(1)}={_REDACTED}", text)
    redacted = _BEARER_RE.sub(lambda m: f"{m.group(1)}{_REDACTED}", redacted)
    return redacted


class SecretRedactingFilter(logging.Filter):
    """Logging filter that scrubs secrets from formatted records.

    Attach to any handler whose output may persist (file handlers,
    syslog, etc.). The filter rewrites ``record.msg`` and clears the
    cached ``record.args`` so that downstream formatting stays in sync.

    ``record.exc_info`` and ``record.stack_info`` are rendered and
    redacted here — the standard ``logging.Formatter`` appends them
    *after* the filter chain runs, so if we don't pre-render them, an
    ``exc_info=True`` log call will leak provider URLs verbatim into
    the traceback section of the file output.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except Exception:
            rendered = None
        if rendered is not None:
            cleaned = redact_secrets(rendered)
            if cleaned != rendered:
                record.msg = cleaned
                record.args = None

        if record.exc_info:
            etype, value, tb = record.exc_info
            text = "".join(traceback.format_exception(etype, value, tb))
            record.exc_text = redact_secrets(text)
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = redact_secrets(record.exc_text)

        if record.stack_info:
            record.stack_info = redact_secrets(record.stack_info)

        return True


__all__ = ["redact_secrets", "SecretRedactingFilter"]
