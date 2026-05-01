"""Tests for the secret-redaction helper (issue #102 P1)."""

from __future__ import annotations

import io
import logging

import pytest

from ingestion._shared.redaction import SecretRedactingFilter, redact_secrets


@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            "https://api.stlouisfed.org/fred/series/observations?series_id=T5YIE"
            "&api_key=00000000000000000000000000000000&file_type=json",
            "https://api.stlouisfed.org/fred/series/observations?series_id=T5YIE"
            "&api_key=***&file_type=json",
        ),
        (
            "GET /v2/data?api_token=ABCDEF12345&format=json",
            "GET /v2/data?api_token=***&format=json",
        ),
        (
            "ConnectionError: token=xyz123 host=api.example.com",
            "ConnectionError: token=*** host=api.example.com",
        ),
        ("Authorization: Bearer abc.def-XYZ=", "Authorization: Bearer ***"),
        ("APIKey=Foo&other=Bar", "APIKey=***&other=Bar"),
        ("no secret in this string", "no secret in this string"),
    ],
)
def test_redacts_known_secret_params(raw: str, expected: str) -> None:
    assert redact_secrets(raw) == expected


def test_redact_is_idempotent() -> None:
    once = redact_secrets("api_key=DEADBEEF&x=1")
    twice = redact_secrets(once)
    assert once == twice == "api_key=***&x=1"


def test_redact_handles_none_and_empty() -> None:
    assert redact_secrets(None) == ""
    assert redact_secrets("") == ""


def test_redact_preserves_unrelated_keyvalue_pairs() -> None:
    # ``key=`` alone must not match — only the named secret params.
    text = "key=42 stored=128 series_id=ABC"
    assert redact_secrets(text) == text


def test_filter_scrubs_log_records() -> None:
    log = logging.getLogger("redaction-test")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.addFilter(SecretRedactingFilter())
    log.addHandler(handler)

    log.warning(
        "fred_daily ERROR: %s",
        "HTTPSConnectionPool: /fred?api_key=DEADBEEF&series=T5YIE",
    )

    output = buf.getvalue().strip()
    assert "DEADBEEF" not in output
    assert "api_key=***" in output


def test_filter_passthrough_when_no_secret() -> None:
    log = logging.getLogger("redaction-test-clean")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.addFilter(SecretRedactingFilter())
    log.addHandler(handler)

    log.info("cycle 5 done in 12.3s")
    assert buf.getvalue().strip() == "cycle 5 done in 12.3s"


def test_filter_scrubs_exception_traceback() -> None:
    """A logger called with ``exc_info=True`` would otherwise leak the
    provider URL embedded in the exception text — the formatter appends
    the traceback after the filter chain. The filter must pre-render
    and redact so the persisted log is clean."""

    log = logging.getLogger("redaction-test-traceback")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.addFilter(SecretRedactingFilter())
    log.addHandler(handler)

    try:
        raise RuntimeError(
            "HTTPSConnectionPool: /fred?api_key=DEADBEEF&series=T5YIE"
        )
    except RuntimeError:
        log.error("ingestion failed", exc_info=True)

    output = buf.getvalue()
    assert "DEADBEEF" not in output
    assert "api_key=***" in output
    # Traceback header should still be present so debuggability is
    # preserved.
    assert "Traceback" in output
