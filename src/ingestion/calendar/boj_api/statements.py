"""Scrape BoJ statement pages for the policy-rate decision.

The statement URL has two shapes (BoJ migrated newer meetings to PDFs
during late 2025; older meetings remain HTML):

- ``boj.or.jp/en/mopo/mpmdeci/state_<YYYY>/k<YYMMDD>a.htm`` (legacy)
- ``boj.or.jp/en/mopo/mpmdeci/mpr_<YYYY>/k<YYMMDD>a.pdf`` (current)

The per-year index page ``state_<YYYY>/index.htm`` lists the canonical
URL for every meeting that year, mixing both shapes. We discover the
URL through that index instead of templating, then dispatch on the URL
suffix to the HTML or PDF parser.

Both shapes carry the same policy-rate sentence::

    "The Bank will encourage the uncollateralized overnight call
     rate to remain at around 0.5 percent."

The sentence uses "remain" as a forward-looking guideline even when
the decision changes the rate — the 2024-07-31 hike from ~0% to
0.25% still phrased the new target as "remain at around 0.25 percent"
from the intermeeting period forward. We therefore extract the
numeric rate only; direction ("hold" vs "hike" vs "cut") belongs to
a downstream diff against the previous MPM's rate, not to the
sentence itself.

Fetch + parse + project are separable: tests feed fixture HTML / PDF
bytes to :func:`parse_statement_html` / :func:`parse_statement_pdf`;
live callers drive :func:`fetch_statement` (which discovers the URL
via the per-year index and returns the source URL plus the parsed
value). :func:`statement_value_to_records` emits a ``(raw, event)``
tuple whose ``provider_event_id`` matches the schedule-side write
exactly (same closing-date ISO anchor), so the ``actual`` value
upserts onto the existing row via the shared projector's merge CASE.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from typing import Any, Callable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import BojIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BOJ_RELEASE_TIME_LOCAL,
    BOJ_RELEASE_TZ,
    PROVIDER,
    BojCalendarEventRecord,
    BojCalendarRawRecord,
)
from .scraper import _BOJ_BROWSER_HEADERS

logger = logging.getLogger(__name__)

BOJ_STATEMENT_INDEX_URL_TEMPLATE = (
    "https://www.boj.or.jp/en/mopo/mpmdeci/state_{year}/index.htm"
)

# Statement file basename: ``k<YYMMDD>a.{htm,pdf}``. The two-digit year
# matches the closing date's two-digit year (BoJ won't recycle these
# until 2100), so a per-year index lookup is unambiguous.
_STATEMENT_HREF_RE = re.compile(
    r"k(?P<yymmdd>\d{6})a\.(?P<ext>htm|pdf)\b",
    re.IGNORECASE,
)


class BojStatementParseError(Exception):
    """Statement page didn't carry a parseable policy-rate sentence."""


class BojStatementUrlNotFoundError(Exception):
    """Per-year index didn't carry a link for the requested closing date."""


@dataclass(frozen=True)
class StatementValue:
    """Parsed BoJ statement outcome.

    ``rate`` is the decimal policy-rate target (``0.5`` for
    ``"around 0.5 percent"``). ``rate_text`` preserves the raw number
    string (``"0.5"``, ``"0.25"``) for the revision-diff audit trail.

    ``release_time_local`` carries the page's own "Release dates and
    times: Statement on Monetary Policy -- <Day>, <Month> <D> at
    HH:MM" clock when parseable — BoJ decisions can publish anywhere
    between 11:25 and 12:58 JST depending on committee deliberation
    length (fixture sample spans 11:25 → 12:56), so pinning the
    value row to a 12:00 JST placeholder would stamp the wrong
    timestamp on every upgrade. ``None`` means the page didn't carry
    a parseable release line; projection falls back to
    :data:`BOJ_RELEASE_TIME_LOCAL`.
    """

    closing_date: date
    rate: float
    rate_text: str
    release_time_local: str | None = None
    source_url: str | None = None


def build_statement_index_url(year: int) -> str:
    """Per-year index URL listing every meeting's statement link."""
    return BOJ_STATEMENT_INDEX_URL_TEMPLATE.format(year=year)


def parse_statement_index(html: str, *, year: int) -> dict[str, str]:
    """Map ``YYMMDD`` → absolute statement URL for one per-year index page.

    The index lists each meeting's statement link as either ``.htm``
    (legacy) or ``.pdf`` (current); both shapes encode the closing date
    as ``k<YYMMDD>a``. We collect every distinct match the page carries
    and return it keyed by the date stem.
    """
    soup = BeautifulSoup(html, "html.parser")
    base = build_statement_index_url(year)
    found: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        match = _STATEMENT_HREF_RE.search(href)
        if match is None:
            continue
        stem = match.group("yymmdd")
        # The first match for a date wins — the index sometimes carries
        # the same statement linked from multiple cells (e.g. a
        # "[PDF]" suffix link plus an icon link); both resolve to the
        # same target.
        found.setdefault(stem, urljoin(base, href))
    return found


def discover_statement_url(
    closing_date: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
    index_cache: dict[int, dict[str, str]] | None = None,
) -> str:
    """Look up the canonical statement URL for one MPM closing date.

    ``index_cache`` is shared across the burst loop / sweep so a 30-attempt
    burst on one connector doesn't re-fetch the per-year index on every
    attempt. Pass ``{}`` (a fresh dict) once per sweep and reuse the same
    object across calls.
    """
    year = closing_date.year
    cache = index_cache if index_cache is not None else {}
    if year not in cache:
        owned_session = session is None
        s = session or requests.Session()
        try:
            response = s.get(
                build_statement_index_url(year),
                headers=_BOJ_BROWSER_HEADERS,
                timeout=timeout,
            )
            response.raise_for_status()
            cache[year] = parse_statement_index(
                response.content.decode("utf-8"), year=year,
            )
        finally:
            if owned_session:
                s.close()
    stem = closing_date.strftime("%y%m%d")
    url = cache[year].get(stem)
    if url is None:
        raise BojStatementUrlNotFoundError(
            f"BoJ {year} statement index has no entry for "
            f"closing_date={closing_date.isoformat()} "
            f"(stem k{stem}a)"
        )
    return url


# ──────────────────────────────────────────────────────────────────────────
# Parser
# ──────────────────────────────────────────────────────────────────────────


def _normalize(text: str) -> str:
    """Collapse whitespace and normalize unicode dash variants."""
    text = text.replace("\xa0", " ")  # NBSP
    for variant in ("‑", "–", "—", "−"):
        text = text.replace(variant, "-")
    return " ".join(text.split())


# BoJ policy-rate sentence. Shape since the 2024 liftoff:
#   "The Bank will encourage the uncollateralized overnight call rate
#    to remain at around 0.5 percent."
# "around" may also render as "around" with the word; some historical
# statements used "at around" without "to remain" (direct
# "encourage... at around 0.1 percent"). The regex tolerates either.
_POLICY_RATE_RE = re.compile(
    r"encourage\s+the\s+uncollateralized\s+overnight\s+call\s+rate\s+"
    r"(?:to\s+(?:remain|be|continue to be)\s+)?"
    r"at\s+around\s+(?P<rate>\d+(?:\.\d+)?)\s*percent",
    re.IGNORECASE,
)


# "Release dates and times: <Statement title> -- <Day>, <Month> <D>
# at HH:MM" — BoJ prints the schedule block near the top of every
# statement page, always in JST, with the statement itself as the
# first entry. The regex finds the first "at HH:MM" following the
# block header so the statement-title wording variation doesn't
# matter (the hike statements carry a longer title like "Change in
# the Guideline for Money Market Operations ..." rather than
# "Statement on Monetary Policy"; both parse identically).
_RELEASE_TIME_RE = re.compile(
    r"Release\s+dates?\s+and\s+times?.*?"
    r"at\s+(?P<time>\d{1,2}:\d{2})",
    re.IGNORECASE | re.DOTALL,
)


def _parse_normalized_text(text: str, closing_date: date) -> StatementValue:
    """Run the policy-rate / release-time regexes against normalized text."""
    match = _POLICY_RATE_RE.search(text)
    if match is None:
        raise BojStatementParseError(
            "policy-rate sentence not found on BoJ statement "
            f"(closing_date={closing_date.isoformat()})"
        )
    rate_text = match.group("rate")
    release_match = _RELEASE_TIME_RE.search(text)
    release_time_local = (
        release_match.group("time") if release_match else None
    )
    return StatementValue(
        closing_date=closing_date,
        rate=float(rate_text),
        rate_text=rate_text,
        release_time_local=release_time_local,
    )


def parse_statement_html(html: str, closing_date: date) -> StatementValue:
    """Extract the policy-rate target from a BoJ statement HTML page.

    Raises :class:`BojStatementParseError` if no policy-rate sentence
    is found — upstream drift must surface loudly rather than silently
    emit a ``None`` value onto an existing schedule row.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = _normalize(soup.get_text(separator=" "))
    return _parse_normalized_text(text, closing_date)


def _extract_pdf_text(data: bytes) -> str:
    """Extract layout-preserving text from a BoJ statement PDF.

    Prefers ``pdftotext -layout`` (clean output for the policy-rate
    sentence); falls back to ``pypdf`` with a whitespace-repair pass
    that re-joins word-internal breaks ``pypdf`` inserts in BoJ PDFs
    (``"uncollateralize d"`` → ``"uncollateralized"``, ``"0. 75"`` →
    ``"0.75"``). Both passes feed the same regex downstream.
    """
    errors: list[str] = []
    if shutil.which("pdftotext"):
        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                tmp.write(data)
                tmp.flush()
                completed = subprocess.run(
                    ("pdftotext", "-layout", tmp.name, "-"),
                    check=False,
                    capture_output=True,
                    timeout=30.0,
                )
                if completed.returncode == 0 and completed.stdout.strip():
                    return completed.stdout.decode("utf-8", errors="replace")
                stderr = completed.stderr.decode("utf-8", errors="replace").strip()
                errors.append(f"pdftotext exited {completed.returncode}: {stderr}")
        except Exception as exc:  # pragma: no cover — environment specific
            # Restricted temp-dirs (read-only /tmp, locked-down sandboxes)
            # raise from `NamedTemporaryFile` before subprocess.run is
            # invoked. Recording the error and falling through lets the
            # in-memory pypdf path still produce a usable extraction.
            errors.append(f"pdftotext: {type(exc).__name__}: {exc}")
    try:
        from pypdf import PdfReader

        reader = PdfReader(BytesIO(data))
        raw = "\n".join(page.extract_text() or "" for page in reader.pages)
        if raw.strip():
            # Repair pypdf's word-internal whitespace before the regex
            # tries to match. The two patterns cover the failures we see
            # on BoJ statements: a digit-period-space-digit decimal split
            # ("0. 75") and a single space inside an English word
            # ("uncollateralize d").
            repaired = re.sub(r"(\d)\s*\.\s*(\d)", r"\1.\2", raw)
            repaired = re.sub(r"([a-z])\s([a-z]\b)", r"\1\2", repaired)
            return repaired
        errors.append("pypdf extracted empty text")
    except Exception as exc:  # pragma: no cover — depends on optional wheel
        errors.append(f"pypdf: {type(exc).__name__}: {exc}")
    detail = "; ".join(errors) if errors else "pdftotext/pypdf unavailable"
    raise BojStatementParseError(f"could not extract BoJ PDF text: {detail}")


def parse_statement_pdf(data: bytes, closing_date: date) -> StatementValue:
    """Extract the policy-rate target from a BoJ statement PDF."""
    text = _normalize(_extract_pdf_text(data))
    return _parse_normalized_text(text, closing_date)


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_statement(
    closing_date: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
    index_cache: dict[int, dict[str, str]] | None = None,
) -> StatementValue:
    """Discover, fetch, and parse one BoJ MPM statement.

    Resolves the canonical URL through the per-year index (cached via
    ``index_cache`` so a 30-attempt burst doesn't re-fetch it on every
    pass), GETs the document, and dispatches to the HTML or PDF parser
    based on the URL suffix.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        url = discover_statement_url(
            closing_date,
            session=s,
            timeout=timeout,
            index_cache=index_cache,
        )
        response = s.get(url, headers=_BOJ_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        if url.lower().endswith(".pdf"):
            value = parse_statement_pdf(response.content, closing_date)
        else:
            value = parse_statement_html(
                response.content.decode("utf-8"), closing_date,
            )
    finally:
        if owned_session:
            s.close()
    return StatementValue(
        closing_date=value.closing_date,
        rate=value.rate,
        rate_text=value.rate_text,
        release_time_local=value.release_time_local,
        source_url=url,
    )


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


def _format_rate(rate: float) -> str:
    """Format the policy-rate target as ``"X.XX"``.

    Two decimals keep ``0.5`` and ``0.25`` on a consistent display
    surface (``"0.50"`` / ``"0.25"``). Matches the FOMC target-range
    output shape.
    """
    return f"{rate:.2f}"


_HASH_FIELDS: tuple[str, ...] = ("rate", "event_time_utc")


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def statement_value_to_records(
    value: StatementValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: BojIndicatorSpec | None = None,
) -> tuple[BojCalendarRawRecord, BojCalendarEventRecord]:
    """Project a :class:`StatementValue` into (raw, event) records.

    ``provider_event_id`` uses the same closing-date ISO anchor as the
    schedule-side write in :mod:`.parser`, so the value row upserts
    onto the existing schedule row through the shared projector's
    merge CASE (stored datetime precision survives the merge; the
    ``actual`` column is filled).
    """
    resolved_spec = spec or INDICATOR_REGISTRY["BOJ_RATE"]

    release_time_local = value.release_time_local or BOJ_RELEASE_TIME_LOCAL
    scheduled = parse_scheduled_release_time(
        value.closing_date,
        release_time_local,
        default_tz=BOJ_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        value.closing_date.isoformat(),
    )

    actual = _format_rate(value.rate)
    # The discovered URL travels on the value through `fetch_statement`;
    # callers that synthesise a value directly (test fixtures) leave it
    # unset and we anchor on the per-year index URL instead so the raw
    # row keeps a real BoJ link.
    statement_url = value.source_url or build_statement_index_url(
        value.closing_date.year,
    )
    reference_label = value.closing_date.strftime("%B %Y")

    payload: dict[str, Any] = {
        "kind":           "boj_statement",
        "closing_date":   value.closing_date.isoformat(),
        "rate":           value.rate,
        "rate_text":      value.rate_text,
        "event_time_utc": event_time_utc,
        "statement_url":  statement_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BojCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BojCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=value.closing_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="JPY",
        unit=resolved_spec.unit,
        actual=actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Japan",
        source_url=statement_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
