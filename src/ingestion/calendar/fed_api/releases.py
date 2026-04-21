"""Scrape ``federalreserve.gov/newsevents/releasedates.htm`` for the
non-FOMC Fed calendar surface (issue #9 P4a).

Scope covers three indicators the Fed publishes outside the FOMC
calendar:

- **Beige Book** — narrative regional summary, ~8 per year, 2 weeks
  before each FOMC meeting, 14:00 ET.
- **H.4.1** — Factors Affecting Reserve Balances, weekly (Thursdays),
  16:30 ET.
- **H.8** — Assets and Liabilities of Commercial Banks, weekly
  (Fridays), 16:15 ET.

Summary of Economic Projections (SEP) is explicitly excluded: it
already rides on the FOMC calendar via :attr:`FomcMeetingEntry.has_sep`
and the title ``"FOMC Rate Decision + SEP"``, so emitting a separate
SEP row from this page would create a duplicate calendar event for
every quarterly FOMC meeting.

Scheduled Fed Chair / Vice-Chair speeches on the release-dates page
are out of P4a scope — they belong in the news / speech pipeline,
not here.

Fetch + parse are separable functions. Tests feed fixture HTML
directly to :func:`parse_releasedates_html`; live callers use
:func:`fetch_releasedates_html` which reuses the browser-UA header
bundle from the FOMC scraper (``federalreserve.gov`` 403s on
default ``python-requests`` UA).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FOMC_RELEASE_TZ,
    PROVIDER,
    FedCalendarEventRecord,
    FedCalendarRawRecord,
)
from .scraper import _FED_BROWSER_HEADERS

logger = logging.getLogger(__name__)

FED_RELEASEDATES_URL = (
    "https://www.federalreserve.gov/newsevents/releasedates.htm"
)

# Release-title match table. Entry order matters: longer / more-
# specific fragments first so ``"H.4.1"`` matches before the
# less-specific ``"H."`` never would. Each entry maps a lowercase
# substring to the target indicator id + default release time
# (applied when the row has no explicit time cell).
_MATCH_ENTRIES: tuple[tuple[str, str, str], ...] = (
    ("beige book",      "BEIGE_BOOK", "2:00 PM"),
    ("h.4.1",           "FED_H41",    "4:30 PM"),
    ("h4.1",            "FED_H41",    "4:30 PM"),
    ("h.8",             "FED_H8",     "4:15 PM"),
    # "h8" bare substring is too common — require the dotted form.
)

# Release titles that must be ignored even when they substring-match
# the table above — currently SEP (rides on FOMC calendar) and the
# speeches / testimony rows that fall outside P4a scope.
_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    "summary of economic projections",
    # Defensive: G.19 Consumer Credit and H.15 Selected Interest Rates
    # happen to share a tokenised shape with some indicators; their
    # ids don't appear in ``_MATCH_ENTRIES`` but an inline title drift
    # ("H.4.1 Consumer Credit" on a future page) would otherwise
    # substring-match and mis-label. Cheap to list here and they
    # don't appear in the whitelist by design.
    "consumer credit - g.19",
    "consumer credit g.19",
    "selected interest rates - h.15",
    "selected interest rates h.15",
)


class FedReleasedatesParseError(ValueError):
    """Raised when the releasedates.htm DOM deviates from expectations."""


@dataclass(frozen=True)
class FedReleaseEntry:
    """One matched row from ``releasedates.htm``, pre-projection."""

    series_id: str          # matches INDICATOR_REGISTRY key
    release_title: str      # verbatim cell text
    release_date: str       # ISO YYYY-MM-DD
    release_time_local: str # verbatim "2:00 PM" (or default from match)
    event_time_utc: str     # ISO datetime with UTC offset


# Date cell shapes observed on Fed releasedates.htm:
#   "January 7, 2026"
#   "Jan 7, 2026"
#   "Jan. 7, 2026"
#   "2026-01-07"
_RELEASE_DATE_RE = re.compile(
    r"([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})"
)
_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
# No trailing ``\b``: the dotted forms (``a.m.`` / ``p.m.``) end on a
# period, which is not a word character, so a trailing ``\b`` would
# refuse to match them and leave ``_extract_time`` with a bare
# ``HH:MM`` that ``parse_scheduled_release_time`` reads as 24-hour —
# placing every PM Fed release 12h early in UTC.
_TIME_RE = re.compile(
    r"\b(\d{1,2})[:.](\d{2})(?:\s*(AM|PM|A\.M\.|P\.M\.))?",
    re.IGNORECASE,
)

_MONTH_NAMES: dict[str, int] = {
    "january":   1, "february":  2, "march":     3, "april":     4,
    "may":       5, "june":      6, "july":      7, "august":    8,
    "september": 9, "october":  10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


def _parse_date_from_text(text: str) -> date | None:
    m = _ISO_DATE_RE.search(text)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            pass
    m = _RELEASE_DATE_RE.search(text)
    if m:
        month_s, day_s, year_s = m.groups()
        month = _MONTH_NAMES.get(month_s.strip(".").lower())
        if month:
            try:
                return date(year=int(year_s), month=month, day=int(day_s))
            except ValueError:
                pass
    return None


def _extract_time(text: str, default: str) -> str:
    """Pull a ``HH:MM am/pm`` time out of the row text, or fall back.

    When the row carries a bare ``HH:MM`` without an AM/PM suffix,
    the per-indicator ``default`` wins rather than the bare form —
    a 24-hour reading of ``"4:30"`` places H.4.1 at 04:30 ET instead
    of 16:30 ET, flipping every PM release 12 hours early.
    """
    m = _TIME_RE.search(text)
    if not m:
        return default
    hh, mm, suffix = m.groups()
    if suffix is None:
        return default
    hour = int(hh)
    suffix_clean = suffix.upper().replace(".", "")
    return f"{hour}:{mm} {suffix_clean}"


def _match_release(title: str) -> tuple[str, str] | None:
    """Return ``(series_id, default_time)`` or ``None`` for the title."""
    lowered = title.lower()
    for fragment in _EXCLUDE_FRAGMENTS:
        if fragment in lowered:
            return None
    for fragment, series_id, default_time in _MATCH_ENTRIES:
        if fragment in lowered:
            return series_id, default_time
    return None


def parse_releasedates_html(
    html: str,
    *,
    row_issues: list[str] | None = None,
) -> list[FedReleaseEntry]:
    """Extract :class:`FedReleaseEntry` rows from releasedates.htm.

    Walks every ``<tr>`` in every ``<table>``. A row matches when its
    text substring-matches one of ``_MATCH_ENTRIES`` (and doesn't
    substring-match an ``_EXCLUDE_FRAGMENTS`` phrase). Matched rows
    without a parseable date are captured in ``row_issues`` rather
    than silently dropped — same shape as BEA P2a's parser.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise FedReleasedatesParseError(
            "no <table> found in releasedates HTML"
        )

    entries: list[FedReleaseEntry] = []
    matched_any = False
    for table in tables:
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            row_text = row.get_text(" ", strip=True)
            if not row_text:
                continue
            match = _match_release(row_text)
            if match is None:
                continue
            matched_any = True
            series_id, default_time = match
            spec = INDICATOR_REGISTRY[series_id]

            release_date = _parse_date_from_text(row_text)
            if release_date is None:
                if row_issues is not None:
                    row_issues.append(
                        f"{row_text[:80]!r}: no parseable date"
                    )
                continue

            release_time = _extract_time(row_text, default=default_time)
            try:
                scheduled = parse_scheduled_release_time(
                    release_date, release_time,
                    default_tz=FOMC_RELEASE_TZ,
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{row_text[:80]!r} time={release_time!r}: {exc}"
                    )
                continue

            # Title = the release-name portion of the row, not the full
            # row text. Best-effort extraction: strip leading date +
            # time tokens, keep the rest. Falls back to the registry
            # title when the regex doesn't match.
            title_fragment = _extract_title_fragment(row_text, spec)
            entries.append(
                FedReleaseEntry(
                    series_id=series_id,
                    release_title=title_fragment,
                    release_date=release_date.isoformat(),
                    release_time_local=release_time,
                    event_time_utc=scheduled.utc.isoformat(),
                )
            )

    if not matched_any:
        raise FedReleasedatesParseError(
            "no rows matching the releasedates whitelist fragments"
        )
    if not entries:
        # Codex P4a — if every whitelisted row hit a row-level parse
        # failure (Fed date or time format drifted on every match),
        # returning an empty list would let ``fetch_fed_releasedates``
        # commit a successful zero-row run and mask the outage.
        # ``row_issues`` still carries the per-row detail for
        # operator diagnosis.
        detail = f" ({len(row_issues or ())} row issues)" if row_issues else ""
        raise FedReleasedatesParseError(
            "whitelist matched but every row failed parsing" + detail
        )
    return entries


_DATE_PREFIX_RE = re.compile(
    r"^\s*(?:[A-Za-z]+\.?\s+\d{1,2},\s*\d{4}|\d{4}-\d{2}-\d{2})\s*"
)


def _extract_title_fragment(row_text: str, spec: FedIndicatorSpec) -> str:
    """Pull the release-name portion out of a full row's text.

    Row text on releasedates.htm typically reads ``"January 7, 2026
    4:30 PM Factors Affecting Reserve Balances - H.4.1"``. Strip the
    date prefix + time token to keep only the release name. Falls
    back to the registry title when nothing remains.
    """
    stripped = _DATE_PREFIX_RE.sub("", row_text)
    stripped = _TIME_RE.sub("", stripped, count=1).strip(" -—–")
    return stripped or spec.title


# ──────────────────────────────────────────────────────────────────────────
# Projection
# ──────────────────────────────────────────────────────────────────────────


def release_entry_to_records(
    entry: FedReleaseEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: FedIndicatorSpec | None = None,
) -> tuple[FedCalendarRawRecord, FedCalendarEventRecord]:
    """Project a :class:`FedReleaseEntry` to (raw, event) records.

    ``provider_event_id`` anchors on ``(indicator, release_date)`` so
    each release is a distinct calendar event. H.4.1 / H.8 publish
    weekly, so two release dates in the same week still resolve to
    different ids. Beige Book ships 8 times per year, each with a
    unique release date.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in Fed INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        entry.release_date,
    )

    payload: dict[str, Any] = {
        "kind":               "fed_release",
        "series_id":          entry.series_id,
        "release_title":      entry.release_title,
        "release_date":       entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc":     entry.event_time_utc,
    }
    content_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = FedCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = FedCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.release_date,
        reference_label=entry.release_date,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="USD",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Federal Reserve",
        source_url=FED_RELEASEDATES_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_releasedates_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Fed releasedates page and return HTML text.

    Reuses the browser-UA header bundle from
    :mod:`ingestion.calendar.fed_api.scraper` — the same user agent
    works for both ``fomccalendars.htm`` and ``releasedates.htm``.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            FED_RELEASEDATES_URL,
            headers=_FED_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
