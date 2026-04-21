"""ECB release-calendar scraper (issue #9 P3a).

ECB publishes two calendar surfaces under ``ecb.europa.eu/press/calendars/``:

- ``mgcgc/`` — Governing Council **monetary-policy** meetings. Each entry
  is a decision date + press-conference time (typically 14:15 / 14:45
  CET on the meeting day), the headline trader-visible event.
- ``economic-bulletin/`` — Economic Bulletin publications, ~two weeks
  after each policy meeting.

Both surfaces publish schedule-only events: there's no SDMX time series
corresponding to "ECB rate decision" as a single data point. The
rate-level series in ``FM`` dataflow (:mod:`.indicators`) anchors on
the **effective** date of each rate level — effective dates differ
from decision dates by several days (next main refinancing settlement
day), so schedule-side decision events and API-side rate-level rows
have different natural anchors. P3a ships them as distinct indicators
(``ECB_MP_DECISION`` / ``ECB_BULLETIN``); the issue's "shared
provider_event_id" goal would require deciding between effective-date
and decision-date anchoring across both sides and is left to a
follow-up slice.

Schedule rows land with ``actual=NULL`` /
``event_time_precision='datetime'`` and anchor on the event date
itself (decision date for MP meetings, publication date for Bulletin).
Schedule-only indicators flow through :func:`project_events` (full
writer) rather than :func:`project_schedule_events` — same reasoning
as BEA P2a's GDP handling.

Fetch + parse are separable functions. Tests feed HTML via
:func:`fetch_gc_meetings_html` / :func:`fetch_bulletin_html`; live
callers hit the URLs with the browser-user-agent headers the ECB
page requires.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .parser import (
    PROVIDER,
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
)

logger = logging.getLogger(__name__)

ECB_GC_MEETINGS_URL = (
    "https://www.ecb.europa.eu/press/calendars/mgcgc/html/index.en.html"
)
ECB_BULLETIN_URL = (
    "https://www.ecb.europa.eu/press/calendars/economic-bulletin/"
    "html/index.en.html"
)
ECB_RELEASE_TZ = "Europe/Berlin"  # CET/CEST


class ECBScheduleParseError(ValueError):
    """Raised when the press-calendar HTML deviates from known shape."""


@dataclass(frozen=True)
class ECBScheduleIndicatorSpec:
    """Schedule-only ECB indicator metadata.

    Distinct from :class:`ECBIndicatorSpec` — these aren't SDMX
    series; they're scheduled calendar events (rate-decision
    meetings, Bulletin publications) with no API-side observation
    counterpart.
    """

    indicator: str           # canonical token (ECB_MP_DECISION / ECB_BULLETIN)
    country_code: str
    title: str
    unit: str
    importance: str
    category: str


ECB_MP_DECISION_SPEC = ECBScheduleIndicatorSpec(
    indicator="ECB Monetary Policy Decision",
    country_code="EU",
    title="ECB Monetary Policy Decision",
    unit="event",
    importance="high",
    category="Monetary Policy",
)

ECB_BULLETIN_SPEC = ECBScheduleIndicatorSpec(
    indicator="ECB Economic Bulletin",
    country_code="EU",
    title="ECB Economic Bulletin",
    unit="event",
    importance="medium",
    category="Monetary Policy",
)


@dataclass(frozen=True)
class ECBScheduleEntry:
    """One scraped schedule row, pre-projection."""

    kind: str                # "mp_decision" | "bulletin"
    event_date: str          # ISO YYYY-MM-DD
    release_time_local: str  # "14:15" — verbatim, CET/CEST
    event_time_utc: str      # ISO datetime with UTC offset
    reference_label: str     # verbatim date text from the page


# ──────────────────────────────────────────────────────────────────────────
# HTML parsers
# ──────────────────────────────────────────────────────────────────────────

# Accepts "23 October 2025" / "Thursday, 23 October 2025" / "Oct 23, 2025".
_MONTH_NAMES: dict[str, int] = {
    "january":   1, "february":  2, "march":     3, "april":     4,
    "may":       5, "june":      6, "july":      7, "august":    8,
    "september": 9, "october":  10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
_EUROPEAN_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b"
)
_US_DATE_RE = re.compile(
    r"\b([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})\b"
)
_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_TIME_RE = re.compile(r"\b(\d{1,2})[:.](\d{2})\b")

# Row markers that indicate a Governing Council meeting is *not* a
# monetary-policy decision. The mgcgc page URL is specific to MP
# meetings, but the surrounding document occasionally references
# non-MP meetings (General Council / Supervisory Board / ad-hoc
# administrative sessions); without these guards the parser would
# label a General Council meeting date as a rate-decision event.
_NON_MP_MARKERS: tuple[str, ...] = (
    "non-monetary",
    "non monetary",
    "general council",
    "supervisory board",
    "administrative",
)


def _parse_date_from_text(text: str) -> date | None:
    """Best-effort date extractor for ECB press-calendar rows.

    Tries European day-month-year first (``"23 October 2025"``), then
    US month-day-year (``"October 23, 2025"``), then ISO. Returns the
    first successful parse or ``None``.
    """
    m = _ISO_DATE_RE.search(text)
    if m:
        try:
            return date.fromisoformat(m.group(1))
        except ValueError:
            pass
    m = _EUROPEAN_DATE_RE.search(text)
    if m:
        day_s, month_s, year_s = m.groups()
        month = _MONTH_NAMES.get(month_s.lower())
        if month:
            try:
                return date(year=int(year_s), month=month, day=int(day_s))
            except ValueError:
                pass
    m = _US_DATE_RE.search(text)
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
    """Pull a ``HH:MM`` or ``HH.MM`` time out of the row text.

    Default applies when no time token is present (ECB's calendar
    pages don't always print the press-conference time inline; the
    GC monetary-policy meetings default to 14:15 CET for the rate
    decision, Bulletin releases to 10:00 CET for publication)."""
    m = _TIME_RE.search(text)
    if not m:
        return default
    hh, mm = m.groups()
    return f"{int(hh):02d}:{mm}"


_HEADING_TAGS: frozenset[str] = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})
_ROW_TAGS:     frozenset[str] = frozenset({"li", "tr"})


def _parse_calendar_rows(
    html: str,
    *,
    kind: str,
    default_time: str,
    row_issues: list[str] | None = None,
) -> list[ECBScheduleEntry]:
    """Shared parser for both GC-meetings and Bulletin pages.

    Walks the document in source order, tracking the most recent
    heading (``<h1>``…``<h6>``) encountered before each ``<li>`` or
    ``<tr>`` row. Both ECB calendar pages historically use list-of-
    dates structures (``<ul>`` with a ``<li>`` per month; or a table
    with one row per release) — this routine tolerates both rather
    than encoding the exact DOM twice.

    For ``kind="mp_decision"`` (Governing Council monetary-policy
    meetings), the exclude test applies to *both* the current row
    text AND the most recent heading. ECB groups non-monetary-policy
    meetings under a section heading with date-only ``<li>``/``<tr>``
    rows underneath — relying on per-row markers only would still
    mis-write those dates as rate-decision events.
    """
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    entries: list[ECBScheduleEntry] = []

    exclude_non_mp = kind == "mp_decision"
    current_heading_lower = ""

    for element in soup.find_all(_HEADING_TAGS | _ROW_TAGS):
        if element.name in _HEADING_TAGS:
            current_heading_lower = element.get_text(" ", strip=True).lower()
            continue

        text = element.get_text(" ", strip=True)
        if not text:
            continue
        if exclude_non_mp:
            lowered = text.lower()
            # Row-level markers first: a per-row qualifier overrides
            # section context.
            if any(marker in lowered for marker in _NON_MP_MARKERS):
                continue
            # Section-level markers: if the nearest preceding heading
            # names a non-monetary-policy context, every row below it
            # inherits the exclude until the next heading.
            if current_heading_lower and any(
                marker in current_heading_lower for marker in _NON_MP_MARKERS
            ):
                continue
        event_date = _parse_date_from_text(text)
        if event_date is None:
            continue
        key = event_date.isoformat()
        if key in seen:
            continue
        seen.add(key)
        release_time = _extract_time(text, default=default_time)
        try:
            scheduled = parse_scheduled_release_time(
                event_date, release_time, default_tz=ECB_RELEASE_TZ,
            )
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(
                    f"{kind} {event_date.isoformat()} time={release_time!r}: {exc}"
                )
            continue
        entries.append(
            ECBScheduleEntry(
                kind=kind,
                event_date=event_date.isoformat(),
                release_time_local=release_time,
                event_time_utc=scheduled.utc.isoformat(),
                reference_label=text[:120],
            )
        )

    if not entries:
        raise ECBScheduleParseError(
            f"no dated rows found in {kind} HTML"
        )
    return entries


def parse_gc_meetings_html(
    html: str,
    *,
    row_issues: list[str] | None = None,
) -> list[ECBScheduleEntry]:
    """Extract Governing Council monetary-policy meeting rows.

    Default time 14:15 — ECB's standard rate-decision press-release
    time in CET/CEST.
    """
    return _parse_calendar_rows(
        html, kind="mp_decision",
        default_time="14:15", row_issues=row_issues,
    )


def parse_bulletin_html(
    html: str,
    *,
    row_issues: list[str] | None = None,
) -> list[ECBScheduleEntry]:
    """Extract Economic Bulletin publication rows.

    Default time 10:00 — ECB's standard Bulletin release time (CET).
    """
    return _parse_calendar_rows(
        html, kind="bulletin",
        default_time="10:00", row_issues=row_issues,
    )


# ──────────────────────────────────────────────────────────────────────────
# Projection
# ──────────────────────────────────────────────────────────────────────────


def _spec_for_kind(kind: str) -> ECBScheduleIndicatorSpec:
    if kind == "mp_decision":
        return ECB_MP_DECISION_SPEC
    if kind == "bulletin":
        return ECB_BULLETIN_SPEC
    raise KeyError(f"unknown ECB schedule kind {kind!r}")


def schedule_entry_to_records(
    entry: ECBScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[ECBCalendarRawRecord, ECBCalendarEventRecord]:
    """Project an :class:`ECBScheduleEntry` to (raw, event) records.

    ``provider_event_id`` anchors on the event date — decision date
    for MP meetings, publication date for Bulletin. No stage
    qualification: each scheduled ECB event is unique per date (no
    preliminary/revised pattern).
    """
    spec = _spec_for_kind(entry.kind)
    indicator_canonical = canonicalize_indicator(spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        entry.event_date,
    )

    schedule_payload: dict[str, Any] = {
        "kind":                 f"ecb_{entry.kind}",
        "event_date":           entry.event_date,
        "release_time_local":   entry.release_time_local,
        "event_time_utc":       entry.event_time_utc,
        "reference_label":      entry.reference_label,
    }
    content_hash = hashlib.sha256(
        json.dumps(schedule_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload_json = json.dumps(
        schedule_payload, sort_keys=True, ensure_ascii=False,
    )

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = ECBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    source_url = (
        ECB_GC_MEETINGS_URL if entry.kind == "mp_decision"
        else ECB_BULLETIN_URL
    )
    event_record = ECBCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.event_date,
        reference_label=entry.event_date,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="EUR",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="ECB",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


# ──────────────────────────────────────────────────────────────────────────
# Fetchers
# ──────────────────────────────────────────────────────────────────────────


_ECB_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _get_html(
    url: str, session: requests.Session | None, timeout: float,
) -> str:
    owned = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_ECB_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned:
            s.close()


def fetch_gc_meetings_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ECB Governing Council monetary-policy meetings page."""
    return _get_html(ECB_GC_MEETINGS_URL, session, timeout)


def fetch_bulletin_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ECB Economic Bulletin publications page."""
    return _get_html(ECB_BULLETIN_URL, session, timeout)
