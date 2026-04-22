"""BEA release-schedule scraper (issue #9 P2a + P2b-schedule-drift).

Scrape ``https://www.bea.gov/news/schedule`` — BEA publishes one
monolithic page covering every news release, unlike BLS's one-page-
per-indicator model. The scraper walks the release-list table and
filters rows whose release-title column matches a whitelisted
indicator fragment.

Parsed schedule rows land in ``cal_econ_event`` with ``actual=NULL``
and ``event_time_precision='datetime'``. Subsequent API-side value
writes upsert their values onto the same ``provider_event_id``; the
shared projector preserves datetime precision once the schedule has
set it.

GDP publishes three staged releases per quarter (Advance / Second /
Third). The scraper emits one ``BEAScheduleEntry`` per row with
``release_stage`` set from the release-title qualifier; the stage
folds into the synthesised ``provider_event_id`` so distinct staged
events don't collide in ``cal_econ_event``. Personal Income and
Outlays is unstaged — one row per reference month — and its
schedule-side id matches the API-side id so the merge path works
end-to-end.

The live DOM drifted between P2a and P2b-live (2026-04-22 probe):
the former ``Release Date`` / ``Release Time`` / ``Reference Period``
header columns collapsed into a single ``Year YYYY`` column with
date + time stacked inside one cell (``<div class="release-date">``
+ ``<small class="text-muted">``), the reference period moved into
the release-title cell, and the release-name column now uses the
``GDP`` abbreviation instead of ``Gross Domestic Product``. The
parser targets the new shape; old-shape HTML is no longer supported
(BEA serves only the new layout).

Fetch + parse are separable. Tests feed HTML via the
:func:`fetch_schedule_html` seam; live callers hit the URL directly
with the browser-user-agent headers that BEA requires.
"""

from __future__ import annotations

import calendar as _calendar
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

from .indicators import INDICATOR_REGISTRY, BEAIndicatorSpec
from .parser import (
    PROVIDER,
    BEACalendarEventRecord,
    BEACalendarRawRecord,
)

logger = logging.getLogger(__name__)

BEA_SCHEDULE_URL = "https://www.bea.gov/news/schedule"
BEA_RELEASE_TZ = "America/New_York"


class BEAScheduleParseError(ValueError):
    """Raised when the release-schedule table deviates from the known
    shape. Flagged loudly so the next live run catches upstream DOM
    changes rather than silently dropping rows."""


# Release-title fragment → series_id. Order matters: more-specific
# fragments first, so ``"personal income and outlays"`` matches
# before the less-specific ``"personal income"``. ``"gdp"`` and
# ``"gross domestic product"`` both map to Real GDP — the live page
# moved to the abbreviated form in 2026 but the full form may return
# on rewrites, and tests exercise both to lock the behavior down.
_MATCH_FRAGMENTS: tuple[tuple[str, str], ...] = (
    ("personal income and outlays", "BEA_NIPA_T20600_1"),
    ("personal income",             "BEA_NIPA_T20600_1"),
    ("gross domestic product",      "BEA_NIPA_T10101_1"),
    ("gdp",                         "BEA_NIPA_T10101_1"),
)

# Fragments that must veto a match when they appear in the release-
# title's leading segment (everything before the first comma).
# Leading-segment scoping is what lets a row like
# ``"GDP (Third Estimate), Industries, Corporate Profits, State GDP,
# and State Personal Income, 1st Quarter 2026"`` still project as the
# headline GDP release — the excluded phrases live past the first
# comma and are other indicators riding on the same time slot, not
# the headline release itself. Without leading-segment scoping, the
# title-wide substring check would drop every such row.
_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    # Industry decompositions.
    "gross domestic product by industry",
    "gdp by industry",
    "personal income by industry",
    "value added by industry",
    # Regional / sub-national breakdowns.
    "by state",
    "by county",
    "by metropolitan area",
    "by msa",
    "by region",
    # Standalone regional Personal Income releases.
    "state personal income",
    "county personal income",
    "local personal income",
    "metropolitan personal income",
    "regional personal income",
    # Standalone regional GDP releases.
    "state gdp",
    "county gdp",
    "local gdp",
    "metropolitan gdp",
    "regional gdp",
)

# Stage tokens observed on BEA release titles (e.g. ``"GDP (Advance
# Estimate), 4th Quarter 2025"`` or ``"Gross Domestic Product
# (Second Estimate)"``). Word-boundary matching avoids false
# positives from words like ``"ad-valorem"``.
_STAGE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\badvance\b",         re.IGNORECASE), "advance"),
    (re.compile(r"\bsecond\b",          re.IGNORECASE), "second"),
    (re.compile(r"\bthird\b",           re.IGNORECASE), "third"),
    (re.compile(r"\bpreliminary\b",     re.IGNORECASE), "preliminary"),
    (re.compile(r"\brevised\b",         re.IGNORECASE), "revised"),
    (re.compile(r"\bfinal\b",           re.IGNORECASE), "final"),
)

_MONTH_NAMES: dict[str, int] = {
    "january":   1, "february":  2, "march":     3, "april":     4,
    "may":       5, "june":      6, "july":      7, "august":    8,
    "september": 9, "october":  10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
# ``"April 30"`` — release-date div contents (year lives on the
# table's header, not inside each row).
_MONTH_DAY_RE = re.compile(
    r"^\s*([A-Za-z]+)\.?\s+(\d{1,2})\s*$"
)
_REFERENCE_MONTH_RE = re.compile(
    r"^\s*([A-Za-z]+)\s+(\d{4})\s*$"
)
_QUARTER_ORDINAL_RE = re.compile(
    r"^\s*(1st|2nd|3rd|4th|first|second|third|fourth)\s+quarter[,\s]+(\d{4})\s*$",
    re.IGNORECASE,
)
_QUARTER_SHORT_RE = re.compile(
    r"^\s*Q([1-4])[,\s]+(\d{4})\s*$",
    re.IGNORECASE,
)
_YEAR_HEADER_RE = re.compile(r"\bYear\s+(\d{4})\b")
_ORDINAL_TO_Q: dict[str, int] = {
    "1st": 1, "2nd": 2, "3rd": 3, "4th": 4,
    "first": 1, "second": 2, "third": 3, "fourth": 4,
}
_QUARTER_END_MONTH: dict[int, int] = {1: 3, 2: 6, 3: 9, 4: 12}


@dataclass(frozen=True)
class BEAScheduleEntry:
    """One matched row from the BEA release-schedule table, pre-projection."""

    series_id: str
    # Monthly: first of reference month. Quarterly: last day of
    # reference quarter. Matches :meth:`BEAClient._normalize_time_period`
    # so the schedule-side event id aligns with API-side obs.date for
    # unstaged indicators (Personal Income).
    reference_date: str       # ISO YYYY-MM-DD
    reference_label: str      # verbatim reference-period text
    release_title: str        # verbatim release-name cell
    release_date: str         # ISO YYYY-MM-DD
    release_time_local: str   # verbatim "8:30 AM"
    event_time_utc: str       # ISO datetime with UTC offset
    # Release stage (``"advance"`` / ``"second"`` / ``"third"`` /
    # ``"preliminary"`` / ``"revised"`` / ``"final"``) for staged
    # indicators (GDP), ``""`` for unstaged (Personal Income). Folds
    # into the provider_event_id when non-empty so the three GDP
    # stages of a single quarter don't collide on one id.
    release_stage: str = ""


def _leading_segment(title: str) -> str:
    """Return everything before the first comma, lowercased, stripped.

    Leading-segment scoping lets multi-indicator rows like
    ``"GDP (Third Estimate), Industries, Corporate Profits, State
    GDP, and State Personal Income, 1st Quarter 2026"`` still match
    as the headline GDP release — the regional-indicator fragments
    live past the first comma and describe *other* releases sharing
    the time slot, not the headline.
    """
    return title.split(",", 1)[0].strip().lower()


def _match_release(title: str) -> str | None:
    """Return the whitelisted series id for ``title``, or ``None``.

    Exclude and match checks both run against the leading segment
    (everything before the first comma). Rows whose headline is a
    regional / industry decomposition are vetoed; rows whose headline
    is a whitelisted indicator match even when the tail lists other
    releases on the same slot.
    """
    leading = _leading_segment(title)
    for fragment in _EXCLUDE_FRAGMENTS:
        if fragment in leading:
            return None
    for fragment, series_id in _MATCH_FRAGMENTS:
        if fragment in leading:
            return series_id
    return None


def _extract_stage(title: str) -> str:
    for pattern, stage in _STAGE_PATTERNS:
        if pattern.search(title):
            return stage
    return ""


def _parse_release_date(month_day_text: str, year: int) -> date:
    """Combine the row's ``"Month Day"`` text with the table's year."""
    match = _MONTH_DAY_RE.match(month_day_text)
    if not match:
        raise BEAScheduleParseError(
            f"unparseable release date: {month_day_text!r}"
        )
    month_raw, day_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.strip(".").lower())
    if month is None:
        raise BEAScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=year, month=month, day=int(day_raw))


def _parse_quarterly_reference(text: str) -> date | None:
    """Parse ``"4th Quarter 2025"`` / ``"Q4 2025"`` → end-of-quarter."""
    m = _QUARTER_ORDINAL_RE.match(text)
    if m:
        q = _ORDINAL_TO_Q[m.group(1).lower()]
        year = int(m.group(2))
    else:
        m = _QUARTER_SHORT_RE.match(text)
        if not m:
            return None
        q = int(m.group(1))
        year = int(m.group(2))
    month = _QUARTER_END_MONTH[q]
    last_day = _calendar.monthrange(year, month)[1]
    return date(year=year, month=month, day=last_day)


def _parse_reference_period(text: str) -> date:
    """Parse monthly (``"January 2026"``) or quarterly
    (``"4th Quarter 2025"`` / ``"Q4 2025"``) reference text.

    Returns end-of-quarter for quarterly (matching
    :meth:`BEAClient._normalize_time_period`) and first-of-month for
    monthly. Raises :class:`BEAScheduleParseError` when neither
    format matches.
    """
    quarterly = _parse_quarterly_reference(text)
    if quarterly is not None:
        return quarterly
    match = _REFERENCE_MONTH_RE.match(text)
    if not match:
        raise BEAScheduleParseError(
            f"unparseable reference text: {text!r}"
        )
    month_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.lower())
    if month is None:
        raise BEAScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=1)


_INLINE_REFERENCE_RE = re.compile(
    r",\s*("
    # Ordinal-form quarters require the word "quarter": "4th Quarter 2025".
    r"(?:1st|2nd|3rd|4th|first|second|third|fourth)\s+quarter[,\s]+\d{4}"
    r"|"
    # Short-form quarters skip the word: "Q4 2025" (documented BEA
    # alternative; the column-cell path always accepted it, so the
    # inline path must too — Codex P2 on 2026-04-22).
    r"Q[1-4][,\s]+\d{4}"
    r"|"
    # Monthly: "January 2026", "April 2026", …
    r"(?:january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+\d{4}"
    r")",
    re.IGNORECASE,
)


def _extract_inline_reference(title: str) -> str:
    """Pull a reference period substring out of a release title.

    The live DOM stopped shipping a separate reference-period column;
    every whitelisted row now carries the reference inline after the
    first comma (``"GDP (Advance Estimate), 4th Quarter 2025"``,
    ``"Personal Income and Outlays, December 2025"``). Returns ``""``
    when no recognisable substring is present — caller logs a row
    issue so operators see upstream drift instead of silently dropping
    the row.
    """
    match = _INLINE_REFERENCE_RE.search(title)
    return match.group(1).strip() if match else ""


def _extract_year_from_header(table) -> int | None:
    """Pull the year from ``<thead>`` ``"Year YYYY"`` column header."""
    head = table.find("thead") or table
    for th in head.find_all(["th", "td"]):
        text = th.get_text(" ", strip=True)
        match = _YEAR_HEADER_RE.search(text)
        if match:
            return int(match.group(1))
    return None


def _resolve_release_date_cell(row) -> tuple[str, str] | None:
    """Read ``(month_day_text, time_text)`` out of the scheduled-date cell.

    Returns ``None`` for TBA rows — the live page emits entries with a
    ``<small>To Be Announced<br/>Spring 2026</small>`` cell (no
    ``release-date`` div) for placeholders. Those aren't scheduleable
    and don't belong in the calendar.
    """
    release_div = row.find(class_="release-date")
    if release_div is None:
        return None
    month_day = release_div.get_text(" ", strip=True)
    time_el = row.find(class_="text-muted")
    time_text = (
        time_el.get_text(" ", strip=True)
        if time_el is not None
        else "8:30 AM"
    )
    # BEA's default template wraps the time in the same ``text-muted``
    # styled <small> used for TBA placeholders on rows without a
    # release-date div; keep the default-fallback for the rare case
    # the time is empty.
    if not time_text:
        time_text = "8:30 AM"
    return month_day, time_text


def _resolve_release_title(row) -> str:
    """Return the release-name cell text, stripped."""
    title_cell = row.find(class_="release-title")
    if title_cell is None:
        return ""
    return title_cell.get_text(" ", strip=True)


def parse_schedule_html(
    html: str,
    *,
    row_issues: list[str] | None = None,
) -> list[BEAScheduleEntry]:
    """Extract :class:`BEAScheduleEntry` rows from the BEA release
    calendar page.

    Targets the live page's single ``<table id="release-schedule-table">``
    with a ``Year YYYY`` header column. Each data row's
    ``<td class="scheduled-date">`` stacks the date
    (``<div class="release-date">April 30</div>``) and time
    (``<small class="text-muted">8:30 AM</small>``); the
    ``<td class="release-title">`` carries the release name with the
    reference period inline after the first comma. Rows without a
    ``release-date`` div (``"To Be Announced"`` placeholders) are
    silently skipped.

    Rows whose release-title leading segment doesn't match a
    whitelisted fragment are silently dropped — BEA's calendar carries
    releases well outside our whitelist (Trade in Goods and Services,
    International Transactions, regional / industry decompositions).

    ``row_issues`` is an optional list the caller may pass to capture
    per-row failures *after* a whitelisted match: a matched GDP or
    Personal Income release whose release-date or inline reference
    fails to parse is recorded here rather than silently dropped, so
    operators see upstream format drift (e.g. ``"4th Quarter and
    Year 2025"``) instead of an event missing from
    ``cal_econ_event``. Rows that don't match a whitelist fragment at
    all are still dropped silently — they're not our responsibility.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise BEAScheduleParseError("no <table> found in BEA schedule HTML")

    entries: list[BEAScheduleEntry] = []
    matched_any_table = False
    for table in tables:
        year = _extract_year_from_header(table)
        if year is None:
            continue
        matched_any_table = True

        body = table.find("tbody") or table
        for row in body.find_all("tr"):
            date_cell = _resolve_release_date_cell(row)
            if date_cell is None:
                # TBA rows, header rows, or otherwise malformed —
                # nothing scheduleable here.
                continue
            month_day, time_text = date_cell
            release_title = _resolve_release_title(row)
            if not release_title:
                continue

            series_id = _match_release(release_title)
            if series_id is None:
                continue

            try:
                release_date = _parse_release_date(month_day, year)
            except BEAScheduleParseError as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_title!r} release date {month_day!r}: {exc}"
                    )
                continue

            stage = (
                _extract_stage(release_title)
                if series_id == "BEA_NIPA_T10101_1"
                else ""
            )

            reference_text = _extract_inline_reference(release_title)
            if not reference_text:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_title!r}: no reference period in title"
                    )
                continue
            try:
                reference = _parse_reference_period(reference_text)
            except BEAScheduleParseError as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_title!r} reference {reference_text!r}: {exc}"
                    )
                continue

            scheduled = parse_scheduled_release_time(
                release_date, time_text, default_tz=BEA_RELEASE_TZ,
            )
            entries.append(
                BEAScheduleEntry(
                    series_id=series_id,
                    reference_date=reference.isoformat(),
                    reference_label=reference_text,
                    release_title=release_title,
                    release_date=release_date.isoformat(),
                    release_time_local=time_text,
                    event_time_utc=scheduled.utc.isoformat(),
                    release_stage=stage,
                )
            )

    if not matched_any_table:
        raise BEAScheduleParseError(
            "no table with a 'Year YYYY' header column found"
        )
    return entries


def schedule_entry_to_records(
    entry: BEAScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: BEAIndicatorSpec | None = None,
) -> tuple[BEACalendarRawRecord, BEACalendarEventRecord]:
    """Project a :class:`BEAScheduleEntry` to (raw, event) records.

    ``provider_event_id`` anchors on ``entry.reference_date``, plus
    the stage for staged indicators (GDP), so distinct releases of
    the same quarter don't collide on a single id.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in BEA INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    anchor = (
        f"{entry.reference_date}|{entry.release_stage}"
        if entry.release_stage
        else entry.reference_date
    )
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        anchor,
    )

    schedule_payload: dict[str, Any] = {
        "kind":               "bea_schedule",
        "series_id":          entry.series_id,
        "reference_label":    entry.reference_label,
        "reference_date":     entry.reference_date,
        "release_title":      entry.release_title,
        "release_date":       entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc":     entry.event_time_utc,
        "release_stage":      entry.release_stage,
    }
    content_hash = hashlib.sha256(
        json.dumps(schedule_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload_json = json.dumps(schedule_payload, sort_keys=True, ensure_ascii=False)

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()

    raw_record = BEACalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BEACalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="BEA",
        source_url=BEA_SCHEDULE_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_BEA_BROWSER_HEADERS: dict[str, str] = {
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


def fetch_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the BEA release-calendar page and return HTML text.

    Uses a browser-style header bundle — ``bea.gov`` serves the default
    ``python-requests`` user agent but often in a reduced template
    that omits the release table. The browser UA bundle consistently
    returns the full page.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            BEA_SCHEDULE_URL, headers=_BEA_BROWSER_HEADERS, timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
