"""BEA release-schedule scraper (issue #9 P2a).

Scrape ``https://www.bea.gov/news/schedule`` — BEA publishes one
monolithic page covering every news release, unlike BLS's one-page-
per-indicator model. The scraper walks the release-list table and
filters rows whose *Release* column matches a whitelisted indicator
fragment.

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
# before the less-specific ``"personal income"``. Titles that match
# ``_EXCLUDE_FRAGMENTS`` are explicitly skipped — without that,
# ``"gross domestic product by industry"`` would substring-match
# ``"gross domestic product"`` and get mis-labelled as the headline
# GDP release.
_EXCLUDE_FRAGMENTS: tuple[str, ...] = (
    # Industry decompositions — distinct releases from the headline
    # quarterly GDP estimate.
    "gross domestic product by industry",
    "gdp by industry",
    "personal income by industry",
    "value added by industry",
    # Regional decompositions. BEA's monolithic calendar regularly
    # carries state / county / metropolitan-area / regional
    # breakdowns of both GDP and Personal Income. Without these
    # excludes the substring ``"gross domestic product"`` or
    # ``"personal income"`` would match the regional title and
    # project a wrong-indicator headline calendar event.
    "by state",
    "by county",
    "by metropolitan area",
    "by msa",
    "by region",
    # State-prefixed / regional Personal Income releases.
    "state personal income",
    "county personal income",
    "local personal income",
    "metropolitan personal income",
    "regional personal income",
)
_MATCH_FRAGMENTS: tuple[tuple[str, str], ...] = (
    ("personal income and outlays", "BEA_NIPA_T20600_1"),
    ("personal income",             "BEA_NIPA_T20600_1"),
    ("gross domestic product",      "BEA_NIPA_T10101_1"),
)

# Stage tokens observed on BEA release titles (e.g. "Gross Domestic
# Product, 4th Quarter 2025 (Advance Estimate)"). Word-boundary
# matching avoids false positives from words like "ad-valorem".
_STAGE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\badvance\b",         re.IGNORECASE), "advance"),
    (re.compile(r"\bsecond\b",          re.IGNORECASE), "second"),
    (re.compile(r"\bthird\b",           re.IGNORECASE), "third"),
    (re.compile(r"\bpreliminary\b",     re.IGNORECASE), "preliminary"),
    (re.compile(r"\brevised\b",         re.IGNORECASE), "revised"),
    (re.compile(r"\bfinal\b",           re.IGNORECASE), "final"),
)

# "January 30, 2026" — full month name, day, year.
_MONTH_NAMES: dict[str, int] = {
    "january":   1, "february":  2, "march":     3, "april":     4,
    "may":       5, "june":      6, "july":      7, "august":    8,
    "september": 9, "october":  10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
_RELEASE_DATE_RE = re.compile(
    r"^\s*([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})\s*$"
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
    reference_label: str      # verbatim reference-period cell
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


def _match_release(title: str) -> str | None:
    """Return the whitelisted series id for ``title``, or ``None``."""
    lowered = title.lower()
    for fragment in _EXCLUDE_FRAGMENTS:
        if fragment in lowered:
            return None
    for fragment, series_id in _MATCH_FRAGMENTS:
        if fragment in lowered:
            return series_id
    return None


def _extract_stage(title: str) -> str:
    for pattern, stage in _STAGE_PATTERNS:
        if pattern.search(title):
            return stage
    return ""


def _parse_release_date(text: str) -> date:
    match = _RELEASE_DATE_RE.match(text)
    if not match:
        raise BEAScheduleParseError(f"unparseable release date: {text!r}")
    month_raw, day_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.strip(".").lower())
    if month is None:
        raise BEAScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=int(day_raw))


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
    (``"4th Quarter 2025"`` / ``"Q4 2025"``) reference cell.

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
            f"unparseable reference cell: {text!r}"
        )
    month_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.lower())
    if month is None:
        raise BEAScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=1)


def parse_schedule_html(
    html: str,
    *,
    row_issues: list[str] | None = None,
) -> list[BEAScheduleEntry]:
    """Extract :class:`BEAScheduleEntry` rows from the BEA release
    calendar page.

    Targets tables containing the ``Release Date`` / ``Release`` /
    ``Reference Period`` column set (either three columns or four, with
    an optional ``Release Time`` column). Rows whose release-title
    column doesn't match a whitelisted indicator fragment are silently
    skipped — BEA's calendar carries releases well outside our
    whitelist (BEA by-Industry, State Personal Income, Trade, …).

    ``row_issues`` is an optional list the caller may pass to capture
    per-row failures *after* a whitelisted match: a matched GDP or
    Personal Income release whose reference label or release date
    fails to parse is recorded here rather than silently dropped, so
    operators see upstream label drift (e.g. BEA's ``"4th Quarter and
    Year 2025"`` shape) instead of an event missing from
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
        header = table.find("tr")
        if header is None:
            continue
        header_texts = [
            c.get_text(strip=True).lower() for c in header.find_all(["th", "td"])
        ]
        if "release date" not in header_texts or "release" not in header_texts:
            continue
        matched_any_table = True
        date_idx = header_texts.index("release date")
        release_idx = header_texts.index("release")
        time_idx = (
            header_texts.index("release time")
            if "release time" in header_texts
            else None
        )
        reference_idx = (
            header_texts.index("reference period")
            if "reference period" in header_texts
            else None
        )

        body = table.find("tbody") or table
        for row in body.find_all("tr"):
            cells = row.find_all("td")
            min_cells = max(
                date_idx, release_idx,
                time_idx if time_idx is not None else 0,
                reference_idx if reference_idx is not None else 0,
            ) + 1
            if len(cells) < min_cells:
                continue
            date_cell = cells[date_idx].get_text(strip=True)
            release_cell = cells[release_idx].get_text(strip=True)
            if not date_cell or not release_cell:
                continue

            series_id = _match_release(release_cell)
            if series_id is None:
                continue
            spec = INDICATOR_REGISTRY[series_id]

            try:
                release_date = _parse_release_date(date_cell)
            except BEAScheduleParseError as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_cell!r} release date {date_cell!r}: {exc}"
                    )
                continue

            stage = _extract_stage(release_cell) if series_id == "BEA_NIPA_T10101_1" else ""

            # Reference period: prefer the dedicated cell, fall back to
            # extracting from the release-title when BEA bundles it
            # inline ("Gross Domestic Product, 4th Quarter 2025
            # (Advance Estimate)").
            reference_text = ""
            if reference_idx is not None:
                reference_text = cells[reference_idx].get_text(strip=True)
            if not reference_text:
                reference_text = _extract_inline_reference(release_cell)
            if not reference_text:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_cell!r}: no reference period in row"
                    )
                continue
            try:
                reference = _parse_reference_period(reference_text)
            except BEAScheduleParseError as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{release_cell!r} reference {reference_text!r}: {exc}"
                    )
                continue

            time_cell = (
                cells[time_idx].get_text(strip=True)
                if time_idx is not None
                else "8:30 AM"
            )
            scheduled = parse_scheduled_release_time(
                release_date, time_cell, default_tz=BEA_RELEASE_TZ,
            )
            entries.append(
                BEAScheduleEntry(
                    series_id=series_id,
                    reference_date=reference.isoformat(),
                    reference_label=reference_text,
                    release_title=release_cell,
                    release_date=release_date.isoformat(),
                    release_time_local=time_cell,
                    event_time_utc=scheduled.utc.isoformat(),
                    release_stage=stage,
                )
            )

    if not matched_any_table:
        raise BEAScheduleParseError(
            "no table with 'Release Date' + 'Release' header columns found"
        )
    return entries


_INLINE_REFERENCE_RE = re.compile(
    r",\s*((?:1st|2nd|3rd|4th|first|second|third|fourth|Q[1-4])"
    r"\s+quarter[,\s]+\d{4}|"
    r"(?:january|february|march|april|may|june|july|august|september|"
    r"october|november|december)\s+\d{4})",
    re.IGNORECASE,
)


def _extract_inline_reference(title: str) -> str:
    """Pull a reference period substring out of a release title.

    Catches the "Gross Domestic Product, 4th Quarter 2025 (Advance
    Estimate)" shape when BEA omits the separate Reference Period
    column. Returns ``""`` when no recognisable substring is present.
    """
    match = _INLINE_REFERENCE_RE.search(title)
    return match.group(1).strip() if match else ""


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
