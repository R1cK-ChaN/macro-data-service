"""BLS release-schedule scraper (issue #9 P1a).

Scrape ``https://www.bls.gov/schedule/news_release/<series>.htm`` and
land the upcoming-release rows into ``cal_econ_event`` with
``actual=NULL`` / ``event_time_precision='datetime'``. The matching API
rows that arrive later upsert their values onto the same
``provider_event_id`` without clobbering the scheduled datetime —
:func:`projector.project_events` preserves datetime precision once a
schedule-side row has set it.

The BLS schedule pages ship one table per indicator with a stable
DOM: ``<table class="release-list">`` with three columns — *Reference
Month* (``"November 2025"``), *Release Date* (``"Dec. 18, 2025"``),
*Release Time* (``"08:30 AM"``). No timezone marker on the page;
BLS publishes all major economic indicators at **8:30 AM Eastern
Time** — that's the industry-wide convention, encoded here as a
constant.

Fetch + parse are separable functions. Tests feed HTML directly to
:func:`parse_schedule_html`; live callers use :func:`fetch_schedule_html`
which drives ``requests.Session`` with a browser-user-agent header
bundle (bls.gov 403s on the default ``python-requests`` UA).
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

from .indicators import INDICATOR_REGISTRY, BLSIndicatorSpec
from .parser import (
    PROVIDER,
    BLSCalendarEventRecord,
    BLSCalendarRawRecord,
)

logger = logging.getLogger(__name__)

BLS_SCHEDULE_BASE = "https://www.bls.gov/schedule/news_release"
BLS_RELEASE_TZ = "America/New_York"

# series_id → schedule URL slug. Adding a series is a one-liner once
# the upstream page is confirmed to exist in the same shape.
SCHEDULE_URL_SLUG: dict[str, str] = {
    "CUUR0000SA0":     "cpi.htm",
    "CES0000000001":   "empsit.htm",
}

# "Dec. 18, 2025" → (date). BLS normalises to 3-letter month + period.
# "January 2026" etc. (full month name) appear in the Reference-Month
# column; also supported.
_MONTH_NAMES = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}
_RELEASE_DATE_RE = re.compile(
    r"^\s*([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})\s*$"
)
_REFERENCE_MONTH_RE = re.compile(
    r"^\s*([A-Za-z]+)\s+(\d{4})\s*$"
)


class BLSScheduleParseError(ValueError):
    """Raised when the release-list table deviates from the known
    shape. Flagged loudly so the next live run catches upstream DOM
    changes rather than silently dropping rows."""


@dataclass(frozen=True)
class BLSScheduleEntry:
    """One row from a BLS schedule page, pre-projection."""

    series_id: str
    reference_date: str       # ISO YYYY-MM-DD (first of reference month)
    reference_label: str      # "November 2025" (verbatim)
    release_date: str         # ISO YYYY-MM-DD
    release_time_local: str   # verbatim "08:30 AM"
    event_time_utc: str       # ISO datetime with UTC offset


def _parse_short_date(text: str) -> date:
    match = _RELEASE_DATE_RE.match(text)
    if not match:
        raise BLSScheduleParseError(f"unparseable release date: {text!r}")
    month_raw, day_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.strip(".").lower())
    if month is None:
        raise BLSScheduleParseError(f"unknown month abbrev: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=int(day_raw))


def _parse_reference_month(text: str) -> date:
    match = _REFERENCE_MONTH_RE.match(text)
    if not match:
        raise BLSScheduleParseError(
            f"unparseable reference-month cell: {text!r}"
        )
    month_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.lower())
    if month is None:
        raise BLSScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=1)


def parse_schedule_html(html: str, *, series_id: str) -> list[BLSScheduleEntry]:
    """Extract :class:`BLSScheduleEntry` rows from a schedule page.

    The function targets the single ``<table class="release-list">``
    and expects three ``<td>`` cells per row: Reference Month, Release
    Date, Release Time. Any deviation raises
    :class:`BLSScheduleParseError` so the caller surfaces the DOM
    drift rather than silently truncating the schedule.
    """
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="release-list")
    if table is None:
        raise BLSScheduleParseError(
            "no <table class='release-list'> found in schedule HTML"
        )
    body = table.find("tbody") or table
    entries: list[BLSScheduleEntry] = []
    for row in body.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            # Table header rows (<th>-only) and footer rows lack <td>s.
            continue
        ref_cell = cells[0].get_text(strip=True)
        date_cell = cells[1].get_text(strip=True)
        time_cell = cells[2].get_text(strip=True)
        if not ref_cell or not date_cell or not time_cell:
            continue

        reference = _parse_reference_month(ref_cell)
        release = _parse_short_date(date_cell)
        scheduled = parse_scheduled_release_time(
            release, time_cell, default_tz=BLS_RELEASE_TZ,
        )
        entries.append(
            BLSScheduleEntry(
                series_id=series_id,
                reference_date=reference.isoformat(),
                reference_label=ref_cell,
                release_date=release.isoformat(),
                release_time_local=time_cell,
                event_time_utc=scheduled.utc.isoformat(),
            )
        )
    return entries


def schedule_entry_to_records(
    entry: BLSScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: BLSIndicatorSpec | None = None,
) -> tuple[BLSCalendarRawRecord, BLSCalendarEventRecord]:
    """Project a :class:`BLSScheduleEntry` to (raw, event) records.

    ``provider_event_id`` anchors on ``entry.reference_date`` — the
    same anchor the API-side parser uses in
    :mod:`ingestion.calendar.bls_api.parser`, so schedule row + API
    row land on the same ``(provider, provider_event_id)`` key and
    merge in the event table."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        entry.reference_date,
    )

    # Schedule-side content hash: any of (event_time, release_date,
    # release_time, reference_label) changing = BLS adjusted the
    # schedule. Different namespace from the API-side hash so the two
    # sources don't conflict in cal_econ_raw.
    schedule_payload: dict[str, Any] = {
        "kind":               "bls_schedule",
        "series_id":          entry.series_id,
        "reference_label":    entry.reference_label,
        "reference_date":     entry.reference_date,
        "release_date":       entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc":     entry.event_time_utc,
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

    raw_record = BLSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BLSCalendarEventRecord(
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
        source="BLS",
        source_url=f"{BLS_SCHEDULE_BASE}/{SCHEDULE_URL_SLUG[entry.series_id]}",
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


# Browser user-agent header bundle. bls.gov returns 403 on default
# ``python-requests`` UA, hence the full header block. Tuned from
# curl experimentation on 2026-04-21 against the live schedule pages.
_BLS_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def fetch_schedule_html(
    series_id: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the BLS schedule page for ``series_id`` and return HTML text.

    Uses a browser-style header bundle so ``bls.gov`` serves the real
    page (the default ``python-requests`` user-agent receives an
    ``Access Denied`` stub).

    Callers may pass a shared :class:`requests.Session`; the function
    constructs one when ``session is None`` and leaves caller-owned
    sessions unclosed.
    """
    slug = SCHEDULE_URL_SLUG.get(series_id)
    if slug is None:
        raise KeyError(
            f"series_id {series_id!r} has no schedule URL registered"
        )
    url = f"{BLS_SCHEDULE_BASE}/{slug}"
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_BLS_BROWSER_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
