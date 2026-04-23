"""Census Bureau economic-indicator release-calendar scraper."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, CensusIndicatorSpec
from .parser import (
    PROVIDER,
    CensusCalendarEventRecord,
    CensusCalendarRawRecord,
)

logger = logging.getLogger(__name__)

CENSUS_CALENDAR_URL = "https://www.census.gov/economic-indicators/calendar-listview.html"
CENSUS_RELEASE_TZ = "America/New_York"


class CensusScheduleParseError(ValueError):
    """Raised when the release-calendar table shape drifts."""


@dataclass(frozen=True)
class CensusScheduleEntry:
    """One matched Census release-calendar row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: str
    release_time_local: str
    event_time_utc: str
    source_url: str


_MONTH_NAMES: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}
_LONG_DATE_RE = re.compile(
    r"^\s*([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})\s*$"
)
_REFERENCE_MONTH_RE = re.compile(
    r"^\s*([A-Za-z]+)\s+(\d{4})\s*$"
)


def _parse_long_date(text: str) -> date:
    match = _LONG_DATE_RE.match(text)
    if not match:
        raise CensusScheduleParseError(f"unparseable release date: {text!r}")
    month_raw, day_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.strip(".").lower())
    if month is None:
        raise CensusScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=int(day_raw))


def _parse_reference_period(text: str) -> date:
    match = _REFERENCE_MONTH_RE.match(text)
    if not match:
        raise CensusScheduleParseError(f"unparseable reference period: {text!r}")
    month_raw, year_raw = match.groups()
    month = _MONTH_NAMES.get(month_raw.lower())
    if month is None:
        raise CensusScheduleParseError(f"unknown month name: {month_raw!r}")
    return date(year=int(year_raw), month=month, day=1)


def _matching_specs(title: str) -> list[CensusIndicatorSpec]:
    lowered = " ".join(title.lower().split())
    matches: list[CensusIndicatorSpec] = []
    for spec in INDICATOR_REGISTRY.values():
        if any(fragment in lowered for fragment in spec.schedule_title_fragments):
            matches.append(spec)
    return matches


def parse_schedule_html(
    html: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[CensusScheduleEntry]:
    """Extract whitelisted release rows from the Census list-view calendar."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="calendar")
    if table is None:
        raise CensusScheduleParseError("no <table id='calendar'> found")

    entries: list[CensusScheduleEntry] = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        title = cells[0].get_text(" ", strip=True)
        release_date_text = cells[1].get_text(" ", strip=True)
        release_time_text = cells[2].get_text(" ", strip=True)
        reference_text = cells[3].get_text(" ", strip=True)
        if not title:
            continue
        specs = _matching_specs(title)
        if series_ids is not None:
            specs = [spec for spec in specs if spec.series_id in series_ids]
        if not specs:
            continue

        try:
            release = _parse_long_date(release_date_text)
            reference = _parse_reference_period(reference_text)
            scheduled = parse_scheduled_release_time(
                release,
                release_time_text,
                default_tz=CENSUS_RELEASE_TZ,
            )
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(f"{title}: {type(exc).__name__}: {exc}")
                continue
            raise

        link = cells[0].find("a", href=True)
        source_url = (
            urljoin(CENSUS_CALENDAR_URL, str(link["href"]))
            if link is not None
            else CENSUS_CALENDAR_URL
        )
        for spec in specs:
            entries.append(
                CensusScheduleEntry(
                    series_id=spec.series_id,
                    reference_date=reference.isoformat(),
                    reference_label=reference_text,
                    release_title=title,
                    release_date=release.isoformat(),
                    release_time_local=release_time_text,
                    event_time_utc=scheduled.utc.isoformat(),
                    source_url=source_url,
                )
            )
    return entries


def schedule_entry_to_records(
    entry: CensusScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: CensusIndicatorSpec | None = None,
) -> tuple[CensusCalendarRawRecord, CensusCalendarEventRecord]:
    """Project one Census schedule entry to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in INDICATOR_REGISTRY"
        )

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    schedule_payload: dict[str, Any] = {
        "kind": "census_schedule",
        "series_id": entry.series_id,
        "reference_label": entry.reference_label,
        "reference_date": entry.reference_date,
        "release_title": entry.release_title,
        "release_date": entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc": entry.event_time_utc,
        "source_url": entry.source_url,
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
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    raw_record = CensusCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = CensusCalendarEventRecord(
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
        source="Census Bureau",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_CENSUS_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Census list-view calendar and return HTML text."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            CENSUS_CALENDAR_URL,
            headers=_CENSUS_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        response.encoding = response.encoding or "ISO-8859-1"
        return response.text
    finally:
        if owned_session:
            s.close()
