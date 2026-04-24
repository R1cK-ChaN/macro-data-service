"""Scrape Statistics Bureau schedule pages for CPI and Labour Force Survey."""

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

from .indicators import INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    STAT_BUREAU_CPI_SCHEDULE_URL,
    STAT_BUREAU_LFS_SCHEDULE_URL,
    STAT_BUREAU_RELEASE_TIME_LOCAL,
    STAT_BUREAU_RELEASE_TZ,
    StatBureauCalendarEventRecord,
    StatBureauCalendarRawRecord,
)

logger = logging.getLogger(__name__)


class StatBureauCalendarParseError(ValueError):
    """Raised when a Statistics Bureau schedule surface drifts."""


@dataclass(frozen=True)
class StatBureauScheduleEntry:
    """One scheduled Statistics Bureau economic release."""

    indicator: str
    reference_date: date
    reference_label: str
    release_date: date
    release_time_local: str
    source_url: str
    payload: dict[str, Any]


STAT_BUREAU_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_MONTHS: dict[str, int] = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}
_MONTH_NAME_BY_NUM = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}
_MONTH_NAME_PATTERN = (
    r"January|February|March|April|May|June|July|August|"
    r"September|Sept|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)
_MONTH_YEAR_RE = re.compile(
    rf"\b(?P<month>{_MONTH_NAME_PATTERN})\.?\s*,?\s*(?P<year>\d{{4}})\b",
    re.IGNORECASE,
)
_YEAR_MONTH_RE = re.compile(
    rf"\b(?P<year>\d{{4}})\s+(?P<month>{_MONTH_NAME_PATTERN})\.?\b",
    re.IGNORECASE,
)
_BARE_MONTH_RE = re.compile(
    rf"^\s*(?P<month>{_MONTH_NAME_PATTERN})\.?\b",
    re.IGNORECASE,
)
_RELEASE_DATE_RE = re.compile(
    rf"\b(?P<month>{_MONTH_NAME_PATTERN})\.?\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?"
    r"(?:\s*,?\s*(?P<year>\d{4}))?\b",
    re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")


def _month_number(token: str) -> int:
    month = _MONTHS.get(token.strip().lower().rstrip("."))
    if month is None:
        raise StatBureauCalendarParseError(
            f"unknown Statistics Bureau month token: {token!r}"
        )
    return month


def _reference_label(reference: date) -> str:
    return f"{_MONTH_NAME_BY_NUM[reference.month]} {reference.year}"


def _normalize_cell(text: str) -> str:
    return _SPACE_RE.sub(" ", (text or "").replace("\xa0", " ")).strip()


def _parse_reference_month(
    text: str,
    *,
    inferred_year: int | None,
    previous_reference: date | None,
) -> date | None:
    """Parse the monthly reference from a schedule-table cell."""
    normalized = _normalize_cell(text)
    for pattern in (_MONTH_YEAR_RE, _YEAR_MONTH_RE):
        match = pattern.search(normalized)
        if match is None:
            continue
        return date(
            int(match.group("year")),
            _month_number(match.group("month")),
            1,
        )

    if inferred_year is None:
        return None
    bare = _BARE_MONTH_RE.search(normalized)
    if bare is None:
        return None
    month = _month_number(bare.group("month"))
    year = inferred_year
    if previous_reference is not None and month < previous_reference.month:
        year = previous_reference.year + 1
    return date(year, month, 1)


def _parse_release_date(text: str, *, reference: date) -> date | None:
    normalized = _normalize_cell(text)
    match = _RELEASE_DATE_RE.search(normalized)
    if match is None:
        return None
    month = _month_number(match.group("month"))
    year_text = match.group("year")
    year = int(year_text) if year_text else reference.year
    if year_text is None and month < reference.month:
        year += 1
    return date(year, month, int(match.group("day")))


def _parse_schedule_table(
    html: str,
    *,
    indicator: str,
    source_url: str,
) -> list[StatBureauScheduleEntry]:
    """Extract monthly reference/release rows from the first schedule table."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    entries: list[StatBureauScheduleEntry] = []
    inferred_year: int | None = None
    previous_reference: date | None = None
    seen: set[tuple[str, date, date]] = set()

    for row in rows:
        cells = [
            _normalize_cell(cell.get_text(" ", strip=True))
            for cell in row.find_all(["th", "td"])
        ]
        if len(cells) < 2:
            continue
        reference = _parse_reference_month(
            cells[0],
            inferred_year=inferred_year,
            previous_reference=previous_reference,
        )
        if reference is None:
            continue
        inferred_year = reference.year
        previous_reference = reference
        release = _parse_release_date(cells[1], reference=reference)
        if release is None:
            continue
        key = (indicator, reference, release)
        if key in seen:
            continue
        seen.add(key)
        entries.append(StatBureauScheduleEntry(
            indicator=indicator,
            reference_date=reference,
            reference_label=_reference_label(reference),
            release_date=release,
            release_time_local=STAT_BUREAU_RELEASE_TIME_LOCAL,
            source_url=source_url,
            payload={
                "kind": f"{indicator.lower()}_release_schedule",
                "reference_text": cells[0],
                "release_text": cells[1],
                "reference_date": reference.isoformat(),
                "release_date": release.isoformat(),
                "release_time_local": STAT_BUREAU_RELEASE_TIME_LOCAL,
            },
        ))

    return entries


def parse_cpi_release_schedule_html(html: str) -> list[StatBureauScheduleEntry]:
    """Parse the Japan column from the CPI release schedule page."""
    return _parse_schedule_table(
        html,
        indicator="CORE_CPI",
        source_url=STAT_BUREAU_CPI_SCHEDULE_URL,
    )


def parse_lfs_release_schedule_html(html: str) -> list[StatBureauScheduleEntry]:
    """Parse the Basic tabulation column from the Labour Force schedule."""
    return _parse_schedule_table(
        html,
        indicator="UNEMPLOYMENT_RATE",
        source_url=STAT_BUREAU_LFS_SCHEDULE_URL,
    )


def _record_id(indicator: str, reference: date) -> str:
    spec = INDICATOR_REGISTRY[indicator]
    return synthesize_event_id(
        PROVIDER,
        spec.country_code,
        canonicalize_indicator(spec.title),
        reference.isoformat(),
    )


def schedule_entry_to_records(
    entry: StatBureauScheduleEntry,
    *,
    snapshot_epoch_ms: int,
) -> tuple[StatBureauCalendarRawRecord, StatBureauCalendarEventRecord]:
    """Convert one parsed schedule entry into raw + event records."""
    spec = INDICATOR_REGISTRY[entry.indicator]
    provider_event_id = _record_id(entry.indicator, entry.reference_date)
    release_time = parse_scheduled_release_time(
        entry.release_date,
        entry.release_time_local,
        default_tz=STAT_BUREAU_RELEASE_TZ,
    )
    payload = dict(entry.payload)
    payload.update({
        "provider": PROVIDER,
        "provider_event_id": provider_event_id,
        "indicator": entry.indicator,
        "title": spec.title,
        "event_time_utc": release_time.utc.isoformat(),
        "schedule_source_url": entry.source_url,
        "source_url": spec.source_url,
    })
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw = StatBureauCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event = StatBureauCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=release_time.utc.isoformat(),
        event_time_precision="datetime",
        reference_date=entry.reference_date.isoformat(),
        reference_label=entry.reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Statistics Bureau of Japan",
        source_url=spec.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def fetch_cpi_release_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    client = session or requests.Session()
    response = client.get(
        STAT_BUREAU_CPI_SCHEDULE_URL,
        headers=STAT_BUREAU_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_lfs_release_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    client = session or requests.Session()
    response = client.get(
        STAT_BUREAU_LFS_SCHEDULE_URL,
        headers=STAT_BUREAU_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text
