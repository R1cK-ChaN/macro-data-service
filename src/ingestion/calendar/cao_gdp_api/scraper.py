"""Scrape ESRI GDP release-archive pages.

The yearly archive page
``esri.cao.go.jp/en/sna/data/sokuhou/files/<YYYY>/toukei_<YYYY>.html``
lists past first and second preliminary GDP releases. Each row links
to a per-release report menu (``qeYYQ[/_2]/gdemenuea.html``), which
the value side uses to locate the CSV carrying the real GDP QoQ
headline.
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import (
    FIRST_PRELIMINARY,
    SECOND_PRELIMINARY,
    SPEC_BY_STAGE,
    CaoGdpIndicatorSpec,
)
from .parser import (
    CAO_GDP_ARCHIVE_INDEX_URL,
    CAO_GDP_ARCHIVE_YEAR_URL_TEMPLATE,
    CAO_GDP_RELEASE_TIME_LOCAL,
    CAO_GDP_RELEASE_TZ,
    CAO_GDP_SOURCE,
    PROVIDER,
    CaoGdpCalendarEventRecord,
    CaoGdpCalendarRawRecord,
)

logger = logging.getLogger(__name__)


class CaoGdpArchiveParseError(ValueError):
    """Raised when an ESRI GDP archive page changes shape."""


@dataclass(frozen=True)
class CaoGdpReleaseEntry:
    """One GDP release row parsed from a yearly ESRI archive."""

    reference_date: date
    reference_label: str
    release_date: date
    release_stage: str
    report_url: str
    release_title: str


CAO_GDP_BROWSER_HEADERS: dict[str, str] = {
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

_ARCHIVE_YEAR_RE = re.compile(r"/(?P<year>\d{4})/toukei_(?P=year)\.html$")
_MONTHS: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}
_RELEASE_DATE_RE = re.compile(
    r"(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2}),\s*(?P<year>\d{4})"
)
_REFERENCE_PERIOD_RE = re.compile(
    r"(?P<start>Jan|Apr|Jul|Oct)\.?\s*[-–—]\s*"
    r"(?P<end>Mar|Jun|Sep|Dec)\.?\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
_FIRST_STAGE_RE = re.compile(r"\b(first|1st)\s+preliminary\b", re.IGNORECASE)
_SECOND_STAGE_RE = re.compile(r"\b(second|2nd)\s+preliminary\b", re.IGNORECASE)
_QUARTER_END_BY_START_MONTH = {1: (1, 3), 4: (2, 6), 7: (3, 9), 10: (4, 12)}


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _resolve_month(token: str) -> int:
    key = token.strip().lower().rstrip(".")
    month = _MONTHS.get(key)
    if month is None:
        raise CaoGdpArchiveParseError(f"unknown CAO GDP month token: {token!r}")
    return month


def parse_gdp_archive_index_html(html: str) -> list[int]:
    """Return archive years from ``toukei_top.html`` in descending order."""
    soup = BeautifulSoup(html, "html.parser")
    years: set[int] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"]).strip()
        match = _ARCHIVE_YEAR_RE.search(href)
        if match is None:
            continue
        years.add(int(match.group("year")))
    return sorted(years, reverse=True)


def select_archive_years(years: list[int], *, limit: int = 2) -> list[int]:
    """Pick the latest unique archive years for a default scrape."""
    return sorted(set(years), reverse=True)[:limit]


def _parse_release_date(text: str) -> date:
    normalized = _normalize_text(text)
    match = _RELEASE_DATE_RE.search(normalized)
    if match is None:
        raise CaoGdpArchiveParseError(
            f"unparseable CAO GDP release date: {text!r}"
        )
    month = _resolve_month(match.group("month"))
    day = int(match.group("day"))
    year = int(match.group("year"))
    try:
        return date(year, month, day)
    except ValueError as exc:
        raise CaoGdpArchiveParseError(
            f"invalid CAO GDP release date: {text!r}"
        ) from exc


def _parse_reference_period(title: str) -> tuple[date, str]:
    normalized = _normalize_text(title)
    match = _REFERENCE_PERIOD_RE.search(normalized)
    if match is None:
        raise CaoGdpArchiveParseError(
            f"unparseable CAO GDP reference period: {title!r}"
        )
    start_month = _resolve_month(match.group("start"))
    expected_quarter, end_month = _QUARTER_END_BY_START_MONTH[start_month]
    parsed_end = _resolve_month(match.group("end"))
    if parsed_end != end_month:
        raise CaoGdpArchiveParseError(
            f"CAO GDP reference period has unexpected end month: {title!r}"
        )
    year = int(match.group("year"))
    last_day = _calendar.monthrange(year, end_month)[1]
    reference = date(year, end_month, last_day)
    label = f"Q{expected_quarter} {year}"
    return reference, label


def _parse_release_stage(title: str) -> str:
    if _FIRST_STAGE_RE.search(title):
        return FIRST_PRELIMINARY
    if _SECOND_STAGE_RE.search(title):
        return SECOND_PRELIMINARY
    raise CaoGdpArchiveParseError(
        f"unparseable CAO GDP release stage: {title!r}"
    )


def parse_gdp_archive_html(
    html: str,
    *,
    base_url: str,
) -> list[CaoGdpReleaseEntry]:
    """Extract GDP release rows from one yearly archive page."""
    soup = BeautifulSoup(html, "html.parser")
    entries: list[CaoGdpReleaseEntry] = []
    seen: set[tuple[date, str]] = set()
    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"], recursive=False)
        if len(cells) < 2:
            continue
        anchor = cells[1].find("a", href=True)
        if anchor is None:
            continue
        title = _normalize_text(anchor.get_text(" ", strip=True))
        if "quarterly estimates of gdp" not in title.lower():
            continue
        release_date = _parse_release_date(cells[0].get_text(" ", strip=True))
        reference_date, reference_label = _parse_reference_period(title)
        stage = _parse_release_stage(title)
        key = (reference_date, stage)
        if key in seen:
            raise CaoGdpArchiveParseError(
                f"duplicate CAO GDP archive row: {reference_date} {stage}"
            )
        seen.add(key)
        report_url = urljoin(base_url, str(anchor["href"]).strip())
        entries.append(
            CaoGdpReleaseEntry(
                reference_date=reference_date,
                reference_label=reference_label,
                release_date=release_date,
                release_stage=stage,
                report_url=report_url,
                release_title=title,
            )
        )
    return entries


_HASH_FIELDS: tuple[str, ...] = (
    "reference_date",
    "release_date",
    "release_stage",
    "event_time_utc",
    "report_url",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _anchor(reference: date, release_stage: str) -> str:
    return f"{reference.isoformat()}|{release_stage}"


def schedule_entry_to_records(
    entry: CaoGdpReleaseEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: CaoGdpIndicatorSpec | None = None,
) -> tuple[CaoGdpCalendarRawRecord, CaoGdpCalendarEventRecord]:
    """Project one GDP archive entry to ``(raw, event)`` records."""
    resolved_spec = spec or SPEC_BY_STAGE[entry.release_stage]
    scheduled = parse_scheduled_release_time(
        entry.release_date,
        CAO_GDP_RELEASE_TIME_LOCAL,
        default_tz=CAO_GDP_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()
    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        _anchor(entry.reference_date, entry.release_stage),
    )

    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    payload: dict[str, Any] = {
        "kind":            "cao_gdp_schedule",
        "indicator":       resolved_spec.indicator,
        "reference_date":  entry.reference_date.isoformat(),
        "reference_label": entry.reference_label,
        "release_date":    entry.release_date.isoformat(),
        "release_stage":   entry.release_stage,
        "release_title":   entry.release_title,
        "event_time_utc":  event_time_utc,
        "report_url":      entry.report_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    raw_record = CaoGdpCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = CaoGdpCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.reference_date.isoformat(),
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
        source=CAO_GDP_SOURCE,
        source_url=entry.report_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


def archive_year_url(year: int) -> str:
    return CAO_GDP_ARCHIVE_YEAR_URL_TEMPLATE.format(year=year)


def fetch_gdp_archive_index_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ESRI GDP archive index page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            CAO_GDP_ARCHIVE_INDEX_URL,
            headers=CAO_GDP_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_gdp_archive_year_html(
    year: int,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a yearly ESRI GDP archive page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            archive_year_url(year),
            headers=CAO_GDP_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
