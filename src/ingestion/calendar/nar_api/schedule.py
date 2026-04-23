"""NAR statistical news release schedule scraper."""

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

from .indicators import INDICATOR_REGISTRY, NAR_SCHEDULE_URL, NARIndicatorSpec
from .parser import PROVIDER, NARCalendarEventRecord, NARCalendarRawRecord

logger = logging.getLogger(__name__)

NAR_RELEASE_TZ = "America/New_York"
NAR_RELEASE_TIME_LOCAL = "10:00 AM"


class NARScheduleParseError(ValueError):
    """Raised when the NAR schedule page drifts."""


@dataclass(frozen=True)
class NARScheduleEntry:
    """One NAR release-schedule row, pre-projection."""

    series_id: str
    raw_title: str
    reference_date: str
    reference_label: str
    release_date: str
    release_time_local: str
    event_time_utc: str
    source_url: str


_MONTH_NAMES: dict[str, int] = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTH_RE = (
    r"Jan\.?|January|Feb\.?|February|Mar\.?|March|Apr\.?|April|May|"
    r"Jun\.?|June|Jul\.?|July|Aug\.?|August|Sep\.?|Sept\.?|"
    r"September|Oct\.?|October|Nov\.?|November|Dec\.?|December"
)
_SCHEDULE_YEAR_RE = re.compile(
    r"\b(?P<year>20\d{2})\s+(?:NAR\s+)?Statistical\s+News\s+"
    r"Release\s+Schedule\b",
    re.IGNORECASE,
)
_SCHEDULE_ROW_RE = re.compile(
    rf"\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?,?\s+"
    rf"(?P<release_month>{_MONTH_RE})\s+(?P<release_day>\d{{1,2}})\s+"
    rf"(?P<title>(?:(?!(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.?,?\s+"
    rf"(?:{_MONTH_RE})\s+\d{{1,2}}).)+?"
    r"(?:Existing-Home Sales|Pending Home Sales Index))\b",
    re.IGNORECASE,
)


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _month_number(name: str) -> int:
    normalized = name.strip().rstrip(".").lower()
    month = _MONTH_NAMES.get(normalized)
    if month is None:
        raise NARScheduleParseError(f"unknown month name: {name!r}")
    return month


def _reference_label(ref: date) -> str:
    return f"{ref.strftime('%B')} {ref.year}"


def _schedule_year(text: str) -> int:
    match = _SCHEDULE_YEAR_RE.search(text)
    if match is None:
        raise NARScheduleParseError("NAR schedule year not found")
    return int(match.group("year"))


def _resolve_series(
    series_ids: set[str] | None,
) -> list[NARIndicatorSpec]:
    specs = list(INDICATOR_REGISTRY.values())
    if series_ids is None:
        return specs
    return [spec for spec in specs if spec.series_id in series_ids]


def _reference_month_text(raw_title: str, spec: NARIndicatorSpec) -> str:
    title = raw_title
    fragment_re = re.compile(re.escape(spec.schedule_title_fragment), re.IGNORECASE)
    title = fragment_re.sub("", title).strip()
    return title


def _reference_date(
    *,
    reference_month_text: str,
    release_date: date,
) -> date:
    ref_month = _month_number(reference_month_text)
    ref_year = release_date.year - 1 if ref_month > release_date.month else release_date.year
    return date(ref_year, ref_month, 1)


def parse_schedule_html(
    html: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[NARScheduleEntry]:
    """Extract whitelisted rows from the NAR statistical release schedule."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    year = _schedule_year(text)
    specs = _resolve_series(series_ids)
    entries: list[NARScheduleEntry] = []
    for match in _SCHEDULE_ROW_RE.finditer(_normalize(text)):
        raw_title = _normalize(match.group("title"))
        for spec in specs:
            if spec.schedule_title_fragment.lower() not in raw_title.lower():
                continue
            try:
                release = date(
                    year,
                    _month_number(match.group("release_month")),
                    int(match.group("release_day")),
                )
                ref = _reference_date(
                    reference_month_text=_reference_month_text(raw_title, spec),
                    release_date=release,
                )
                scheduled = parse_scheduled_release_time(
                    release,
                    NAR_RELEASE_TIME_LOCAL,
                    default_tz=NAR_RELEASE_TZ,
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(f"{raw_title}: {type(exc).__name__}: {exc}")
                    continue
                raise
            entries.append(
                NARScheduleEntry(
                    series_id=spec.series_id,
                    raw_title=raw_title,
                    reference_date=ref.isoformat(),
                    reference_label=_reference_label(ref),
                    release_date=release.isoformat(),
                    release_time_local=NAR_RELEASE_TIME_LOCAL,
                    event_time_utc=scheduled.utc.isoformat(),
                    source_url=NAR_SCHEDULE_URL,
                )
            )
    return entries


def schedule_entry_to_records(
    entry: NARScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: NARIndicatorSpec | None = None,
) -> tuple[NARCalendarRawRecord, NARCalendarEventRecord]:
    """Project one NAR schedule row to records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in INDICATOR_REGISTRY")

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    payload: dict[str, Any] = {
        "kind": "nar_schedule",
        "series_id": entry.series_id,
        "raw_title": entry.raw_title,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_date": entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc": entry.event_time_utc,
        "source_url": entry.source_url,
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
    raw_record = NARCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = NARCalendarEventRecord(
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
        source="National Association of Realtors",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_NAR_HTTP_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


def fetch_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the public NAR statistical-release schedule page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(NAR_SCHEDULE_URL, headers=_NAR_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_current_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a public NAR housing-statistics page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_NAR_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
