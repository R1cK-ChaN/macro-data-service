"""GfK / NIM Consumer Climate release-date parser."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import (
    GFK_ALL_RELEASES_URL,
    GFK_BASE_URL,
    GFK_CONSUMER_CLIMATE_URL,
    INDICATOR_REGISTRY,
    GfKIndicatorSpec,
    reference_label_en,
)
from .parser import (
    PROVIDER,
    GfKCalendarEventRecord,
    GfKCalendarRawRecord,
)

GFK_RELEASE_TZ = "Europe/Berlin"
GFK_DEFAULT_RELEASE_TIME = "08:00"


class GfKScheduleParseError(ValueError):
    """Raised when a GfK / NIM schedule page cannot be projected."""


@dataclass(frozen=True)
class GfKScheduleEntry:
    """One matched GfK / NIM schedule row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    event_time_utc: str
    event_time_precision: str
    source_url: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class GfKResolvedPressRelease:
    """One release page resolved from a GfK / NIM listing page."""

    title: str
    release_date: str
    source_url: str
    raw: dict[str, Any]


_MONTHS: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_WEEKDAYS = r"monday|tuesday|wednesday|thursday|friday|saturday|sunday"

# "Monday, April 27, 2026, 8:00 a.m."
_DATE_TIME_RE = re.compile(
    rf"(?:{_WEEKDAYS}),\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),\s+(20\d{2})"
    r"(?:,\s+(\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm)?))?",
    re.I,
)


def _page_text(html: str | bytes) -> str:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(" ", strip=True)


def _reference_date(release_date: date) -> date:
    return date(release_date.year, release_date.month, 1)


def _event_time(release_date: date, release_time: str) -> str:
    scheduled = parse_scheduled_release_time(
        release_date,
        release_time,
        default_tz=GFK_RELEASE_TZ,
    )
    return scheduled.utc.isoformat()


def parse_release_dates_html(
    html: str | bytes,
    *,
    series_ids: set[str] | None = None,
    source_url: str | None = None,
    row_issues: list[str] | None = None,
) -> list[GfKScheduleEntry]:
    """Extract GfK / NIM Consumer Climate schedule rows from the main page."""
    text = _page_text(html)
    entries: list[GfKScheduleEntry] = []
    spec = INDICATOR_REGISTRY["GFK_CONSUMER_CLIMATE"]
    if series_ids is not None and spec.series_id not in series_ids:
        return entries

    seen: set[date] = set()
    for match in _DATE_TIME_RE.finditer(text):
        month_raw, day_raw, year_raw, time_raw = match.groups()
        try:
            release_date = date(
                int(year_raw),
                _MONTHS[month_raw.lower()],
                int(day_raw),
            )
            if release_date in seen:
                continue
            seen.add(release_date)
            reference = _reference_date(release_date)
            label = reference_label_en(reference)
            release_time = time_raw or GFK_DEFAULT_RELEASE_TIME
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(f"{spec.series_id}: {type(exc).__name__}: {exc}")
                continue
            raise
        entries.append(
            GfKScheduleEntry(
                series_id=spec.series_id,
                reference_date=reference.isoformat(),
                reference_label=label,
                release_title=spec.title,
                release_date=release_date,
                event_time_utc=_event_time(release_date, release_time),
                event_time_precision="datetime",
                source_url=source_url or GFK_CONSUMER_CLIMATE_URL,
                raw={
                    "release_date": release_date.isoformat(),
                    "release_time": release_time,
                    "source_url": source_url or GFK_CONSUMER_CLIMATE_URL,
                },
            )
        )
    if not entries:
        where = f" at {source_url}" if source_url else ""
        raise GfKScheduleParseError(
            f"GfK / NIM consumer-climate page contained no {spec.series_id} dates{where}"
        )
    entries.sort(key=lambda entry: (entry.event_time_utc, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: GfKScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: GfKIndicatorSpec | None = None,
) -> tuple[GfKCalendarRawRecord, GfKCalendarEventRecord]:
    """Project one GfK schedule row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in GfK INDICATOR_REGISTRY")
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    payload = {
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "event_time_utc": entry.event_time_utc,
        "event_time_precision": entry.event_time_precision,
        "source_url": entry.source_url,
        "raw": entry.raw,
    }
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()

    raw_record = GfKCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = GfKCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=entry.event_time_utc,
        event_time_precision=entry.event_time_precision,
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
        source="GfK / NIM",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_GFK_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
}


def fetch_release_dates_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the NIM Consumer Climate page."""
    http = session or requests.Session()
    response = http.get(
        GFK_CONSUMER_CLIMATE_URL,
        headers=_GFK_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_all_releases_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the NIM Consumer Climate all-releases listing."""
    http = session or requests.Session()
    response = http.get(
        GFK_ALL_RELEASES_URL,
        headers=_GFK_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_press_release_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET one GfK / NIM press-release page."""
    http = session or requests.Session()
    response = http.get(url, headers=_GFK_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


_LISTING_DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(20\d{2})",
    re.I,
)


def resolve_press_release_link(
    html: str | bytes,
    *,
    release_date: date,
) -> GfKResolvedPressRelease:
    """Resolve the monthly GfK / NIM Consumer Climate release from a listing page."""
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if "/en/consumer-climate/detail-consumer-climate/" not in href:
            continue
        container = anchor.find_parent(["li", "article", "div"]) or anchor
        container_text = container.get_text(" ", strip=True)
        haystack = container_text.lower()
        if "consumer climate" not in haystack and "consumer sentiment" not in haystack:
            continue
        match = _LISTING_DATE_RE.search(container_text)
        if not match:
            continue
        month_raw, day_raw, year_raw = match.groups()
        try:
            listed = date(
                int(year_raw),
                _MONTHS[month_raw.lower()],
                int(day_raw),
            )
        except (KeyError, ValueError):
            continue
        if listed != release_date:
            continue
        title = anchor.get_text(" ", strip=True) or container_text
        return GfKResolvedPressRelease(
            title=title,
            release_date=release_date.isoformat(),
            source_url=urljoin(GFK_BASE_URL, href),
            raw={"listing_text": container_text[:1000]},
        )
    raise GfKScheduleParseError(
        f"GfK / NIM press release not found for {release_date.isoformat()}"
    )


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for NIM's rolling release calendar."""
    base = today or datetime.now(ZoneInfo(GFK_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year, 12, 31)
