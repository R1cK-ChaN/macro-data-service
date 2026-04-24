"""ZEW economic-sentiment release-date parser."""

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
    INDICATOR_REGISTRY,
    ZEW_BASE_URL,
    ZEW_PRESS_RELEASES_URL,
    ZEWIndicatorSpec,
    reference_label_en,
    release_dates_url,
)
from .parser import (
    PROVIDER,
    ZEWCalendarEventRecord,
    ZEWCalendarRawRecord,
)

ZEW_RELEASE_TZ = "Europe/Berlin"
ZEW_DEFAULT_RELEASE_TIME = "11:05 a.m."


class ZEWScheduleParseError(ValueError):
    """Raised when a ZEW schedule page cannot be projected."""


@dataclass(frozen=True)
class ZEWScheduleEntry:
    """One matched ZEW schedule row, pre-projection."""

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
class ZEWResolvedPressRelease:
    """One release page resolved from a ZEW listing page."""

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
_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(20\d{2})\b",
    re.I,
)
_TIME_RE = re.compile(
    r"at\s+(\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm)?)\s+Frankfurt Time",
    re.I,
)
_LISTING_DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.20\d{2})\b")


def _page_text(html: str | bytes) -> str:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(" ", strip=True)


def _release_time_text(text: str) -> str:
    match = _TIME_RE.search(text)
    return match.group(1) if match else ZEW_DEFAULT_RELEASE_TIME


def _reference_date(release_date: date) -> date:
    return date(release_date.year, release_date.month, 1)


def _event_time(release_date: date, release_time: str) -> str:
    scheduled = parse_scheduled_release_time(
        release_date,
        release_time,
        default_tz=ZEW_RELEASE_TZ,
    )
    return scheduled.utc.isoformat()


def parse_release_dates_html(
    html: str | bytes,
    *,
    series_ids: set[str] | None = None,
    source_url: str | None = None,
    row_issues: list[str] | None = None,
) -> list[ZEWScheduleEntry]:
    """Extract ZEW economic-sentiment schedule rows from the annual page."""
    text = _page_text(html)
    release_time = _release_time_text(text)
    entries: list[ZEWScheduleEntry] = []
    spec = INDICATOR_REGISTRY["ZEW_ECONOMIC_SENTIMENT"]
    if series_ids is not None and spec.series_id not in series_ids:
        return entries

    for match in _DATE_RE.finditer(text):
        day_raw, month_raw, year_raw = match.groups()
        try:
            release_date = date(
                int(year_raw),
                _MONTHS[month_raw.lower()],
                int(day_raw),
            )
            reference = _reference_date(release_date)
            label = reference_label_en(reference)
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(f"{spec.series_id}: {type(exc).__name__}: {exc}")
                continue
            raise
        entries.append(
            ZEWScheduleEntry(
                series_id=spec.series_id,
                reference_date=reference.isoformat(),
                reference_label=label,
                release_title=spec.title,
                release_date=release_date,
                event_time_utc=_event_time(release_date, release_time),
                event_time_precision="datetime",
                source_url=source_url or release_dates_url(release_date.year),
                raw={
                    "release_date": release_date.isoformat(),
                    "release_time": release_time,
                    "source_url": source_url or release_dates_url(release_date.year),
                },
            )
        )
    if not entries:
        where = f" at {source_url}" if source_url else ""
        raise ZEWScheduleParseError(
            f"ZEW release-date page contained no {spec.series_id} dates{where}"
        )
    entries.sort(key=lambda entry: (entry.event_time_utc, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: ZEWScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: ZEWIndicatorSpec | None = None,
) -> tuple[ZEWCalendarRawRecord, ZEWCalendarEventRecord]:
    """Project one ZEW schedule row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in ZEW INDICATOR_REGISTRY")
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

    raw_record = ZEWCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ZEWCalendarEventRecord(
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
        source="ZEW",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_ZEW_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
}


def fetch_release_dates_html(
    year: int,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the annual ZEW release-dates page."""
    http = session or requests.Session()
    response = http.get(
        release_dates_url(year),
        headers=_ZEW_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_latest_press_releases_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET ZEW's latest press-release listing."""
    http = session or requests.Session()
    response = http.get(
        ZEW_PRESS_RELEASES_URL,
        headers=_ZEW_BROWSER_HEADERS,
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
    """GET one ZEW press-release page."""
    http = session or requests.Session()
    response = http.get(url, headers=_ZEW_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def resolve_press_release_link(
    html: str | bytes,
    *,
    release_date: date,
) -> ZEWResolvedPressRelease:
    """Resolve the monthly ZEW economic-sentiment release from a listing page."""
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    target_date = release_date.strftime("%d.%m.%Y")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if "/en/press/latest-press-releases/" not in href:
            continue
        container = anchor.find_parent(["li", "article", "div"]) or anchor
        container_text = container.get_text(" ", strip=True)
        if target_date not in container_text:
            continue
        if "ZEW Indicator of Economic Sentiment" not in container_text:
            continue
        title = anchor.get_text(" ", strip=True)
        return ZEWResolvedPressRelease(
            title=title,
            release_date=target_date,
            source_url=urljoin(ZEW_BASE_URL, href),
            raw={"listing_text": container_text[:1000]},
        )
    raise ZEWScheduleParseError(
        f"ZEW press release not found for {target_date}"
    )


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for ZEW's annual release calendar."""
    base = today or datetime.now(ZoneInfo(ZEW_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year, 12, 31)
