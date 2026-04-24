"""Destatis weekly release-table parser."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
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

from .indicators import DestatisIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    DestatisCalendarEventRecord,
    DestatisCalendarRawRecord,
    _normalise,
    parse_period,
)

logger = logging.getLogger(__name__)

DESTATIS_RELEASE_TABLE_URL = (
    "https://www.destatis.de/DE/Presse/Termine/"
    "Veroeffentlichungstabelle/_inhalt.html"
)
DESTATIS_RELEASE_TZ = "Europe/Berlin"
DESTATIS_DEFAULT_RELEASE_TIME = "08:00"


class DestatisScheduleParseError(ValueError):
    """Raised when Destatis' release-table shape drifts."""


@dataclass(frozen=True)
class DestatisScheduleEntry:
    """One matched Destatis release-table row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    event_time_utc: str
    event_time_precision: str
    press_number: str
    evas_codes: tuple[str, ...]
    source_url: str
    raw: dict[str, Any]


_GERMAN_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\.\s*(?P<month>[^\W\d_]+\.?)\s*(?P<year>\d{4})"
)
_NUMERIC_DATE_RE = re.compile(
    r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})"
)
_TIME_RE = re.compile(r"(?P<hour>\d{1,2})[:.](?P<minute>\d{2})\s*uhr", re.I)
_EVAS_RE = re.compile(r"\b\d{5}\b")
_MONTHS: dict[str, int] = {
    "januar": 1,
    "jan": 1,
    "februar": 2,
    "feb": 2,
    "maerz": 3,
    "marz": 3,
    "mrz": 3,
    "maer": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "mai": 5,
    "juni": 6,
    "jun": 6,
    "juli": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "oktober": 10,
    "okt": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "dezember": 12,
    "dez": 12,
    "dec": 12,
}


def _month_number(raw: str) -> int:
    key = _normalise(raw).rstrip(".")
    month = _MONTHS.get(key)
    if month is None:
        raise DestatisScheduleParseError(f"unknown month name: {raw!r}")
    return month


def _parse_release_date(text: str) -> date:
    cleaned = text.replace("\xa0", " ").strip()
    match = _NUMERIC_DATE_RE.search(cleaned)
    if match:
        return date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    match = _GERMAN_DATE_RE.search(cleaned)
    if match:
        return date(
            int(match.group("year")),
            _month_number(match.group("month")),
            int(match.group("day")),
        )
    raise DestatisScheduleParseError(f"unparseable release date: {text!r}")


def _event_time(
    release_date: date,
    *,
    row_text: str,
) -> tuple[str, str]:
    normalized = _normalise(row_text)
    if "im laufe des tages" in normalized:
        return datetime(
            release_date.year,
            release_date.month,
            release_date.day,
            tzinfo=timezone.utc,
        ).isoformat(), "date"

    match = _TIME_RE.search(normalized)
    release_time = (
        f"{int(match.group('hour')):02d}:{int(match.group('minute')):02d}"
        if match
        else DESTATIS_DEFAULT_RELEASE_TIME
    )
    scheduled = parse_scheduled_release_time(
        release_date,
        release_time,
        default_tz=DESTATIS_RELEASE_TZ,
    )
    return scheduled.utc.isoformat(), "datetime"


def _matching_specs(row_text: str) -> list[DestatisIndicatorSpec]:
    normalized = _normalise(row_text)
    out: list[DestatisIndicatorSpec] = []
    for spec in INDICATOR_REGISTRY.values():
        if all(
            _normalise(fragment) in normalized
            for fragment in spec.schedule_title_fragments
        ):
            out.append(spec)
    return out


def _row_cells(soup: BeautifulSoup) -> list[tuple[list[str], list[str]]]:
    rows: list[tuple[list[str], list[str]]] = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"], recursive=False)
        texts = [cell.get_text(" ", strip=True) for cell in cells]
        if len(texts) < 4:
            continue
        normalized = _normalise(" ".join(texts))
        if "erscheinungstermin" in normalized:
            continue
        urls: list[str] = []
        for link in tr.find_all("a", href=True):
            urls.append(urljoin(DESTATIS_RELEASE_TABLE_URL, str(link["href"])))
        rows.append((texts, urls))
    return rows


def parse_release_table_html(
    html: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[DestatisScheduleEntry]:
    """Extract whitelisted releases from Destatis' weekly table."""
    soup = BeautifulSoup(html, "html.parser")
    table_rows = _row_cells(soup)
    if not table_rows:
        raise DestatisScheduleParseError("release table rows not found")

    entries: list[DestatisScheduleEntry] = []
    for cells, urls in table_rows:
        row_text = " ".join(cells)
        specs = _matching_specs(row_text)
        if series_ids is not None:
            specs = [spec for spec in specs if spec.series_id in series_ids]
        if not specs:
            continue

        if len(cells) >= 5:
            press_number, evas_text, title, reference_label, release_text = cells[:5]
        else:
            press_number = cells[0]
            title = cells[-3]
            reference_label = cells[-2]
            release_text = cells[-1]
            evas_text = " ".join(cells[1:-3])
        evas_codes = tuple(_EVAS_RE.findall(evas_text))
        source_url = DESTATIS_RELEASE_TABLE_URL
        for spec in specs:
            try:
                reference = parse_period(
                    reference_label,
                    cadence=spec.reference_cadence,
                )
                release_date = _parse_release_date(release_text)
                event_time_utc, precision = _event_time(
                    release_date,
                    row_text=row_text,
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(
                        f"{title}: {type(exc).__name__}: {exc}"
                    )
                    continue
                raise
            entries.append(
                DestatisScheduleEntry(
                    series_id=spec.series_id,
                    reference_date=reference.isoformat(),
                    reference_label=reference_label,
                    release_title=title,
                    release_date=release_date,
                    event_time_utc=event_time_utc,
                    event_time_precision=precision,
                    press_number=press_number,
                    evas_codes=evas_codes,
                    source_url=source_url,
                    raw={
                        "cells": list(cells),
                        "urls": list(urls),
                    },
                )
            )
    return entries


def schedule_entry_to_records(
    entry: DestatisScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: DestatisIndicatorSpec | None = None,
) -> tuple[DestatisCalendarRawRecord, DestatisCalendarEventRecord]:
    """Project one release-table row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in Destatis INDICATOR_REGISTRY"
        )

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    schedule_payload: dict[str, Any] = {
        "kind": "destatis_schedule",
        "series_id": entry.series_id,
        "reference_label": entry.reference_label,
        "reference_date": entry.reference_date,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "event_time_utc": entry.event_time_utc,
        "event_time_precision": entry.event_time_precision,
        "press_number": entry.press_number,
        "evas_codes": list(entry.evas_codes),
        "source_url": entry.source_url,
        "raw": entry.raw,
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

    raw_record = DestatisCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = DestatisCalendarEventRecord(
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
        source="Destatis",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_DESTATIS_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.7,en;q=0.6",
}


def fetch_release_table_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Destatis weekly release table."""
    http = session or requests.Session()
    response = http.get(
        DESTATIS_RELEASE_TABLE_URL,
        headers=_DESTATIS_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window around the weekly release table."""
    base = today or datetime.now(ZoneInfo(DESTATIS_RELEASE_TZ)).date()
    return base - timedelta(days=14), base + timedelta(days=45)


def midnight_utc(day: date) -> str:
    """Expose date-precision timestamp construction for tests."""
    return datetime.combine(day, time.min).replace(tzinfo=timezone.utc).isoformat()
