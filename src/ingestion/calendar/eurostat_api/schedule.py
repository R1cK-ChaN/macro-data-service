"""Eurostat release-calendar JSON parser."""

from __future__ import annotations

import calendar
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import EurostatIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    EurostatCalendarEventRecord,
    EurostatCalendarRawRecord,
)

logger = logging.getLogger(__name__)

EUROSTAT_RELEASE_CALENDAR_URL = "https://ec.europa.eu/eurostat/news/release-calendar"
EUROSTAT_EVENTS_JSON_URL = "https://ec.europa.eu/eurostat/o/calendars/eventsJson"
EUROSTAT_RELEASE_TZ = "Europe/Luxembourg"


class EurostatScheduleParseError(ValueError):
    """Raised when Eurostat's release-calendar JSON shape drifts."""


@dataclass(frozen=True)
class EurostatScheduleEntry:
    """One matched Eurostat release-calendar row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    event_time_utc: str
    record_id: str
    period: str
    dataset_codes: tuple[str, ...]
    source_url: str
    raw: dict[str, Any]


_MONTH_NAMES: dict[str, int] = {
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
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}
_REFERENCE_MONTH_RE = re.compile(r"^\s*([A-Za-z]+)\.?\s+(\d{4})\s*$")
_ISO_MONTH_RE = re.compile(r"^\s*(\d{4})-(\d{2})\s*$")
_QUARTER_SLASH_RE = re.compile(r"^\s*Q([1-4])\s*/\s*(\d{4})\s*$", re.I)
_QUARTER_YEAR_RE = re.compile(r"^\s*(\d{4})[- ]?Q([1-4])\s*$", re.I)


def _dataset_codes(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, list):
        return tuple(str(v).strip() for v in value if str(v).strip())
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _quarter_end(year: int, quarter: int) -> date:
    month = (quarter - 1) * 3 + 3
    day = calendar.monthrange(year, month)[1]
    return date(year, month, day)


def _reference_date(period: str, cadence: str) -> date:
    period = " ".join((period or "").split())
    if cadence == "quarterly":
        match = _QUARTER_SLASH_RE.match(period)
        if match:
            quarter, year = match.groups()
            return _quarter_end(int(year), int(quarter))
        match = _QUARTER_YEAR_RE.match(period)
        if match:
            year, quarter = match.groups()
            return _quarter_end(int(year), int(quarter))

    match = _ISO_MONTH_RE.match(period)
    if match:
        year, month = match.groups()
        return date(int(year), int(month), 1)
    match = _REFERENCE_MONTH_RE.match(period)
    if match:
        month_raw, year_raw = match.groups()
        month = _MONTH_NAMES.get(month_raw.strip(".").lower())
        if month is None:
            raise EurostatScheduleParseError(
                f"unknown month name: {month_raw!r}"
            )
        return date(int(year_raw), month, 1)
    raise EurostatScheduleParseError(f"unparseable reference period: {period!r}")


def _normalise_title(text: str) -> str:
    return " ".join((text or "").lower().split())


def _matching_specs(title: str, dataset_codes: tuple[str, ...]) -> list[EurostatIndicatorSpec]:
    lowered = _normalise_title(title)
    matches: list[EurostatIndicatorSpec] = []
    for spec in INDICATOR_REGISTRY.values():
        title_match = any(
            fragment in lowered for fragment in spec.schedule_title_fragments
        )
        if title_match:
            matches.append(spec)
    return matches


def _parse_event_time(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        raise EurostatScheduleParseError("missing start timestamp")
    normalised = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalised)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(EUROSTAT_RELEASE_TZ))
    return parsed.astimezone(timezone.utc).isoformat()


def parse_release_calendar_json(
    payload: str | bytes | list[dict[str, Any]],
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[EurostatScheduleEntry]:
    """Extract whitelisted Eurostat release events from JSON payload."""
    if isinstance(payload, bytes):
        rows = json.loads(payload.decode("utf-8"))
    elif isinstance(payload, str):
        rows = json.loads(payload)
    else:
        rows = payload
    if not isinstance(rows, list):
        raise EurostatScheduleParseError("calendar JSON root is not a list")

    entries: list[EurostatScheduleEntry] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        title = str(raw.get("title") or "")
        period = str(raw.get("period") or "")
        codes = _dataset_codes(raw.get("datasetCodes"))
        specs = _matching_specs(title, codes)
        if series_ids is not None:
            specs = [spec for spec in specs if spec.series_id in series_ids]
        if not specs:
            continue

        for spec in specs:
            try:
                ref = _reference_date(period, spec.reference_cadence)
                event_time_utc = _parse_event_time(raw.get("start"))
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(f"{title}: {type(exc).__name__}: {exc}")
                    continue
                raise
            entries.append(
                EurostatScheduleEntry(
                    series_id=spec.series_id,
                    reference_date=ref.isoformat(),
                    reference_label=period,
                    release_title=title,
                    event_time_utc=event_time_utc,
                    record_id=str(raw.get("recordid") or ""),
                    period=period,
                    dataset_codes=codes,
                    source_url=EUROSTAT_RELEASE_CALENDAR_URL,
                    raw=dict(raw),
                )
            )
    return entries


def schedule_entry_to_records(
    entry: EurostatScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: EurostatIndicatorSpec | None = None,
) -> tuple[EurostatCalendarRawRecord, EurostatCalendarEventRecord]:
    """Project one Eurostat release-calendar entry to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {entry.series_id!r} not in Eurostat INDICATOR_REGISTRY"
        )

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    schedule_payload: dict[str, Any] = {
        "kind": "eurostat_schedule",
        "series_id": entry.series_id,
        "reference_label": entry.reference_label,
        "reference_date": entry.reference_date,
        "release_title": entry.release_title,
        "event_time_utc": entry.event_time_utc,
        "record_id": entry.record_id,
        "period": entry.period,
        "dataset_codes": list(entry.dataset_codes),
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
    raw_record = EurostatCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EurostatCalendarEventRecord(
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
        source="Eurostat",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_EUROSTAT_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _calendar_datetime_text(day: date) -> str:
    zone = ZoneInfo(EUROSTAT_RELEASE_TZ)
    return datetime.combine(day, time.min).replace(tzinfo=zone).isoformat()


def _calendar_end_datetime_text(day: date) -> str:
    zone = ZoneInfo(EUROSTAT_RELEASE_TZ)
    return datetime.combine(day, time.max).replace(tzinfo=zone).isoformat()


def fetch_release_calendar_json(
    start_date: date,
    end_date: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """Fetch Eurostat's Euro-indicators calendar JSON for a date window."""
    http = session or requests.Session()
    params = {
        "theme": "2",
        "category": "2",
        "keywords": "",
        "isEuroindicator": "",
        "authorInclude": "",
        "authorExclude": "",
        "start": _calendar_datetime_text(start_date),
        "end": _calendar_end_datetime_text(end_date),
    }
    response = http.get(
        EUROSTAT_EVENTS_JSON_URL,
        params=params,
        headers=_EUROSTAT_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window: recent past through roughly one year ahead."""
    base = today or datetime.now(ZoneInfo(EUROSTAT_RELEASE_TZ)).date()
    end_year = base.year + (1 if base.month >= 10 else 0)
    return base - timedelta(days=14), date(end_year, 12, 31)


def month_end(reference: date) -> date:
    """Expose month-end calculation for tests and validation scripts."""
    return date(
        reference.year,
        reference.month,
        calendar.monthrange(reference.year, reference.month)[1],
    )
