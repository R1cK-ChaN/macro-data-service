"""Conference Board economic-indicator release-calendar scraper."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
from html import unescape
import json
import logging
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import (
    CONFERENCE_BOARD_CALENDAR_URL,
    INDICATOR_REGISTRY,
    ConferenceBoardIndicatorSpec,
)
from .parser import (
    PROVIDER,
    ConferenceBoardCalendarEventRecord,
    ConferenceBoardCalendarRawRecord,
)

logger = logging.getLogger(__name__)

CONFERENCE_BOARD_RELEASE_TZ = "America/New_York"


class ConferenceBoardScheduleParseError(ValueError):
    """Raised when the Conference Board calendar endpoint drifts."""


@dataclass(frozen=True)
class ConferenceBoardScheduleEntry:
    """One Conference Board calendar row, pre-projection."""

    series_id: str
    calendar_event_id: str
    raw_title: str
    reference_date: str
    reference_label: str
    release_date: str
    release_time_local: str
    event_time_utc: str
    source_url: str


_TIME_RE = re.compile(r"\b(?P<time>\d{1,2}:\d{2}\s*(?:AM|PM))\b", re.IGNORECASE)


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _clean_title(title: str) -> str:
    text = BeautifulSoup(unescape(title), "html.parser").get_text(" ", strip=True)
    return _normalize(text)


def _resolve_series(
    series_ids: set[str] | None,
) -> list[ConferenceBoardIndicatorSpec]:
    specs = list(INDICATOR_REGISTRY.values())
    if series_ids is None:
        return specs
    return [spec for spec in specs if spec.series_id in series_ids]


def _shift_months(ref: date, months: int) -> date:
    month_index = ref.year * 12 + ref.month - 1 - months
    year, month_zero = divmod(month_index, 12)
    return date(year, month_zero + 1, 1)


def _reference_label(ref: date) -> str:
    return f"{ref.strftime('%B')} {ref.year}"


def _release_time_label(title: str, local_dt: datetime) -> str:
    match = _TIME_RE.search(title)
    if match is not None:
        return _normalize(match.group("time")).upper()
    return local_dt.strftime("%I:%M %p").lstrip("0")


def _calendar_payload(data: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(data, dict):
        return data
    text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConferenceBoardScheduleParseError(
            f"calendar JSON decode failed: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ConferenceBoardScheduleParseError("calendar payload is not an object")
    return payload


def parse_calendar_events_json(
    data: str | bytes | dict[str, Any],
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[ConferenceBoardScheduleEntry]:
    """Extract whitelisted US rows from the Conference Board calendar JSON."""
    payload = _calendar_payload(data)
    rows = payload.get("result")
    if not isinstance(rows, list):
        raise ConferenceBoardScheduleParseError("calendar result list not found")

    specs = _resolve_series(series_ids)
    entries: list[ConferenceBoardScheduleEntry] = []
    eastern = ZoneInfo(CONFERENCE_BOARD_RELEASE_TZ)
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_title = _clean_title(str(row.get("title") or ""))
        title_lower = raw_title.lower()
        if not title_lower.startswith("us:"):
            continue
        for spec in specs:
            if spec.schedule_title_fragment.lower() not in title_lower:
                continue
            try:
                start_ms = int(str(row["start"]))
                utc_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
                local_dt = utc_dt.astimezone(eastern)
                release_date = local_dt.date()
                reference = _shift_months(release_date, spec.reference_month_lag)
                raw_url = str(row.get("url") or "")
                source_url = (
                    urljoin("https://www.conference-board.org/", raw_url)
                    if raw_url
                    else spec.source_url
                )
            except Exception as exc:
                if row_issues is not None:
                    event_id = row.get("id", "")
                    row_issues.append(
                        f"{event_id}: {type(exc).__name__}: {exc}"
                    )
                    continue
                raise
            entries.append(
                ConferenceBoardScheduleEntry(
                    series_id=spec.series_id,
                    calendar_event_id=str(row.get("id") or ""),
                    raw_title=raw_title,
                    reference_date=reference.isoformat(),
                    reference_label=_reference_label(reference),
                    release_date=release_date.isoformat(),
                    release_time_local=_release_time_label(raw_title, local_dt),
                    event_time_utc=utc_dt.isoformat(),
                    source_url=source_url,
                )
            )
    return entries


def schedule_entry_to_records(
    entry: ConferenceBoardScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: ConferenceBoardIndicatorSpec | None = None,
) -> tuple[ConferenceBoardCalendarRawRecord, ConferenceBoardCalendarEventRecord]:
    """Project one Conference Board schedule entry to records."""
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
        "kind": "conference_board_schedule",
        "series_id": entry.series_id,
        "calendar_event_id": entry.calendar_event_id,
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
    raw_record = ConferenceBoardCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ConferenceBoardCalendarEventRecord(
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
        source="The Conference Board",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_CONFERENCE_BOARD_HTTP_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,*/*;q=0.8",
}


def fetch_calendar_json(
    *,
    from_epoch_ms: int,
    to_epoch_ms: int,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the public Conference Board calendar JSON endpoint."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            CONFERENCE_BOARD_CALENDAR_URL,
            params={"from": from_epoch_ms, "to": to_epoch_ms},
            headers=_CONFERENCE_BOARD_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_indicator_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET a public Conference Board current-release page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            url,
            headers=_CONFERENCE_BOARD_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
