"""ISM Manufacturing PMI release-calendar scraper."""

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

from .indicators import (
    INDICATOR_REGISTRY,
    ISMIndicatorSpec,
    ISM_RELEASE_CALENDAR_URL,
    ISM_REPORTS_URL,
)
from .parser import (
    PROVIDER,
    ISMCalendarEventRecord,
    ISMCalendarRawRecord,
)

logger = logging.getLogger(__name__)

ISM_RELEASE_TZ = "America/New_York"
ISM_RELEASE_TIME_LOCAL = "10:00 AM"


class ISMScheduleParseError(ValueError):
    """Raised when the ISM release-calendar table shape drifts."""


@dataclass(frozen=True)
class ISMScheduleEntry:
    """One ISM release-calendar row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_month_label: str
    release_date: str
    release_time_local: str
    event_time_utc: str
    source_url: str


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
}
_MONTH_YEAR_RE = re.compile(
    r"^\s*(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})\s*$",
    re.IGNORECASE,
)
_DAY_RE = re.compile(r"\b(\d{1,2})\b")


def _previous_month(ref: date) -> date:
    if ref.month == 1:
        return date(ref.year - 1, 12, 1)
    return date(ref.year, ref.month - 1, 1)


def _parse_month_year(text: str) -> date:
    match = _MONTH_YEAR_RE.match(" ".join(text.split()))
    if match is None:
        raise ISMScheduleParseError(f"unparseable release month: {text!r}")
    month = _MONTH_NAMES[match.group(1).lower()]
    return date(year=int(match.group(2)), month=month, day=1)


def _parse_day(text: str) -> int:
    match = _DAY_RE.search(text)
    if match is None:
        raise ISMScheduleParseError(f"unparseable release day: {text!r}")
    return int(match.group(1))


def _header_index(headers: list[str], fragment: str) -> int | None:
    wanted = fragment.lower()
    for idx, header in enumerate(headers):
        if wanted in header.lower():
            return idx
    return None


def _resolve_series(
    series_ids: set[str] | None,
) -> list[ISMIndicatorSpec]:
    specs = list(INDICATOR_REGISTRY.values())
    if series_ids is None:
        return specs
    return [spec for spec in specs if spec.series_id in series_ids]


def parse_schedule_html(
    html: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[ISMScheduleEntry]:
    """Extract whitelisted release rows from the ISM release calendar."""
    soup = BeautifulSoup(html, "html.parser")
    specs = _resolve_series(series_ids)
    entries: list[ISMScheduleEntry] = []
    tables = soup.find_all("table")
    for table in tables:
        header_cells = table.find_all("th")
        headers = [
            " ".join(cell.get_text(" ", strip=True).split())
            for cell in header_cells[:8]
        ]
        if not headers or "month" not in headers[0].lower():
            continue
        column_by_series: dict[str, int] = {}
        for spec in specs:
            idx = _header_index(headers, spec.schedule_column_fragment)
            if idx is not None:
                column_by_series[spec.series_id] = idx
        if not column_by_series:
            continue

        body = table.find("tbody") or table
        for row in body.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) <= max(column_by_series.values(), default=0):
                continue
            release_month_text = cells[0].get_text(" ", strip=True)
            try:
                release_month = _parse_month_year(release_month_text)
            except ISMScheduleParseError:
                continue
            for spec in specs:
                column_idx = column_by_series.get(spec.series_id)
                if column_idx is None:
                    continue
                try:
                    day = _parse_day(cells[column_idx].get_text(" ", strip=True))
                    release = date(release_month.year, release_month.month, day)
                    reference = _previous_month(release_month)
                    scheduled = parse_scheduled_release_time(
                        release,
                        ISM_RELEASE_TIME_LOCAL,
                        default_tz=ISM_RELEASE_TZ,
                    )
                except Exception as exc:
                    if row_issues is not None:
                        row_issues.append(
                            f"{release_month_text}: {type(exc).__name__}: {exc}"
                        )
                        continue
                    raise
                entries.append(
                    ISMScheduleEntry(
                        series_id=spec.series_id,
                        reference_date=reference.isoformat(),
                        reference_label=(
                            f"{reference.strftime('%B')} {reference.year}"
                        ),
                        release_month_label=release_month_text,
                        release_date=release.isoformat(),
                        release_time_local=ISM_RELEASE_TIME_LOCAL,
                        event_time_utc=scheduled.utc.isoformat(),
                        source_url=ISM_RELEASE_CALENDAR_URL,
                    )
                )
    if not tables:
        raise ISMScheduleParseError("no release-calendar tables found")
    return entries


def schedule_entry_to_records(
    entry: ISMScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: ISMIndicatorSpec | None = None,
) -> tuple[ISMCalendarRawRecord, ISMCalendarEventRecord]:
    """Project one ISM schedule entry to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in INDICATOR_REGISTRY")

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    schedule_payload: dict[str, Any] = {
        "kind": "ism_schedule",
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_month_label": entry.release_month_label,
        "release_date": entry.release_date,
        "release_time_local": entry.release_time_local,
        "event_time_utc": entry.event_time_utc,
        "source_url": entry.source_url,
    }
    content_hash = hashlib.sha256(
        json.dumps(schedule_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    payload_json = json.dumps(
        schedule_payload, sort_keys=True, ensure_ascii=False,
    )
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    raw_record = ISMCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ISMCalendarEventRecord(
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
        source="Institute for Supply Management",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_ISM_HTTP_HEADERS: dict[str, str] = {
    # ISM's edge currently serves a captcha interstitial to both the
    # default python-requests UA and a browser-like UA. The curl UA gets
    # the public HTML that a plain `curl -sS <url>` returns.
    "User-Agent": "curl/8.5.0",
    "Accept": "*/*",
}


def fetch_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the public ISM release-date calendar page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            ISM_RELEASE_CALENDAR_URL,
            headers=_ISM_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def discover_current_report_url(
    html: str,
    *,
    series_id: str = "ISM_MANUFACTURING_PMI",
) -> str:
    """Extract the current Manufacturing PMI report URL from ISM's hub."""
    spec = INDICATOR_REGISTRY[series_id]
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.find_all("a", href=True):
        text = " ".join(link.get_text(" ", strip=True).split()).lower()
        href = str(link["href"])
        if "view report" in text and spec.report_path_fragment in href:
            return urljoin(ISM_REPORTS_URL, href)
    raise ISMScheduleParseError("current ISM Manufacturing PMI report URL not found")


def fetch_reports_landing_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the ISM PMI reports landing page."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            ISM_REPORTS_URL,
            headers=_ISM_HTTP_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


def fetch_report_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET one ISM report page by URL."""
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(url, headers=_ISM_HTTP_HEADERS, timeout=timeout)
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()
