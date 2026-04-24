"""Spain INE publication-calendar parser."""

from __future__ import annotations

import calendar
import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INEIndicatorSpec, INDICATOR_REGISTRY, press_release_url
from .parser import (
    PROVIDER,
    INECalendarEventRecord,
    INECalendarRawRecord,
    _normalise,
)

INE_CALENDAR_URL = "https://www.ine.es/dynt3/Calendario/es/calenHTML.htm"
INE_RELEASE_TZ = "Europe/Madrid"
INE_DEFAULT_RELEASE_TIME = "09:00"


class INEScheduleParseError(ValueError):
    """Raised when INE's publication calendar shape drifts."""


@dataclass(frozen=True)
class INEScheduleEntry:
    """One matched INE calendar row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    event_time_utc: str
    event_time_precision: str
    source_url: str
    raw: dict[str, Any]


_ROW_RE = re.compile(r"^\s*(\d{1,2})\s+([^\W\d_]+)\s+(.+?)\s*$", re.I)
_CPI_REFERENCE_RE = re.compile(
    r"^\s*avance\.?\s+([^\W\d_]+)\s+(\d{4})\s*$", re.I,
)
_GDP_REFERENCE_RE = re.compile(r"^\s*trimestre\s+([1-4])\s*/\s*(\d{4})\s*$", re.I)
_MONTHS: dict[str, int] = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def _month_number(raw: str) -> int:
    key = _normalise(raw)
    month = _MONTHS.get(key)
    if month is None:
        raise INEScheduleParseError(f"unknown Spanish month name: {raw!r}")
    return month


def _quarter_end(year: int, quarter: int) -> date:
    month = quarter * 3
    return date(year, month, calendar.monthrange(year, month)[1])


def _parse_release_date(day_raw: str, month_raw: str, year: int) -> date:
    return date(year, _month_number(month_raw), int(day_raw))


def _parse_cpi_reference(label: str) -> date:
    match = _CPI_REFERENCE_RE.match(_normalise(label))
    if not match:
        raise INEScheduleParseError(f"unparseable CPI reference: {label!r}")
    month_raw, year_raw = match.groups()
    return date(int(year_raw), _month_number(month_raw), 1)


def _parse_gdp_reference(label: str) -> date:
    match = _GDP_REFERENCE_RE.match(_normalise(label))
    if not match:
        raise INEScheduleParseError(f"unparseable GDP reference: {label!r}")
    quarter_raw, year_raw = match.groups()
    return _quarter_end(int(year_raw), int(quarter_raw))


def _event_time(release_date: date) -> str:
    scheduled = parse_scheduled_release_time(
        release_date,
        INE_DEFAULT_RELEASE_TIME,
        default_tz=INE_RELEASE_TZ,
    )
    return scheduled.utc.isoformat()


def _calendar_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    out: list[str] = []
    for line in lines:
        cleaned = unicodedata.normalize("NFKC", line).strip()
        if cleaned:
            out.append(cleaned)
    return out


def _section_key(text: str) -> str | None:
    norm = _normalise(text)
    if norm == "indice de precios de consumo":
        return "cpi"
    if norm == "contabilidad nacional trimestral de espana":
        return "gdp"
    return None


def _parse_row_cells(
    cells: list[str],
    *,
    current_year: int | None,
) -> tuple[date, str, str] | None:
    if current_year is None or len(cells) < 3:
        return None
    day_raw, month_raw, reference_label = cells[0], cells[1], " ".join(cells[2:])
    if not re.match(r"^\d{1,2}$", day_raw.strip()):
        return None
    try:
        release_date = _parse_release_date(day_raw, month_raw, current_year)
    except Exception:
        return None
    raw_line = " ".join(cells)
    return release_date, reference_label, raw_line


def _section_rows_from_tables(html: str) -> dict[str, list[tuple[date, str, str]]]:
    soup = BeautifulSoup(html, "html.parser")
    sections: dict[str, list[tuple[date, str, str]]] = {
        "cpi": [],
        "gdp": [],
    }
    current: str | None = None
    current_year: int | None = None
    for node in soup.find_all(["h1", "h2", "h3", "h4", "caption", "p", "li", "tr"]):
        node_text = unicodedata.normalize(
            "NFKC", node.get_text(" ", strip=True),
        ).strip()
        if not node_text:
            continue
        norm = _normalise(node_text)
        year_match = re.search(r"calendario\s+(\d{4})", norm)
        if year_match:
            current_year = int(year_match.group(1))
        section = _section_key(node_text)
        if section is not None:
            current = section
            continue
        if current is None or getattr(node, "name", "") != "tr":
            continue
        cells = [
            unicodedata.normalize("NFKC", cell.get_text(" ", strip=True)).strip()
            for cell in node.find_all(["td", "th"])
        ]
        parsed = _parse_row_cells(cells, current_year=current_year)
        if parsed is not None:
            sections[current].append(parsed)
    return sections


def _section_rows_from_lines(html: str) -> dict[str, list[tuple[date, str, str]]]:
    lines = _calendar_lines(html)
    sections: dict[str, list[tuple[date, str, str]]] = {
        "cpi": [],
        "gdp": [],
    }
    current: str | None = None
    current_year: int | None = None
    for line in lines:
        norm = _normalise(line)
        year_match = re.search(r"calendario\s+(\d{4})", norm)
        if year_match:
            current_year = int(year_match.group(1))
        section = _section_key(line)
        if section is not None:
            current = section
            continue
        if current is None or norm.startswith("dia mes"):
            continue
        match = _ROW_RE.match(line)
        if not match or current_year is None:
            continue
        day_raw, month_raw, reference_label = match.groups()
        try:
            release_date = _parse_release_date(day_raw, month_raw, current_year)
        except Exception:
            continue
        sections[current].append((release_date, reference_label, line))
    return sections


def _section_rows(html: str) -> dict[str, list[tuple[date, str, str]]]:
    sections = _section_rows_from_tables(html)
    if any(sections.values()):
        return sections
    return _section_rows_from_lines(html)


def _gdp_advance_rows(
    rows: list[tuple[date, str, str]]
) -> list[tuple[date, str, str]]:
    by_reference: dict[str, tuple[date, str, str]] = {}
    for release_date, reference_label, raw_line in rows:
        try:
            reference = _parse_gdp_reference(reference_label).isoformat()
        except INEScheduleParseError:
            continue
        existing = by_reference.get(reference)
        if existing is None or release_date < existing[0]:
            by_reference[reference] = (release_date, reference_label, raw_line)
    return [by_reference[key] for key in sorted(by_reference)]


def parse_calendar_html(
    html: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[INEScheduleEntry]:
    """Extract CPI/GDP advance releases from INE's yearly calendar."""
    sections = _section_rows(html)
    if not any(sections.values()):
        raise INEScheduleParseError("INE calendar sections not found")

    entries: list[INEScheduleEntry] = []
    cpi_spec = INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"]
    gdp_spec = INDICATOR_REGISTRY["INE_GDP_ADVANCE_QOQ"]
    planned_rows: list[tuple[INEIndicatorSpec, date, str, str]] = []
    for release_date, reference_label, raw_line in sections["cpi"]:
        if "avance" in _normalise(reference_label):
            planned_rows.append((cpi_spec, release_date, reference_label, raw_line))
    for release_date, reference_label, raw_line in _gdp_advance_rows(sections["gdp"]):
        planned_rows.append((gdp_spec, release_date, reference_label, raw_line))

    for spec, release_date, reference_label, raw_line in planned_rows:
        if series_ids is not None and spec.series_id not in series_ids:
            continue
        try:
            reference = (
                _parse_cpi_reference(reference_label)
                if spec.release_kind == "cpi_advance"
                else _parse_gdp_reference(reference_label)
            )
            source_url = press_release_url(spec, reference)
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(
                    f"{spec.series_id}: {type(exc).__name__}: {exc}"
                )
                continue
            raise
        entries.append(
            INEScheduleEntry(
                series_id=spec.series_id,
                reference_date=reference.isoformat(),
                reference_label=reference_label,
                release_title=spec.schedule_title,
                release_date=release_date,
                event_time_utc=_event_time(release_date),
                event_time_precision="datetime",
                source_url=source_url,
                raw={"line": raw_line},
            )
        )
    entries.sort(key=lambda entry: (entry.release_date, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: INEScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: INEIndicatorSpec | None = None,
) -> tuple[INECalendarRawRecord, INECalendarEventRecord]:
    """Project one INE calendar row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in INE INDICATOR_REGISTRY")

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    schedule_payload: dict[str, Any] = {
        "kind": "ine_schedule",
        "series_id": entry.series_id,
        "reference_label": entry.reference_label,
        "reference_date": entry.reference_date,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "event_time_utc": entry.event_time_utc,
        "event_time_precision": entry.event_time_precision,
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

    raw_record = INECalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = INECalendarEventRecord(
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
        source="INE Spain",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_INE_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en-US;q=0.7,en;q=0.6",
}


def fetch_calendar_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the INE yearly publication calendar."""
    http = session or requests.Session()
    response = http.get(
        INE_CALENDAR_URL,
        headers=_INE_BROWSER_HEADERS,
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
    """GET one INE press-release page."""
    http = session or requests.Session()
    response = http.get(url, headers=_INE_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for INE's current-year calendar."""
    base = today or datetime.now(ZoneInfo(INE_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year, 12, 31)
