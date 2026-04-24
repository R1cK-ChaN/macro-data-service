"""Italy ISTAT press-release calendar parser."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import BytesIO
import json
import re
import unicodedata
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

from .indicators import ISTATIndicatorSpec, INDICATOR_REGISTRY, press_release_url
from .parser import (
    PROVIDER,
    ISTATCalendarEventRecord,
    ISTATCalendarRawRecord,
    _normalise,
)

ISTAT_PRESS_CALENDAR_PAGE = (
    "https://www.istat.it/en/information-and-services-for-users/"
    "journalists/press-releases/press-calendar/"
)
ISTAT_RELEASE_TZ = "Europe/Rome"


class ISTATScheduleParseError(ValueError):
    """Raised when ISTAT's press-calendar shape drifts."""


@dataclass(frozen=True)
class ISTATScheduleEntry:
    """One matched ISTAT calendar row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    release_time_local: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    raw: dict[str, Any]


_MONTH_NAMES: dict[str, int] = {
    name.lower(): idx
    for idx, name in enumerate(calendar.month_name)
    if name
}
_MONTH_HEADER_RE = re.compile(
    r"^\s*("
    + "|".join(calendar.month_name[1:])
    + r")\s+(20\d{2})\s*$",
    re.I | re.M,
)
_TIME_RE = re.compile(
    r"\b\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?)\b",
    re.I,
)
_CPI_ROW_RE = re.compile(
    r"Consumer\s+prices\s+P\s+"
    r"(?P<ref_month>[A-Za-z]+)\s+(?P<ref_year>20\d{2})\s+"
    r"(?P<weekday>[A-Za-z]+)\s+(?P<day>\d{1,2})\s+"
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))",
    re.I | re.S,
)
_GDP_ROW_RE = re.compile(
    r"Preliminary\s+estimate\s+of\s+Gdp\s+"
    r"(?P<quarter>I{1,3}|IV)\s+Quarter\s+(?P<ref_year>20\d{2})\s+"
    r"(?P<weekday>[A-Za-z]+)\s+(?P<day>\d{1,2})\s+"
    r"(?P<time>\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))",
    re.I | re.S,
)
_ROMAN_QUARTER: dict[str, int] = {"I": 1, "II": 2, "III": 3, "IV": 4}


def _clean_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return re.sub(r"\s+", " ", text).strip()


def _month_number(raw: str) -> int:
    month = _MONTH_NAMES.get(raw.lower())
    if month is None:
        raise ISTATScheduleParseError(f"unknown month name: {raw!r}")
    return month


def _quarter_end(year: int, quarter: int) -> date:
    month = quarter * 3
    return date(year, month, calendar.monthrange(year, month)[1])


def _format_time(raw: str) -> str:
    compact = raw.lower().replace(".", "").strip()
    match = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*([ap])m$", compact)
    if not match:
        raise ISTATScheduleParseError(f"unparseable ISTAT time: {raw!r}")
    hour, minute, meridiem = match.groups()
    return f"{int(hour)}:{minute or '00'} {'AM' if meridiem == 'a' else 'PM'}"


def _event_time(release_date: date, time_raw: str) -> tuple[str, str]:
    time_text = _format_time(time_raw)
    scheduled = parse_scheduled_release_time(
        release_date,
        time_text,
        default_tz=ISTAT_RELEASE_TZ,
    )
    return scheduled.utc.isoformat(), time_text


def _calendar_chunks(text: str) -> list[tuple[str, int, str]]:
    normalized = unicodedata.normalize("NFKC", text)
    headers = list(_MONTH_HEADER_RE.finditer(normalized))
    chunks: list[tuple[str, int, str]] = []
    for index, header in enumerate(headers):
        start = header.end()
        end = headers[index + 1].start() if index + 1 < len(headers) else len(normalized)
        month = _month_number(header.group(1))
        year = int(header.group(2))
        chunks.append((header.group(1), year, _clean_text(normalized[start:end])))
        if month != _month_number(header.group(1)):
            raise ISTATScheduleParseError("inconsistent ISTAT month header")
    return chunks


def _release_month_from_header(header_month: str) -> int:
    return _month_number(header_month)


def _entry_from_cpi_match(
    match: re.Match[str],
    *,
    header_month: str,
    header_year: int,
) -> ISTATScheduleEntry:
    spec = INDICATOR_REGISTRY["ISTAT_CPI_PROVISIONAL_YOY"]
    release_date = date(
        header_year,
        _release_month_from_header(header_month),
        int(match.group("day")),
    )
    reference = date(
        int(match.group("ref_year")),
        _month_number(match.group("ref_month")),
        1,
    )
    event_time_utc, release_time_local = _event_time(
        release_date, match.group("time")
    )
    label = f"{match.group('ref_month')} {match.group('ref_year')} provisional"
    return ISTATScheduleEntry(
        series_id=spec.series_id,
        reference_date=reference.isoformat(),
        reference_label=label,
        release_title="Consumer prices P",
        release_date=release_date,
        release_time_local=release_time_local,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        source_url=press_release_url(spec, reference),
        raw={
            "calendar": "press_release_calendar",
            "release_title": "Consumer prices P",
            "reference_label": label,
            "release_date": release_date.isoformat(),
            "release_time": release_time_local,
            "raw_match": _clean_text(match.group(0)),
        },
    )


def _entry_from_gdp_match(
    match: re.Match[str],
    *,
    header_month: str,
    header_year: int,
) -> ISTATScheduleEntry:
    spec = INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"]
    release_date = date(
        header_year,
        _release_month_from_header(header_month),
        int(match.group("day")),
    )
    quarter = _ROMAN_QUARTER[match.group("quarter").upper()]
    reference = _quarter_end(int(match.group("ref_year")), quarter)
    event_time_utc, release_time_local = _event_time(
        release_date, match.group("time")
    )
    label = f"Q{quarter} {match.group('ref_year')} preliminary"
    return ISTATScheduleEntry(
        series_id=spec.series_id,
        reference_date=reference.isoformat(),
        reference_label=label,
        release_title="Preliminary estimate of GDP",
        release_date=release_date,
        release_time_local=release_time_local,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        source_url=press_release_url(spec, reference),
        raw={
            "calendar": "press_release_calendar",
            "release_title": "Preliminary estimate of GDP",
            "reference_label": label,
            "release_date": release_date.isoformat(),
            "release_time": release_time_local,
            "raw_match": _clean_text(match.group(0)),
        },
    )


def parse_calendar_text(
    text: str,
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[ISTATScheduleEntry]:
    """Extract CPI/GDP releases from ISTAT's annual press-calendar text."""
    allowed = set(INDICATOR_REGISTRY) if series_ids is None else series_ids
    entries: list[ISTATScheduleEntry] = []
    chunks = _calendar_chunks(text)
    if not chunks:
        raise ISTATScheduleParseError("ISTAT calendar month sections not found")
    for header_month, header_year, chunk in chunks:
        for match in _CPI_ROW_RE.finditer(chunk):
            if "ISTAT_CPI_PROVISIONAL_YOY" not in allowed:
                continue
            try:
                entries.append(
                    _entry_from_cpi_match(
                        match,
                        header_month=header_month,
                        header_year=header_year,
                    )
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(f"CPI row {match.group(0)!r}: {exc}")
                    continue
                raise
        for match in _GDP_ROW_RE.finditer(chunk):
            if "ISTAT_GDP_PRELIMINARY_QOQ" not in allowed:
                continue
            try:
                entries.append(
                    _entry_from_gdp_match(
                        match,
                        header_month=header_month,
                        header_year=header_year,
                    )
                )
            except Exception as exc:
                if row_issues is not None:
                    row_issues.append(f"GDP row {match.group(0)!r}: {exc}")
                    continue
                raise
    entries.sort(key=lambda entry: (entry.release_date, entry.series_id))
    return entries


def extract_calendar_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from ISTAT's annual calendar PDF using packaged pypdf."""
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise ISTATScheduleParseError(
            "pypdf is required to extract ISTAT calendar PDFs"
        ) from exc
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def schedule_entry_to_records(
    entry: ISTATScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: ISTATIndicatorSpec | None = None,
) -> tuple[ISTATCalendarRawRecord, ISTATCalendarEventRecord]:
    """Project one ISTAT calendar row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in ISTAT INDICATOR_REGISTRY")
    canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonical,
        entry.reference_date,
    )
    payload = {
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "release_time_local": entry.release_time_local,
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
    content_hash = __import__("hashlib").sha256(
        payload_json.encode("utf-8")
    ).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=ZoneInfo("UTC")
    ).isoformat()
    raw_record = ISTATCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ISTATCalendarEventRecord(
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
        source="ISTAT",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_ISTAT_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
}


def discover_calendar_pdf_url(page_html: str, *, year: int) -> str:
    """Find the annual ISTAT calendar PDF URL from the press-calendar page."""
    soup = BeautifulSoup(page_html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        text = _normalise(anchor.get_text(" ", strip=True) + " " + href)
        if str(year) in text and "calendar" in text and href.lower().endswith(".pdf"):
            return urljoin(ISTAT_PRESS_CALENDAR_PAGE, href)
    raise ISTATScheduleParseError(f"ISTAT calendar PDF link not found for {year}")


def fetch_calendar_pdf(
    *,
    session: requests.Session | None = None,
    year: int | None = None,
    timeout: float = 30.0,
) -> bytes:
    """GET ISTAT's annual press-release calendar PDF."""
    target_year = year or datetime.now(ZoneInfo(ISTAT_RELEASE_TZ)).year
    http = session or requests.Session()
    page = http.get(
        ISTAT_PRESS_CALENDAR_PAGE,
        headers=_ISTAT_BROWSER_HEADERS,
        timeout=timeout,
    )
    page.raise_for_status()
    pdf_url = discover_calendar_pdf_url(page.text, year=target_year)
    pdf = http.get(pdf_url, headers=_ISTAT_BROWSER_HEADERS, timeout=timeout)
    pdf.raise_for_status()
    return pdf.content


def fetch_press_release_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET one ISTAT press-release page."""
    http = session or requests.Session()
    response = http.get(url, headers=_ISTAT_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for ISTAT's current-year calendar."""
    base = today or datetime.now(ZoneInfo(ISTAT_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year, 12, 31)
