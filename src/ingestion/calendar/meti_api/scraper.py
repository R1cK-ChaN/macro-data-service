"""Scrape METI schedule surfaces for issue #14 P5."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
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

from .indicators import INDICATOR_REGISTRY
from .parser import (
    METI_IIP_RELEASE_CALENDAR_URL,
    METI_IIP_RELEASE_TIME_LOCAL,
    METI_RELEASE_TZ,
    METI_RETAIL_PAGE_URL,
    PROVIDER,
    MetiCalendarEventRecord,
    MetiCalendarRawRecord,
    build_iip_report_url,
)

logger = logging.getLogger(__name__)


class MetiCalendarParseError(ValueError):
    """Raised when a METI schedule surface drifts."""


@dataclass(frozen=True)
class MetiScheduleEntry:
    """One scheduled METI economic release."""

    indicator: str
    reference_date: date
    reference_label: str
    release_date: date
    release_time_local: str
    source_url: str
    report_url: str
    payload: dict[str, Any]


METI_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_MONTHS: dict[str, int] = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

_MONTH_NAME_BY_NUM = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}

_MONTH_YEAR_RE = re.compile(
    r"\b(?P<month>[A-Za-z]{3,9})\.?\s*,?\s*(?P<year>\d{4})\b"
)
_LONG_DATE_RE = re.compile(
    r"\b(?P<month>[A-Za-z]{3,9})\.?\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*,?\s*"
    r"(?P<year>\d{4})\b"
)
_ISO_DATE_RE = re.compile(r"\b(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\b")
_TIME_RE = re.compile(
    r"\b(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"(?P<ampm>a\.?m\.?|p\.?m\.?)?\b",
    re.IGNORECASE,
)
_IIP_HEADER_RE = re.compile(
    r"Preliminary\s+Report\s+for\s+"
    r"(?P<month>[A-Za-z]{3,9})\.?\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_RETAIL_NEXT_RE = re.compile(
    r"The\s+Preliminary\s+Report\s+for\s+(?P<ref_month>[A-Za-z]{3,9})"
    r"\s+will\s+be\s+published\s+on\s+"
    r"(?P<rel_month>[A-Za-z]{3,9})\s+(?P<day>\d{1,2})"
    r"(?:st|nd|rd|th)?\s*,\s*(?P<year>\d{4})\s+at\s+"
    r"(?P<time>\d{1,2}:\d{2})\s*(?P<ampm>a\.?m\.?|p\.?m\.?)",
    re.IGNORECASE,
)


def _month_number(token: str) -> int:
    month = _MONTHS.get(token.strip().lower().rstrip("."))
    if month is None:
        raise MetiCalendarParseError(f"unknown METI month token: {token!r}")
    return month


def _reference_label(reference: date) -> str:
    return f"{_MONTH_NAME_BY_NUM[reference.month]} {reference.year}"


def _parse_month_year(text: str) -> date | None:
    match = _MONTH_YEAR_RE.search(text or "")
    if match is None:
        return None
    return date(
        int(match.group("year")),
        _month_number(match.group("month")),
        1,
    )


def _parse_long_date(text: str) -> date | None:
    match = _LONG_DATE_RE.search(text or "")
    if match is not None:
        return date(
            int(match.group("year")),
            _month_number(match.group("month")),
            int(match.group("day")),
        )
    iso = _ISO_DATE_RE.search(text or "")
    if iso is not None:
        return date(
            int(iso.group("year")),
            int(iso.group("month")),
            int(iso.group("day")),
        )
    return None


def _parse_time(text: str, *, default: str) -> str:
    match = _TIME_RE.search(text or "")
    if match is None:
        return default
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    ampm = (match.group("ampm") or "").lower().replace(".", "")
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}"


def _element_texts(element: ET.Element) -> list[str]:
    values: list[str] = []
    for node in element.iter():
        if node.text and node.text.strip():
            values.append(node.text.strip())
        values.extend(str(v).strip() for v in node.attrib.values() if str(v).strip())
    return values


def _direct_element_texts(element: ET.Element) -> list[str]:
    values: list[str] = []
    if element.text and element.text.strip():
        values.append(element.text.strip())
    values.extend(str(v).strip() for v in element.attrib.values() if str(v).strip())
    for child in list(element):
        if child.text and child.text.strip():
            values.append(child.text.strip())
        values.extend(str(v).strip() for v in child.attrib.values() if str(v).strip())
    return values


def _find_by_tag_fragment(element: ET.Element, fragments: tuple[str, ...]) -> str:
    for node in element.iter():
        tag = node.tag.rsplit("}", 1)[-1].lower()
        if any(fragment in tag for fragment in fragments):
            text = " ".join(_element_texts(node)).strip()
            if text:
                return text
    return ""


def _entry_from_iip_xml_element(element: ET.Element) -> MetiScheduleEntry | None:
    direct_blob = " ".join(_direct_element_texts(element))
    direct_lowered = direct_blob.lower()
    if "preliminary" not in direct_lowered and "prelim" not in direct_lowered:
        return None
    if all(
        token not in direct_lowered
        for token in ("industrial", "iip", "production")
    ):
        return None

    blob = " ".join(_element_texts(element))

    reference_text = _find_by_tag_fragment(
        element,
        ("reference", "target", "period", "month"),
    )
    reference = None
    header_match = _IIP_HEADER_RE.search(blob)
    if header_match is not None:
        reference = date(
            int(header_match.group("year")),
            _month_number(header_match.group("month")),
            1,
        )
    if reference is None:
        reference = _parse_month_year(reference_text) or _parse_month_year(blob)
    if reference is None:
        raise MetiCalendarParseError(
            f"METI IIP XML row lacks reference month: {blob[:160]!r}"
        )

    release_text = _find_by_tag_fragment(
        element,
        ("release", "publish", "publication", "date"),
    ) or blob
    release_date = _parse_long_date(release_text)
    if release_date is None:
        raise MetiCalendarParseError(
            f"METI IIP XML row lacks release date: {blob[:160]!r}"
        )
    release_time = _parse_time(release_text, default=METI_IIP_RELEASE_TIME_LOCAL)

    return MetiScheduleEntry(
        indicator="INDUSTRIAL_PRODUCTION",
        reference_date=reference,
        reference_label=_reference_label(reference),
        release_date=release_date,
        release_time_local=release_time,
        source_url=METI_IIP_RELEASE_CALENDAR_URL,
        report_url=build_iip_report_url(reference),
        payload={
            "kind": "iip_release_calendar",
            "raw_text": blob,
            "reference_date": reference.isoformat(),
            "release_date": release_date.isoformat(),
            "release_time_local": release_time,
        },
    )


def parse_iip_release_calendar_xml(xml_text: str) -> list[MetiScheduleEntry]:
    """Extract IIP preliminary release dates from METI's XML calendar."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise MetiCalendarParseError(f"METI IIP XML parse failed: {exc}") from exc

    entries: dict[tuple[date, date], MetiScheduleEntry] = {}
    for element in root.iter():
        entry = _entry_from_iip_xml_element(element)
        if entry is None:
            continue
        entries[(entry.reference_date, entry.release_date)] = entry
    return sorted(entries.values(), key=lambda e: (e.release_date, e.reference_date))


def parse_retail_schedule_html(html: str) -> MetiScheduleEntry:
    """Parse the next Current Survey of Commerce preliminary release."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    match = _RETAIL_NEXT_RE.search(text)
    if match is None:
        raise MetiCalendarParseError("METI retail next-release sentence missing")

    release_date = date(
        int(match.group("year")),
        _month_number(match.group("rel_month")),
        int(match.group("day")),
    )
    release_time = _parse_time(
        f"{match.group('time')} {match.group('ampm')}",
        default="08:50",
    )
    reference_month = _month_number(match.group("ref_month"))
    reference_year = release_date.year
    if reference_month > release_date.month:
        reference_year -= 1
    reference = date(reference_year, reference_month, 1)

    return MetiScheduleEntry(
        indicator="RETAIL_SALES",
        reference_date=reference,
        reference_label=_reference_label(reference),
        release_date=release_date,
        release_time_local=release_time,
        source_url=METI_RETAIL_PAGE_URL,
        report_url=METI_RETAIL_PAGE_URL,
        payload={
            "kind": "retail_next_release",
            "reference_date": reference.isoformat(),
            "release_date": release_date.isoformat(),
            "release_time_local": release_time,
            "source_url": METI_RETAIL_PAGE_URL,
        },
    )


def _record_id(indicator: str, reference: date) -> str:
    spec = INDICATOR_REGISTRY[indicator]
    canonical = canonicalize_indicator(spec.title)
    return synthesize_event_id(
        PROVIDER,
        spec.country_code,
        canonical,
        reference.isoformat(),
    )


def schedule_entry_to_records(
    entry: MetiScheduleEntry,
    *,
    snapshot_epoch_ms: int,
) -> tuple[MetiCalendarRawRecord, MetiCalendarEventRecord]:
    """Convert one parsed schedule entry into raw + event records."""
    spec = INDICATOR_REGISTRY[entry.indicator]
    provider_event_id = _record_id(entry.indicator, entry.reference_date)
    release_time = parse_scheduled_release_time(
        entry.release_date,
        entry.release_time_local,
        default_tz=METI_RELEASE_TZ,
    )
    payload = dict(entry.payload)
    payload.update({
        "provider": PROVIDER,
        "provider_event_id": provider_event_id,
        "indicator": entry.indicator,
        "title": spec.title,
        "event_time_utc": release_time.utc.isoformat(),
    })
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw = MetiCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event = MetiCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=release_time.utc.isoformat(),
        event_time_precision="datetime",
        reference_date=entry.reference_date.isoformat(),
        reference_label=entry.reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="METI",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def fetch_iip_release_calendar_xml(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    client = session or requests.Session()
    response = client.get(
        METI_IIP_RELEASE_CALENDAR_URL,
        headers=METI_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_retail_schedule_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    client = session or requests.Session()
    response = client.get(
        METI_RETAIL_PAGE_URL,
        headers=METI_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text
