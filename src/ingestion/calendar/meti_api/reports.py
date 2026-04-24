"""Parse METI value-side reports for IIP and Retail Sales."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import parse_scheduled_release_time

from .indicators import INDICATOR_REGISTRY
from .parser import (
    METI_IIP_RELEASE_TIME_LOCAL,
    METI_RELEASE_TZ,
    METI_RETAIL_PAGE_URL,
    PROVIDER,
    MetiCalendarEventRecord,
    MetiCalendarRawRecord,
    build_iip_report_url,
)
from .scraper import (
    METI_BROWSER_HEADERS,
    _MONTHS,
    _MONTH_NAME_BY_NUM,
    _parse_time,
    _record_id,
)

logger = logging.getLogger(__name__)


class MetiReportParseError(Exception):
    """Raised when a METI value report has an unexpected shape."""


@dataclass(frozen=True)
class IipReportValue:
    """Parsed IIP preliminary headline."""

    reference_date: date
    reference_label: str
    release_date: date
    release_time_local: str
    production_index_sa: str
    production_mom_percent: str
    production_original_index: str
    production_yoy_percent: str
    source_url: str


@dataclass(frozen=True)
class RetailPageSummary:
    """Current Survey of Commerce landing-page metadata."""

    reference_date: date
    reference_label: str
    release_date: date
    outline_pdf_url: str


@dataclass(frozen=True)
class RetailReportValue:
    """Parsed Current Survey of Commerce retail-sales headline."""

    reference_date: date
    reference_label: str
    release_date: date
    retail_sales_billion_yen: str
    retail_sales_yoy_percent: str
    retail_sales_mom_sa_percent: str
    source_url: str


_IIP_HEADER_RE = re.compile(
    r"Preliminary\s+Report\s+for\s+"
    r"(?P<ref_month>[A-Za-z]{3,9})\.?\s+(?P<ref_year>\d{4})\s*"
    r"\(released\s+at\s+(?P<time>\d{1,2}:\d{2})\s*,\s*"
    r"(?P<rel_month>[A-Za-z]{3,9})\.?\s+"
    r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*,\s*"
    r"(?P<rel_year>\d{4})\)",
    re.IGNORECASE,
)
_RETAIL_PAGE_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})-(?P<month>[A-Za-z]{3,9})-(?P<year>\d{4})\b"
)
_RETAIL_REFERENCE_RE = re.compile(
    r"\b(?P<month>[A-Za-z]{3,9})\s*,\s*(?P<year>\d{4})\b"
)
_RETAIL_TABLE_RE = re.compile(
    r"(?P<com_value>\d{1,3}(?:,\d{3})*)\s+"
    r"(?P<com_yoy>-?\d+(?:\.\d+)?)\s+"
    r"(?P<com_mom>-?\d+(?:\.\d+)?)\s+"
    r"(?P<wh_value>\d{1,3}(?:,\d{3})*)\s+"
    r"(?P<wh_yoy>-?\d+(?:\.\d+)?)\s+"
    r"(?P<wh_mom>-?\d+(?:\.\d+)?)\s+"
    r"(?P<retail_value>\d{1,3}(?:,\d{3})*)\s+"
    r"(?P<retail_yoy>-?\d+(?:\.\d+)?)\s+"
    r"(?P<retail_mom>-?\d+(?:\.\d+)?)"
)

_FULLWIDTH_TRANSLATION = str.maketrans(
    "０１２３４５６７８９．，－",
    "0123456789.,-",
)


def _month_number(token: str) -> int:
    month = _MONTHS.get(token.strip().lower().rstrip("."))
    if month is None:
        raise MetiReportParseError(f"unknown METI month token: {token!r}")
    return month


def _reference_label(reference: date) -> str:
    return f"{_MONTH_NAME_BY_NUM[reference.month]} {reference.year}"


def _clean_number(value: str) -> str:
    cleaned = (
        value.strip()
        .translate(_FULLWIDTH_TRANSLATION)
        .replace(",", "")
        .replace(" ", "")
    )
    cleaned = cleaned.replace("\u25b2", "-").replace("\u25b3", "-")
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]
    return cleaned


def _normalize_pdf_text(text: str) -> str:
    normalized = text.translate(_FULLWIDTH_TRANSLATION)
    normalized = normalized.replace("\u25b2", "-").replace("\u25b3", "-")
    normalized = re.sub(r"-\s+", "-", normalized)
    return re.sub(r"\s+", " ", normalized)


def parse_iip_report_html(
    html: str,
    *,
    source_url: str,
) -> IipReportValue:
    """Extract the Production MoM preliminary headline from an IIP page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    header = _IIP_HEADER_RE.search(text)
    if header is None:
        raise MetiReportParseError("METI IIP preliminary header missing")

    reference = date(
        int(header.group("ref_year")),
        _month_number(header.group("ref_month")),
        1,
    )
    release_date = date(
        int(header.group("rel_year")),
        _month_number(header.group("rel_month")),
        int(header.group("day")),
    )
    release_time = _parse_time(header.group("time"), default=METI_IIP_RELEASE_TIME_LOCAL)

    for row in soup.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
        if not cells:
            continue
        if cells[0].strip().lower() != "production":
            continue
        values = [_clean_number(cell) for cell in cells[1:]]
        values = [v for v in values if v]
        if len(values) < 4:
            raise MetiReportParseError("METI IIP Production row has too few cells")
        return IipReportValue(
            reference_date=reference,
            reference_label=_reference_label(reference),
            release_date=release_date,
            release_time_local=release_time,
            production_index_sa=values[0],
            production_mom_percent=values[1],
            production_original_index=values[2],
            production_yoy_percent=values[3],
            source_url=source_url,
        )

    raise MetiReportParseError("METI IIP Production row missing")


def parse_retail_current_page_html(html: str) -> RetailPageSummary:
    """Parse current retail release metadata and outline PDF link."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    reference_match = _RETAIL_REFERENCE_RE.search(text)
    if reference_match is None:
        raise MetiReportParseError("METI retail reference month missing")
    reference = date(
        int(reference_match.group("year")),
        _month_number(reference_match.group("month")),
        1,
    )

    release_match = _RETAIL_PAGE_DATE_RE.search(text)
    if release_match is None:
        raise MetiReportParseError("METI retail release date missing")
    release_date = date(
        int(release_match.group("year")),
        _month_number(release_match.group("month")),
        int(release_match.group("day")),
    )

    outline_url = ""
    for link in soup.find_all("a"):
        link_text = link.get_text(" ", strip=True).lower()
        href = link.get("href")
        if href and "outline" in link_text:
            outline_url = urljoin(METI_RETAIL_PAGE_URL, href)
            break
    if not outline_url:
        raise MetiReportParseError("METI retail outline PDF link missing")

    return RetailPageSummary(
        reference_date=reference,
        reference_label=_reference_label(reference),
        release_date=release_date,
        outline_pdf_url=outline_url,
    )


def parse_retail_outline_text(
    text: str,
    *,
    page: RetailPageSummary,
) -> RetailReportValue:
    """Extract Retail Sales YoY from the Current Survey outline text."""
    normalized = _normalize_pdf_text(text)
    match = _RETAIL_TABLE_RE.search(normalized)
    if match is None:
        raise MetiReportParseError("METI retail summary table row missing")
    return RetailReportValue(
        reference_date=page.reference_date,
        reference_label=page.reference_label,
        release_date=page.release_date,
        retail_sales_billion_yen=_clean_number(match.group("retail_value")),
        retail_sales_yoy_percent=_clean_number(match.group("retail_yoy")),
        retail_sales_mom_sa_percent=_clean_number(match.group("retail_mom")),
        source_url=page.outline_pdf_url,
    )


def _value_record(
    *,
    indicator: str,
    reference_date: date,
    reference_label: str,
    release_date: date,
    release_time_local: str,
    event_time_utc: str,
    actual: str,
    payload: dict[str, str],
    source_url: str,
    snapshot_epoch_ms: int,
) -> tuple[MetiCalendarRawRecord, MetiCalendarEventRecord]:
    spec = INDICATOR_REGISTRY[indicator]
    provider_event_id = _record_id(indicator, reference_date)
    if not event_time_utc:
        parsed_time = parse_scheduled_release_time(
            release_date,
            release_time_local,
            default_tz=METI_RELEASE_TZ,
        )
        event_time_utc = parsed_time.utc.isoformat()

    merged_payload = dict(payload)
    merged_payload.update({
        "provider": PROVIDER,
        "provider_event_id": provider_event_id,
        "indicator": indicator,
        "title": spec.title,
        "reference_date": reference_date.isoformat(),
        "event_time_utc": event_time_utc,
        "actual": actual,
        "source_url": source_url,
    })
    payload_json = json.dumps(merged_payload, sort_keys=True, ensure_ascii=False)
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
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="",
        unit=spec.unit,
        actual=actual,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="METI",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def iip_value_to_records(
    value: IipReportValue,
    *,
    snapshot_epoch_ms: int,
    event_time_utc: str = "",
) -> tuple[MetiCalendarRawRecord, MetiCalendarEventRecord]:
    """Convert an IIP value scrape into raw + event records."""
    return _value_record(
        indicator="INDUSTRIAL_PRODUCTION",
        reference_date=value.reference_date,
        reference_label=value.reference_label,
        release_date=value.release_date,
        release_time_local=value.release_time_local,
        event_time_utc=event_time_utc,
        actual=value.production_mom_percent,
        payload={
            "kind": "iip_preliminary_value",
            "production_index_sa": value.production_index_sa,
            "production_mom_percent": value.production_mom_percent,
            "production_original_index": value.production_original_index,
            "production_yoy_percent": value.production_yoy_percent,
        },
        source_url=value.source_url,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )


def retail_value_to_records(
    value: RetailReportValue,
    *,
    snapshot_epoch_ms: int,
    event_time_utc: str = "",
) -> tuple[MetiCalendarRawRecord, MetiCalendarEventRecord]:
    """Convert a retail-sales value scrape into raw + event records."""
    return _value_record(
        indicator="RETAIL_SALES",
        reference_date=value.reference_date,
        reference_label=value.reference_label,
        release_date=value.release_date,
        release_time_local="08:50",
        event_time_utc=event_time_utc,
        actual=value.retail_sales_yoy_percent,
        payload={
            "kind": "retail_sales_value",
            "retail_sales_billion_yen": value.retail_sales_billion_yen,
            "retail_sales_yoy_percent": value.retail_sales_yoy_percent,
            "retail_sales_mom_sa_percent": value.retail_sales_mom_sa_percent,
        },
        source_url=value.source_url,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )


def fetch_iip_report_html(
    reference: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    client = session or requests.Session()
    response = client.get(
        build_iip_report_url(reference),
        headers=METI_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_retail_current_page_html(
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


def fetch_retail_outline_pdf_bytes(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> bytes:
    client = session or requests.Session()
    response = client.get(url, headers=METI_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a METI outline PDF using packaged ``pypdf``."""
    try:
        from pypdf import PdfReader
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pypdf is required to extract METI retail outline PDFs"
        ) from exc
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(page.extract_text() or "" for page in reader.pages)
