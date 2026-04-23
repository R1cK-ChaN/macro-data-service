"""Project ISM Manufacturing PMI report values into calendar records."""

from __future__ import annotations

import calendar
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, ISMIndicatorSpec

PROVIDER = "ism"


@dataclass(frozen=True)
class ISMCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ISMCalendarEventRecord:
    """One row destined for ``cal_econ_event``."""

    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str | None
    currency: str
    unit: str
    actual: str | None
    previous: str | None
    revised: str | None
    forecast: str | None
    consensus_forecast: str | None
    ticker: str
    source: str
    source_url: str
    content_hash: str
    last_update_epoch_ms: int | None
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class ISMReportValue:
    """Parsed value from the current ISM Manufacturing PMI report."""

    series_id: str
    reference_date: str
    reference_label: str
    actual: str
    previous: str | None
    report_title: str
    source_url: str


class ISMReportParseError(ValueError):
    """Raised when a Manufacturing PMI report page drifts."""


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
_HEADLINE_RE = re.compile(
    r"\bmanufacturing\s+pmi\b.*?\bat\s+(?P<actual>\d+(?:\.\d+)?)\s*%?",
    re.IGNORECASE,
)
_TITLE_RE = re.compile(
    r"\b(?P<month>january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(?P<year>\d{4})\b"
    r".*?\bmanufacturing\s+pmi\b.*?\breport\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _reference_from_title(title: str) -> tuple[date, str]:
    match = _TITLE_RE.search(title)
    if match is None:
        raise ISMReportParseError(
            "Manufacturing PMI report title with month/year not found"
        )
    month_name = match.group("month").lower()
    year = int(match.group("year"))
    month = _MONTH_NAMES[month_name]
    ref = date(year=year, month=month, day=1)
    return ref, f"{month_name.capitalize()} {year}"


def _extract_actual(soup: BeautifulSoup) -> str:
    for heading in soup.find_all("h1"):
        text = _normalize(heading.get_text(" ", strip=True))
        match = _HEADLINE_RE.search(text)
        if match is not None:
            return match.group("actual")
    raise ISMReportParseError("Manufacturing PMI headline value not found")


def _extract_report_title(soup: BeautifulSoup) -> str:
    for heading in soup.find_all("h1"):
        text = _normalize(heading.get_text(" ", strip=True))
        if _TITLE_RE.search(text):
            return text
    raise ISMReportParseError("Manufacturing PMI report title not found")


def _extract_previous(soup: BeautifulSoup, *, actual: str) -> str | None:
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 3:
            continue
        label = _normalize(cells[0].get_text(" ", strip=True)).lower()
        if "manufacturing pmi" not in label:
            continue
        values: list[str] = []
        for cell in cells[1:]:
            match = _NUMBER_RE.search(cell.get_text(" ", strip=True))
            if match is not None:
                values.append(match.group(0))
        if len(values) >= 2 and values[0] == actual:
            return values[1]
    return None


def parse_report_html(
    html: str,
    *,
    source_url: str,
    series_id: str = "ISM_MANUFACTURING_PMI",
) -> ISMReportValue:
    """Extract the current Manufacturing PMI value from an ISM report."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    actual = _extract_actual(soup)
    report_title = _extract_report_title(soup)
    ref, label = _reference_from_title(report_title)
    previous = _extract_previous(soup, actual=actual)
    return ISMReportValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=label,
        actual=actual,
        previous=previous,
        report_title=report_title,
        source_url=source_url,
    )


def _end_of_month(ref: date) -> date:
    return ref.replace(day=calendar.monthrange(ref.year, ref.month)[1])


def _content_hash(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("actual") or ""),
        str(payload.get("previous") or ""),
        str(payload.get("report_title") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def report_value_to_records(
    value: ISMReportValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: ISMIndicatorSpec | None = None,
) -> tuple[ISMCalendarRawRecord, ISMCalendarEventRecord]:
    """Project a parsed ISM report value to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(value.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {value.series_id!r} not in INDICATOR_REGISTRY")

    ref = date.fromisoformat(value.reference_date)
    ref_end = _end_of_month(ref)
    event_time_utc = datetime(
        ref_end.year, ref_end.month, ref_end.day, tzinfo=timezone.utc,
    ).isoformat()
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        value.reference_date,
    )
    payload = {
        "kind": "ism_report_value",
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "reference_label": value.reference_label,
        "actual": value.actual,
        "previous": value.previous,
        "report_title": value.report_title,
        "source_url": value.source_url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
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
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=value.reference_date,
        reference_label=value.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=value.actual,
        previous=value.previous,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Institute for Supply Management",
        source_url=value.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
