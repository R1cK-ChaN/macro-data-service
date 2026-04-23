"""Project NAR current housing values into calendar records."""

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

from .indicators import (
    INDICATOR_REGISTRY,
    NAR_EXISTING_HOME_SALES_URL,
    NAR_PENDING_HOME_SALES_URL,
    NARIndicatorSpec,
)

PROVIDER = "nar"


@dataclass(frozen=True)
class NARCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class NARCalendarEventRecord:
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
class NARCurrentValue:
    """Parsed current value from a NAR housing-statistics page."""

    series_id: str
    reference_date: str
    reference_label: str
    actual: str
    previous: str | None
    report_title: str
    source_url: str
    raw_change: str | None = None


class NARResultsParseError(ValueError):
    """Raised when a NAR current page drifts."""


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
_MONTH_RE = (
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december"
)
_NUMBER_RE = r"[+\-]?\d+(?:\.\d+)?"
_EHS_SNAPSHOT_RE = re.compile(
    rf"\b(?P<month>{_MONTH_RE})\s+(?P<year>\d{{4}})\s+brought\s+"
    rf"(?P<actual>{_NUMBER_RE})\s+million\s+in\s+sales\b",
    re.IGNORECASE,
)
_EHS_MOM_RE = re.compile(
    rf"\bExisting-home\s+sales\s+(?P<direction>[a-z\s-]+?)\s+by\s+"
    rf"(?P<num>{_NUMBER_RE})\s*%\s+in\s+(?P<month>{_MONTH_RE})\s+"
    rf"(?P<year>\d{{4}})",
    re.IGNORECASE,
)
_PENDING_MOM_RE = re.compile(
    rf"\bIn\s+(?P<month>{_MONTH_RE})\s+(?P<year>\d{{4}}),\s+"
    rf"pending\s+home\s+sales\s+(?P<direction>[a-z\s-]+?)\s+"
    rf"(?P<num>{_NUMBER_RE})\s*%\s+month\s+over\s+month\b",
    re.IGNORECASE,
)


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _month_number(name: str) -> int:
    month = _MONTH_NAMES.get(name.lower())
    if month is None:
        raise NARResultsParseError(f"unknown month name: {name!r}")
    return month


def _month_label(ref: date) -> str:
    return f"{calendar.month_name[ref.month]} {ref.year}"


def _clean_number(value: str) -> str:
    value = value.strip()
    if value.startswith("+"):
        value = value[1:]
    return value


def _signed_change(num: str, direction: str) -> str:
    value = _clean_number(num)
    if value.startswith("-"):
        return value
    direction_lower = direction.lower()
    negative_tokens = (
        "decrease",
        "decline",
        "fall",
        "fell",
        "slid",
        "lower",
        "drop",
        "down",
    )
    if any(token in direction_lower for token in negative_tokens):
        return f"-{value}"
    return value


def _extract_report_title(soup: BeautifulSoup, fragment: str) -> str:
    wanted = fragment.lower()
    for heading in soup.find_all(["h1", "h2"]):
        text = _normalize(heading.get_text(" ", strip=True))
        if wanted in text.lower():
            return text
    title = soup.find("title")
    if title is not None:
        text = _normalize(title.get_text(" ", strip=True))
        if text:
            return text
    raise NARResultsParseError(f"{fragment} report title not found")


def parse_existing_home_sales_html(
    html: str,
    *,
    source_url: str = NAR_EXISTING_HOME_SALES_URL,
    series_id: str = "NAR_EXISTING_HOME_SALES",
) -> NARCurrentValue:
    """Extract current Existing Home Sales million-SAAR value."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_report_title(soup, "Existing-Home Sales")
    text = _normalize(soup.get_text(" ", strip=True))
    match = _EHS_SNAPSHOT_RE.search(text)
    if match is None:
        raise NARResultsParseError("Existing Home Sales snapshot sentence not found")
    ref = date(
        year=int(match.group("year")),
        month=_month_number(match.group("month")),
        day=1,
    )
    change: str | None = None
    change_match = _EHS_MOM_RE.search(text)
    if change_match is not None:
        change = _signed_change(
            change_match.group("num"),
            change_match.group("direction"),
        )
    return NARCurrentValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=_month_label(ref),
        actual=_clean_number(match.group("actual")),
        previous=None,
        report_title=title,
        source_url=source_url,
        raw_change=change,
    )


def parse_pending_home_sales_html(
    html: str,
    *,
    source_url: str = NAR_PENDING_HOME_SALES_URL,
    series_id: str = "NAR_PENDING_HOME_SALES_MOM",
) -> NARCurrentValue:
    """Extract current Pending Home Sales month-over-month percent change."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_report_title(soup, "Pending Home Sales")
    text = _normalize(soup.get_text(" ", strip=True))
    match = _PENDING_MOM_RE.search(text)
    if match is None:
        raise NARResultsParseError("Pending Home Sales MoM sentence not found")
    ref = date(
        year=int(match.group("year")),
        month=_month_number(match.group("month")),
        day=1,
    )
    actual = _signed_change(match.group("num"), match.group("direction"))
    return NARCurrentValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=_month_label(ref),
        actual=actual,
        previous=None,
        report_title=title,
        source_url=source_url,
    )


def parse_current_value_html(
    html: str,
    *,
    source_url: str,
    series_id: str,
) -> NARCurrentValue:
    """Dispatch to the parser for the requested NAR series."""
    spec = INDICATOR_REGISTRY.get(series_id)
    if spec is None:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    if spec.value_kind == "existing_home_sales":
        return parse_existing_home_sales_html(
            html, source_url=source_url, series_id=series_id,
        )
    if spec.value_kind == "pending_home_sales_mom":
        return parse_pending_home_sales_html(
            html, source_url=source_url, series_id=series_id,
        )
    raise NARResultsParseError(f"unsupported NAR value kind: {spec.value_kind!r}")


def _end_of_month(ref: date) -> date:
    return ref.replace(day=calendar.monthrange(ref.year, ref.month)[1])


def _content_hash(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("actual") or ""),
        str(payload.get("previous") or ""),
        str(payload.get("report_title") or ""),
        str(payload.get("raw_change") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def current_value_to_records(
    value: NARCurrentValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: NARIndicatorSpec | None = None,
) -> tuple[NARCalendarRawRecord, NARCalendarEventRecord]:
    """Project a parsed NAR current value to records."""
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
    payload: dict[str, Any] = {
        "kind": "nar_current_value",
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "reference_label": value.reference_label,
        "actual": value.actual,
        "previous": value.previous,
        "report_title": value.report_title,
        "source_url": value.source_url,
        "raw_change": value.raw_change,
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
    raw_record = NARCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = NARCalendarEventRecord(
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
        source="National Association of Realtors",
        source_url=value.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
