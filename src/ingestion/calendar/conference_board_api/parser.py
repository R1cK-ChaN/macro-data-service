"""Project Conference Board current values into calendar records."""

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
    CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    INDICATOR_REGISTRY,
    ConferenceBoardIndicatorSpec,
)

PROVIDER = "conference-board"


@dataclass(frozen=True)
class ConferenceBoardCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ConferenceBoardCalendarEventRecord:
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
class ConferenceBoardCurrentValue:
    """Parsed value from a Conference Board current release page."""

    series_id: str
    reference_date: str
    reference_label: str
    actual: str
    previous: str | None
    report_title: str
    source_url: str
    index_level: str | None = None


class ConferenceBoardResultsParseError(ValueError):
    """Raised when a Conference Board current-release page drifts."""


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
_NUMBER_RE = r"[+\-−–]?\d+(?:\.\d+)?"
_CCI_SOURCE_RE = re.compile(
    rf"\bSource:\s*(?P<month>{_MONTH_RE})\s+(?P<year>\d{{4}})\s+"
    r".*?Consumer\s+Confidence\s+Survey",
    re.IGNORECASE,
)
_CCI_VALUE_RE = re.compile(
    rf"Consumer\s+Confidence\s+Index.*?\bin\s+(?P<month>{_MONTH_RE})\s+"
    rf"to\s+(?P<actual>{_NUMBER_RE})\s*\(1985\s*=\s*100\).*?"
    rf"\bfrom\s+(?P<previous>{_NUMBER_RE})",
    re.IGNORECASE,
)
_UPDATED_YEAR_RE = re.compile(r"\bUpdated:\s*.*?,\s+(?P<year>\d{4})\b")
_LEI_VALUE_RE = re.compile(
    rf"The\s+Conference\s+Board\s+Leading\s+Economic\s+Index.*?"
    rf"\(LEI\)\s+for\s+the\s+US\s+(?P<direction>.*?)"
    rf"(?:by\s+(?P<change>{_NUMBER_RE})\s*%\s+)?"
    rf"in\s+(?P<month>{_MONTH_RE})\s+(?P<year>\d{{4}})\s+"
    rf"(?:to|at)\s+(?P<level>{_NUMBER_RE})\s*\(2016\s*=\s*100\)",
    re.IGNORECASE,
)
_LEI_PREVIOUS_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"\bfollowing\s+(?:an?\s+)?(?P<num>{_NUMBER_RE})\s*%\s+"
        r"(?P<direction>decline|increase|decrease|gain|rise)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\bafter\s+(?:an?\s+)?(?P<direction>increase|decline|decrease|gain|rise)"
        rf"\s+of\s+(?P<num>{_NUMBER_RE})\s*%",
        re.IGNORECASE,
    ),
)


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def _month_number(name: str) -> int:
    month = _MONTH_NAMES.get(name.lower())
    if month is None:
        raise ConferenceBoardResultsParseError(f"unknown month name: {name!r}")
    return month


def _month_label(ref: date) -> str:
    return f"{calendar.month_name[ref.month]} {ref.year}"


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
    raise ConferenceBoardResultsParseError(f"{fragment} report title not found")


def _updated_year(text: str) -> int | None:
    match = _UPDATED_YEAR_RE.search(text)
    if match is None:
        return None
    return int(match.group("year"))


def _clean_number(text: str) -> str:
    value = text.replace("−", "-").replace("–", "-").strip()
    if value.startswith("+"):
        value = value[1:]
    return value


def _signed_change(num: str | None, direction: str) -> str:
    if num is None or num.strip() == "":
        return "0.0"
    value = _clean_number(num)
    if value.startswith("-"):
        return value
    direction_lower = direction.lower()
    if any(
        token in direction_lower
        for token in (
            "decline",
            "decrease",
            "down",
            "fell",
            "fall",
            "dropped",
            "drop",
            "slipped",
            "contract",
        )
    ):
        return f"-{value}"
    if "unchanged" in direction_lower:
        return "0.0"
    return value


def _extract_lei_previous(text: str) -> str | None:
    for pattern in _LEI_PREVIOUS_PATTERNS:
        match = pattern.search(text)
        if match is None:
            continue
        return _signed_change(match.group("num"), match.group("direction"))
    return None


def parse_consumer_confidence_html(
    html: str,
    *,
    source_url: str = CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    series_id: str = "TCB_CONSUMER_CONFIDENCE",
) -> ConferenceBoardCurrentValue:
    """Extract the current Consumer Confidence value."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    report_title = _extract_report_title(soup, "Consumer Confidence")
    text = _normalize(soup.get_text(" ", strip=True))
    source_match = _CCI_SOURCE_RE.search(text)
    value_match = _CCI_VALUE_RE.search(text)
    if value_match is None:
        raise ConferenceBoardResultsParseError(
            "Consumer Confidence Index value sentence not found"
        )
    if source_match is not None:
        month_name = source_match.group("month").lower()
        year = int(source_match.group("year"))
    else:
        month_name = value_match.group("month").lower()
        year = _updated_year(text)
        if year is None:
            raise ConferenceBoardResultsParseError(
                "Consumer Confidence reference year not found"
            )
    ref = date(year=year, month=_month_number(month_name), day=1)
    return ConferenceBoardCurrentValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=_month_label(ref),
        actual=_clean_number(value_match.group("actual")),
        previous=_clean_number(value_match.group("previous")),
        report_title=report_title,
        source_url=source_url,
    )


def parse_leading_index_html(
    html: str,
    *,
    source_url: str = CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    series_id: str = "TCB_LEADING_INDEX",
) -> ConferenceBoardCurrentValue:
    """Extract the current US Leading Economic Index monthly change."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    report_title = _extract_report_title(soup, "Leading Economic Index")
    text = _normalize(soup.get_text(" ", strip=True))
    match = _LEI_VALUE_RE.search(text)
    if match is None:
        raise ConferenceBoardResultsParseError(
            "US Leading Economic Index value sentence not found"
        )
    month_name = match.group("month").lower()
    year = int(match.group("year"))
    ref = date(year=year, month=_month_number(month_name), day=1)
    actual = _signed_change(match.group("change"), match.group("direction"))
    return ConferenceBoardCurrentValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=_month_label(ref),
        actual=actual,
        previous=_extract_lei_previous(text),
        report_title=report_title,
        source_url=source_url,
        index_level=_clean_number(match.group("level")),
    )


def parse_current_value_html(
    html: str,
    *,
    source_url: str,
    series_id: str,
) -> ConferenceBoardCurrentValue:
    """Dispatch to the current-page parser for ``series_id``."""
    if series_id == "TCB_CONSUMER_CONFIDENCE":
        return parse_consumer_confidence_html(
            html, source_url=source_url, series_id=series_id,
        )
    if series_id == "TCB_LEADING_INDEX":
        return parse_leading_index_html(
            html, source_url=source_url, series_id=series_id,
        )
    raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")


def _end_of_month(ref: date) -> date:
    return ref.replace(day=calendar.monthrange(ref.year, ref.month)[1])


def _content_hash(payload: dict[str, Any]) -> str:
    parts = [
        str(payload.get("actual") or ""),
        str(payload.get("previous") or ""),
        str(payload.get("index_level") or ""),
        str(payload.get("report_title") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def current_value_to_records(
    value: ConferenceBoardCurrentValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: ConferenceBoardIndicatorSpec | None = None,
) -> tuple[ConferenceBoardCalendarRawRecord, ConferenceBoardCalendarEventRecord]:
    """Project a parsed Conference Board value to (raw, event) records."""
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
        "kind": "conference_board_current_value",
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "reference_label": value.reference_label,
        "actual": value.actual,
        "previous": value.previous,
        "index_level": value.index_level,
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
        source="The Conference Board",
        source_url=value.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
