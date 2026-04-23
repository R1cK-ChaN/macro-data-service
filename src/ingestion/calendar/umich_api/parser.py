"""Project U Michigan Consumer Sentiment values into calendar records."""

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

from .indicators import INDICATOR_REGISTRY, UMichIndicatorSpec, UMICH_MAIN_URL

PROVIDER = "umich"


@dataclass(frozen=True)
class UMichCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class UMichCalendarEventRecord:
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
class UMichCurrentValue:
    """Parsed value from the current U Michigan results page."""

    series_id: str
    reference_date: str
    reference_label: str
    release_stage: str
    actual: str
    previous: str | None
    report_title: str
    source_url: str


class UMichResultsParseError(ValueError):
    """Raised when the U Michigan results page shape drifts."""


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
_RESULTS_TITLE_RE = re.compile(
    r"\b(?P<stage>preliminary|final)\s+results\s+for\s+"
    r"(?P<month>january|february|march|april|may|june|july|august|"
    r"september|october|november|december)\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")


def _normalize(text: str) -> str:
    return " ".join(text.replace("\xa0", " ").split())


def normalize_release_stage(text: str) -> str:
    """Return ``preliminary`` or ``final`` for U Michigan stage labels."""
    value = _normalize(text).lower()
    if value.startswith("prelim"):
        return "preliminary"
    if value.startswith("final"):
        return "final"
    raise UMichResultsParseError(f"unknown U Michigan release stage: {text!r}")


def stage_label(stage: str) -> str:
    normalized = normalize_release_stage(stage)
    return "Prelim" if normalized == "preliminary" else "Final"


def event_anchor(reference_date: str, release_stage: str) -> str:
    return f"{reference_date}|{normalize_release_stage(release_stage)}"


def title_for_stage(spec: UMichIndicatorSpec, release_stage: str) -> str:
    return f"{spec.title} {stage_label(release_stage)}"


def _extract_report_title(soup: BeautifulSoup) -> str:
    heading = soup.find("h1")
    if heading is None:
        raise UMichResultsParseError("results heading not found")
    title = _normalize(heading.get_text(" ", strip=True))
    if _RESULTS_TITLE_RE.search(title) is None:
        raise UMichResultsParseError("results heading month/stage not found")
    return title


def _reference_from_title(title: str) -> tuple[date, str, str]:
    match = _RESULTS_TITLE_RE.search(title)
    if match is None:
        raise UMichResultsParseError("results heading month/stage not found")
    stage = normalize_release_stage(match.group("stage"))
    month_name = match.group("month").lower()
    year = int(match.group("year"))
    month = _MONTH_NAMES[month_name]
    ref = date(year=year, month=month, day=1)
    return ref, f"{month_name.capitalize()} {year} {stage_label(stage)}", stage


def _extract_number(text: str) -> str:
    match = _NUMBER_RE.search(_normalize(text))
    if match is None:
        raise UMichResultsParseError(f"numeric value not found: {text!r}")
    return match.group(0)


def _extract_sentiment_values(soup: BeautifulSoup) -> tuple[str, str | None]:
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = _normalize(cells[0].get_text(" ", strip=True)).lower()
        if label != "index of consumer sentiment":
            continue
        actual = _extract_number(cells[1].get_text(" ", strip=True))
        previous = (
            _extract_number(cells[2].get_text(" ", strip=True))
            if len(cells) >= 3
            else None
        )
        return actual, previous
    raise UMichResultsParseError("Index of Consumer Sentiment row not found")


def parse_current_results_html(
    html: str,
    *,
    source_url: str = UMICH_MAIN_URL,
    series_id: str = "UMICH_CONSUMER_SENTIMENT",
) -> UMichCurrentValue:
    """Extract the current Consumer Sentiment value from the main page."""
    if series_id not in INDICATOR_REGISTRY:
        raise KeyError(f"series_id {series_id!r} not in INDICATOR_REGISTRY")
    soup = BeautifulSoup(html, "html.parser")
    report_title = _extract_report_title(soup)
    ref, label, stage = _reference_from_title(report_title)
    actual, previous = _extract_sentiment_values(soup)
    return UMichCurrentValue(
        series_id=series_id,
        reference_date=ref.isoformat(),
        reference_label=label,
        release_stage=stage,
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
        str(payload.get("release_stage") or ""),
        str(payload.get("report_title") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def current_value_to_records(
    value: UMichCurrentValue,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: UMichIndicatorSpec | None = None,
) -> tuple[UMichCalendarRawRecord, UMichCalendarEventRecord]:
    """Project a parsed U Michigan current value to (raw, event) records."""
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
        event_anchor(value.reference_date, value.release_stage),
    )
    payload = {
        "kind": "umich_current_value",
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "reference_label": value.reference_label,
        "release_stage": value.release_stage,
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
    raw_record = UMichCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = UMichCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=value.reference_date,
        reference_label=value.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=title_for_stage(resolved_spec, value.release_stage),
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=value.actual,
        previous=value.previous,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="University of Michigan Surveys of Consumers",
        source_url=value.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
