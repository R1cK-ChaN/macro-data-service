"""Project ISTAT press-release observations into calendar records."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import ISTATIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "istat"


@dataclass(frozen=True)
class ISTATValueObservation:
    """One value parsed from an ISTAT press release."""

    series_id: str
    value: str
    reference_date: str
    reference_label: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    payload_text: str
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class ISTATCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ISTATCalendarEventRecord:
    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str | None
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str
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


class ISTATPressReleaseParseError(ValueError):
    """Raised when an ISTAT press-release page cannot be projected."""


_HASH_FIELDS: tuple[str, ...] = ("value", "reference_date", "series_id", "source_url")
_NUMBER_RE = r"([+-]?\d+(?:[,.]\d+)?)"
_UNIT_RE = r"(?:%|percent|per cent)"
_CPI_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"(?:consumer price index|cpi).*?{_NUMBER_RE}\s*{_UNIT_RE}.*?"
        rf"(?:previous month|monthly).*?(?:and|,)\s*{_NUMBER_RE}\s*{_UNIT_RE}.*?"
        rf"(?:annual|year[- ]over[- ]year)",
        re.I | re.S,
    ),
    re.compile(
        rf"(?:consumer price index|cpi).*?(?:annual|year[- ]over[- ]year).*?"
        rf"{_NUMBER_RE}\s*{_UNIT_RE}",
        re.I | re.S,
    ),
)
_GDP_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"(?:gross domestic product|gdp).*?"
        rf"(?P<direction>increased|decreased|grew|fell|rose|was|is).*?"
        rf"(?P<value>{_NUMBER_RE})\s*{_UNIT_RE}.*?"
        rf"(?:previous quarter|respect to the previous quarter)",
        re.I | re.S,
    ),
    re.compile(
        rf"(?:gross domestic product|gdp).*?(?P<value>{_NUMBER_RE})\s*{_UNIT_RE}.*?"
        rf"(?:quarter[- ]on[- ]quarter|qoq)",
        re.I | re.S,
    ),
)
_NEGATIVE_GDP_DIRECTIONS: frozenset[str] = frozenset({"decreased", "fell"})


def _normalise(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = (
        text.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
        .replace("\u202f", " ")
    )
    return re.sub(r"\s+", " ", text).strip().lower()


def _page_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(" ", strip=True)


def _content_hash(payload: dict[str, Any]) -> str:
    stable = json.dumps(
        {field: payload.get(field) for field in _HASH_FIELDS},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _value_text(raw: str) -> str:
    try:
        value = Decimal(raw.replace(",", ".").replace("+", ""))
    except InvalidOperation as exc:
        raise ISTATPressReleaseParseError(f"invalid ISTAT value: {raw!r}") from exc
    return format(value.normalize(), "f")


def _signed_gdp_value(match: re.Match[str]) -> str:
    raw = match.group("value")
    direction = (match.groupdict().get("direction") or "").lower()
    if direction in _NEGATIVE_GDP_DIRECTIONS and not raw.startswith(("-", "+")):
        raw = f"-{raw}"
    return _value_text(raw)


def _extract_value(text: str, spec: ISTATIndicatorSpec) -> str:
    if spec.release_kind == "cpi_provisional":
        patterns = _CPI_PATTERNS
        capture_index = 2
    elif spec.release_kind == "gdp_preliminary":
        for pattern in _GDP_PATTERNS:
            match = pattern.search(text)
            if match:
                return _signed_gdp_value(match)
        raise ISTATPressReleaseParseError(
            f"headline value not found for {spec.series_id}"
        )
    else:
        raise KeyError(f"unknown ISTAT release kind: {spec.release_kind!r}")
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            return _value_text(groups[min(capture_index, len(groups)) - 1])
    raise ISTATPressReleaseParseError(
        f"headline value not found for {spec.series_id}"
    )


def parse_press_release_value(
    html: str,
    *,
    spec: ISTATIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
) -> ISTATValueObservation:
    """Extract a headline value from an ISTAT press-release page."""
    text = _page_text(html)
    normalized = _normalise(text)
    value = _extract_value(normalized, spec)
    observed_at_epoch_ms = int(
        datetime.fromisoformat(event_time_utc).timestamp() * 1000
    )
    return ISTATValueObservation(
        series_id=spec.series_id,
        value=value,
        reference_date=reference_date,
        reference_label=reference_label,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url or spec.source_url,
        payload_text=text,
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


def parse_observation(
    obs: ISTATValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: ISTATIndicatorSpec | None = None,
) -> tuple[ISTATCalendarRawRecord, ISTATCalendarEventRecord]:
    """Convert one ISTAT observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {obs.series_id!r} not in ISTAT INDICATOR_REGISTRY")
    canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonical,
        obs.reference_date,
    )
    payload = {
        "series_id": obs.series_id,
        "value": obs.value,
        "reference_date": obs.reference_date,
        "reference_label": obs.reference_label,
        "event_time_utc": obs.event_time_utc,
        "event_time_precision": obs.event_time_precision,
        "source_url": obs.source_url,
        "payload_text": obs.payload_text,
    }
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )

    raw_record = ISTATCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=_content_hash(payload),
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ISTATCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=obs.event_time_utc,
        event_time_precision=obs.event_time_precision,
        reference_date=obs.reference_date,
        reference_label=obs.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="ISTAT",
        source_url=obs.source_url,
        content_hash=raw_record.content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
