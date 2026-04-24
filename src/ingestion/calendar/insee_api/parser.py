"""Project INSEE press-release observations into calendar records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import unicodedata
from typing import Any

from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INSEEIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "insee"


@dataclass(frozen=True)
class INSEEValueObservation:
    """One value parsed from an INSEE release page."""

    series_id: str
    value: str
    reference_date: str
    reference_label: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    release_title: str
    payload_text: str
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class INSEECalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class INSEECalendarEventRecord:
    """One row destined for ``cal_econ_event``."""

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


class INSEEPressReleaseParseError(ValueError):
    """Raised when an INSEE release page cannot be projected."""


_HASH_FIELDS: tuple[str, ...] = ("value", "reference_date", "series_id", "source_url")
_NUMBER_RE = r"(?P<value>[+-]?\d+(?:[,.]\d+)?)"
_CPI_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"consumer prices (?P<direction>increased|rose|fell|decreased|"
        rf"would increase|would rise|would fall|would decrease|"
        rf"were up|were down).*?by\s+{_NUMBER_RE}\s*%.*?"
        rf"(?:year on year|over a year|over one year)",
        re.I | re.S,
    ),
    re.compile(
        rf"over a year.*?consumer prices.*?"
        rf"(?P<direction>increase|rise|fall|decrease).*?"
        rf"{_NUMBER_RE}\s*%",
        re.I | re.S,
    ),
)
_GDP_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"gdp [^.()]*?\(\s*{_NUMBER_RE}\s*%\s+after",
        re.I | re.S,
    ),
    re.compile(
        rf"gross domestic product .*?"
        rf"(?P<direction>increasing|decreasing|increased|decreased|"
        rf"grew|fell|rebounded|slowed down|was stable|is stable).*?"
        rf"{_NUMBER_RE}\s*%.*?(?:after|in the)",
        re.I | re.S,
    ),
)
_NEGATIVE_DIRECTIONS: frozenset[str] = frozenset(
    {"fell", "fall", "decreased", "decrease", "decreasing", "would fall", "would decrease", "were down"}
)


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


def _page_text(html: str | bytes) -> str:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
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
    cleaned = raw.replace(",", ".").replace("+", "").strip()
    try:
        value = Decimal(cleaned)
    except InvalidOperation as exc:
        raise INSEEPressReleaseParseError(f"invalid INSEE value: {raw!r}") from exc
    return format(value.normalize(), "f")


def _signed_value(match: re.Match[str]) -> str:
    raw = match.group("value")
    direction = (match.groupdict().get("direction") or "").lower()
    if direction in _NEGATIVE_DIRECTIONS and not raw.startswith(("-", "+")):
        raw = f"-{raw}"
    return _value_text(raw)


def _extract_value(text: str, spec: INSEEIndicatorSpec) -> str:
    patterns: tuple[re.Pattern[str], ...]
    if spec.release_kind == "cpi_provisional":
        patterns = _CPI_PATTERNS
    elif spec.release_kind == "gdp_first_estimate":
        patterns = _GDP_PATTERNS
    else:
        raise KeyError(f"unknown INSEE release kind: {spec.release_kind!r}")
    for pattern in patterns:
        match = pattern.search(text)
        if match:
            return _signed_value(match)
    raise INSEEPressReleaseParseError(
        f"headline value not found for {spec.series_id}"
    )


def _release_title(text: str, spec: INSEEIndicatorSpec) -> str:
    if spec.release_kind == "cpi_provisional":
        match = re.search(
            r"(consumer price index - provisional results - [a-z]+ 20\d{2})",
            text,
            re.I,
        )
    else:
        match = re.search(
            r"(quarterly national accounts - first estimate - "
            r"(?:first|second|third|fourth) quarter 20\d{2})",
            text,
            re.I,
        )
    return match.group(1) if match else spec.title


def parse_press_release_value(
    html: str | bytes,
    *,
    spec: INSEEIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
) -> INSEEValueObservation:
    """Extract a headline value from an INSEE release page."""
    text = _page_text(html)
    normalised = _normalise(text)
    value = _extract_value(normalised, spec)
    observed_at_epoch_ms = int(
        datetime.fromisoformat(event_time_utc).timestamp() * 1000
    )
    return INSEEValueObservation(
        series_id=spec.series_id,
        value=value,
        reference_date=reference_date,
        reference_label=reference_label,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url or spec.source_url,
        release_title=_release_title(normalised, spec),
        payload_text=text,
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


def parse_observation(
    obs: INSEEValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: INSEEIndicatorSpec | None = None,
) -> tuple[INSEECalendarRawRecord, INSEECalendarEventRecord]:
    """Convert one INSEE observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {obs.series_id!r} not in INSEE INDICATOR_REGISTRY")
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
        "release_title": obs.release_title,
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
    content_hash = _content_hash(payload)

    raw_record = INSEECalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = INSEECalendarEventRecord(
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
        source="INSEE",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
