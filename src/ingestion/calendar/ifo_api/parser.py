"""Project Ifo press-release observations into calendar records."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, IfoIndicatorSpec

PROVIDER = "ifo"


@dataclass(frozen=True)
class IfoValueObservation:
    """One headline value parsed from an Ifo press release."""

    series_id: str
    reference_date: str
    reference_label: str
    value: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    release_title: str
    raw: dict[str, Any]
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class IfoCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class IfoCalendarEventRecord:
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


class IfoPressReleaseParseError(ValueError):
    """Raised when an Ifo press-release page cannot be projected."""


_HASH_FIELDS: tuple[str, ...] = ("value", "reference_date", "series_id", "source_url")
_NUMBER_RE = r"(\d+(?:[,.]\d+)?)"
_CHANGE_VERB = (
    r"(?:fell|rose|climbed|dropped|increased|decreased|declined|"
    r"edged\s+down|edged\s+up|went\s+up|went\s+down)"
)
_HOLD_VERB = r"(?:remained|held\s+steady|stood|is|was)"
_VALUE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"ifo business climate index\s+{_CHANGE_VERB}"
        rf"[^.]*?\bto\s+{_NUMBER_RE}\s+points",
        re.I | re.S,
    ),
    re.compile(
        rf"ifo business climate index\s+{_HOLD_VERB}"
        rf"[^.]*?\bat\s+{_NUMBER_RE}\s+points",
        re.I | re.S,
    ),
)


def _normalise(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = (
        text.replace("–", "-")
        .replace("—", "-")
        .replace("−", "-")
        .replace("‑", "-")
    )
    return re.sub(r"\s+", " ", text).strip().lower()


def _page_text(html: str | bytes) -> str:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(" ", strip=True)


def _page_title(html: str | bytes, fallback: str) -> str:
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    heading = soup.find(["h1", "title"])
    if heading:
        title = heading.get_text(" ", strip=True)
        if title:
            return title
    return fallback


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _value_text(raw: str) -> str:
    cleaned = str(raw or "").strip().replace(" ", "").replace(",", ".")
    try:
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise IfoPressReleaseParseError(f"invalid Ifo value: {raw!r}") from exc
    return cleaned


def _extract_value(text: str, spec: IfoIndicatorSpec) -> str:
    if spec.release_kind != "business_climate":
        raise KeyError(f"unknown Ifo release kind: {spec.release_kind!r}")
    for pattern in _VALUE_PATTERNS:
        match = pattern.search(text)
        if match:
            return _value_text(match.group(1))
    raise IfoPressReleaseParseError(
        f"headline value not found for {spec.series_id}"
    )


def parse_press_release_value(
    html: str | bytes,
    *,
    spec: IfoIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
) -> IfoValueObservation:
    """Extract the Ifo Business Climate headline index level."""
    raw_text = _page_text(html)
    value = _extract_value(_normalise(raw_text), spec)
    observed_at_epoch_ms = int(
        datetime.fromisoformat(event_time_utc).timestamp() * 1000
    )
    return IfoValueObservation(
        series_id=spec.series_id,
        reference_date=reference_date,
        reference_label=reference_label,
        value=value,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url or spec.source_url,
        release_title=_page_title(html, spec.title),
        raw={"text": raw_text[:4000]},
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


def parse_observation(
    obs: IfoValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: IfoIndicatorSpec | None = None,
) -> tuple[IfoCalendarRawRecord, IfoCalendarEventRecord]:
    """Convert one Ifo observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {obs.series_id!r} not in Ifo INDICATOR_REGISTRY")

    payload: dict[str, Any] = {
        "series_id": obs.series_id,
        "reference_date": obs.reference_date,
        "reference_label": obs.reference_label,
        "value": obs.value,
        "source_url": obs.source_url,
        "release_title": obs.release_title,
        "raw": obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        obs.reference_date,
    )
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = IfoCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = IfoCalendarEventRecord(
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
        source="Ifo",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
