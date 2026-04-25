"""Project EC BCS press-release observations into calendar records."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, EcBcsIndicatorSpec

PROVIDER = "ec-bcs"


@dataclass(frozen=True)
class EcBcsValueObservation:
    """One headline value parsed from an EC BCS press release."""

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
class EcBcsCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class EcBcsCalendarEventRecord:
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


class EcBcsPressReleaseParseError(ValueError):
    """Raised when an EC BCS press release cannot be projected."""


_HASH_FIELDS: tuple[str, ...] = ("value", "reference_date", "series_id", "source_url")
_NUMBER = r"(-?\d+(?:[.,]\d+)?)"

# ESI: "the Economic Sentiment Indicator (ESI) declined in both the EU
# (-1.5 points to 96.7) and the euro area (-1.6 points to 96.6)" — the
# euro-area parenthetical is the trader-impact figure. The body uses
# decimal points inside the change/level numbers, so we can't bound the
# fan-out with ``[^.]`` — keep ``.*?`` lazy and pin on the trailing
# ``points to <num>`` cue inside the euro-area parenthetical.
_ESI_PATTERN = re.compile(
    r"economic\s+sentiment\s+indicator.*?"
    r"euro\s+area\s*\([^)]*?points\s+to\s+"
    rf"{_NUMBER}",
    re.I | re.S,
)

# Flash CCI: "At -19.4 (EU) and -20.6 (euro area) points" — euro-area
# value is the second parenthesised aggregate.
_CCI_FLASH_PATTERN = re.compile(
    rf"at\s+{_NUMBER}\s*\(\s*eu\s*\)\s+and\s+{_NUMBER}\s*\(\s*euro\s*area\s*\)",
    re.I | re.S,
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
        raise EcBcsPressReleaseParseError(f"invalid EC BCS value: {raw!r}") from exc
    return cleaned


def _extract_value(text: str, spec: EcBcsIndicatorSpec) -> str:
    if spec.release_kind == "esi":
        match = _ESI_PATTERN.search(text)
        if match:
            return _value_text(match.group(1))
    elif spec.release_kind == "cci_flash":
        match = _CCI_FLASH_PATTERN.search(text)
        if match:
            return _value_text(match.group(2))
    else:
        raise KeyError(f"unknown EC BCS release kind: {spec.release_kind!r}")
    raise EcBcsPressReleaseParseError(
        f"headline value not found for {spec.series_id}"
    )


def parse_press_release_value(
    text: str | bytes,
    *,
    spec: EcBcsIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
) -> EcBcsValueObservation:
    """Extract the EC BCS headline (euro-area aggregate) from press text."""
    raw_text = (
        text.decode("utf-8", errors="replace") if isinstance(text, bytes) else text
    )
    value = _extract_value(_normalise(raw_text), spec)
    observed_at_epoch_ms = int(
        datetime.fromisoformat(event_time_utc).timestamp() * 1000
    )
    return EcBcsValueObservation(
        series_id=spec.series_id,
        reference_date=reference_date,
        reference_label=reference_label,
        value=value,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url or spec.source_url,
        release_title=spec.title,
        raw={"text": raw_text[:4000]},
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


def parse_observation(
    obs: EcBcsValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: EcBcsIndicatorSpec | None = None,
) -> tuple[EcBcsCalendarRawRecord, EcBcsCalendarEventRecord]:
    """Convert one EC BCS observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in EC BCS INDICATOR_REGISTRY"
        )

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

    raw_record = EcBcsCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EcBcsCalendarEventRecord(
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
        source="European Commission",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record
