"""Project Eurostat JSON-stat observations into calendar records."""

from __future__ import annotations

import calendar
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)
from ingestion.timeseries.sdmx._types import SDMXObservation

from .indicators import EurostatIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "eurostat"


@dataclass(frozen=True)
class EurostatCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class EurostatCalendarEventRecord:
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


_HASH_FIELDS: tuple[str, ...] = ("value", "date", "series_id", "dataset")


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _period_end(reference: date, cadence: str) -> date:
    if cadence == "quarterly":
        month = reference.month + 2
        year = reference.year
        while month > 12:
            month -= 12
            year += 1
        day = calendar.monthrange(year, month)[1]
        return date(year, month, day)
    day = calendar.monthrange(reference.year, reference.month)[1]
    return date(reference.year, reference.month, day)


def _reference_date(reference: date, cadence: str) -> date:
    if cadence == "quarterly":
        return _period_end(reference, cadence)
    return reference


def parse_observation(
    obs: SDMXObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    spec: EurostatIndicatorSpec | None = None,
) -> tuple[EurostatCalendarRawRecord, EurostatCalendarEventRecord]:
    """Convert one Eurostat observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in Eurostat INDICATOR_REGISTRY"
        )

    if raw_payload is None:
        raw_payload = {
            "series_id": obs.series_id,
            "date": obs.date,
            "value": obs.value,
            "dataset": obs.dataset or resolved_spec.dataset,
            "params": dict(resolved_spec.params),
        }
    payload_with_sid = {"seriesID": obs.series_id, **raw_payload}
    content_hash = _content_hash(raw_payload)
    payload_json = json.dumps(payload_with_sid, sort_keys=True, ensure_ascii=False)

    source_reference = date.fromisoformat(obs.date)
    reference = _reference_date(
        source_reference, resolved_spec.reference_cadence,
    )
    event_date = _period_end(
        source_reference, resolved_spec.reference_cadence,
    )
    reference_iso = reference.isoformat()
    event_time_utc = datetime(
        event_date.year,
        event_date.month,
        event_date.day,
        tzinfo=timezone.utc,
    ).isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        reference_iso,
    )

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = EurostatCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EurostatCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=reference_iso,
        reference_label=reference_iso,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=str(obs.value),
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Eurostat",
        source_url=resolved_spec.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_asdict = asdict
