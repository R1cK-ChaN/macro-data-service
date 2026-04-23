"""Project Census EITS observations into calendar storage records."""

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

from .client import CensusEITSObservation
from .indicators import INDICATOR_REGISTRY, CensusIndicatorSpec

PROVIDER = "census"


@dataclass(frozen=True)
class CensusCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class CensusCalendarEventRecord:
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


_HASH_FIELDS: tuple[str, ...] = (
    "cell_value",
    "error_data",
    "time_slot_id",
    "time_slot_name",
)


def _content_hash(obs_dict: dict[str, Any]) -> str:
    parts = []
    for field in _HASH_FIELDS:
        value = obs_dict.get(field)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _reference_date(time_value: str) -> date:
    """Return the first day of a monthly EITS period."""
    parts = str(time_value).split("-")
    if len(parts) != 2:
        raise ValueError(f"unsupported Census EITS time value: {time_value!r}")
    return date(year=int(parts[0]), month=int(parts[1]), day=1)


def _end_of_month(ref: date) -> date:
    return ref.replace(day=calendar.monthrange(ref.year, ref.month)[1])


def parse_observation(
    obs: CensusEITSObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    spec: CensusIndicatorSpec | None = None,
) -> tuple[CensusCalendarRawRecord, CensusCalendarEventRecord]:
    """Convert a Census EITS observation into (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in Census INDICATOR_REGISTRY"
        )

    payload = raw_payload or dict(obs.raw)
    payload_with_sid = {
        "series_id": obs.series_id,
        "dataset": obs.dataset,
        **payload,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload_with_sid, sort_keys=True, ensure_ascii=False)

    ref = _reference_date(obs.time)
    ref_iso = ref.isoformat()
    ref_end = _end_of_month(ref)
    event_time_utc = datetime(
        ref_end.year, ref_end.month, ref_end.day, tzinfo=timezone.utc,
    ).isoformat()

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        ref_iso,
    )

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = CensusCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = CensusCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=ref_iso,
        reference_label=obs.time,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=obs.cell_value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Census Bureau",
        source_url=resolved_spec.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_asdict = asdict
