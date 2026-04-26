"""Project an EIA weekly observation into calendar storage records.

The EIA v2 endpoint returns one row per (series, period). Each row
becomes one ``cal_econ_event`` row keyed on the ``period`` (which
EIA uses to mean the week-ending date) — the same anchor the parity
comparator buckets on.

``event_time_utc`` resolves the standing publication time
(Wednesday 10:30 ET for petroleum, Thursday 10:30 ET for natural
gas) on the next-after-period business day. EIA doesn't publish a
forward calendar; this connector trusts the API's period column as
the authoritative week-ending anchor and synthesizes the release
datetime from the spec's ``release_dow`` + ``release_time_local``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import EIAIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "eia"


@dataclass(frozen=True)
class EIACalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class EIACalendarEventRecord:
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
class EIAObservation:
    """One observation pulled from the EIA v2 API for a calendar series."""

    indicator: str
    period: str          # week-ending ISO date (YYYY-MM-DD)
    value: str
    unit: str
    raw: dict[str, Any]


_HASH_FIELDS = ("indicator", "period", "value", "unit")


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for f in _HASH_FIELDS:
        v = payload.get(f)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _next_release_datetime(
    period: date, *, dow: int, time_local: str,
) -> str:
    """Resolve the publication datetime for an observation week.

    EIA petroleum stocks publish the Wednesday after the period's
    Friday-end; natural-gas storage publishes the Thursday after.
    Counts forward from ``period`` (the week-ending Friday) until we
    hit the spec's day-of-week, then layers the time + Eastern
    timezone via the shared release-time helper.
    """
    advance = (dow - period.weekday()) % 7
    if advance == 0:
        # Period date is the same DOW as release — bump a week so we
        # don't pin the release to the same day as the observation.
        advance = 7
    release_date = period + timedelta(days=advance)
    scheduled = parse_scheduled_release_time(
        release_date, time_local, default_tz="America/New_York",
    )
    return scheduled.utc.isoformat()


def observation_to_records(
    obs: EIAObservation,
    *,
    snapshot_epoch_ms: int,
    spec: EIAIndicatorSpec | None = None,
    base_url: str = "https://api.eia.gov/v2",
) -> tuple[EIACalendarRawRecord, EIACalendarEventRecord]:
    """Project an :class:`EIAObservation` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {obs.indicator!r} not in EIA INDICATOR_REGISTRY"
        )

    period_date = date.fromisoformat(obs.period)
    event_time_utc = _next_release_datetime(
        period_date,
        dow=resolved_spec.release_dow,
        time_local=resolved_spec.release_time_local,
    )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        obs.period,
    )

    payload: dict[str, Any] = {
        "kind":            "eia_weekly_stocks",
        "indicator":       resolved_spec.indicator,
        "series_id":       resolved_spec.series_id,
        "period":          obs.period,
        "value":           obs.value,
        "unit":            obs.unit,
        "event_time_utc":  event_time_utc,
        "raw":             obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    source_url = f"{base_url.rstrip('/')}/{resolved_spec.route.rstrip('/')}/"

    raw_record = EIACalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EIACalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=obs.period,
        reference_label=obs.period,
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
        source="US Energy Information Administration",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "EIACalendarEventRecord",
    "EIACalendarRawRecord",
    "EIAObservation",
    "PROVIDER",
    "observation_to_records",
]
