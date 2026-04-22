"""Project an :class:`SDMXObservation` from the ECB Data Portal into
calendar storage records.

Pure functions — no DB, no HTTP. Callers feed in the observation plus
a snapshot timestamp; :mod:`projector` handles persistence.

Event-time note (P3 scaffold):

The ECB Data Portal returns observations with the **effective date**
of the rate level — not the decision date. For the three ECB key
policy rates (MRO / DFR / MLF), the Governing Council meets roughly
every six weeks, announces new rates around 14:15 CET / CEST, and
the new level becomes effective from the following main refinancing
settlement day. Calendar consumers typically want the decision
datetime, not the effective date.

P3 scaffold takes the simplest path: ``event_time_utc`` is the
effective date at 00:00 UTC with ``event_time_precision='approximate'``.
The P3a slice will scrape ``ecb.europa.eu/press/calendars/`` for the
Governing Council meeting calendar and upgrade the placeholder to the
announcement datetime (CET-aware, DST-correct) on the shared
``provider_event_id`` — same dual-write pattern as BLS P1a.

The ``provider_event_id`` anchors on the effective date so the id is
stable across the value → value+schedule upgrade. Repeated fetches
of the same series produce the same event id per observation.
"""

from __future__ import annotations

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

from .indicators import ECBIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "ecb"


@dataclass(frozen=True)
class ECBCalendarRawRecord:
    """One row destined for ``cal_econ_raw``.

    Parallel to BLS/BEA raw records — promotion to ``_official_shared``
    deferred per the discipline rule on abstractions (three identical
    shapes today, still cheap to keep in sync by copy).
    """

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ECBCalendarEventRecord:
    """One row destined for ``cal_econ_event`` (PIT projection)."""

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


# An ECB rate observation's mutable fields are the level itself plus
# the date (which only changes if ECB republishes an effective date).
# The series id is part of the event identity, not a mutable field —
# hashing the pair is enough to detect revisions.
_HASH_FIELDS: tuple[str, ...] = ("value", "date", "series_id")


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def parse_observation(
    obs: SDMXObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    spec: ECBIndicatorSpec | None = None,
) -> tuple[ECBCalendarRawRecord, ECBCalendarEventRecord]:
    """Convert an ECB observation into (raw, event) records.

    Parameters
    ----------
    obs:
        The parsed :class:`SDMXObservation` returned by ``ECBClient.get_data``.
    snapshot_epoch_ms:
        Fetch-time anchor; stored verbatim on the raw record.
    observed_at_epoch_ms:
        Usually equal to ``snapshot_epoch_ms``. Separable for tests
        and replay paths.
    raw_payload:
        The original row dict when available. The SDMX JSON parser
        does not currently expose the raw dict (the live client
        returns typed :class:`SDMXObservation`), so this is typically
        reconstructed from the typed observation — revisions are
        still detected via the content hash on
        ``(value, date, series_id)``.
    spec:
        Indicator metadata. Resolved from :data:`INDICATOR_REGISTRY`
        when absent.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in ECB INDICATOR_REGISTRY"
        )

    if raw_payload is None:
        raw_payload = {
            "series_id": obs.series_id,
            "date": obs.date,
            "value": obs.value,
            "dataflow": obs.dataflow,
        }
    payload_with_sid = {"seriesID": obs.series_id, **raw_payload}
    content_hash = _content_hash(raw_payload)
    payload_json = json.dumps(payload_with_sid, sort_keys=True, ensure_ascii=False)

    # ECB effective-date strings are already ISO YYYY-MM-DD after the
    # SDMX client's ``_normalize_date`` hook. Trust that shape — the
    # live probe will flag any deviation.
    ref = date.fromisoformat(obs.date)
    event_time_utc = datetime(
        ref.year, ref.month, ref.day, tzinfo=timezone.utc,
    ).isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        obs.date,
    )

    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = ECBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = ECBCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=obs.date,
        reference_label=obs.date,
        country_code=resolved_spec.country_code,
        indicator_id=None,     # resolved by a later alias-learning pass
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="EUR",
        unit=resolved_spec.unit,
        actual=str(obs.value),
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="ECB",
        source_url=(
            "https://data-api.ecb.europa.eu/service/data/"
            f"{resolved_spec.dataflow_id}/{resolved_spec.series_key}"
        ),
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record


_asdict = asdict
