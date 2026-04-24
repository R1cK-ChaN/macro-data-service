"""Project a scraped BoJ MPM entry into calendar storage records.

Pure functions — no DB, no HTTP. The scraper (:mod:`scraper`) produces
:class:`BojMpmEntry` values; this module turns each one into a
``(raw, event)`` tuple; :mod:`projector` handles persistence.

Event-time shape:

BoJ announces MPM rate decisions around **noon JST on the closing
day of the meeting**. The release time is not stamped on the
schedule page — individual meetings close any time between 11:30
and 12:30 JST depending on how long the committee deliberates — so
we anchor the scheduled event at 12:00 JST and let the value-side
writer update the datetime if the statement page publishes a
different release clock.

``provider_event_id`` anchors on the closing-day ISO date so the id
is stable across the schedule → value upgrade lifecycle (the
statement-value scraper upserts on the same id).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import BojIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "boj"
BOJ_RELEASE_TZ = "Asia/Tokyo"
BOJ_RELEASE_TIME_LOCAL = "12:00"
BOJ_MPM_CALENDAR_URL = (
    "https://www.boj.or.jp/en/mopo/mpmsche_minu/"
)


@dataclass(frozen=True)
class BojMpmEntry:
    """One MPM row parsed from the BoJ schedule page.

    ``closing_date`` is the day the rate decision is announced — the
    second date in the upstream "Jan. 22 (Thurs.), 23 (Fri.)" cell.
    ``date_cell`` is the verbatim cell text for audit. ``year`` is
    the year from the enclosing ``<h2 id="pYYYY">`` heading.
    """

    year: int                       # from the <h2 id="pYYYY"> header
    date_cell: str                  # verbatim "Jan. 22 (Thurs.), 23 (Fri.)"
    closing_date: date              # ISO date of the second day (rate decision day)


@dataclass(frozen=True)
class BojCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BojCalendarEventRecord:
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


# Hash inputs for revision detection. The schedule-side payload is
# stable unless upstream reschedules the meeting; a change in the
# verbatim date cell counts as a revision.
_HASH_FIELDS: tuple[str, ...] = (
    "closing_date", "date_cell", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def mpm_entry_to_records(
    entry: BojMpmEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: BojIndicatorSpec | None = None,
) -> tuple[BojCalendarRawRecord, BojCalendarEventRecord]:
    """Project a :class:`BojMpmEntry` to (raw, event) records.

    Uses the 12:00 JST convention to compute ``event_time_utc``;
    ``provider_event_id`` hashes on the closing-date ISO string so
    the id is stable through a later value-side upgrade.
    """
    resolved_spec = spec or INDICATOR_REGISTRY["BOJ_RATE"]

    scheduled = parse_scheduled_release_time(
        entry.closing_date,
        BOJ_RELEASE_TIME_LOCAL,
        default_tz=BOJ_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        entry.closing_date.isoformat(),
    )

    reference_label = entry.closing_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":          "boj_mpm",
        "year":          entry.year,
        "date_cell":     entry.date_cell,
        "closing_date":  entry.closing_date.isoformat(),
        "event_time_utc": event_time_utc,
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

    raw_record = BojCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BojCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.closing_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="JPY",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Japan",
        source_url=BOJ_MPM_CALENDAR_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
