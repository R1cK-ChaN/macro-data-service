"""Project a scraped FOMC meeting entry into calendar storage records.

Pure functions — no DB, no HTTP. The scraper (:mod:`scraper`) produces
:class:`FomcMeetingEntry` values; this module turns each one into a
``(raw, event)`` tuple; :mod:`projector` handles persistence.

Event-time shape (P4 scaffold):

The Federal Reserve announces every FOMC rate decision at **14:00 ET
on the closing day of the meeting**. This time has held continuously
since 2013 and is the shape traders price against. The scraper extracts
the closing day; this module combines it with the 14:00 ET convention
through :func:`_official_shared.parse_scheduled_release_time` to compute
a DST-correct UTC timestamp.

``provider_event_id`` anchors on the closing-day ISO date so the id is
stable across the schedule → value upgrade lifecycle (P4a will ship a
value-side pass that scrapes the statement / implementation note for
the actual target-range number; it upserts on the same id).
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

from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "federal-reserve"
FOMC_RELEASE_TZ = "America/New_York"
FOMC_RELEASE_TIME_LOCAL = "2:00 PM"
FOMC_CALENDAR_URL = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)


@dataclass(frozen=True)
class FomcMeetingEntry:
    """One meeting row parsed from ``fomccalendars.htm``.

    ``closing_date`` is the day the rate decision is announced —
    the last digit in the upstream "27-28" / "17-18*" date column
    (or the sole day for one-day meetings). ``has_sep`` is ``True``
    when the upstream row carries an asterisk next to the date or
    a "Projection Materials" block; it's surfaced as metadata for
    P4a's follow-up but does not produce a separate event in this
    scaffold (SEP shares the meeting's 14:00 ET announcement time,
    so a single FOMC_RATE row already covers the trader-impact
    flagship event).
    """

    year: int
    month_name: str                 # "January", "March", …
    date_cell: str                  # verbatim, "27-28" or "17-18*"
    closing_date: date              # ISO date of day 2 (rate decision day)
    has_sep: bool                   # meeting publishes SEP / dot-plot


@dataclass(frozen=True)
class FedCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class FedCalendarEventRecord:
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
# stable so long as upstream doesn't reschedule; the asterisk toggling
# (SEP added / removed for a given meeting) counts as a revision.
_HASH_FIELDS: tuple[str, ...] = (
    "closing_date", "date_cell", "has_sep", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def meeting_entry_to_records(
    entry: FomcMeetingEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: FedIndicatorSpec | None = None,
) -> tuple[FedCalendarRawRecord, FedCalendarEventRecord]:
    """Project a :class:`FomcMeetingEntry` to (raw, event) records.

    Uses the canonical 14:00 ET announcement convention to compute
    ``event_time_utc``; DST is resolved against ``entry.closing_date``
    via :func:`parse_scheduled_release_time`. ``provider_event_id``
    hashes on the closing-date ISO string so the id is stable even if
    a later pass upgrades the title or fills in ``actual``.
    """
    resolved_spec = spec or INDICATOR_REGISTRY["FOMC_RATE"]

    scheduled = parse_scheduled_release_time(
        entry.closing_date,
        FOMC_RELEASE_TIME_LOCAL,
        default_tz=FOMC_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        entry.closing_date.isoformat(),
    )

    reference_label = f"{entry.month_name} {entry.year}"
    payload: dict[str, Any] = {
        "kind":          "fomc_meeting",
        "year":          entry.year,
        "month":         entry.month_name,
        "date_cell":     entry.date_cell,
        "closing_date":  entry.closing_date.isoformat(),
        "has_sep":       entry.has_sep,
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

    title = resolved_spec.title + (" + SEP" if entry.has_sep else "")

    raw_record = FedCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = FedCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=entry.closing_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=title,
        importance=resolved_spec.importance,
        currency="USD",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Federal Reserve",
        source_url=FOMC_CALENDAR_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
