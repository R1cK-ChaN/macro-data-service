"""Parse 22-field TE calendar rows into storage records.

Pure functions: no DB, no HTTP. Callers feed in a raw dict and a snapshot
timestamp; callers drive persistence separately in :mod:`projector`.

Content hash is computed over the six fields TE revises
(``Actual, Previous, Revised, Forecast, TEForecast, LastUpdate``). Two rows
with identical mutable-field values produce identical hashes; any
revision produces a new hash → a new ``cal_econ_raw`` row.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ingestion._shared.common import IMPORTANCE_MAP
from ingestion.calendar.scrapers.tradingeconomics import TE_COUNTRY_MAP

PROVIDER = "tradingeconomics"

# The six fields TE mutates between snapshots. Ordering is stable; changes
# here require re-hashing historical raw rows.
MUTABLE_FIELDS: tuple[str, ...] = (
    "Actual",
    "Previous",
    "Revised",
    "Forecast",
    "TEForecast",
    "LastUpdate",
)

ALL_TE_FIELDS: frozenset[str] = frozenset(
    {
        "CalendarId",
        "Date",
        "Country",
        "Category",
        "Event",
        "Reference",
        "ReferenceDate",
        "Source",
        "SourceURL",
        "Actual",
        "Previous",
        "Forecast",
        "TEForecast",
        "Revised",
        "URL",
        "DateSpan",
        "Importance",
        "Currency",
        "Unit",
        "Ticker",
        "Symbol",
        "LastUpdate",
    }
)


@dataclass(frozen=True)
class CalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class CalendarEventRecord:
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


def row_content_hash(row: dict[str, Any]) -> str:
    """SHA256 over the six mutable fields, in fixed order, joined by ``|``.

    Missing keys are treated as empty strings (TE sometimes omits a field
    rather than returning an empty string — both normalize to the same
    hash). NULL is likewise normalized to ``""`` so a field flipping
    between missing and empty doesn't spuriously register as a revision.
    """
    parts = []
    for field in MUTABLE_FIELDS:
        value = row.get(field)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _iso_to_epoch_ms(value: str | None) -> int | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _country_code(raw_country: str) -> str:
    key = (raw_country or "").strip().lower()
    code = TE_COUNTRY_MAP.get(key)
    if code:
        return code
    # Fallback: uppercase 2-letter slice for unknown countries. Lets
    # unmapped TE countries still reach the DB (we add to TE_COUNTRY_MAP
    # when observed).
    return (raw_country or "").strip().upper()[:2] or ""


def _stringify_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def parse_calendar_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarRawRecord, CalendarEventRecord]:
    """Convert a single 22-field TE row into (raw, event) records.

    Parameters
    ----------
    row:
        The raw TE JSON dict as returned by ``/calendar/*``.
    snapshot_epoch_ms:
        Timestamp attached to the raw row (usually the per-request fetch
        time in UTC ms). All rows from the same HTTP response share a
        snapshot timestamp so revisions are diff-able by ``snapshot``.
    observed_at_epoch_ms:
        Usually equal to ``snapshot_epoch_ms``; separable for tests and
        for backfilled replays.
    """
    calendar_id = row.get("CalendarId")
    if calendar_id is None:
        raise ValueError("TE row missing CalendarId")
    provider_event_id = str(calendar_id)

    content_hash = row_content_hash(row)
    payload_json = json.dumps(row, sort_keys=True, ensure_ascii=False)

    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms
    fetched_at = datetime.fromtimestamp(snapshot_epoch_ms / 1000, tz=timezone.utc).isoformat()

    raw_record = CalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    importance_int = row.get("Importance")
    importance: str | None
    if isinstance(importance_int, int):
        importance = IMPORTANCE_MAP.get(importance_int)
    else:
        importance = None

    last_update_ms = _iso_to_epoch_ms(row.get("LastUpdate"))
    event_time = str(row.get("Date") or "").strip()
    event_record = CalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time,
        event_time_precision="datetime" if "T" in event_time else "date",
        reference_date=_stringify_optional(row.get("ReferenceDate")),
        reference_label=str(row.get("Reference") or ""),
        country_code=_country_code(str(row.get("Country") or "")),
        indicator_id=None,  # resolved by a later slice's alias-learning pass
        category=str(row.get("Category") or ""),
        title=str(row.get("Event") or ""),
        importance=importance,
        currency=str(row.get("Currency") or ""),
        unit=str(row.get("Unit") or ""),
        actual=_stringify_optional(row.get("Actual")),
        previous=_stringify_optional(row.get("Previous")),
        revised=_stringify_optional(row.get("Revised")),
        forecast=_stringify_optional(row.get("TEForecast")),
        consensus_forecast=_stringify_optional(row.get("Forecast")),
        ticker=str(row.get("Ticker") or ""),
        source=str(row.get("Source") or ""),
        source_url=str(row.get("SourceURL") or ""),
        content_hash=content_hash,
        last_update_epoch_ms=last_update_ms,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
