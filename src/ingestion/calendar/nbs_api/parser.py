"""Project a scraped NBS release entry into calendar storage records.

Pure functions — no DB, no HTTP. The scraper (:mod:`scraper`) produces
:class:`NBSReleaseEntry` values; this module turns each one into a
``(raw, event)`` tuple; :mod:`projector` handles persistence.

Event-time shape (P5 scaffold):

NBS publishes a yearly calendar document (``Regular Press Release
Calendar of NBS in YYYY``) that lists every scheduled release with
day-of-month, day-of-week, and publish time. For the CPI anchor the
standing time is **09:30 Asia/Shanghai** on a fixed day each month.
The scraper pulls the entry verbatim; this parser combines date +
time through :func:`_official_shared.parse_scheduled_release_time`
to compute a DST-correct UTC timestamp (Asia/Shanghai doesn't
observe DST, but the helper still resolves the IANA zone correctly
in case a future indicator crosses into a DST-observing locale).

``provider_event_id`` anchors on the ISO release-date + canonical
indicator so the id is stable across the schedule → value upgrade
lifecycle (a P5a slice could scrape the published CPI value from
the corresponding press-release URL and upsert on the same id).
"""

from __future__ import annotations

import calendar as _calendar
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

from .indicators import NBSIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "nbs"
NBS_RELEASE_TZ = "Asia/Shanghai"
NBS_CALENDAR_URL_BASE = (
    "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
)


@dataclass(frozen=True)
class NBSReleaseEntry:
    """One scheduled release parsed from the NBS yearly calendar.

    Fields mirror what the yearly calendar table exposes: numeric
    year / month / day, the release time as a verbatim string
    (``"9:30"``), and the canonical indicator token the scraper
    matched the row on.

    ``date_cell`` carries the verbatim cell text as parsed (``"9/Fri"``,
    ``"4/Wed Note5"``). Any NBS footnote reference riding on the cell
    (``"Note5"``) lives here rather than in structured fields — it
    feeds into the raw payload so a note-only upstream revision
    produces a different ``content_hash`` and doesn't silently lose
    the audit trail for an irregular release.
    """

    year: int
    month: int                      # 1-12
    day: int
    release_time_local: str         # verbatim "9:30" / "10:00"
    indicator: str                  # canonical token ("CPI")
    weekday_label: str = ""         # "Fri", "Mon" — audit only
    date_cell: str = ""             # verbatim cell ("9/Fri", "4/Wed Note5")


@dataclass(frozen=True)
class NBSCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class NBSCalendarEventRecord:
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


# Hash inputs for revision detection. Any of (release-date,
# release-time) changing = NBS rescheduled the release.
_HASH_FIELDS: tuple[str, ...] = (
    "release_date", "release_time_local", "event_time_utc", "indicator",
    "date_cell",
)

# For indicators with ``reference_cadence="quarterly"`` (currently
# GDP only — see ``indicators.py``). Each map key is the release
# month; the value is ``(reference_year_delta, reference_quarter)``
# where ``reference_year_delta`` is added to the release year to get
# the reference year. January's quarterly release covers the prior
# year's Q4 (year_delta = -1); April / July / October cover the
# immediately preceding quarter of the current year (year_delta = 0).
_QUARTERLY_RELEASE_MAP: dict[int, tuple[int, int]] = {
    1:  (-1, 4),
    4:  (0,  1),
    7:  (0,  2),
    10: (0,  3),
}
_QUARTER_END_MONTH: dict[int, int] = {1: 3, 2: 6, 3: 9, 4: 12}


def _quarterly_reference(release_date: date) -> tuple[str, str]:
    """Translate a quarterly-cadence release date to its reference period.

    Returns ``(reference_date_iso, reference_label)``. The reference
    date is the last day of the reference quarter (``"2025-12-31"`` for
    Q4 2025); the label follows the ``"YYYY-QN"`` shape so downstream
    consumers (parity harness, UI) distinguish quarterly from monthly
    buckets at a glance.

    Release months outside the quarterly set fall back to release-
    month-first semantics — defence-in-depth against a scraper-side
    regression that lets an off-cadence month slip through the
    ``publishing_months`` filter on the spec.
    """
    mapping = _QUARTERLY_RELEASE_MAP.get(release_date.month)
    if mapping is None:
        reference_date = date(release_date.year, release_date.month, 1)
        return reference_date.isoformat(), release_date.strftime("%Y-%m")
    year_delta, quarter = mapping
    year = release_date.year + year_delta
    month = _QUARTER_END_MONTH[quarter]
    day = _calendar.monthrange(year, month)[1]
    return date(year, month, day).isoformat(), f"{year}-Q{quarter}"


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def release_entry_to_records(
    entry: NBSReleaseEntry,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: NBSIndicatorSpec | None = None,
    calendar_url: str | None = None,
) -> tuple[NBSCalendarRawRecord, NBSCalendarEventRecord]:
    """Project an :class:`NBSReleaseEntry` to (raw, event) records.

    ``calendar_url`` is the specific yearly-calendar page the entry
    came from — preserved on the event record so the audit trail
    points back at the source document rather than the generic
    release-calendar index.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {entry.indicator!r} not in NBS INDICATOR_REGISTRY"
        )

    release_date = date(year=entry.year, month=entry.month, day=entry.day)
    scheduled = parse_scheduled_release_time(
        release_date,
        entry.release_time_local,
        default_tz=NBS_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        release_date.isoformat(),
    )

    if resolved_spec.reference_cadence == "quarterly":
        reference_date_iso, reference_label = _quarterly_reference(release_date)
    else:
        reference_date_iso = date(entry.year, entry.month, 1).isoformat()
        reference_label = release_date.strftime("%Y-%m")

    payload: dict[str, Any] = {
        "kind":               "nbs_scheduled_release",
        "indicator":          resolved_spec.indicator,
        "year":               entry.year,
        "month":              entry.month,
        "day":                entry.day,
        "release_date":       release_date.isoformat(),
        "release_time_local": entry.release_time_local,
        "event_time_utc":     event_time_utc,
        "weekday_label":      entry.weekday_label,
        "date_cell":          entry.date_cell,
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

    source_url = calendar_url or NBS_CALENDAR_URL_BASE

    raw_record = NBSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = NBSCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=reference_date_iso,
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="CNY",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="National Bureau of Statistics of China",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record
