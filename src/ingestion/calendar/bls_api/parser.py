"""Project a :class:`BLSObservation` into calendar storage records.

Pure functions — no DB, no HTTP. Callers feed in the observation plus
a snapshot timestamp; :mod:`projector` handles persistence.

Event-time note (P1 scaffold):

The BLS Public Data API returns observations *without* release dates.
The BLS release schedule is a separate HTML feed (``bls.gov/schedule/``)
that P1a will scrape and merge in. For P1-scaffold rows we therefore
use the **end of the reference period** as a placeholder
``event_time_utc`` and mark ``event_time_precision='approximate'``.
When P1a lands the scheduled-release-time scraper, it overwrites these
rows with the true scheduled datetime and promotes the precision to
``'datetime'``.

This is explicit, not a silent hack — the ``'approximate'`` enum value
is exactly the schema affordance for "we know the release happened,
we don't yet have the wall-clock time".
"""

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
from ingestion.timeseries.scrapers.bls import BLSObservation

from .indicators import INDICATOR_REGISTRY, BLSIndicatorSpec

PROVIDER = "bls"


@dataclass(frozen=True)
class BLSCalendarRawRecord:
    """One row destined for ``cal_econ_raw``.

    Shape identical to :class:`ingestion.calendar.te_api.parser.CalendarRawRecord`.
    Not shared yet — deferring promotion to ``_official_shared`` until
    the third connector (BEA in P2) lands, per the discipline rule on
    abstractions.
    """

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BLSCalendarEventRecord:
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


# The single mutable field on a BLS calendar event is the value itself
# (plus the BLS footnote/revision metadata the observation carries).
# Hashing ``value`` alone is enough to detect revisions — BLS republishes
# the same (series, year, period) with a new value when a revision lands.
_HASH_FIELDS: tuple[str, ...] = ("value", "footnotes", "periodName")


def _content_hash(obs_dict: dict[str, Any]) -> str:
    parts = []
    for field in _HASH_FIELDS:
        value = obs_dict.get(field)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _end_of_reference_period(reference_ymd: str) -> date:
    """Return the last day of the reference period.

    ``BLSClient._normalize_period`` already maps BLS year + period to
    the *start* of the period for monthly data (e.g. ``"2024-04-01"``
    for April 2024) and to the *end* for quarterly / semi-annual /
    annual. Monthly needs promotion to month-end so that ``event_time_utc``
    sits after the reference window closed.
    """
    ref = date.fromisoformat(reference_ymd)
    last_day = calendar.monthrange(ref.year, ref.month)[1]
    return ref.replace(day=last_day) if ref.day == 1 else ref


def parse_observation(
    obs: BLSObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    spec: BLSIndicatorSpec | None = None,
) -> tuple[BLSCalendarRawRecord, BLSCalendarEventRecord]:
    """Convert a BLS observation into (raw, event) records.

    Parameters
    ----------
    obs:
        The parsed :class:`BLSObservation` returned by the BLS client.
    snapshot_epoch_ms:
        Fetch-time anchor; stored verbatim on the raw record so
        revisions diff by snapshot.
    observed_at_epoch_ms:
        Usually equal to ``snapshot_epoch_ms``. Separable for tests
        and replay paths.
    raw_payload:
        The original BLS observation dict (with ``year`` / ``period`` /
        ``value`` / ``footnotes``) when available. Used for the content
        hash + persisted verbatim in ``cal_econ_raw.payload_json``.
        Reconstructed from the typed observation when absent.
    spec:
        Indicator metadata. Resolved from :data:`INDICATOR_REGISTRY`
        when absent.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in BLS INDICATOR_REGISTRY"
        )

    # Hash + raw audit row both need the full upstream dict. Preference
    # order:
    #   1. Caller-supplied ``raw_payload`` (explicit replay paths).
    #   2. ``obs.raw`` populated by the live BLS client (includes
    #      ``footnotes`` + ``periodName`` + ``latest`` — fields
    #      _HASH_FIELDS mutates on).
    #   3. Minimal reconstruction from the typed fields (test fixtures
    #      that build BLSObservation manually). A footnote-only BLS
    #      revision that lands via path (3) will *not* register as a
    #      new raw row — acceptable only in test paths.
    if raw_payload is None:
        if obs.raw:
            raw_payload = dict(obs.raw)
        else:
            raw_payload = {
                "year": obs.date.split("-")[0],
                "period": obs.period,
                "value": str(obs.value),
            }
    payload_with_sid = {"seriesID": obs.series_id, **raw_payload}
    content_hash = _content_hash(raw_payload)
    payload_json = json.dumps(payload_with_sid, sort_keys=True, ensure_ascii=False)

    ref_end = _end_of_reference_period(obs.date)
    event_time_utc = datetime(
        ref_end.year, ref_end.month, ref_end.day,
        tzinfo=timezone.utc,
    ).isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    # Anchor on the reference-period start (BLS-canonical "YYYY-MM-01"
    # for monthly series), NOT ``event_time_utc``. The P1 placeholder
    # ``event_time_utc`` is the reference month-end; P1a will overwrite
    # it with the scraper-sourced scheduled release datetime. Anchoring
    # on ``obs.date`` keeps the id stable across that promotion so the
    # projector upserts the same row instead of inserting a duplicate.
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
        snapshot_epoch_ms / 1000, tz=timezone.utc
    ).isoformat()

    raw_record = BLSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BLSCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=obs.date,
        reference_label=obs.period,
        country_code=resolved_spec.country_code,
        indicator_id=None,         # resolved by a later alias-learning pass
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
        source="BLS",
        source_url=f"https://data.bls.gov/timeseries/{obs.series_id}",
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record


# Export asdict for tests that want to inspect record contents as a dict
# without reaching into dataclass internals. Not part of the public API.
_asdict = asdict
