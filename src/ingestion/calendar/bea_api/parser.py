"""Project a :class:`BEAObservation` into calendar storage records.

Pure functions — no DB, no HTTP. Callers feed in the observation plus
a snapshot timestamp; :mod:`projector` handles persistence.

Event-time note (P2 scaffold):

The BEA REST API returns observations *without* a release datetime.
BEA's release schedule lives in HTML at ``bea.gov/news/schedule`` and
will be scraped by a later P2a slice (mirroring the BLS P1 → P1a
split). For P2-scaffold rows we therefore use the **end of the
reference period** as a placeholder ``event_time_utc`` and mark
``event_time_precision='approximate'``. When the schedule scraper
lands, its write path will upgrade the placeholder to the true
scheduled datetime on the same ``provider_event_id``; the projector's
cross-source merge rule (shared with BLS) preserves the upgrade.

The reference-period date is the anchor for ``provider_event_id``
(not ``event_time_utc``): quarterly observations already arrive as
end-of-quarter via :meth:`BEAClient._normalize_time_period` so the
anchor is stable across the value → value+schedule lifecycle.
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
from ingestion.timeseries.scrapers.bea import BEAObservation

from .indicators import INDICATOR_REGISTRY, BEAIndicatorSpec

PROVIDER = "bea"


@dataclass(frozen=True)
class BEACalendarRawRecord:
    """One row destined for ``cal_econ_raw``.

    Shape identical to :class:`ingestion.calendar.bls_api.parser.BLSCalendarRawRecord`.
    Kept local because the three connectors (TE / BLS / BEA) are still
    within the "two concrete callers" rule; promotion of the record
    shape and the projector into ``_official_shared`` happens once a
    P3-era caller makes the abstraction pay for itself.
    """

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BEACalendarEventRecord:
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


# Fields mutated when BEA republishes a line: the value itself, plus
# the line-description (BEA occasionally relabels lines) and NoteRef
# (which flags methodology notes / release-stage indicators). Hashing
# these detects a revision even if the value is unchanged.
_HASH_FIELDS: tuple[str, ...] = ("DataValue", "LineDescription", "NoteRef")


def _content_hash(obs_dict: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = obs_dict.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _end_of_reference_period(reference_ymd: str, frequency: str) -> date:
    """Return the last day of the reference period.

    ``BEAClient._normalize_time_period`` already maps quarterly periods
    to end-of-quarter (``"2024Q1"`` → ``"2024-03-31"``) and annual to
    end-of-year. Only monthly (``"YYYY-MM-01"``) needs promotion to
    month-end so ``event_time_utc`` sits after the reference window
    closed — the same convention BLS uses.
    """
    ref = date.fromisoformat(reference_ymd)
    if frequency == "M" and ref.day == 1:
        last_day = calendar.monthrange(ref.year, ref.month)[1]
        return ref.replace(day=last_day)
    return ref


def parse_observation(
    obs: BEAObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    raw_payload: dict[str, Any] | None = None,
    spec: BEAIndicatorSpec | None = None,
) -> tuple[BEACalendarRawRecord, BEACalendarEventRecord]:
    """Convert a BEA observation into (raw, event) records.

    Parameters
    ----------
    obs:
        The parsed :class:`BEAObservation` returned by the BEA client.
    snapshot_epoch_ms:
        Fetch-time anchor; stored verbatim on the raw record so
        revisions diff by snapshot.
    observed_at_epoch_ms:
        Usually equal to ``snapshot_epoch_ms``. Separable for tests
        and replay paths.
    raw_payload:
        The original BEA row dict (with ``TimePeriod`` / ``DataValue`` /
        ``LineNumber`` / ``LineDescription`` / ``NoteRef``). Used for
        the content hash and persisted verbatim in
        ``cal_econ_raw.payload_json``. Falls back to ``obs.raw`` when
        absent; minimal reconstruction from the typed fields when both
        are missing (test fixtures — BEA footnote / NoteRef changes
        won't register on that path).
    spec:
        Indicator metadata. Resolved from :data:`INDICATOR_REGISTRY`
        when absent.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in BEA INDICATOR_REGISTRY"
        )

    # Preference order mirrors BLS:
    #   1. Caller-supplied ``raw_payload`` (explicit replay paths).
    #   2. ``obs.raw`` populated by the live BEA client.
    #   3. Minimal reconstruction from the typed fields (test fixtures
    #      that build BEAObservation manually). A NoteRef-only revision
    #      that lands via path (3) will *not* register a new raw row —
    #      accepted only in test paths.
    if raw_payload is None:
        if obs.raw:
            raw_payload = dict(obs.raw)
        else:
            raw_payload = {
                "TimePeriod": obs.date,
                "DataValue": "" if obs.value is None else str(obs.value),
                "LineNumber": obs.line_number,
                "LineDescription": obs.line_description,
            }
    payload_with_sid = {"seriesID": obs.series_id, **raw_payload}
    content_hash = _content_hash(raw_payload)
    payload_json = json.dumps(payload_with_sid, sort_keys=True, ensure_ascii=False)

    ref_end = _end_of_reference_period(obs.date, resolved_spec.frequency)
    event_time_utc = datetime(
        ref_end.year, ref_end.month, ref_end.day,
        tzinfo=timezone.utc,
    ).isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    # Anchor on ``obs.date`` (the BEA-canonical reference-period end or
    # start — stable per-period and set by ``_normalize_time_period``).
    # Same reasoning as BLS: the placeholder ``event_time_utc`` will be
    # overwritten when P2a's schedule scraper lands, so anchoring the
    # id there would cycle and produce duplicates on promotion.
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

    raw_record = BEACalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )

    event_record = BEACalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=obs.date,
        reference_label=obs.date,
        country_code=resolved_spec.country_code,
        indicator_id=None,         # resolved by a later alias-learning pass
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=None if obs.value is None else str(obs.value),
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="BEA",
        source_url=(
            f"https://apps.bea.gov/api/data?DatasetName={resolved_spec.dataset}"
            f"&TableName={resolved_spec.table}"
        ),
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )

    return raw_record, event_record


# Export asdict for tests that want to inspect record contents as a dict
# without reaching into dataclass internals. Not part of the public API.
_asdict = asdict
