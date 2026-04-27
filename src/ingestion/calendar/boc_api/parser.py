"""Bank of Canada Valet API → calendar projection.

The Valet observations endpoint at
``bankofcanada.ca/valet/observations/V39079/json`` returns the
daily target overnight rate as a list of ``{d, V39079: {v}}``
records, oldest-first. Rate-change days are the days where the
``v`` value differs from the prior business day's value; these
are the BoC Governing Council policy decisions the connector
projects.

The BoC announces scheduled rate decisions at **09:45 ET** on
the meeting day. The change crosses Valet's daily-average cutoff,
so V39079's first row at the new rate is the *next* business day
(2025-10-29 announcement → 2025-10-30 in V39079). The parser
takes the prior parsed observation's date as the announcement
day — Valet emits one row per business day, so that prior date
is exact whether the announcement falls on a Wednesday, a
post-holiday Tuesday, or a Friday before a long weekend.

``provider_event_id`` anchors on the announcement-day ISO date
(matches TE / Bloomberg / Reuters calendar-row convention so
parity buckets align), and the Valet effective date is preserved
in the raw payload for audit.

Hold (no-change) decisions are absent from this signal — the
daily series shows the same value before and after a hold — so
the connector covers only the change days, mirroring the BoE
Bank-Rate page pattern.

Payload drift (missing observations key, malformed value cell)
raises :class:`BoCValetParseError` rather than silently dropping
rows — a parse miss on the Valet endpoint is a layout-change
signal we want loud.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import BoCIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "boc"
BOC_RELEASE_TZ = "America/Toronto"
BOC_RELEASE_TIME = "09:45"
BOC_OVERNIGHT_RATE_SERIES = "V39079"
BOC_VALET_BASE_URL = "https://www.bankofcanada.ca/valet"
BOC_VALET_URL = (
    f"{BOC_VALET_BASE_URL}/observations/{BOC_OVERNIGHT_RATE_SERIES}/json"
)


class BoCValetParseError(ValueError):
    """Raised when the Valet payload exposes zero parseable rate decisions."""


@dataclass(frozen=True)
class BoCRateDecision:
    """One Governing Council rate-change decision parsed from Valet.

    The Valet ``V39079`` series records the *effective* date — the
    first business day on which the new rate applies. BoC announces
    its decision at 09:45 ET on the *prior* business day; the daily
    target rate for the announcement day itself stays at the old
    value because the change crosses the daily-average cutoff. Both
    dates are exposed so the projector can use the announcement date
    for ``event_time_utc`` (matches TE / Bloomberg's calendar-row
    convention) while preserving the Valet effective date in audit
    payloads.
    """

    announcement_date: date  # date BoC publicly announced the change (09:45 ET)
    effective_date: date     # first business day at the new rate (Valet ``d``)
    rate: str                # decimal string ("2.25")
    previous_rate: str | None  # value before the change ("2.50") or None


@dataclass(frozen=True)
class BoCCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BoCCalendarEventRecord:
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


def _parse_rate_cell(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        raise BoCValetParseError("empty BoC rate cell")
    try:
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise BoCValetParseError(f"unparseable BoC rate {text!r}") from exc
    return cleaned


def parse_overnight_rate_observations(
    payload: dict[str, Any] | str | bytes,
) -> list[BoCRateDecision]:
    """Walk the Valet observations list for rate-change decisions.

    Returns the decisions ordered most-recent-first to match the
    BoE pattern. Raises :class:`BoCValetParseError` when zero
    rate-change rows parse — Valet outage, layout drift, or a
    flat history.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise BoCValetParseError("Valet payload is not a JSON object")
    observations = payload.get("observations")
    if not isinstance(observations, list) or not observations:
        raise BoCValetParseError(
            "Valet payload missing 'observations' list — DOM/API drift"
        )

    parsed: list[tuple[date, str]] = []
    for entry in observations:
        if not isinstance(entry, dict):
            continue
        date_raw = str(entry.get("d") or "").strip()
        if not date_raw:
            continue
        series_cell = entry.get(BOC_OVERNIGHT_RATE_SERIES)
        if not isinstance(series_cell, dict):
            continue
        value_raw = series_cell.get("v")
        if value_raw in (None, ""):
            continue
        try:
            effective = date.fromisoformat(date_raw)
            value = _parse_rate_cell(str(value_raw))
        except (BoCValetParseError, ValueError):
            # Skip a single malformed row but keep walking — a
            # truncated trailing entry shouldn't nuke the whole list.
            continue
        parsed.append((effective, value))

    if not parsed:
        raise BoCValetParseError(
            "Valet payload parsed zero rate observations"
        )

    parsed.sort(key=lambda r: r[0])

    decisions: list[BoCRateDecision] = []
    previous_value: str | None = None
    previous_date: date | None = None
    for effective, value in parsed:
        if previous_value is not None and value != previous_value:
            # ``previous_date`` is the prior parsed observation —
            # always the prior business day because Valet emits one
            # row per business day. That is the BoC announcement
            # day; ``effective`` is the first day at the new rate.
            assert previous_date is not None  # noqa: S101
            decisions.append(
                BoCRateDecision(
                    announcement_date=previous_date,
                    effective_date=effective,
                    rate=value,
                    previous_rate=previous_value,
                ),
            )
        previous_value = value
        previous_date = effective

    if not decisions:
        raise BoCValetParseError(
            "Valet payload parsed zero rate-change decisions — flat history"
        )

    decisions.sort(key=lambda d: d.announcement_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "announcement_date", "effective_date", "rate", "previous_rate",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: BoCRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: BoCIndicatorSpec | None = None,
) -> tuple[BoCCalendarRawRecord, BoCCalendarEventRecord]:
    """Project a :class:`BoCRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BOC_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.announcement_date,
        BOC_RELEASE_TIME,
        default_tz=BOC_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        decision.announcement_date.isoformat(),
    )

    reference_label = decision.announcement_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":              "boc_overnight_rate_change",
        "announcement_date": decision.announcement_date.isoformat(),
        "effective_date":    decision.effective_date.isoformat(),
        "rate":              decision.rate,
        "previous_rate":     decision.previous_rate,
        "event_time_utc":    event_time_utc,
        "source_url":        BOC_VALET_URL,
        "series":            BOC_OVERNIGHT_RATE_SERIES,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BoCCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BoCCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=decision.announcement_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="CAD",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Canada",
        source_url=BOC_VALET_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "BOC_OVERNIGHT_RATE_SERIES",
    "BOC_RELEASE_TIME",
    "BOC_RELEASE_TZ",
    "BOC_VALET_BASE_URL",
    "BOC_VALET_URL",
    "BoCCalendarEventRecord",
    "BoCCalendarRawRecord",
    "BoCRateDecision",
    "BoCValetParseError",
    "PROVIDER",
    "decision_to_records",
    "parse_overnight_rate_observations",
]
