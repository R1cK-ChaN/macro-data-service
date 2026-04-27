"""StatCan WDS payload → calendar projection.

The Web Data Service ``getDataFromVectorsAndLatestNPeriods`` endpoint
returns one entry per requested vector with this shape::

    {
      "status": "SUCCESS",
      "object": {
        "vectorId": 41690973,
        "productId": 18100004,
        "vectorDataPoint": [
          {
            "refPer": "2026-03-01",
            "value": 167.4,
            "decimals": 1,
            "releaseTime": "2026-04-20T08:30",
            "frequencyCode": 6,
            ...
          }
        ]
      }
    }

The latest observation per vector carries both the reference
period (``refPer``) and the release wall-clock (``releaseTime``,
ET local time, no timezone marker). The parser converts
``releaseTime`` to a UTC datetime via the canonical 08:30
``America/Toronto`` release convention so the resulting
``event_time_utc`` lines up with what TE shows for the same
release.

``provider_event_id`` is the standard
``synthesize_event_id(provider, country, canonical, anchor)``
keyed on the reference period, so the id stays stable across
revision rounds — StatCan revisions keep the period and update
``value``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, StatCanIndicatorSpec

PROVIDER = "statcan"
STATCAN_RELEASE_TZ = "America/Toronto"
STATCAN_RELEASE_TIME_DEFAULT = "08:30"
STATCAN_WDS_BASE_URL = "https://www150.statcan.gc.ca/t1/wds/rest"
STATCAN_TABLE_BASE_URL = "https://www150.statcan.gc.ca/t1/tbl1/en/tv.action"


class StatCanWDSParseError(ValueError):
    """StatCan WDS response did not expose a parseable observation."""


@dataclass(frozen=True)
class StatCanCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class StatCanCalendarEventRecord:
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
class StatCanValueObservation:
    """One headline StatCan observation pulled from a vector response."""

    indicator: str
    release_date: str        # ISO date — Eastern Canada local date of release
    release_time_local: str  # ``"HH:MM"`` 24h ET wall clock
    reference_date: str      # ISO date — first day of the period
    reference_label: str     # human-readable ("March 2026")
    value: str
    product_id: int
    source_url: str
    raw: dict[str, Any]


_FREQUENCY_LABEL = {
    "monthly":   "%B %Y",
    "quarterly": "Q%q %Y",       # placeholder; quarterly callers compute manually
}


def _reference_anchor(ref_per: str, frequency: str) -> tuple[date, str]:
    """Return ``(reference_date, label)`` for a StatCan period token.

    StatCan reports the reference period as an ISO date already
    anchored on the first day of the period (``2026-03-01`` for
    March 2026, ``2025-10-01`` for Q4 2025), so the conversion is
    straightforward — but quarterly series use a Q-prefixed label
    for display consistency with the ONS pattern.
    """
    try:
        ref = date.fromisoformat(ref_per)
    except ValueError as exc:
        raise StatCanWDSParseError(
            f"unparseable reference period: {ref_per!r}"
        ) from exc
    if frequency == "monthly":
        return ref, ref.strftime("%B %Y")
    if frequency == "quarterly":
        quarter = (ref.month - 1) // 3 + 1
        return ref, f"Q{quarter} {ref.year}"
    raise StatCanWDSParseError(f"unsupported frequency: {frequency!r}")


def _release_wallclock(release_time_raw: str) -> tuple[date, str]:
    """Split ``releaseTime`` (``"YYYY-MM-DDTHH:MM"`` ET local) into (date, ``"HH:MM"``).

    StatCan reports ``releaseTime`` without a timezone marker; the
    documented convention is Eastern Time, so the local wall clock
    is preserved as-is and the caller passes the date through
    :func:`parse_scheduled_release_time` with
    ``default_tz="America/Toronto"`` to land on a DST-correct UTC
    datetime.
    """
    cleaned = release_time_raw.strip()
    if "T" not in cleaned:
        raise StatCanWDSParseError(
            f"unparseable releaseTime: {release_time_raw!r}"
        )
    date_part, _, time_part = cleaned.partition("T")
    try:
        release_day = date.fromisoformat(date_part)
    except ValueError as exc:
        raise StatCanWDSParseError(
            f"unparseable releaseTime date: {release_time_raw!r}"
        ) from exc
    if not time_part:
        time_part = STATCAN_RELEASE_TIME_DEFAULT
    # WDS sometimes appends seconds; normalise to ``HH:MM``.
    time_part = ":".join(time_part.split(":")[:2])
    return release_day, time_part


def parse_vector_response(
    payload: dict[str, Any] | list[Any] | str | bytes,
    *,
    spec: StatCanIndicatorSpec,
) -> StatCanValueObservation:
    """Pick the headline observation out of one WDS vector response.

    The response is the JSON list returned by
    ``getDataFromVectorsAndLatestNPeriods``. The parser locates the
    entry whose ``object.vectorId`` matches ``spec.vector_id`` (so
    callers may pass a multi-vector batch and let the parser pick
    the right one) and reads the most-recent ``vectorDataPoint``.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        payload = json.loads(payload)
    entries = payload if isinstance(payload, list) else [payload]
    matching: dict[str, Any] | None = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        obj = entry.get("object")
        if not isinstance(obj, dict):
            continue
        if obj.get("vectorId") == spec.vector_id:
            matching = entry
            break
    if matching is None:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: vector "
            f"{spec.vector_id} not in response"
        )
    if matching.get("status") != "SUCCESS":
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: WDS status "
            f"{matching.get('status')!r} for vector {spec.vector_id}"
        )
    obj = matching["object"]
    points = obj.get("vectorDataPoint")
    if not isinstance(points, list) or not points:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: empty vectorDataPoint list"
        )
    # Latest observation is the last entry — WDS returns oldest-first
    # within the latest-N window; iterate in reverse to skip suppressed
    # rows. The WDS docs say ``statusCode`` is non-zero for suppressed
    # / preliminary / confidential observations, and ``value`` may also
    # arrive as the empty string when an estimate is withdrawn. Either
    # case must fall through to an earlier observation.
    latest: dict[str, Any] | None = None
    for entry in reversed(points):
        if not isinstance(entry, dict):
            continue
        value_raw = entry.get("value")
        if value_raw is None:
            continue
        if isinstance(value_raw, str) and not value_raw.strip():
            continue
        status_code = entry.get("statusCode")
        if status_code not in (None, 0):
            continue
        latest = entry
        break
    if latest is None:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: no observation carries a value"
        )

    ref_per_raw = str(latest.get("refPer") or "").strip()
    if not ref_per_raw:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: latest observation missing 'refPer'"
        )
    release_time_raw = str(latest.get("releaseTime") or "").strip()
    if not release_time_raw:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: latest observation missing 'releaseTime'"
        )
    value_str = str(latest["value"])
    try:
        Decimal(value_str)
    except InvalidOperation as exc:
        raise StatCanWDSParseError(
            f"StatCan {spec.indicator}: unparseable value {value_str!r}"
        ) from exc

    reference_date, reference_label = _reference_anchor(
        ref_per_raw, spec.frequency,
    )
    release_day, release_time_local = _release_wallclock(release_time_raw)
    product_id = int(obj.get("productId") or 0)
    source_url = (
        f"{STATCAN_TABLE_BASE_URL}?pid={product_id}"
        if product_id else STATCAN_WDS_BASE_URL
    )

    return StatCanValueObservation(
        indicator=spec.indicator,
        release_date=release_day.isoformat(),
        release_time_local=release_time_local,
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        value=value_str,
        product_id=product_id,
        source_url=source_url,
        raw={
            "refPer":      ref_per_raw,
            "value":       value_str,
            "releaseTime": release_time_raw,
            "vectorId":    spec.vector_id,
            "productId":   product_id,
        },
    )


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "value", "release_date",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def value_observation_to_records(
    obs: StatCanValueObservation,
    *,
    snapshot_epoch_ms: int,
    spec: StatCanIndicatorSpec | None = None,
) -> tuple[StatCanCalendarRawRecord, StatCanCalendarEventRecord]:
    """Project one observation onto (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {obs.indicator!r} not in StatCan INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        obs.reference_date,
    )

    release_day = date.fromisoformat(obs.release_date)
    scheduled = parse_scheduled_release_time(
        release_day,
        obs.release_time_local,
        default_tz=STATCAN_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    payload: dict[str, Any] = {
        "kind":              "statcan_wds_value",
        "indicator":         resolved_spec.indicator,
        "release_date":      obs.release_date,
        "release_time_local": obs.release_time_local,
        "reference_date":    obs.reference_date,
        "reference_label":   obs.reference_label,
        "value":             obs.value,
        "vector_id":         resolved_spec.vector_id,
        "product_id":        obs.product_id,
        "source_url":        obs.source_url,
        "raw":               obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = StatCanCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = StatCanCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=obs.reference_date,
        reference_label=obs.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="CAD",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Statistics Canada",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "STATCAN_RELEASE_TIME_DEFAULT",
    "STATCAN_RELEASE_TZ",
    "STATCAN_TABLE_BASE_URL",
    "STATCAN_WDS_BASE_URL",
    "StatCanCalendarEventRecord",
    "StatCanCalendarRawRecord",
    "StatCanValueObservation",
    "StatCanWDSParseError",
    "parse_vector_response",
    "value_observation_to_records",
]
