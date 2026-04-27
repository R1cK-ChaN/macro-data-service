"""ONS timeseries JSON → calendar projection.

Each ONS timeseries JSON document carries ``years`` / ``quarters``
/ ``months`` lists. Inside each list, observations look like:

    {
      "date": "2026 MAR",
      "value": "3.3",
      "label": "2026 MAR",
      "year": "2026", "month": "March", "quarter": "",
      "sourceDataset": "MM23",
      "updateDate": "2026-04-21T23:00:00.000Z"
    }

Per indicator we read the most-recent observation from the spec's
``frequency`` list (``months`` for CPI / unemployment, ``quarters``
for QoQ GDP). The reference period maps to:

- monthly (CPI / UR): first day of the named month
  (``"2026 MAR"`` → ``date(2026, 3, 1)``).
- quarterly (GDP): first day of the named quarter
  (``"2025 Q4"`` → ``date(2025, 10, 1)``).

The release timestamp is built from ``updateDate`` converted to
UK local time (``Europe/London``); ONS publishes statistical
bulletins at 07:00 UK on the publication day, and ``updateDate``
typically lands at 23:00Z the night before (= 00:00 BST on the
release day). Anchoring the wall clock at 07:00 UK gives a
DST-correct UTC release timestamp that lines up with what TE
shows for the same release. ``parse_scheduled_release_time``
handles the BST/GMT switch.

``provider_event_id`` is the standard
``synthesize_event_id(provider, country, canonical, anchor)``
where ``anchor`` is the reference period ISO date — stable across
revision rounds (an ONS revision keeps the period; only ``value``
and ``updateDate`` change).
"""

from __future__ import annotations

import hashlib
import json
import re
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

from .indicators import INDICATOR_REGISTRY, ONSIndicatorSpec

PROVIDER = "ons"
ONS_RELEASE_TZ = "Europe/London"
ONS_RELEASE_TIME = "07:00"
ONS_BASE_URL = "https://www.ons.gov.uk"


class ONSTimeseriesParseError(ValueError):
    """ONS JSON did not expose a parseable headline observation."""


@dataclass(frozen=True)
class ONSCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ONSCalendarEventRecord:
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
class ONSValueObservation:
    """One headline ONS observation extracted from a timeseries JSON."""

    indicator: str
    release_date: str        # ISO date — UK local date of the release
    reference_date: str      # ISO date — first day of the period
    reference_label: str     # human-readable ("March 2026", "Q4 2025")
    value: str
    update_date: str         # raw updateDate carried for audit
    source_url: str
    raw: dict[str, Any]


_MONTHS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_QUARTER_FIRST_MONTH: dict[str, int] = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}

_MONTH_DATE_RE = re.compile(r"^\s*(\d{4})\s+([A-Z]{3,4})\s*$")
_QUARTER_DATE_RE = re.compile(r"^\s*(\d{4})\s+(Q[1-4])\s*$")


def _reference_anchor(period: str, frequency: str) -> tuple[date, str]:
    """Return ``(reference_date, label)`` for an ONS period token."""
    if frequency == "months":
        m = _MONTH_DATE_RE.match(period.upper())
        if not m:
            raise ONSTimeseriesParseError(
                f"unparseable monthly period: {period!r}"
            )
        year = int(m.group(1))
        month = _MONTHS.get(m.group(2)[:3])
        if month is None:
            raise ONSTimeseriesParseError(
                f"unknown month token in period: {period!r}"
            )
        ref = date(year, month, 1)
        label = ref.strftime("%B %Y")
        return ref, label
    if frequency == "quarters":
        m = _QUARTER_DATE_RE.match(period.upper())
        if not m:
            raise ONSTimeseriesParseError(
                f"unparseable quarterly period: {period!r}"
            )
        year = int(m.group(1))
        first_month = _QUARTER_FIRST_MONTH[m.group(2)]
        ref = date(year, first_month, 1)
        label = f"{m.group(2)} {year}"
        return ref, label
    raise ONSTimeseriesParseError(f"unsupported frequency: {frequency!r}")


def _release_date_uk(update_date_raw: str) -> date:
    """Convert ``updateDate`` (UTC ISO) to the UK-local release date.

    ONS publishes at 07:00 UK on the release day; the JSON's
    ``updateDate`` typically lands at 23:00Z the night before
    (= 00:00 BST). Converting to ``Europe/London`` and taking the
    date component yields the publication-day date in both DST
    halves of the year.
    """
    cleaned = update_date_raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(ZoneInfo(ONS_RELEASE_TZ)).date()


def parse_timeseries_json(
    payload: dict[str, Any] | str | bytes,
    *,
    spec: ONSIndicatorSpec,
) -> ONSValueObservation:
    """Pick the headline observation out of an ONS timeseries JSON.

    Reads from ``payload[spec.frequency]`` (``"months"`` or
    ``"quarters"``) and returns the most-recent observation. Raises
    :class:`ONSTimeseriesParseError` when the JSON shape is missing
    fields the projector needs.
    """
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ONSTimeseriesParseError("ONS timeseries payload is not a JSON object")
    series = payload.get(spec.frequency)
    if not isinstance(series, list) or not series:
        raise ONSTimeseriesParseError(
            f"ONS {spec.indicator}: no '{spec.frequency}' observations in payload"
        )
    # Latest observation is the last entry — ONS publishes oldest-first.
    latest: dict[str, Any] | None = None
    for entry in reversed(series):
        if not isinstance(entry, dict):
            continue
        value_raw = entry.get("value")
        if value_raw in (None, ""):
            continue
        latest = entry
        break
    if latest is None:
        raise ONSTimeseriesParseError(
            f"ONS {spec.indicator}: no observation carries a value"
        )
    period = str(latest.get("date") or "").strip()
    if not period:
        raise ONSTimeseriesParseError(
            f"ONS {spec.indicator}: latest observation missing 'date'"
        )
    update_date_raw = str(latest.get("updateDate") or "").strip()
    if not update_date_raw:
        raise ONSTimeseriesParseError(
            f"ONS {spec.indicator}: latest observation missing 'updateDate'"
        )
    value_str = str(latest["value"]).strip()
    try:
        Decimal(value_str)
    except InvalidOperation as exc:
        raise ONSTimeseriesParseError(
            f"ONS {spec.indicator}: unparseable value {value_str!r}"
        ) from exc

    reference_date, reference_label = _reference_anchor(period, spec.frequency)
    release_date = _release_date_uk(update_date_raw)
    source_url = (
        f"{ONS_BASE_URL}/{spec.path}/timeseries/{spec.ts_id}/{spec.dataset_id}"
    )

    return ONSValueObservation(
        indicator=spec.indicator,
        release_date=release_date.isoformat(),
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        value=value_str,
        update_date=update_date_raw,
        source_url=source_url,
        raw={
            "date": period,
            "value": value_str,
            "updateDate": update_date_raw,
            "sourceDataset": latest.get("sourceDataset"),
        },
    )


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "value", "update_date",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def value_observation_to_records(
    obs: ONSValueObservation,
    *,
    snapshot_epoch_ms: int,
    spec: ONSIndicatorSpec | None = None,
) -> tuple[ONSCalendarRawRecord, ONSCalendarEventRecord]:
    """Project one observation onto (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {obs.indicator!r} not in ONS INDICATOR_REGISTRY"
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
        release_day, ONS_RELEASE_TIME, default_tz=ONS_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    payload: dict[str, Any] = {
        "kind":            "ons_timeseries_value",
        "indicator":       resolved_spec.indicator,
        "release_date":    obs.release_date,
        "reference_date":  obs.reference_date,
        "reference_label": obs.reference_label,
        "value":           obs.value,
        "update_date":     obs.update_date,
        "source_url":      obs.source_url,
        "ts_id":           resolved_spec.ts_id,
        "dataset_id":      resolved_spec.dataset_id,
        "raw":             obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = ONSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ONSCalendarEventRecord(
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
        currency="GBP",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="UK Office for National Statistics",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "ONSCalendarEventRecord",
    "ONSCalendarRawRecord",
    "ONSTimeseriesParseError",
    "ONSValueObservation",
    "ONS_RELEASE_TIME",
    "ONS_RELEASE_TZ",
    "PROVIDER",
    "parse_timeseries_json",
    "value_observation_to_records",
]
