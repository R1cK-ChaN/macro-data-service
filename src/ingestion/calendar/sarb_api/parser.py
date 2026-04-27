"""SARB repo-rate history JSON → calendar projection.

The SARB Web Indicators service exposes the repo-rate change history
at::

    https://custom.resbank.co.za/SarbWebApi/WebIndicators/Shared/
        GetTimeseriesObservations/MRDREPOR

The TsCode ``MRDREPOR`` is SARB's "Dates of change in the repurchase
rate" series. The endpoint returns a JSON array of observations, one
per row::

    [
      {
        "Period":      "2025-11-21T00:00:00",
        "Timeseries":  "Dates of change in the repurchase rate",
        "Description": "Dates of change in the repurchase rate",
        "Value":       6.75,
        "FormatNumber": "0.00",
        "FormatDate":  "yyyy-MM-dd"
      },
      ...
    ]

**Coverage is rate-change-only.** Hold decisions are absent from the
``MRDREPOR`` series — same shape as TCMB's 1-week repo history.
Reconstructing hold-meeting coverage and authoritative MPC
announcement dates needs the per-meeting ``mpc-statements`` PDF
archive scrape and is deferred to P2.

``Period`` is the **effective date** of the rate change. Under SARB's
modern cadence the MPC meets and announces on Thursday afternoon
(15:00 SAST documented), and the new rate takes effect the next
business day. The connector stores ``Period`` as the calendar
event's reference date verbatim; reconstructing the exact MPC
announcement date for every backfill row needs the per-meeting MPC
statement and is deferred to P2.

Time is set to 15:00 ``Africa/Johannesburg`` — SARB's documented
afternoon announcement window. South Africa observes UTC+2 year-round
(no DST), so the conversion to UTC is a fixed −2 hour offset for
every backfill row.

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the effective date. Daily parity matching against TE
(announcement-date convention) is intentionally **deferred** — the
``(ZA, SARB_RATE)`` pair is not on the parity whitelist in P1
because (a) the off-by-one drift between effective and announcement
dates would generate false MissingRelease alerts and (b) the change-
only coverage means TE's hold rows have no agency counterpart, which
would compound the false alerts. Same deferral pattern as the BoC
Valet / TCMB rate-history connectors.
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

from .indicators import INDICATOR_REGISTRY, SARBIndicatorSpec

PROVIDER = "sarb"
SARB_RELEASE_TZ = "Africa/Johannesburg"
# SARB publishes the MPC decision in the afternoon — documented at
# 15:00 SAST on Thursday under the modern cadence. Used as the
# default wall-clock release time when the per-decision
# ``event_time_utc`` is resolved.
SARB_RELEASE_TIME = "15:00"
SARB_BASE_URL = "https://custom.resbank.co.za"
SARB_RATE_HISTORY_URL = (
    f"{SARB_BASE_URL}/SarbWebApi/WebIndicators/Shared/"
    f"GetTimeseriesObservations/MRDREPOR"
)
# Public landing page — surfaced as ``source_url`` on the event row so
# an operator can browse the decision in context.
SARB_PUBLIC_HISTORY_URL = (
    "https://www.resbank.co.za/en/home/publications/statements/mpc-statements"
)


class SARBRateHistoryParseError(ValueError):
    """SARB rate-history JSON did not expose a parseable timeseries."""


@dataclass(frozen=True)
class SARBRateDecision:
    """One repo-rate change parsed from the SARB MRDREPOR JSON.

    ``effective_date`` is the ``Period`` field — the day the new rate
    takes effect, which under SARB's modern cadence falls one business
    day *after* the MPC announcement. Reconstructing the exact
    announcement date for every backfill row needs the per-meeting MPC
    statement; that scrape is deferred to P2.
    """

    effective_date: date
    rate: str                    # decimal string ("6.75")
    previous_rate: str | None    # rate before this decision (None for #1)


@dataclass(frozen=True)
class SARBCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class SARBCalendarEventRecord:
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


def _parse_period(value: str) -> date | None:
    """Convert a SARB ``Period`` ISO datetime to a :class:`date`.

    SARB returns ``"2025-11-21T00:00:00"`` — a date-anchored datetime
    with a midnight time component. Defensive against either the full
    datetime form or a bare ``YYYY-MM-DD`` (some adjacent SARB
    timeseries surface as plain dates).
    """
    text = (value or "").strip()
    if not text:
        return None
    if "T" in text:
        text = text.split("T", 1)[0]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_rate(value: Any) -> str | None:
    """Validate and normalise a SARB ``Value`` cell.

    SARB returns rates as JSON numbers (``6.75`` not ``"6.75"``). The
    parser accepts either a numeric type or a string and rounds the
    result through :class:`Decimal` quantised to two places — the SARB
    JSON declares ``"FormatNumber": "0.00"`` for the repo-rate series
    and downstream comparisons stay stable on the canonical 2-decimal
    string form.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        as_decimal = Decimal(text)
    except InvalidOperation as exc:
        raise SARBRateHistoryParseError(
            f"unparseable SARB rate value {value!r}",
        ) from exc
    return f"{as_decimal.quantize(Decimal('0.01')):.2f}"


def parse_repo_rate_history(
    payload: str | bytes | list[Any],
) -> list[SARBRateDecision]:
    """Walk the MRDREPOR JSON for every parseable rate-change row.

    Returns the decisions ordered most-recent-first (matches the
    captured fixture's display order). Raises
    :class:`SARBRateHistoryParseError` on payloads that aren't a JSON
    array (DOM / API drift signal) or when zero rows parse cleanly.
    Empty arrays are *not* legitimate here — the MRDREPOR series has
    been populated since the modern repo-rate regime began, so an
    empty response is itself a drift signal.
    """
    if isinstance(payload, (bytes, bytearray)):
        try:
            data = json.loads(payload.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SARBRateHistoryParseError(
                "SARB rate-history payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except ValueError as exc:
            raise SARBRateHistoryParseError(
                "SARB rate-history payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, list):
        data = payload
    else:
        raise SARBRateHistoryParseError(
            f"SARB rate-history payload type not supported: "
            f"{type(payload).__name__}",
        )

    if not isinstance(data, list):
        raise SARBRateHistoryParseError(
            "SARB rate-history response is not a JSON array — "
            "DOM/API drift",
        )

    parsed: list[tuple[date, str]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        effective = _parse_period(str(row.get("Period") or ""))
        if effective is None:
            continue
        try:
            rate = _normalize_rate(row.get("Value"))
        except SARBRateHistoryParseError:
            # A truncated / corrupted row must not nuke the whole list
            # — skip it and keep walking. Mirrors the TCMB / Banxico
            # parser defensive shape.
            continue
        if rate is None:
            continue
        parsed.append((effective, rate))

    if not parsed:
        raise SARBRateHistoryParseError(
            "SARB MRDREPOR timeseries parsed zero rate-change rows — "
            "layout drift",
        )

    parsed.sort(key=lambda r: r[0])
    decisions: list[SARBRateDecision] = []
    previous_rate: str | None = None
    for effective, rate in parsed:
        decisions.append(SARBRateDecision(
            effective_date=effective,
            rate=rate,
            previous_rate=previous_rate,
        ))
        previous_rate = rate

    decisions.sort(key=lambda d: d.effective_date, reverse=True)
    return decisions


_HASH_FIELDS: tuple[str, ...] = (
    "effective_date", "rate", "previous_rate",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: SARBRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: SARBIndicatorSpec | None = None,
) -> tuple[SARBCalendarRawRecord, SARBCalendarEventRecord]:
    """Project a :class:`SARBRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["SARB_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.effective_date,
        SARB_RELEASE_TIME,
        default_tz=SARB_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        decision.effective_date.isoformat(),
    )

    reference_label = decision.effective_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":           "sarb_rate_decision",
        "effective_date": decision.effective_date.isoformat(),
        "rate":           decision.rate,
        "previous_rate":  decision.previous_rate,
        "event_time_utc": event_time_utc,
        "ts_code":        "MRDREPOR",
        "source_url":     SARB_RATE_HISTORY_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = SARBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = SARBCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=decision.effective_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="ZAR",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="South African Reserve Bank",
        source_url=SARB_PUBLIC_HISTORY_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "SARB_BASE_URL",
    "SARB_PUBLIC_HISTORY_URL",
    "SARB_RATE_HISTORY_URL",
    "SARB_RELEASE_TIME",
    "SARB_RELEASE_TZ",
    "SARBCalendarEventRecord",
    "SARBCalendarRawRecord",
    "SARBRateDecision",
    "SARBRateHistoryParseError",
    "decision_to_records",
    "parse_repo_rate_history",
]
