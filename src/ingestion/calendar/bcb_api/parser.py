"""Banco Central do Brasil Copom history JSON → calendar projection.

The BCB exposes the full Copom decision history as a single JSON
service at
``bcb.gov.br/api/servico/sitebcb/historicotaxasjuros``. The response
is a single object ``{"conteudo": [<decision>, ...]}`` whose array
spans every Copom meeting since #1 (26 June 1996). Each element
carries:

- ``NumeroReuniaoCopom`` — meeting number (1.0, 2.0, ...).
- ``ReuniaoExtraordinaria`` — bool, True for the rare intra-cycle
  extraordinary meeting (3 in the historical record).
- ``DataReuniaoCopom`` — the meeting's announcement date as an ISO
  timestamp ``"YYYY-MM-DDT03:00:00Z"``. The literal time is São Paulo
  midnight encoded as a UTC offset (Brazil dropped DST in 2019, so
  the offset is constant), so only the date portion is meaningful.
- ``Vies`` — bias (``"alta"`` / ``"baixa"`` / ``"neutro"`` / ``"n/a"``).
- ``DataInicioVigencia`` / ``DataFimVigencia`` — effective period of
  the decision. Like ``DataReuniaoCopom`` these encode São Paulo
  midnights; preserved verbatim in the audit payload.
- ``MetaSelic`` — the new target Selic rate (decimal, e.g. ``14.75``).
- ``TaxaSelicEfetivaVigencia`` / ``TaxaSelicEfetivaAnualizada`` —
  realised rates during the validity window; populated only after the
  validity period closes.
- ``descisaoMonocraticaPres`` — bool/None, True when the BCB president
  acted unilaterally outside a Copom meeting (8 in the historical
  record).

The parser materialises every row as one :class:`BCBRateDecision`,
including holds (``MetaSelic`` unchanged from the prior decision —
detected by chronological diff). The projector treats every decision
the same way: one calendar event per decision, with ``actual`` set to
the new ``MetaSelic`` and ``previous`` to the prior decision's rate.

Copom announces the decision after market close on the second day of
the meeting; the BCB documents 18:30 BRT as the conventional release
time. Brazil sits at UTC−3 year-round (DST abolished 2019), so the
``parse_scheduled_release_time`` against ``America/Sao_Paulo`` resolves
the historical 2008-2018 DST window for backfill rows.

``provider_event_id`` / ``event_time_utc`` / ``reference_date`` all
anchor on the announcement date so the parity bucket aligns with TE /
Bloomberg / Reuters convention. The ``DataInicioVigencia`` (effective
date — typically the next business day) is preserved verbatim in the
audit payload but does not influence the calendar event time.

Payload drift (``conteudo`` missing, malformed rate, empty array)
raises :class:`BCBCopomParseError` rather than silently dropping rows
— a parse miss on this surface is a layout-change signal we want loud.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, BCBIndicatorSpec

PROVIDER = "bcb"
BCB_RELEASE_TZ = "America/Sao_Paulo"
# Copom announces the rate decision after the second-day close, around
# 18:30 BRT (per BCB's documented release convention). Used as the
# default release time when ``parse_scheduled_release_time`` resolves
# the per-decision ``event_time_utc``.
BCB_RELEASE_TIME = "18:30"
BCB_BASE_URL = "https://www.bcb.gov.br"
BCB_COPOM_HISTORY_URL = (
    f"{BCB_BASE_URL}/api/servico/sitebcb/historicotaxasjuros"
)
BCB_COPOM_PUBLIC_URL = f"{BCB_BASE_URL}/controleinflacao/historicotaxasjuros"


class BCBCopomParseError(ValueError):
    """Copom history JSON did not expose a parseable decision array."""


@dataclass(frozen=True)
class BCBRateDecision:
    """One Copom rate decision parsed from the historical-rates service."""

    meeting_number: int          # ``NumeroReuniaoCopom`` rounded to int
    announcement_date: date      # date BCB publicly announced the decision
    effective_date: date | None  # ``DataInicioVigencia`` — first day at the new rate
    end_date: date | None        # ``DataFimVigencia`` — None for the current period
    rate: str                    # decimal string ("14.75")
    previous_rate: str | None    # rate before this decision (None for #1)
    bias: str                    # ``Vies`` — "alta" / "baixa" / "neutro" / "n/a"
    extraordinary: bool          # ``ReuniaoExtraordinaria``
    monocratic_president: bool   # ``descisaoMonocraticaPres`` (None coerced to False)


@dataclass(frozen=True)
class BCBCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BCBCalendarEventRecord:
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


_ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def _parse_announcement_date(value: str | None) -> date | None:
    """Take the date portion of a BCB ISO timestamp.

    BCB encodes dates as ``"YYYY-MM-DDT03:00:00Z"`` — the time is
    São Paulo midnight encoded as a UTC-offset (UTC−3 → 03:00 UTC).
    Only the date portion is meaningful; reading off the literal
    ``YYYY-MM-DD`` prefix avoids any timezone-arithmetic surprise on
    historical rows that span DST boundaries.
    """
    if not value:
        return None
    match = _ISO_DATE_RE.match(value)
    if match is None:
        return None
    try:
        return date(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
        )
    except ValueError:
        return None


def _parse_rate(value: Any) -> str:
    if value is None:
        raise BCBCopomParseError("missing MetaSelic on Copom decision")
    text = str(value).strip()
    try:
        Decimal(text)
    except InvalidOperation as exc:
        raise BCBCopomParseError(
            f"unparseable Copom MetaSelic {value!r}",
        ) from exc
    return text


def parse_copom_history(payload: str | bytes | dict[str, Any]) -> list[BCBRateDecision]:
    """Walk the Copom history JSON for every parseable rate decision.

    Returns the decisions ordered most-recent-first. Raises
    :class:`BCBCopomParseError` when the response shape is malformed
    (missing ``conteudo``, empty array, every row malformed).
    """
    if isinstance(payload, (bytes, bytearray)):
        try:
            data = json.loads(payload.decode("utf-8", errors="replace"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise BCBCopomParseError(
                "Copom history payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, str):
        try:
            data = json.loads(payload)
        except ValueError as exc:
            raise BCBCopomParseError(
                "Copom history payload is not parseable JSON",
            ) from exc
    elif isinstance(payload, dict):
        data = payload
    else:
        raise BCBCopomParseError(
            f"Copom history payload type not supported: {type(payload).__name__}",
        )

    rows = data.get("conteudo") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise BCBCopomParseError(
            "Copom history JSON missing 'conteudo' array — DOM/API drift",
        )

    parsed: list[tuple[date, dict[str, Any], str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        announcement = _parse_announcement_date(row.get("DataReuniaoCopom"))
        if announcement is None:
            continue
        try:
            rate_text = _parse_rate(row.get("MetaSelic"))
        except BCBCopomParseError:
            # A truncated row with an unparseable rate must not nuke the
            # whole list — skip it and keep walking. Mirrors the RBA
            # parser's defensive shape.
            continue
        parsed.append((announcement, row, rate_text))

    if not parsed:
        raise BCBCopomParseError(
            "Copom history JSON parsed zero decisions — layout drift",
        )

    parsed.sort(key=lambda r: r[0])
    decisions: list[BCBRateDecision] = []
    previous_rate: str | None = None
    for announcement, row, rate_text in parsed:
        meeting_number_raw = row.get("NumeroReuniaoCopom")
        try:
            meeting_number = int(meeting_number_raw or 0)
        except (TypeError, ValueError):
            meeting_number = 0
        decisions.append(BCBRateDecision(
            meeting_number=meeting_number,
            announcement_date=announcement,
            effective_date=_parse_announcement_date(row.get("DataInicioVigencia")),
            end_date=_parse_announcement_date(row.get("DataFimVigencia")),
            rate=rate_text,
            previous_rate=previous_rate,
            bias=str(row.get("Vies") or "").strip(),
            extraordinary=bool(row.get("ReuniaoExtraordinaria")),
            monocratic_president=bool(row.get("descisaoMonocraticaPres")),
        ))
        previous_rate = rate_text

    decisions.sort(key=lambda d: d.announcement_date, reverse=True)
    return decisions


# ``end_date`` lives in the hash so the raw audit row updates when a
# Copom period closes — BCB flips ``DataFimVigencia`` from null to the
# next decision date once the validity window ends. Without the field,
# the new raw record reuses the prior ``(provider, provider_event_id,
# content_hash)`` key and ``store_raw`` silently drops the closing
# update.
_HASH_FIELDS: tuple[str, ...] = (
    "announcement_date", "effective_date", "end_date", "rate",
    "previous_rate", "meeting_number", "extraordinary",
    "monocratic_president",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def decision_to_records(
    decision: BCBRateDecision,
    *,
    snapshot_epoch_ms: int,
    spec: BCBIndicatorSpec | None = None,
) -> tuple[BCBCalendarRawRecord, BCBCalendarEventRecord]:
    """Project a :class:`BCBRateDecision` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BCB_RATE"]

    scheduled = parse_scheduled_release_time(
        decision.announcement_date,
        BCB_RELEASE_TIME,
        default_tz=BCB_RELEASE_TZ,
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
        "kind":                 "bcb_copom_decision",
        "meeting_number":       decision.meeting_number,
        "announcement_date":    decision.announcement_date.isoformat(),
        "effective_date":       (
            decision.effective_date.isoformat()
            if decision.effective_date is not None else None
        ),
        "end_date":             (
            decision.end_date.isoformat()
            if decision.end_date is not None else None
        ),
        "rate":                 decision.rate,
        "previous_rate":        decision.previous_rate,
        "bias":                 decision.bias,
        "extraordinary":        decision.extraordinary,
        "monocratic_president": decision.monocratic_president,
        "event_time_utc":       event_time_utc,
        "source_url":           BCB_COPOM_PUBLIC_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BCBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BCBCalendarEventRecord(
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
        currency="BRL",
        unit=resolved_spec.unit,
        actual=decision.rate,
        previous=decision.previous_rate,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Banco Central do Brasil",
        source_url=BCB_COPOM_PUBLIC_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "BCB_BASE_URL",
    "BCB_COPOM_HISTORY_URL",
    "BCB_COPOM_PUBLIC_URL",
    "BCB_RELEASE_TIME",
    "BCB_RELEASE_TZ",
    "BCBCalendarEventRecord",
    "BCBCalendarRawRecord",
    "BCBCopomParseError",
    "BCBRateDecision",
    "PROVIDER",
    "decision_to_records",
    "parse_copom_history",
]
