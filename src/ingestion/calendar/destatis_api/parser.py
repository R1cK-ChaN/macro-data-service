"""Project Destatis GENESIS observations into calendar records."""

from __future__ import annotations

import calendar
import csv
import hashlib
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import DestatisIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "destatis"


@dataclass(frozen=True)
class DestatisObservation:
    """One value-bearing row parsed from a GENESIS flat CSV table."""

    series_id: str
    table_name: str
    period: str
    value: str
    raw: dict[str, Any]


@dataclass(frozen=True)
class DestatisCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class DestatisCalendarEventRecord:
    """One row destined for ``cal_econ_event``."""

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


class DestatisGenesisParseError(ValueError):
    """Raised when a GENESIS CSV payload cannot be projected."""


_HASH_FIELDS: tuple[str, ...] = ("value", "period", "series_id", "table_name")
_VALUE_COLUMNS: tuple[str, ...] = ("wert", "value", "obs_value", "obs value")
_PERIOD_COLUMNS: tuple[str, ...] = (
    "zeit",
    "time",
    "time_period",
    "time period",
    "period",
    "berichtszeitraum",
)
_MONTHS: dict[str, int] = {
    "januar": 1,
    "jan": 1,
    "februar": 2,
    "feb": 2,
    "maerz": 3,
    "marz": 3,
    "mrz": 3,
    "maer": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "mai": 5,
    "juni": 6,
    "jun": 6,
    "juli": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "oktober": 10,
    "okt": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "dezember": 12,
    "dez": 12,
    "dec": 12,
}
_ISO_MONTH_RE = re.compile(r"^\s*(\d{4})[-/.]?(?:M)?(\d{2})\s*$", re.I)
_ISO_DATE_RE = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$")
_QUARTER_RE = re.compile(
    r"^\s*(?:(\d{4})[- ]?Q([1-4])|Q([1-4])[- /]*(\d{4})|([1-4])\.\s*quartal\s*(\d{4}))\s*$",
    re.I,
)
_MONTH_NAME_RE = re.compile(r"^\s*([A-Za-z.]+)\s+(\d{4})\s*$", re.I)


def _normalise(text: str) -> str:
    text = text.replace("\ufeff", "").replace("\xa0", " ")
    text = (
        text.replace("\u00e4", "ae")
        .replace("\u00f6", "oe")
        .replace("\u00fc", "ue")
        .replace("\u00c4", "Ae")
        .replace("\u00d6", "Oe")
        .replace("\u00dc", "Ue")
        .replace("\u00df", "ss")
    )
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _period_end(reference: date, cadence: str) -> date:
    if cadence == "quarterly":
        month = reference.month + 2
        year = reference.year
        while month > 12:
            month -= 12
            year += 1
        day = calendar.monthrange(year, month)[1]
        return date(year, month, day)
    day = calendar.monthrange(reference.year, reference.month)[1]
    return date(reference.year, reference.month, day)


def _quarter_start(year: int, quarter: int) -> date:
    return date(year, (quarter - 1) * 3 + 1, 1)


def _month_from_name(raw: str) -> int:
    key = _normalise(raw).rstrip(".")
    month = _MONTHS.get(key)
    if month is None:
        raise DestatisGenesisParseError(f"unknown month name: {raw!r}")
    return month


def parse_period(period: str, *, cadence: str) -> date:
    """Parse a GENESIS period label into the connector reference date."""
    text = _normalise(period)
    match = _ISO_DATE_RE.match(text)
    if match:
        year, month, day = match.groups()
        parsed = date(int(year), int(month), int(day))
        if cadence == "quarterly":
            quarter = (parsed.month - 1) // 3 + 1
            return _period_end(_quarter_start(parsed.year, quarter), cadence)
        return date(parsed.year, parsed.month, 1)

    match = _QUARTER_RE.match(text)
    if match:
        year = int(match.group(1) or match.group(4) or match.group(6))
        quarter = int(match.group(2) or match.group(3) or match.group(5))
        return _period_end(_quarter_start(year, quarter), "quarterly")

    match = _ISO_MONTH_RE.match(text)
    if match:
        year, month = match.groups()
        return date(int(year), int(month), 1)

    match = _MONTH_NAME_RE.match(text)
    if match:
        month_name, year = match.groups()
        return date(int(year), _month_from_name(month_name), 1)

    if len(text) == 4 and text.isdigit():
        return date(int(text), 1, 1)

    raise DestatisGenesisParseError(f"unparseable period: {period!r}")


def _value_text(raw: str) -> str | None:
    cleaned = (
        str(raw or "")
        .strip()
        .replace("\xa0", "")
        .replace("%", "")
        .replace("+", "")
        .replace(" ", "")
    )
    if cleaned in {"", ".", "-", "x", "X", "/"}:
        return None
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        Decimal(cleaned)
    except InvalidOperation:
        return None
    return cleaned


def _header_key(raw: str) -> str:
    return _normalise(raw).replace("_", " ")


def _column(row: dict[str, Any], candidates: Iterable[str]) -> str | None:
    wanted = {_header_key(c) for c in candidates}
    for key, value in row.items():
        if _header_key(str(key)) in wanted:
            text = str(value or "").strip()
            if text:
                return text
    return None


def _csv_dict_rows(payload: str | bytes) -> list[dict[str, str]]:
    text = payload.decode("utf-8-sig") if isinstance(payload, bytes) else payload
    lines = [line for line in text.splitlines() if line.strip()]
    header_idx = 0
    for idx, line in enumerate(lines):
        lowered = _normalise(line)
        if ";" in line and any(col in lowered for col in ("wert", "value", "obs")):
            header_idx = idx
            break
    reader = csv.DictReader(lines[header_idx:], delimiter=";")
    return [
        {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
        for row in reader
    ]


def parse_genesis_csv_table(
    payload: str | bytes,
    *,
    series_id: str | None = None,
    spec: DestatisIndicatorSpec | None = None,
) -> list[DestatisObservation]:
    """Extract whitelisted observations from a GENESIS flat CSV table."""
    resolved_spec = spec
    if resolved_spec is None:
        if series_id is None:
            raise KeyError("series_id is required when spec is absent")
        resolved_spec = INDICATOR_REGISTRY[series_id]
    rows = _csv_dict_rows(payload)
    matches: list[DestatisObservation] = []
    required = tuple(_normalise(f) for f in resolved_spec.row_match_fragments)
    for row in rows:
        joined = _normalise(" ".join(str(v) for v in row.values()))
        if any(fragment not in joined for fragment in required):
            continue
        period = _column(row, _PERIOD_COLUMNS)
        raw_value = _column(row, _VALUE_COLUMNS)
        value = _value_text(raw_value or "")
        if period is None or value is None:
            continue
        parse_period(period, cadence=resolved_spec.reference_cadence)
        matches.append(
            DestatisObservation(
                series_id=resolved_spec.series_id,
                table_name=resolved_spec.table_name,
                period=period,
                value=value,
                raw=dict(row),
            )
        )
    return matches


def parse_observation(
    obs: DestatisObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: DestatisIndicatorSpec | None = None,
) -> tuple[DestatisCalendarRawRecord, DestatisCalendarEventRecord]:
    """Convert one Destatis observation into raw + PIT event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(
            f"series_id {obs.series_id!r} not in Destatis INDICATOR_REGISTRY"
        )

    reference = parse_period(
        obs.period,
        cadence=resolved_spec.reference_cadence,
    )
    event_date = (
        reference
        if resolved_spec.reference_cadence == "quarterly"
        else _period_end(reference, resolved_spec.reference_cadence)
    )
    reference_iso = reference.isoformat()
    event_time_utc = datetime(
        event_date.year,
        event_date.month,
        event_date.day,
        tzinfo=timezone.utc,
    ).isoformat()
    payload: dict[str, Any] = {
        "series_id": obs.series_id,
        "table_name": obs.table_name,
        "period": obs.period,
        "value": obs.value,
        "raw": obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        reference_iso,
    )
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = DestatisCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = DestatisCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="approximate",
        reference_date=reference_iso,
        reference_label=reference_iso,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Destatis",
        source_url=resolved_spec.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


_asdict = asdict
