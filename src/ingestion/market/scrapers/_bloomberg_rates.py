"""CSV parser for licensed Bloomberg-compatible rate exports."""

from __future__ import annotations

import csv
import io
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ingestion.market._bloomberg_rates import BloombergRateEntry


@dataclass(frozen=True)
class BloombergRateObservation:
    date: str
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)


_DATE_KEYS = (
    "date",
    "dates",
    "pxdate",
    "pricedate",
    "asofdate",
    "datetime",
    "timestamp",
)

_VALUE_KEYS = (
    "value",
    "rate",
    "mid",
    "last",
    "lastprice",
    "pxlast",
    "close",
    "3m",
    "3month",
    "threemonth",
)

_BID_ASK_KEY_PAIRS = (
    ("bid", "ask"),
    ("3mbid", "3mask"),
    ("3monthbid", "3monthask"),
    ("threemonthbid", "threemonthask"),
)


def parse_bloomberg_rate_csv(
    payload: str | bytes,
    *,
    entry: BloombergRateEntry,
) -> tuple[list[BloombergRateObservation], list[dict[str, str]]]:
    """Parse a Bloomberg BGN or compatible vendor CSV into observations."""
    text = payload.decode("utf-8-sig") if isinstance(payload, bytes) else payload
    reader = csv.DictReader(io.StringIO(text))
    raw_rows: list[dict[str, str]] = []
    by_date: dict[str, BloombergRateObservation] = {}
    if not reader.fieldnames:
        return [], raw_rows

    for row in reader:
        raw = {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}
        raw_rows.append(raw)
        normalized = {_normalize_header(k): v for k, v in raw.items()}
        date_value = _extract_date(normalized)
        rate_value, rate_metadata = _extract_rate(normalized, entry)
        if date_value is None or rate_value is None:
            continue
        by_date[date_value] = BloombergRateObservation(
            date=date_value,
            value=rate_value,
            metadata={
                "curve": entry.curve,
                "tenor": entry.tenor,
                "provider": entry.provider,
                "provider_symbol": entry.provider_symbol,
                **rate_metadata,
            },
        )

    return [by_date[key] for key in sorted(by_date)], raw_rows


def _extract_date(row: dict[str, str]) -> str | None:
    for key, value in row.items():
        if key in _DATE_KEYS:
            return _parse_date(value)
    return None


def _extract_rate(
    row: dict[str, str],
    entry: BloombergRateEntry,
) -> tuple[float | None, dict[str, Any]]:
    provider_key = _normalize_header(entry.provider_symbol)
    ticker_key = _normalize_header(entry.ticker)
    for key in (provider_key, ticker_key, *_VALUE_KEYS):
        value = _float_or_none(row.get(key, ""))
        if value is not None:
            return value, {"value_column": key}

    for bid_key, ask_key in _BID_ASK_KEY_PAIRS:
        bid = _float_or_none(row.get(bid_key, ""))
        ask = _float_or_none(row.get(ask_key, ""))
        if bid is None or ask is None:
            continue
        return (bid + ask) / 2.0, {
            "value_column": f"{bid_key}/{ask_key}",
            "bid": bid,
            "ask": ask,
        }
    return None, {}


def _parse_date(value: str) -> str | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    cleaned = re.sub(r"\b(\d{1,2})(st|nd|rd|th)\b", r"\1", cleaned, flags=re.I)
    iso_candidate = cleaned
    if iso_candidate.upper().endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(iso_candidate).date().isoformat()
    except ValueError:
        pass
    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%d %b %Y",
        "%d %B %Y",
    )
    for fmt in formats:
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _float_or_none(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if cleaned.endswith("%"):
        cleaned = cleaned[:-1].strip()
    if cleaned.lower() in {"", "na", "n/a", "#n/a", "null", "none"}:
        return None
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())
