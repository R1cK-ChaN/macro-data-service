"""Shared constants and helpers used by all scrapers."""

from __future__ import annotations

import hashlib
import re as _re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

OPEN_UTC_PLUS_8 = timezone(timedelta(hours=8))

COUNTRY_MAP = {
    5: "US",
    72: "EU",
    35: "JP",
    4: "UK",
    17: "CA",
    25: "AU",
    6: "CN",
    36: "NZ",
    12: "CH",
    26: "SG",
    34: "DE",
    22: "FR",
}

IMPORTANCE_MAP = {
    1: "low",
    2: "medium",
    3: "high",
}

MACRO_CATEGORIES = {
    "inflation": ["CPI", "PPI", "PCE", "Inflation", "Price Index"],
    "employment": ["NFP", "Nonfarm", "Unemployment", "Jobless", "Employment", "ADP", "Payroll"],
    "growth": ["GDP", "Retail Sales", "Industrial Production", "PMI", "ISM"],
    "policy": ["Interest Rate", "Fed", "FOMC", "ECB", "BOJ", "BOE", "Central Bank"],
    "housing": ["Housing", "Home Sales", "Building Permits"],
    "consumer": ["Consumer Confidence", "Consumer Sentiment", "Michigan"],
    "trade": ["Trade Balance", "Current Account", "Import", "Export"],
    "market": ["Auction", "Treasury", "Bond"],
}


@dataclass(frozen=True)
class ScrapedNewsItem:
    """A news article scraped from a financial site."""
    source: str
    title: str
    url: str
    published_at: str = ""
    description: str = ""
    author: str = ""
    category: str = ""
    importance: str = ""
    image_url: str = ""
    raw_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScrapedIndicator:
    """A macro-economic indicator snapshot scraped from a site."""
    source: str
    country: str
    name: str
    last: str
    previous: str = ""
    highest: str = ""
    lowest: str = ""
    unit: str = ""
    date: str = ""
    url: str = ""
    category: str = ""


@dataclass(frozen=True)
class ScrapedMarketQuote:
    """A market price quote scraped from a site."""
    source: str
    name: str
    asset_class: str
    price: str
    change: str = ""
    change_pct: str = ""
    url: str = ""
    symbol: str = ""
    raw_json: dict[str, Any] = field(default_factory=dict)


def parse_numeric_value(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    multiplier = 1.0
    suffix_map = {
        "K": 1_000.0,
        "M": 1_000_000.0,
        "B": 1_000_000_000.0,
    }
    suffix = cleaned[-1].upper()
    if suffix in suffix_map:
        multiplier = suffix_map[suffix]
        cleaned = cleaned[:-1]
    cleaned = cleaned.replace("%", "").strip()
    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def categorize_event(indicator_name: str) -> str:
    name_upper = indicator_name.upper()
    for category, keywords in MACRO_CATEGORIES.items():
        for keyword in keywords:
            if keyword.upper() in name_upper:
                return category
    return "other"


def generate_event_id(country: str, indicator: str, dt: int | str) -> str:
    raw = f"{country}-{indicator}-{dt}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]


def to_epoch(
    *,
    date_value: str,
    time_value: str | None = None,
    source_timezone: timezone = OPEN_UTC_PLUS_8,
) -> int:
    time_part = (time_value or "00:00").strip().lower()
    if time_part in {"all day", "tentative", "day 1", "day 2"}:
        time_part = "00:00"
    for pattern in ("%Y-%m-%d %H:%M", "%Y-%m-%d %I:%M%p", "%b %d %H:%M", "%a%b %d %H:%M"):
        try:
            candidate = f"{date_value} {time_part}".strip()
            parsed = datetime.strptime(candidate, pattern)
            if pattern.startswith("%b ") or pattern.startswith("%a%b"):
                parsed = parsed.replace(year=datetime.now(source_timezone).year)
            return int(parsed.replace(tzinfo=source_timezone).astimezone(UTC).timestamp())
        except ValueError:
            continue
    try:
        return int(datetime.fromisoformat(date_value).astimezone(UTC).timestamp())
    except ValueError:
        fallback = datetime.now(source_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        return int(fallback.astimezone(UTC).timestamp())


def normalize_indicator_name(name: str) -> str:
    """Lowercase, strip, collapse whitespace, remove trailing month parentheticals."""
    text = name.lower().strip()
    text = _re.sub(r'\s+', ' ', text)
    # Strip trailing month parentheticals like "(Mar)" that TradingEconomics appends
    text = _re.sub(
        r'\s*\((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\)\s*$',
        '', text, flags=_re.IGNORECASE,
    )
    return text
