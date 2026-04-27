"""Calendar-domain query helpers for SQLiteEngineStore.

Covers calendar_events + calendar_event_vintages + calendar_indicator +
calendar_indicator_alias + release_schedule + release_status. Also owns
the module-level calendar query helpers (`_calendar_country_code`,
`_add_event_time_lower_bound`, `_calendar_surprise`, etc.) and the
calendar / event-timestamp safety helpers (`_safe_epoch_ms`,
`_safe_utc_iso`, `_infer_timestamp_precision`, `_matches_scope_tags`)
that other domain mixins import from here.

Public re-export: ``append_calendar_event_vintage_if_changed_with_conn``
is a free function used by ingestion code outside the EngineStore.
``storage.sqlite`` re-exports it for backwards compatibility.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from contracts import (
    normalize_utc_iso,
    to_epoch_ms,
    utc_now,
)
from storage.models.calendar import (
    CalendarEventVintageRecord,
    CalendarIndicatorAliasRecord,
    CalendarIndicatorRecord,
    StoredEventRecord,
)
from storage.models.indicator import (
    ReleaseScheduleRecord,
    ReleaseStatusRecord,
)


def append_calendar_event_vintage_if_changed_with_conn(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    provider: str,
    vintage_date: str,
    observed_at: str,
    actual: str | None,
    forecast: str | None,
    previous: str | None,
    metadata_json: str = "{}",
    scraped_at: str | None = None,
    source_url: str = "",
) -> bool:
    """Append-on-change vintage write using a caller-supplied connection.

    Compares against the vintage immediately at-or-before ``observed_at``
    so out-of-order backfills capture genuine intermediate states.
    Returns True if a row was appended, False on no-op (triple matched
    the predecessor) or on UNIQUE collision via INSERT OR IGNORE.

    ``source_url`` is snapshotted onto the row so revision-time URL
    changes (BLS / BEA publish revisions at distinct press-release URLs)
    keep their per-vintage citation anchor — see issue #36.
    """
    # Use julianday() rather than string <= so fractional-second ISO
    # timestamps compare against whole-second timestamps correctly.
    # Lexicographic <= on '2024-01-01T00:00:00.500Z' vs '2024-01-01T00:00:00Z'
    # would falsely treat the fractional value as <= the whole-second one.
    row = connection.execute(
        "SELECT actual, forecast, previous FROM calendar_event_vintages "
        "WHERE event_id = ? AND provider = ? "
        "AND julianday(observed_at) <= julianday(?) "
        "ORDER BY julianday(observed_at) DESC, id DESC LIMIT 1",
        (event_id, provider, observed_at),
    ).fetchone()
    if row is not None and (row[0], row[1], row[2]) == (actual, forecast, previous):
        return False
    cursor = connection.execute(
        "INSERT OR IGNORE INTO calendar_event_vintages ("
        "event_id, provider, vintage_date, observed_at, "
        "actual, forecast, previous, metadata_json, scraped_at, "
        "source_url"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id, provider, vintage_date, observed_at,
            actual, forecast, previous, metadata_json,
            scraped_at or utc_now().isoformat(),
            source_url or "",
        ),
    )
    return cursor.rowcount > 0


def _matches_scope_tags(text: str, tags: list[str]) -> bool:
    lowered = text.lower()
    return any(re.search(rf"\b{re.escape(tag.lower())}\b", lowered) for tag in tags)


_CALENDAR_KEYWORD_ALIASES: dict[str, tuple[str, ...]] = {
    "cpi": ("consumer price index", "inflation rate"),
    "core cpi": ("consumer price index all items less food and energy",),
    "ppi": ("producer price index",),
    "nfp": (
        "nonfarm payroll",
        "non farm payroll",
        "non-farm payroll",
        "employment situation",
    ),
    "pmi": ("purchasing managers", "manufacturing pmi", "services pmi"),
    "gdp": ("gross domestic product",),
    "zew": (
        "zew economic sentiment",
        "zew economic sentiment index",
        "zew indicator of economic sentiment",
    ),
    "ifo": (
        "ifo business climate",
        "ifo business climate index",
        "germany ifo business climate index",
    ),
    "gfk": (
        "gfk consumer climate",
        "gfk consumer confidence",
        "germany gfk consumer climate",
        "nim consumer climate",
    ),
    "hcob": (
        "hcob manufacturing pmi",
        "hcob services pmi",
        "hcob flash pmi",
        "hcob composite pmi",
        "germany hcob flash pmi",
        "germany hcob manufacturing pmi",
        "germany hcob services pmi",
    ),
    "ec-bcs": (
        "economic sentiment indicator",
        "euro area economic sentiment indicator",
        "consumer confidence flash",
        "flash consumer confidence indicator",
        "euro area consumer confidence flash",
    ),
}

_CALENDAR_COUNTRY_ALIASES: dict[str, str] = {
    "US": "US",
    "USA": "US",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "CN": "CN",
    "CHINA": "CN",
    "JP": "JP",
    "JAPAN": "JP",
    "UK": "UK",
    "GB": "UK",
    "UNITED KINGDOM": "UK",
    "EU": "EU",
    "EURO AREA": "EU",
    "EUROZONE": "EU",
    "EUROPEAN UNION": "EU",
    "DE": "DE",
    "GERMANY": "DE",
    "DEUTSCHLAND": "DE",
    "FR": "FR",
    "FRANCE": "FR",
    "ES": "ES",
    "SPAIN": "ES",
    "ESPANA": "ES",
    "ESPAÑA": "ES",
    "IT": "IT",
    "ITALY": "IT",
    "ITALIA": "IT",
    "CA": "CA",
    "CAN": "CA",
    "CANADA": "CA",
    "AU": "AU",
    "AUS": "AU",
    "AUSTRALIA": "AU",
    "IN": "IN",
    "IND": "IN",
    "INDIA": "IN",
    "KR": "KR",
    "KOR": "KR",
    "KOREA": "KR",
    "SOUTH KOREA": "KR",
    "REPUBLIC OF KOREA": "KR",
    "BR": "BR",
    "BRA": "BR",
    "BRAZIL": "BR",
    "BRASIL": "BR",
    "TR": "TR",
    "TUR": "TR",
    "TURKEY": "TR",
    "TURKIYE": "TR",
    "TÜRKIYE": "TR",
    "TÜRKİYE": "TR",
    "MX": "MX",
    "MEX": "MX",
    "MEXICO": "MX",
    "MÉXICO": "MX",
    "ZA": "ZA",
    "ZAF": "ZA",
    "SOUTH AFRICA": "ZA",
    "RSA": "ZA",
}

_CALENDAR_COUNTRY_DISPLAY: dict[str, str] = {
    "US": "United States",
    "CN": "China",
    "JP": "Japan",
    "UK": "United Kingdom",
    "EU": "Euro Area",
    "DE": "Germany",
    "FR": "France",
    "ES": "Spain",
    "IT": "Italy",
    "CA": "Canada",
    "AU": "Australia",
    "IN": "India",
    "KR": "South Korea",
    "BR": "Brazil",
    "TR": "Turkey",
    "MX": "Mexico",
    "ZA": "South Africa",
}


def _calendar_country_code(value: str) -> str | None:
    normalized = re.sub(r"[\s_-]+", " ", value.strip().upper())
    if not normalized:
        return None
    if normalized in _CALENDAR_COUNTRY_ALIASES:
        return _CALENDAR_COUNTRY_ALIASES[normalized]
    if re.fullmatch(r"[A-Z]{2}", normalized):
        return normalized
    return None


def _calendar_country_display(country_code: str) -> str:
    code = (country_code or "").strip().upper()
    return _CALENDAR_COUNTRY_DISPLAY.get(code, code)


def _add_calendar_keyword_filter(
    conditions: list[str],
    params: list[Any],
    keyword: str | None,
    *,
    connection: sqlite3.Connection | None = None,
) -> bool:
    from ingestion.scrapers._common import normalize_indicator_name

    raw_keyword = (keyword or "").strip()
    base = normalize_indicator_name(raw_keyword)
    if not base or not re.search(r"[0-9A-Za-z]", base):
        return False
    aliases = _CALENDAR_KEYWORD_ALIASES.get(base, ())
    raw_patterns = {raw_keyword, base, *aliases}
    if connection is not None:
        lookup_terms = {raw_keyword, base, *aliases}
        for lookup_term in sorted(term for term in lookup_terms if term):
            like = f"%{lookup_term}%"
            rows = connection.execute(
                """
                SELECT
                    alias_original AS alias_raw,
                    NULL AS canonical_raw,
                    NULL AS indicator_raw
                FROM calendar_indicator_alias
                WHERE alias_original LIKE ?
                UNION
                SELECT
                    NULL AS alias_raw,
                    canonical_name AS canonical_raw,
                    NULL AS indicator_raw
                FROM calendar_indicator
                WHERE canonical_name LIKE ?
                UNION
                SELECT
                    NULL AS alias_raw,
                    NULL AS canonical_raw,
                    indicator_id AS indicator_raw
                FROM calendar_indicator
                WHERE indicator_id LIKE ?
                """,
                (like, like, like),
            ).fetchall()
            for row in rows:
                for column in ("alias_raw", "canonical_raw", "indicator_raw"):
                    raw_pattern = str(row[column] or "").strip()
                    if raw_pattern:
                        raw_patterns.add(raw_pattern)
    if not raw_patterns:
        return False
    clauses: list[str] = []
    for pattern in sorted(raw_patterns):
        like = f"%{pattern}%"
        clauses.extend((
            "title LIKE ?",
            "COALESCE(indicator_id, '') LIKE ?",
            "category LIKE ?",
        ))
        params.extend((like, like, like))
    conditions.append(f"({' OR '.join(clauses)})")
    return True


def _calendar_numeric_value(value: str | None) -> float | None:
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


def _calendar_surprise(
    actual: str | None,
    forecast: str | None,
) -> float | None:
    actual_value = _calendar_numeric_value(actual)
    forecast_value = _calendar_numeric_value(forecast)
    if actual_value is None or forecast_value is None:
        return None
    return round(actual_value - forecast_value, 4)


def _add_event_time_lower_bound(
    conditions: list[str],
    params: list[Any],
    value: str,
) -> None:
    conditions.append(
        "((event_time_precision = 'date' AND date(event_time_utc) >= date(?)) "
        "OR (event_time_precision != 'date' AND datetime(event_time_utc) >= datetime(?)))"
    )
    params.extend((value, value))


def _add_event_time_upper_bound(
    conditions: list[str],
    params: list[Any],
    value: str,
) -> None:
    conditions.append(
        "((event_time_precision = 'date' AND date(event_time_utc) <= date(?)) "
        "OR (event_time_precision != 'date' AND datetime(event_time_utc) <= datetime(?)))"
    )
    params.extend((value, value))


def _add_calendar_country_filter(
    conditions: list[str],
    params: list[Any],
    country: str,
) -> None:
    country_code = _calendar_country_code(country)
    if country_code is None:
        conditions.append("1 = 0")
        return
    conditions.append("country_code = ?")
    params.append(country_code)


def _safe_epoch_ms(value: str | datetime | None) -> int:
    if value in (None, ""):
        return 0
    try:
        return to_epoch_ms(value)
    except (TypeError, ValueError):
        return 0


def _safe_utc_iso(value: str | datetime | None) -> str:
    if value in (None, ""):
        return ""
    try:
        return normalize_utc_iso(value)
    except (TypeError, ValueError):
        return str(value)


def _infer_timestamp_precision(value: str | None) -> str:
    if not value:
        return "estimated"
    if re.search(r"[T ]\d{1,2}:\d{2}", value):
        return "exact"
    return "date_only"


_CALENDAR_INDICATOR_DEFS: list[tuple[str, str, str, str, str, str, str]] = [
    # -- US Inflation --
    ("us.inflation.cpi_mom",      "CPI MoM",               "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.cpi_yoy",      "CPI YoY",               "inflation",  "US", "monthly",   "percent", "us.inflation.cpi_all"),
    ("us.inflation.core_cpi_mom", "Core CPI MoM",          "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.core_cpi_yoy", "Core CPI YoY",          "inflation",  "US", "monthly",   "percent", "us.inflation.cpi_core"),
    ("us.inflation.pce_mom",      "PCE Price Index MoM",   "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.pce_yoy",      "PCE Price Index YoY",   "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.core_pce_mom", "Core PCE MoM",          "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.core_pce_yoy", "Core PCE YoY",          "inflation",  "US", "monthly",   "percent", "us.inflation.pce_core"),
    ("us.inflation.ppi_mom",      "PPI MoM",               "inflation",  "US", "monthly",   "percent", ""),
    ("us.inflation.ppi_yoy",      "PPI YoY",               "inflation",  "US", "monthly",   "percent", ""),
    # -- US Employment --
    ("us.employment.nfp",                "Nonfarm Payrolls",          "employment", "US", "monthly", "thousands", "us.employment.nonfarm_payrolls"),
    ("us.employment.unemployment_rate",  "Unemployment Rate",         "employment", "US", "monthly", "percent",   "us.employment.unemployment"),
    ("us.employment.initial_claims",     "Initial Jobless Claims",    "employment", "US", "weekly",  "thousands", "us.employment.initial_claims"),
    ("us.employment.continuing_claims",  "Continuing Jobless Claims", "employment", "US", "weekly",  "thousands", "us.employment.continuing_claims"),
    ("us.employment.adp",               "ADP Employment Change",     "employment", "US", "monthly", "thousands", ""),
    ("us.employment.jolts",             "JOLTS Job Openings",        "employment", "US", "monthly", "thousands", ""),
    ("us.employment.avg_hourly_earnings","Avg Hourly Earnings MoM",  "employment", "US", "monthly", "percent",   ""),
    # -- US Growth --
    ("us.growth.gdp_qoq",          "GDP QoQ",                  "growth", "US", "quarterly", "percent", "us.growth.gdp_real"),
    ("us.growth.retail_sales_mom",  "Retail Sales MoM",         "growth", "US", "monthly",   "percent", "us.growth.retail_sales"),
    ("us.growth.ism_mfg_pmi",      "ISM Manufacturing PMI",    "growth", "US", "monthly",   "index",   ""),
    ("us.growth.ism_services_pmi",  "ISM Services PMI",         "growth", "US", "monthly",   "index",   ""),
    ("us.growth.industrial_prod",   "Industrial Production MoM","growth", "US", "monthly",   "percent", "us.growth.industrial_production"),
    ("us.growth.durable_goods",     "Durable Goods Orders MoM", "growth", "US", "monthly",   "percent", ""),
    ("us.growth.sp_global_mfg_pmi", "S&P Global Mfg PMI",      "growth", "US", "monthly",   "index",   ""),
    # -- US Policy --
    ("us.policy.fed_rate",       "Fed Interest Rate Decision", "policy", "US", "irregular", "percent", "us.rates.fed_funds"),
    ("us.policy.fomc_minutes",   "FOMC Minutes",               "policy", "US", "irregular", "",        ""),
    ("us.policy.fomc_statement", "FOMC Statement",             "policy", "US", "irregular", "",        ""),
    ("us.policy.fed_chair_speech","Fed Chair Speech",           "policy", "US", "irregular", "",        ""),
    # -- US Housing --
    ("us.housing.existing_home_sales", "Existing Home Sales",  "housing", "US", "monthly", "millions", ""),
    ("us.housing.new_home_sales",      "New Home Sales",       "housing", "US", "monthly", "thousands",""),
    ("us.housing.building_permits",    "Building Permits",     "housing", "US", "monthly", "millions", ""),
    # -- US Consumer --
    ("us.consumer.michigan_sentiment",  "Michigan Consumer Sentiment", "consumer", "US", "monthly", "index", ""),
    ("us.consumer.cb_confidence",       "CB Consumer Confidence",      "consumer", "US", "monthly", "index", ""),
    # -- US Trade --
    ("us.trade.balance", "Trade Balance", "trade", "US", "monthly", "billions_usd", ""),
    # -- EU --
    ("eu.inflation.hicp_yoy",      "HICP YoY",              "inflation",  "EU", "monthly",   "percent", "eu.inflation.hicp"),
    ("eu.inflation.hicp_mom",      "HICP MoM",              "inflation",  "EU", "monthly",   "percent", ""),
    ("eu.inflation.core_hicp_yoy", "Core HICP YoY",         "inflation",  "EU", "monthly",   "percent", ""),
    ("eu.growth.gdp_qoq",         "GDP QoQ",                "growth",     "EU", "quarterly", "percent", "eu.growth.gdp_qoq"),
    ("eu.employment.unemployment", "Unemployment Rate",      "employment", "EU", "monthly",   "percent", "eu.employment.unemployment"),
    ("eu.policy.ecb_rate",         "ECB Interest Rate Decision","policy",  "EU", "irregular", "percent", ""),
    # -- JP --
    ("jp.policy.boj_rate", "BOJ Interest Rate Decision", "policy",    "JP", "irregular", "percent", ""),
    ("jp.inflation.cpi_yoy","CPI YoY",                   "inflation", "JP", "monthly",   "percent", "jp.inflation.cpi"),
    ("jp.growth.gdp_qoq",  "GDP QoQ",                    "growth",    "JP", "quarterly", "percent", ""),
    # -- UK --
    ("gb.policy.boe_rate", "BOE Interest Rate Decision", "policy",    "UK", "irregular", "percent", ""),
    ("gb.inflation.cpi_yoy","CPI YoY",                   "inflation", "UK", "monthly",   "percent", ""),
    ("gb.growth.gdp_qoq",  "GDP QoQ",                    "growth",    "UK", "quarterly", "percent", ""),
    # -- CN --
    ("cn.policy.pboc_rate", "PBOC Interest Rate Decision","policy",    "CN", "irregular", "percent", ""),
    ("cn.inflation.cpi_yoy","CPI YoY",                    "inflation", "CN", "monthly",   "percent", "cn.inflation.cpi"),
    ("cn.growth.gdp_yoy",  "GDP YoY",                     "growth",    "CN", "quarterly", "percent", ""),
    ("cn.growth.mfg_pmi",  "Manufacturing PMI",            "growth",    "CN", "monthly",   "index",   ""),
]


_CALENDAR_ALIAS_DEFS: list[tuple[str, str, str, str]] = [
    # ── US Inflation ─────────────────────────────────────────────
    ("CPI m/m",                        "us.inflation.cpi_mom",      "investing",         "US"),
    ("CPI (MoM)",                      "us.inflation.cpi_mom",      "investing",         "US"),
    ("Consumer Price Index m/m",       "us.inflation.cpi_mom",      "forexfactory",      "US"),
    ("Inflation Rate MoM",             "us.inflation.cpi_mom",      "tradingeconomics",  "US"),
    ("CPI y/y",                        "us.inflation.cpi_yoy",      "investing",         "US"),
    ("CPI (YoY)",                      "us.inflation.cpi_yoy",      "investing",         "US"),
    ("Consumer Price Index (YoY)",     "us.inflation.cpi_yoy",      "investing",         "US"),
    ("Inflation Rate YoY",             "us.inflation.cpi_yoy",      "tradingeconomics",  "US"),
    ("Core CPI m/m",                   "us.inflation.core_cpi_mom", "investing",         "US"),
    ("Core CPI (MoM)",                 "us.inflation.core_cpi_mom", "investing",         "US"),
    ("Core Consumer Price Index m/m",  "us.inflation.core_cpi_mom", "forexfactory",      "US"),
    ("Core Inflation Rate MoM",        "us.inflation.core_cpi_mom", "tradingeconomics",  "US"),
    ("Core CPI y/y",                   "us.inflation.core_cpi_yoy", "investing",         "US"),
    ("Core CPI (YoY)",                 "us.inflation.core_cpi_yoy", "investing",         "US"),
    ("Core Inflation Rate YoY",        "us.inflation.core_cpi_yoy", "tradingeconomics",  "US"),
    ("PCE Price Index m/m",            "us.inflation.pce_mom",      "investing",         "US"),
    ("PCE Prices (MoM)",               "us.inflation.pce_mom",      "investing",         "US"),
    ("Personal Spending m/m",          "us.inflation.pce_mom",      "forexfactory",      "US"),
    ("PCE Price Index MoM",            "us.inflation.pce_mom",      "tradingeconomics",  "US"),
    ("PCE Price Index y/y",            "us.inflation.pce_yoy",      "investing",         "US"),
    ("PCE Prices (YoY)",               "us.inflation.pce_yoy",      "investing",         "US"),
    ("PCE Price Index YoY",            "us.inflation.pce_yoy",      "tradingeconomics",  "US"),
    ("Core PCE Price Index m/m",       "us.inflation.core_pce_mom", "investing",         "US"),
    ("Core PCE Prices (MoM)",          "us.inflation.core_pce_mom", "investing",         "US"),
    ("Core PCE Price Index MoM",       "us.inflation.core_pce_mom", "tradingeconomics",  "US"),
    ("Core PCE Price Index y/y",       "us.inflation.core_pce_yoy", "investing",         "US"),
    ("Core PCE Prices (YoY)",          "us.inflation.core_pce_yoy", "investing",         "US"),
    ("Core PCE Price Index YoY",       "us.inflation.core_pce_yoy", "tradingeconomics",  "US"),
    ("PPI m/m",                        "us.inflation.ppi_mom",      "investing",         "US"),
    ("PPI (MoM)",                      "us.inflation.ppi_mom",      "investing",         "US"),
    ("Producer Price Index m/m",       "us.inflation.ppi_mom",      "forexfactory",      "US"),
    ("PPI MoM",                        "us.inflation.ppi_mom",      "tradingeconomics",  "US"),
    ("PPI y/y",                        "us.inflation.ppi_yoy",      "investing",         "US"),
    ("PPI (YoY)",                      "us.inflation.ppi_yoy",      "investing",         "US"),
    ("PPI YoY",                        "us.inflation.ppi_yoy",      "tradingeconomics",  "US"),
    # ── US Employment ────────────────────────────────────────────
    ("Non-Farm Employment Change",     "us.employment.nfp",                "investing",         "US"),
    ("Nonfarm Payrolls",               "us.employment.nfp",                "investing",         "US"),
    ("Non-Farm Payrolls",              "us.employment.nfp",                "forexfactory",      "US"),
    ("Non Farm Payrolls",              "us.employment.nfp",                "tradingeconomics",  "US"),
    ("Nonfarm Payrolls Change",        "us.employment.nfp",                "tradingeconomics",  "US"),
    ("Unemployment Rate",              "us.employment.unemployment_rate",  "investing",         "US"),
    ("Unemployment Rate",              "us.employment.unemployment_rate",  "forexfactory",      "US"),
    ("Unemployment Rate",              "us.employment.unemployment_rate",  "tradingeconomics",  "US"),
    ("Initial Jobless Claims",         "us.employment.initial_claims",     "investing",         "US"),
    ("Unemployment Claims",            "us.employment.initial_claims",     "forexfactory",      "US"),
    ("Initial Claims",                 "us.employment.initial_claims",     "tradingeconomics",  "US"),
    ("Continuing Jobless Claims",      "us.employment.continuing_claims",  "investing",         "US"),
    ("Continuing Claims",              "us.employment.continuing_claims",  "tradingeconomics",  "US"),
    ("ADP Non-Farm Employment Change", "us.employment.adp",               "investing",         "US"),
    ("ADP Nonfarm Employment Change",  "us.employment.adp",               "investing",         "US"),
    ("ADP Employment Change",          "us.employment.adp",               "forexfactory",      "US"),
    ("ADP Employment Change",          "us.employment.adp",               "tradingeconomics",  "US"),
    ("JOLTS Job Openings",             "us.employment.jolts",             "investing",         "US"),
    ("JOLTs Job Openings",             "us.employment.jolts",             "forexfactory",      "US"),
    ("Job Openings",                   "us.employment.jolts",             "tradingeconomics",  "US"),
    ("Average Hourly Earnings m/m",    "us.employment.avg_hourly_earnings","investing",         "US"),
    ("Average Hourly Earnings (MoM)",  "us.employment.avg_hourly_earnings","investing",         "US"),
    ("Average Hourly Earnings MoM",    "us.employment.avg_hourly_earnings","tradingeconomics",  "US"),
    # ── US Growth ────────────────────────────────────────────────
    ("GDP q/q",                        "us.growth.gdp_qoq",          "investing",         "US"),
    ("Advance GDP q/q",                "us.growth.gdp_qoq",          "investing",         "US"),
    ("GDP (QoQ)",                      "us.growth.gdp_qoq",          "investing",         "US"),
    ("Preliminary GDP q/q",            "us.growth.gdp_qoq",          "investing",         "US"),
    ("Final GDP q/q",                  "us.growth.gdp_qoq",          "investing",         "US"),
    ("GDP Growth Rate QoQ",            "us.growth.gdp_qoq",          "tradingeconomics",  "US"),
    ("Advance GDP",                    "us.growth.gdp_qoq",          "forexfactory",      "US"),
    ("Final GDP",                      "us.growth.gdp_qoq",          "forexfactory",      "US"),
    ("Prelim GDP",                     "us.growth.gdp_qoq",          "forexfactory",      "US"),
    ("Retail Sales m/m",               "us.growth.retail_sales_mom",  "investing",         "US"),
    ("Retail Sales (MoM)",             "us.growth.retail_sales_mom",  "investing",         "US"),
    ("Retail Sales MoM",               "us.growth.retail_sales_mom",  "tradingeconomics",  "US"),
    ("Core Retail Sales m/m",          "us.growth.retail_sales_mom",  "forexfactory",      "US"),
    ("ISM Manufacturing PMI",          "us.growth.ism_mfg_pmi",      "investing",         "US"),
    ("ISM Manufacturing PMI",          "us.growth.ism_mfg_pmi",      "forexfactory",      "US"),
    ("ISM Manufacturing PMI",          "us.growth.ism_mfg_pmi",      "tradingeconomics",  "US"),
    ("ISM Non-Manufacturing PMI",      "us.growth.ism_services_pmi",  "investing",         "US"),
    ("ISM Services PMI",               "us.growth.ism_services_pmi",  "investing",         "US"),
    ("ISM Services PMI",               "us.growth.ism_services_pmi",  "forexfactory",      "US"),
    ("ISM Services PMI",               "us.growth.ism_services_pmi",  "tradingeconomics",  "US"),
    ("Industrial Production m/m",      "us.growth.industrial_prod",   "investing",         "US"),
    ("Industrial Production (MoM)",    "us.growth.industrial_prod",   "investing",         "US"),
    ("Industrial Production MoM",      "us.growth.industrial_prod",   "tradingeconomics",  "US"),
    ("Durable Goods Orders m/m",       "us.growth.durable_goods",     "investing",         "US"),
    ("Core Durable Goods Orders m/m",  "us.growth.durable_goods",     "investing",         "US"),
    ("Durable Goods Orders MoM",       "us.growth.durable_goods",     "tradingeconomics",  "US"),
    ("S&P Global Manufacturing PMI",   "us.growth.sp_global_mfg_pmi", "investing",         "US"),
    ("Flash Manufacturing PMI",        "us.growth.sp_global_mfg_pmi", "investing",         "US"),
    ("S&P Global Manufacturing PMI",   "us.growth.sp_global_mfg_pmi", "tradingeconomics",  "US"),
    # ── US Policy ────────────────────────────────────────────────
    ("Fed Interest Rate Decision",     "us.policy.fed_rate",          "investing",         "US"),
    ("Federal Funds Rate",             "us.policy.fed_rate",          "investing",         "US"),
    ("Federal Funds Rate",             "us.policy.fed_rate",          "forexfactory",      "US"),
    ("Fed Interest Rate Decision",     "us.policy.fed_rate",          "tradingeconomics",  "US"),
    ("FOMC Minutes",                   "us.policy.fomc_minutes",      "investing",         "US"),
    ("FOMC Meeting Minutes",           "us.policy.fomc_minutes",      "investing",         "US"),
    ("FOMC Meeting Minutes",           "us.policy.fomc_minutes",      "forexfactory",      "US"),
    ("FOMC Minutes",                   "us.policy.fomc_minutes",      "tradingeconomics",  "US"),
    ("FOMC Statement",                 "us.policy.fomc_statement",    "investing",         "US"),
    ("FOMC Statement",                 "us.policy.fomc_statement",    "forexfactory",      "US"),
    ("FOMC Statement",                 "us.policy.fomc_statement",    "tradingeconomics",  "US"),
    ("Fed Chair Powell Speaks",        "us.policy.fed_chair_speech",  "investing",         "US"),
    ("FOMC Press Conference",          "us.policy.fed_chair_speech",  "investing",         "US"),
    ("FOMC Press Conference",          "us.policy.fed_chair_speech",  "forexfactory",      "US"),
    # ── US Housing ───────────────────────────────────────────────
    ("Existing Home Sales",            "us.housing.existing_home_sales","investing",        "US"),
    ("Existing Home Sales",            "us.housing.existing_home_sales","forexfactory",     "US"),
    ("Existing Home Sales",            "us.housing.existing_home_sales","tradingeconomics", "US"),
    ("New Home Sales",                 "us.housing.new_home_sales",     "investing",        "US"),
    ("New Home Sales",                 "us.housing.new_home_sales",     "forexfactory",     "US"),
    ("New Home Sales",                 "us.housing.new_home_sales",     "tradingeconomics", "US"),
    ("Building Permits",               "us.housing.building_permits",   "investing",        "US"),
    ("Building Permits",               "us.housing.building_permits",   "forexfactory",     "US"),
    ("Building Permits",               "us.housing.building_permits",   "tradingeconomics", "US"),
    # ── US Consumer ──────────────────────────────────────────────
    ("Michigan Consumer Sentiment",            "us.consumer.michigan_sentiment", "investing",        "US"),
    ("Revised UoM Consumer Sentiment",         "us.consumer.michigan_sentiment", "investing",        "US"),
    ("Prelim UoM Consumer Sentiment",          "us.consumer.michigan_sentiment", "investing",        "US"),
    ("University of Michigan Consumer Sentiment","us.consumer.michigan_sentiment","tradingeconomics","US"),
    ("CB Consumer Confidence",                  "us.consumer.cb_confidence",      "investing",        "US"),
    ("Consumer Confidence",                     "us.consumer.cb_confidence",      "forexfactory",     "US"),
    ("Consumer Confidence",                     "us.consumer.cb_confidence",      "tradingeconomics", "US"),
    # ── US Trade ─────────────────────────────────────────────────
    ("Trade Balance",                  "us.trade.balance",            "investing",         "US"),
    ("Trade Balance",                  "us.trade.balance",            "forexfactory",      "US"),
    ("Trade Balance",                  "us.trade.balance",            "tradingeconomics",  "US"),
    # ── EU ───────────────────────────────────────────────────────
    ("CPI y/y",                        "eu.inflation.hicp_yoy",      "investing",         "EU"),
    ("CPI (YoY)",                      "eu.inflation.hicp_yoy",      "investing",         "EU"),
    ("HICP YoY",                       "eu.inflation.hicp_yoy",      "tradingeconomics",  "EU"),
    ("Inflation Rate YoY",             "eu.inflation.hicp_yoy",      "tradingeconomics",  "EU"),
    ("CPI m/m",                        "eu.inflation.hicp_mom",      "investing",         "EU"),
    ("HICP MoM",                       "eu.inflation.hicp_mom",      "tradingeconomics",  "EU"),
    ("Core CPI y/y",                   "eu.inflation.core_hicp_yoy", "investing",         "EU"),
    ("Core CPI (YoY)",                 "eu.inflation.core_hicp_yoy", "investing",         "EU"),
    ("Core Inflation Rate YoY",        "eu.inflation.core_hicp_yoy", "tradingeconomics",  "EU"),
    ("GDP q/q",                        "eu.growth.gdp_qoq",         "investing",         "EU"),
    ("GDP (QoQ)",                      "eu.growth.gdp_qoq",         "investing",         "EU"),
    ("GDP Growth Rate QoQ",            "eu.growth.gdp_qoq",         "tradingeconomics",  "EU"),
    ("Unemployment Rate",              "eu.employment.unemployment", "investing",         "EU"),
    ("Unemployment Rate",              "eu.employment.unemployment", "tradingeconomics",  "EU"),
    ("ECB Interest Rate Decision",     "eu.policy.ecb_rate",         "investing",         "EU"),
    ("ECB Main Refinancing Rate",      "eu.policy.ecb_rate",         "investing",         "EU"),
    ("Minimum Bid Rate",               "eu.policy.ecb_rate",         "forexfactory",      "EU"),
    ("ECB Interest Rate Decision",     "eu.policy.ecb_rate",         "tradingeconomics",  "EU"),
    # ── JP ───────────────────────────────────────────────────────
    ("BOJ Interest Rate Decision",     "jp.policy.boj_rate",         "investing",         "JP"),
    ("BOJ Policy Rate",                "jp.policy.boj_rate",         "investing",         "JP"),
    ("Monetary Policy Statement",      "jp.policy.boj_rate",         "forexfactory",      "JP"),
    ("BOJ Interest Rate Decision",     "jp.policy.boj_rate",         "tradingeconomics",  "JP"),
    ("CPI y/y",                        "jp.inflation.cpi_yoy",       "investing",         "JP"),
    ("National Core CPI y/y",          "jp.inflation.cpi_yoy",       "investing",         "JP"),
    ("Inflation Rate YoY",             "jp.inflation.cpi_yoy",       "tradingeconomics",  "JP"),
    ("GDP q/q",                        "jp.growth.gdp_qoq",         "investing",         "JP"),
    ("GDP Growth Rate QoQ",            "jp.growth.gdp_qoq",         "tradingeconomics",  "JP"),
    # ── UK ───────────────────────────────────────────────────────
    ("BOE Interest Rate Decision",     "gb.policy.boe_rate",         "investing",         "UK"),
    ("Official Bank Rate",             "gb.policy.boe_rate",         "forexfactory",      "UK"),
    ("BOE Interest Rate Decision",     "gb.policy.boe_rate",         "tradingeconomics",  "UK"),
    ("CPI y/y",                        "gb.inflation.cpi_yoy",       "investing",         "UK"),
    ("Inflation Rate YoY",             "gb.inflation.cpi_yoy",       "tradingeconomics",  "UK"),
    ("GDP q/q",                        "gb.growth.gdp_qoq",         "investing",         "UK"),
    ("GDP Growth Rate QoQ",            "gb.growth.gdp_qoq",         "tradingeconomics",  "UK"),
    # ── CN ───────────────────────────────────────────────────────
    ("PBoC Interest Rate Decision",    "cn.policy.pboc_rate",        "investing",         "CN"),
    ("PBOC Interest Rate Decision",    "cn.policy.pboc_rate",        "tradingeconomics",  "CN"),
    ("Chinese CPI y/y",                "cn.inflation.cpi_yoy",       "investing",         "CN"),
    ("CPI y/y",                        "cn.inflation.cpi_yoy",       "investing",         "CN"),
    ("Inflation Rate YoY",             "cn.inflation.cpi_yoy",       "tradingeconomics",  "CN"),
    ("GDP y/y",                        "cn.growth.gdp_yoy",          "investing",         "CN"),
    ("Chinese GDP q/q",                "cn.growth.gdp_yoy",          "investing",         "CN"),
    ("GDP Growth Rate YoY",            "cn.growth.gdp_yoy",          "tradingeconomics",  "CN"),
    ("Manufacturing PMI",              "cn.growth.mfg_pmi",          "investing",         "CN"),
    ("NBS Manufacturing PMI",          "cn.growth.mfg_pmi",          "investing",         "CN"),
    ("Manufacturing PMI",              "cn.growth.mfg_pmi",          "tradingeconomics",  "CN"),
]



class _CalendarQueriesMixin:
    def upsert_calendar_event(self, event: StoredEventRecord) -> None:
        with self._connection(commit=True) as connection:
            now_iso = utc_now().isoformat()
            append_calendar_event_vintage_if_changed_with_conn(
                connection,
                event_id=event.event_id,
                provider=event.source,
                vintage_date=now_iso,
                observed_at=now_iso,
                actual=event.actual,
                forecast=event.forecast,
                previous=event.previous,
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO calendar_events (
                    source,
                    event_id,
                    timestamp,
                    country,
                    indicator,
                    category,
                    importance,
                    actual,
                    forecast,
                    previous,
                    revised_previous,
                    surprise,
                    currency,
                    unit,
                    raw_json,
                    indicator_id,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.source,
                    event.event_id,
                    event.timestamp,
                    event.country,
                    event.indicator,
                    event.category,
                    event.importance,
                    event.actual,
                    event.forecast,
                    event.previous,
                    event.revised_previous,
                    event.surprise,
                    event.currency,
                    event.unit,
                    json.dumps(event.raw_json, ensure_ascii=True, sort_keys=True),
                    event.indicator_id,
                    utc_now().isoformat(),
                ),
            )

    def list_recent_events(
        self,
        *,
        limit: int = 10,
        days: int = 7,
        released_only: bool = False,
        importance: str | None = None,
        country: str | None = None,
        category: str | None = None,
    ) -> list[StoredEventRecord]:
        cutoff = (utc_now() - timedelta(days=days)).isoformat()
        now_iso = utc_now().isoformat()
        conditions: list[str] = []
        params: list[Any] = []
        _add_event_time_lower_bound(conditions, params, cutoff)
        _add_event_time_upper_bound(conditions, params, now_iso)
        if released_only:
            conditions.append("actual IS NOT NULL")
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            _add_calendar_country_filter(conditions, params, country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM cal_econ_event
                WHERE {' AND '.join(conditions)}
                ORDER BY datetime(event_time_utc) DESC, provider_event_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_econ_event(row) for row in rows]

    def list_upcoming_events(
        self,
        *,
        limit: int = 10,
        importance: str | None = None,
        country: str | None = None,
        category: str | None = None,
    ) -> list[StoredEventRecord]:
        now_iso = utc_now().isoformat()
        conditions: list[str] = []
        params: list[Any] = []
        _add_event_time_lower_bound(conditions, params, now_iso)
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            _add_calendar_country_filter(conditions, params, country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM cal_econ_event
                WHERE {' AND '.join(conditions)}
                ORDER BY datetime(event_time_utc) ASC, provider_event_id ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_econ_event(row) for row in rows]

    def list_events_in_range(
        self,
        *,
        date_from: int,
        date_to: int,
        limit: int = 50,
        importance: str | None = None,
        country: str | None = None,
        category: str | None = None,
        released_only: bool = False,
    ) -> list[StoredEventRecord]:
        from_iso = datetime.fromtimestamp(date_from, tz=timezone.utc).isoformat()
        to_iso = datetime.fromtimestamp(date_to, tz=timezone.utc).isoformat()
        conditions: list[str] = []
        params: list[Any] = []
        _add_event_time_lower_bound(conditions, params, from_iso)
        _add_event_time_upper_bound(conditions, params, to_iso)
        if released_only:
            conditions.append("actual IS NOT NULL")
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            _add_calendar_country_filter(conditions, params, country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM cal_econ_event
                WHERE {' AND '.join(conditions)}
                ORDER BY datetime(event_time_utc) ASC, provider_event_id ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_econ_event(row) for row in rows]

    def list_today_events(
        self,
        *,
        limit: int = 50,
        importance: str | None = None,
        country: str | None = None,
        category: str | None = None,
    ) -> list[StoredEventRecord]:
        today = datetime.now(timezone.utc).date()
        date_from = int(datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp())
        date_to = int(datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        return self.list_events_in_range(
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            importance=importance,
            country=country,
            category=category,
        )

    def latest_released_event(self, *, indicator_keyword: str | None = None) -> StoredEventRecord | None:
        with self._connection(commit=False) as connection:
            params: list[Any] = []
            conditions = ["actual IS NOT NULL"]
            matched_keyword = _add_calendar_keyword_filter(
                conditions, params, indicator_keyword, connection=connection
            )
            if indicator_keyword is not None and not matched_keyword:
                return None
            row = connection.execute(
                f"""
                SELECT * FROM cal_econ_event
                WHERE {' AND '.join(conditions)}
                ORDER BY
                    datetime(event_time_utc) DESC,
                    CASE importance
                        WHEN 'high' THEN 3
                        WHEN 'medium' THEN 2
                        WHEN 'low' THEN 1
                        ELSE 0
                    END DESC,
                    provider_event_id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        return self._row_to_econ_event(row) if row is not None else None

    def _row_to_econ_event(self, row: sqlite3.Row) -> StoredEventRecord:
        parsed_event_time = datetime.fromisoformat(
            str(row["event_time_utc"]).replace("Z", "+00:00")
        )
        if parsed_event_time.tzinfo is None:
            parsed_event_time = parsed_event_time.replace(tzinfo=timezone.utc)
        timestamp = int(parsed_event_time.timestamp())
        return StoredEventRecord(
            source=row["provider"],
            event_id=row["provider_event_id"],
            timestamp=timestamp,
            country=_calendar_country_display(row["country_code"]),
            indicator=row["title"],
            category=row["category"],
            importance=row["importance"] or "medium",
            actual=row["actual"],
            forecast=row["forecast"] or row["consensus_forecast"],
            previous=row["previous"],
            revised_previous=row["revised"],
            surprise=_calendar_surprise(
                row["actual"],
                row["forecast"] or row["consensus_forecast"],
            ),
            currency=row["currency"],
            unit=row["unit"],
            raw_json={
                "event_time_utc": row["event_time_utc"],
                "event_time_precision": row["event_time_precision"],
                "provider_event_id": row["provider_event_id"],
                "provider": row["provider"],
                "country_code": row["country_code"],
                "reference_date": row["reference_date"],
                "reference_label": row["reference_label"],
                "source_url": row["source_url"],
                "content_hash": row["content_hash"],
            },
            indicator_id=row["indicator_id"],
            event_time_utc=row["event_time_utc"],
            event_time_precision=row["event_time_precision"] or "datetime",
        )

    def _row_to_event(self, row: sqlite3.Row) -> StoredEventRecord:
        return StoredEventRecord(
            source=row["source"],
            event_id=row["event_id"],
            timestamp=int(row["timestamp"]),
            country=row["country"],
            indicator=row["indicator"],
            category=row["category"],
            importance=row["importance"],
            actual=row["actual"],
            forecast=row["forecast"],
            previous=row["previous"],
            revised_previous=row["revised_previous"],
            surprise=row["surprise"],
            currency=row["currency"],
            unit=row["unit"],
            raw_json=json.loads(row["raw_json"]),
            indicator_id=row["indicator_id"],
        )

    def append_calendar_event_vintage_if_changed(
        self, vintage: CalendarEventVintageRecord,
    ) -> bool:
        with self._connection(commit=True) as connection:
            return append_calendar_event_vintage_if_changed_with_conn(
                connection,
                event_id=vintage.event_id,
                provider=vintage.provider,
                vintage_date=vintage.vintage_date,
                observed_at=vintage.observed_at,
                actual=vintage.actual,
                forecast=vintage.forecast,
                previous=vintage.previous,
                metadata_json=json.dumps(
                    vintage.metadata, ensure_ascii=True, sort_keys=True
                ),
                source_url=vintage.source_url,
            )

    def calendar_actual_as_of(
        self, event_id: str, provider: str, as_of: str,
    ) -> CalendarEventVintageRecord | None:
        """Return the vintage with the greatest ``observed_at <= as_of`` for
        ``(event_id, provider)``. ``None`` when no vintage exists at or
        before the cutoff.

        On ties (identical ``observed_at``), the row with the larger ``id``
        wins — i.e. the most recently appended vintage at that timestamp.
        """
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT event_id, provider, vintage_date, observed_at, "
                "actual, forecast, previous, metadata_json, "
                "source_url, evidence_archive_url "
                "FROM calendar_event_vintages "
                "WHERE event_id = ? AND provider = ? "
                "AND julianday(observed_at) <= julianday(?) "
                "ORDER BY julianday(observed_at) DESC, id DESC LIMIT 1",
                (event_id, provider, as_of),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_calendar_vintage(row)

    def calendar_vintage_history(
        self, event_id: str, provider: str,
    ) -> list[CalendarEventVintageRecord]:
        """Return every vintage for ``(event_id, provider)`` ordered by
        ``observed_at`` ascending, ``id`` ascending on ties.
        """
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT event_id, provider, vintage_date, observed_at, "
                "actual, forecast, previous, metadata_json, "
                "source_url, evidence_archive_url "
                "FROM calendar_event_vintages "
                "WHERE event_id = ? AND provider = ? "
                "ORDER BY julianday(observed_at) ASC, id ASC",
                (event_id, provider),
            ).fetchall()
        return [self._row_to_calendar_vintage(row) for row in rows]

    @staticmethod
    def _row_to_calendar_vintage(row: sqlite3.Row) -> CalendarEventVintageRecord:
        return CalendarEventVintageRecord(
            event_id=row["event_id"],
            provider=row["provider"],
            vintage_date=row["vintage_date"],
            observed_at=row["observed_at"],
            actual=row["actual"],
            forecast=row["forecast"],
            previous=row["previous"],
            metadata=json.loads(row["metadata_json"]),
            source_url=row["source_url"] or "",
            evidence_archive_url=row["evidence_archive_url"],
        )

    def upsert_calendar_indicator(self, record: CalendarIndicatorRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO calendar_indicator (
                    indicator_id, canonical_name, topic, country_code,
                    frequency, unit, obs_family_id, is_active,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.indicator_id,
                    record.canonical_name,
                    record.topic,
                    record.country_code,
                    record.frequency,
                    record.unit,
                    record.obs_family_id,
                    int(record.is_active),
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_calendar_indicator(self, indicator_id: str) -> CalendarIndicatorRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM calendar_indicator WHERE indicator_id = ?",
                (indicator_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_calendar_indicator(row)

    def list_calendar_items(
        self,
        *,
        domain: str | None = None,
        country: str | None = None,
        ticker: str | None = None,
        subtype: str | None = None,
        provider: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        """Filtered read of ``v_calendar_item`` with offset pagination.

        Returns ``(items, total_count)``. ``items`` is the current
        page as a list of dicts shaped to the ``CalendarItem`` DTO
        contract; ``total_count`` is the number of rows matching the
        filter set (before offset/limit are applied) so HTTP callers
        can compute ``links.next`` themselves.

        Filter semantics — every non-``None`` argument collapses to an
        equality clause on the view column:

        - ``domain``   → ``'economic'`` / ``'corporate'``
        - ``country``  → ISO-3166 alpha-2 (economic lane only; corporate
                         rows have ``NULL`` and won't match any value).
        - ``ticker``   → corporate lane only (economic rows are ``NULL``).
        - ``subtype``  → ``'release'`` for econ, ``dividend`` / ``earnings``
                         / ``earnings_trend`` / ``ipo`` / ``split`` for
                         corp.
        - ``provider`` → ``cal_provider.provider_id`` value.

        ``offset`` is clamped to ``>= 0``; ``limit`` to ``[1, 500]``.
        Rows are ordered by ``event_time_utc`` ascending then
        ``event_id`` for stable pagination.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if domain:
            conditions.append("domain = ?")
            params.append(domain)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if ticker:
            conditions.append("ticker = ?")
            params.append(ticker)
        if subtype:
            conditions.append("subtype = ?")
            params.append(subtype)
        if provider:
            conditions.append("provider = ?")
            params.append(provider)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        safe_offset = max(0, int(offset))
        safe_limit = max(1, min(500, int(limit)))

        with self._connection(commit=False) as connection:
            total_row = connection.execute(
                f"SELECT COUNT(*) FROM v_calendar_item {where}",
                params,
            ).fetchone()
            total_count = int(total_row[0]) if total_row else 0
            rows = connection.execute(
                f"""
                SELECT event_id, domain, subtype, provider, provider_event_id,
                       event_time_utc, event_time_precision, title, country,
                       ticker, exchange, currency, importance, indicator_id,
                       reference_date, actual, previous, forecast,
                       consensus_forecast, source_url, last_update_epoch_ms,
                       observed_at_epoch_ms, payload_json
                FROM v_calendar_item
                {where}
                ORDER BY event_time_utc ASC, event_id ASC
                LIMIT ? OFFSET ?
                """,
                [*params, safe_limit, safe_offset],
            ).fetchall()

        items: list[dict[str, Any]] = []
        for row in rows:
            (
                event_id, domain_val, subtype_val, provider_val, _peid,
                event_time_utc, precision, title, country_val,
                ticker_val, exchange_val, currency_val, importance_val,
                indicator_id, reference_date, actual, previous, forecast,
                consensus_forecast, source_url, last_update_epoch_ms,
                _observed_at, payload_raw,
            ) = row
            values: dict[str, Any] = {}
            if actual is not None:
                values["actual"] = actual
            if previous is not None:
                values["previous"] = previous
            if forecast is not None:
                values["forecast"] = forecast
            if consensus_forecast is not None:
                values["consensus_forecast"] = consensus_forecast
            # Corporate lane: the value-bearing fields (eps_actual,
            # dividend_amount, split_ratio, ipo_price, …) live in
            # ``cal_corp_event.payload_json`` — the economic-column
            # slots are NULL in the view. Flatten scalar keys from the
            # payload into ``values`` so the unified DTO carries the
            # subtype-specific data HTTP clients need. Nested objects
            # / arrays are skipped here because the CalendarItem
            # contract declares ``values: dict[str, str | None]``.
            if payload_raw and domain_val == "corporate":
                try:
                    payload = json.loads(payload_raw)
                except (TypeError, ValueError):
                    payload = None
                if isinstance(payload, dict):
                    for key, val in payload.items():
                        if val is None:
                            continue
                        if isinstance(val, (str, int, float, bool)):
                            values.setdefault(str(key), str(val))
            items.append(
                {
                    "event_id": event_id,
                    "release_time": event_time_utc,
                    "release_time_precision": precision,
                    "indicator": indicator_id or "",
                    "country": country_val or "",
                    "importance": importance_val or "medium",
                    "domain": domain_val,
                    "subtype": subtype_val,
                    "provider": provider_val,
                    "title": title or "",
                    "ticker": ticker_val,
                    "exchange": exchange_val,
                    "currency": currency_val,
                    "reference_date": reference_date,
                    "expected": forecast,
                    "previous": previous,
                    "values": values,
                    "last_update_epoch_ms": last_update_epoch_ms,
                    "source_url": source_url or "",
                    "references": [],
                    "notes": "",
                    "tags": [],
                }
            )
        return items, total_count

    def list_calendar_indicators(
        self,
        *,
        country_code: str | None = None,
        topic: str | None = None,
        active_only: bool = True,
    ) -> list[CalendarIndicatorRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if active_only:
            conditions.append("is_active = 1")
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic:
            conditions.append("topic = ?")
            params.append(topic)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM calendar_indicator {where} ORDER BY indicator_id",
                params,
            ).fetchall()
        return [self._row_to_calendar_indicator(row) for row in rows]

    def upsert_calendar_indicator_alias(self, record: CalendarIndicatorAliasRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO calendar_indicator_alias (
                    alias_normalized, indicator_id, source, country_code,
                    alias_original, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    record.alias_normalized,
                    record.indicator_id,
                    record.source,
                    record.country_code,
                    record.alias_original,
                    record.created_at,
                ),
            )

    def resolve_calendar_alias(
        self, alias_text: str, source: str, country: str,
    ) -> str | None:
        from ingestion.scrapers._common import normalize_indicator_name
        normalized = normalize_indicator_name(alias_text)
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT indicator_id FROM calendar_indicator_alias
                WHERE alias_normalized = ? AND source = ? AND country_code = ?
                """,
                (normalized, source, country),
            ).fetchone()
        return row["indicator_id"] if row else None

    def list_aliases_for_indicator(self, indicator_id: str) -> list[CalendarIndicatorAliasRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM calendar_indicator_alias WHERE indicator_id = ? ORDER BY source, alias_normalized",
                (indicator_id,),
            ).fetchall()
        return [
            CalendarIndicatorAliasRecord(
                alias_normalized=row["alias_normalized"],
                indicator_id=row["indicator_id"],
                source=row["source"],
                country_code=row["country_code"],
                alias_original=row["alias_original"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def seed_calendar_indicators(self) -> None:
        """Populate calendar_indicator and calendar_indicator_alias tables
        from the module-level seed data constants."""
        from ingestion.scrapers._common import normalize_indicator_name
        now = utc_now().isoformat()

        for ind_id, canon, topic, cc, freq, unit, obs_fam in _CALENDAR_INDICATOR_DEFS:
            self.upsert_calendar_indicator(CalendarIndicatorRecord(
                indicator_id=ind_id,
                canonical_name=canon,
                topic=topic,
                country_code=cc,
                frequency=freq,
                unit=unit,
                obs_family_id=obs_fam or None,
                is_active=True,
                created_at=now,
                updated_at=now,
            ))

        for alias_orig, ind_id, source, cc in _CALENDAR_ALIAS_DEFS:
            self.upsert_calendar_indicator_alias(CalendarIndicatorAliasRecord(
                alias_normalized=normalize_indicator_name(alias_orig),
                indicator_id=ind_id,
                source=source,
                country_code=cc,
                alias_original=alias_orig,
                created_at=now,
            ))

    def backfill_calendar_indicator_ids(self) -> int:
        """Set indicator_id on existing calendar_events rows from the alias table.
        Returns the number of rows updated."""
        from ingestion.scrapers._common import normalize_indicator_name  # noqa: F811
        with self._connection(commit=True) as connection:
            cur = connection.execute(
                """
                UPDATE calendar_events SET indicator_id = (
                    SELECT a.indicator_id FROM calendar_indicator_alias a
                    WHERE a.alias_normalized = LOWER(TRIM(calendar_events.indicator))
                      AND a.source = calendar_events.source
                      AND a.country_code = calendar_events.country
                ) WHERE indicator_id IS NULL
                """
            )
        return cur.rowcount or 0

    def list_indicator_releases_by_id(
        self, indicator_id: str, *, limit: int = 12,
    ) -> list[StoredEventRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM cal_econ_event
                WHERE indicator_id = ? AND actual IS NOT NULL
                ORDER BY datetime(event_time_utc) DESC, provider_event_id DESC
                LIMIT ?
                """,
                (indicator_id, limit),
            ).fetchall()
        return [self._row_to_econ_event(row) for row in rows]

    def _row_to_calendar_indicator(self, row: sqlite3.Row) -> CalendarIndicatorRecord:
        return CalendarIndicatorRecord(
            indicator_id=row["indicator_id"],
            canonical_name=row["canonical_name"],
            topic=row["topic"],
            country_code=row["country_code"],
            frequency=row["frequency"],
            unit=row["unit"],
            obs_family_id=row["obs_family_id"],
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    _RELEASE_SCHEDULE_DEFS: list[tuple[str, str, dict[str, Any], str, str, str, str, str]] = [
        # ── US Inflation ──────────────────────────────────────────────
        ("CPI_US",              "day_of_month",     {"day": 12, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "BLS CPI release ~12th"),
        ("CORE_CPI_US",         "day_of_month",     {"day": 12, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Released with CPI"),
        ("CPI_FOOD_US",         "day_of_month",     {"day": 12, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Released with CPI"),
        ("CPI_ENERGY_US",       "day_of_month",     {"day": 12, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Released with CPI"),
        ("CPI_SHELTER_US",      "day_of_month",     {"day": 12, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Released with CPI"),
        ("PPI_US",              "day_of_month",     {"day": 14, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "BLS PPI release ~14th"),
        ("PPI_CORE_US",         "day_of_month",     {"day": 14, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Released with PPI"),
        ("CORE_PCE_US",         "day_of_month",     {"day": 28, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "BEA PCE release ~28th"),
        ("BREAKEVEN_5Y_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "TIPS-derived, daily"),
        ("BREAKEVEN_10Y_US",    "daily",            {},                                     "daily",    "",      "",                 "pattern", "TIPS-derived, daily"),
        # ── US Employment ─────────────────────────────────────────────
        ("NFP_US",              "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "1st Friday of month"),
        ("NFP_PRIVATE_US",      "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "Released with NFP"),
        ("AVG_HOURLY_EARN_US",  "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "Released with NFP"),
        ("AVG_WEEKLY_HOURS_US", "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "Released with NFP"),
        ("UNEMP_US",            "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "Released with NFP"),
        ("LFPR_US",             "weekday_of_month", {"weekday": 4, "ordinal": 1},           "monthly",  "12:30", "America/New_York", "pattern", "Released with NFP"),
        ("JOLTS_OPENINGS_US",   "approximate_window", {"month_offset": 2, "window_days": 10}, "monthly", "14:00", "America/New_York", "approximate", "JOLTS ~2 month lag"),
        ("JOLTS_HIRES_US",      "approximate_window", {"month_offset": 2, "window_days": 10}, "monthly", "14:00", "America/New_York", "approximate", "Released with JOLTS"),
        ("JOLTS_QUITS_US",      "approximate_window", {"month_offset": 2, "window_days": 10}, "monthly", "14:00", "America/New_York", "approximate", "Released with JOLTS"),
        ("ECI_US",              "quarter_lag",      {"lag_days": 35},                       "quarterly","12:30", "America/New_York", "pattern", "BLS ECI ~T+35 after Q"),
        ("INITIAL_CLAIMS_US",   "weekly",           {"weekday": 3},                         "weekly",   "12:30", "America/New_York", "pattern", "Thursday weekly"),
        ("CONTINUING_CLAIMS_US","weekly",           {"weekday": 3},                         "weekly",   "12:30", "America/New_York", "pattern", "Thursday weekly"),
        # ── US Productivity ───────────────────────────────────────────
        ("PRODUCTIVITY_US",     "quarter_lag",      {"lag_days": 35},                       "quarterly","12:30", "America/New_York", "pattern", "BLS quarterly"),
        ("UNIT_LABOR_COST_US",  "quarter_lag",      {"lag_days": 35},                       "quarterly","12:30", "America/New_York", "pattern", "Released with productivity"),
        # ── US Growth ─────────────────────────────────────────────────
        ("GDP_NOMINAL_US",      "quarter_lag",      {"lag_days": 30},                       "quarterly","12:30", "America/New_York", "pattern", "BEA advance GDP ~T+30"),
        ("GDP_REAL_US",         "quarter_lag",      {"lag_days": 30},                       "quarterly","12:30", "America/New_York", "pattern", "BEA advance GDP ~T+30"),
        ("RETAIL_SALES_US",     "day_of_month",     {"day": 15, "tolerance_days": 3},       "monthly",  "12:30", "America/New_York", "pattern", "Census ~15th"),
        ("INDPRO_US",           "day_of_month",     {"day": 16, "tolerance_days": 3},       "monthly",  "13:15", "America/New_York", "pattern", "Fed ~16th"),
        ("GDP_GROWTH_WB_US",    "approximate_window", {"month_offset": 6, "window_days": 60}, "annual",  "",     "",                 "approximate", "World Bank annual"),
        # ── US Rates ──────────────────────────────────────────────────
        ("POLICY_RATE_US",      "daily",            {},                                     "daily",    "",      "",                 "pattern", "NY Fed EFFR daily"),
        ("SOFR_US",             "daily",            {},                                     "daily",    "",      "",                 "pattern", "SOFR daily"),
        ("OBFR_US",             "daily",            {},                                     "daily",    "",      "",                 "pattern", "OBFR daily"),
        ("TREASURY_2Y_US",      "daily",            {},                                     "daily",    "",      "",                 "pattern", "Constant maturity daily"),
        ("TREASURY_10Y_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "Constant maturity daily"),
        ("TREASURY_30Y_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "Constant maturity daily"),
        ("REAL_YIELD_10Y_US",   "daily",            {},                                     "daily",    "",      "",                 "pattern", "TIPS daily"),
        ("SPREAD_10Y2Y_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "Yield curve daily"),
        # ── US Liquidity ──────────────────────────────────────────────
        ("FED_BALANCE_SHEET_US","weekly",           {"weekday": 3},                         "weekly",   "16:30", "America/New_York", "pattern", "Thursday weekly"),
        ("M2_US",               "day_of_month",     {"day": 22, "tolerance_days": 5},       "monthly",  "",      "",                 "pattern", "Fed ~22nd"),
        ("REVERSE_REPO_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "Daily ON RRP"),
        ("TGA_US",              "daily",            {},                                     "daily",    "",      "",                 "pattern", "Treasury daily/weekly"),
        # ── US FX ─────────────────────────────────────────────────────
        ("DOLLAR_INDEX_US",     "daily",            {},                                     "daily",    "",      "",                 "pattern", "Trade-weighted daily"),
        ("CNYUSD",              "daily",            {},                                     "daily",    "",      "",                 "pattern", "FX daily"),
        # ── US Credit ─────────────────────────────────────────────────
        ("HY_OAS_US",           "daily",            {},                                     "daily",    "",      "",                 "pattern", "ICE BofA daily"),
        ("CREDIT_GAP_US",       "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        # ── US Property ───────────────────────────────────────────────
        ("PROPERTY_US",         "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        # ── US Fiscal ─────────────────────────────────────────────────
        ("DEBT_US",             "daily",            {},                                     "daily",    "",      "",                 "pattern", "Treasury daily"),
        ("AVG_INTEREST_RATE_US","day_of_month",     {"day": 1, "tolerance_days": 5},        "monthly",  "",      "",                 "pattern", "Treasury ~1st"),
        # ── US Energy ─────────────────────────────────────────────────
        ("BRENT_CRUDE",         "daily",            {},                                     "daily",    "",      "",                 "pattern", "EIA daily spot"),
        ("WTI_CRUDE",           "daily",            {},                                     "daily",    "",      "",                 "pattern", "EIA daily spot"),
        ("CRUDE_STOCKS_US",     "weekly",           {"weekday": 2},                         "weekly",   "14:30", "America/New_York", "pattern", "EIA Wednesday"),
        ("NATGAS_US",           "daily",            {},                                     "daily",    "",      "",                 "pattern", "Henry Hub daily"),
        ("PETROLEUM_SUPPLY_US", "weekly",           {"weekday": 2},                         "weekly",   "14:30", "America/New_York", "pattern", "EIA Wednesday"),
        # ── US Trade ──────────────────────────────────────────────────
        ("EXPORTS_US",          "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("CURRENT_ACCOUNT_US",  "approximate_window", {"month_offset": 6, "window_days": 60}, "annual",  "",     "",                 "approximate", "World Bank annual"),
        # ── US Sentiment ──────────────────────────────────────────────
        ("CONSUMER_CONF_US",    "monthly_lag",      {"lag_months": 1, "day": 10, "tolerance_days": 10}, "monthly","",  "",           "approximate", "OECD monthly lag"),
        ("BUSINESS_CONF_US",    "monthly_lag",      {"lag_months": 1, "day": 10, "tolerance_days": 10}, "monthly","",  "",           "approximate", "OECD monthly lag"),
        ("CLI_US",              "monthly_lag",      {"lag_months": 2, "day": 10, "tolerance_days": 15}, "monthly","",  "",           "approximate", "OECD CLI ~2 month lag"),
        # ── US Development ────────────────────────────────────────────
        ("GDP_PER_CAPITA_US",   "approximate_window", {"month_offset": 6, "window_days": 60}, "annual",  "",     "",                 "approximate", "World Bank annual"),
        # ── China ─────────────────────────────────────────────────────
        ("CPI_CN",              "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("GDP_REAL_CN",         "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("FX_RESERVES_CN",      "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("POLICY_RATE_CN",      "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("CREDIT_GAP_CN",       "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("PROPERTY_CN",         "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("EER_CN",              "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("CLI_CN",              "monthly_lag",      {"lag_months": 2, "day": 10, "tolerance_days": 15}, "monthly","",  "",           "approximate", "OECD CLI ~2 month lag"),
        ("GDP_PER_CAPITA_CN",   "approximate_window", {"month_offset": 6, "window_days": 60}, "annual",  "",     "",                 "approximate", "World Bank annual"),
        # ── Japan ─────────────────────────────────────────────────────
        ("CPI_JP",              "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("GDP_REAL_JP",         "approximate_window", {"month_offset": 3, "window_days": 30}, "quarterly","",    "",                 "approximate", "IMF quarterly lag"),
        ("POLICY_RATE_JP",      "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("CLI_JP",              "monthly_lag",      {"lag_months": 2, "day": 10, "tolerance_days": 15}, "monthly","",  "",           "approximate", "OECD CLI ~2 month lag"),
        # ── Euro Area ─────────────────────────────────────────────────
        ("CPI_EU",              "monthly_lag",      {"lag_months": 1, "day": 15, "tolerance_days": 10}, "monthly","",  "",           "pattern",     "Eurostat HICP flash ~15th"),
        ("GDP_EU",              "monthly_lag",      {"lag_months": 1, "day": 15, "tolerance_days": 10}, "monthly","",  "",           "pattern",     "Eurostat GDP ~45 day lag"),
        ("UNEMP_EU",            "monthly_lag",      {"lag_months": 1, "day": 15, "tolerance_days": 10}, "monthly","",  "",           "pattern",     "Eurostat unemployment"),
        ("INDPRO_EU",           "monthly_lag",      {"lag_months": 1, "day": 15, "tolerance_days": 10}, "monthly","",  "",           "pattern",     "Eurostat industrial prod"),
        ("ESI_EU",              "monthly_lag",      {"lag_months": 1, "day": 15, "tolerance_days": 10}, "monthly","",  "",           "pattern",     "Eurostat ESI"),
        ("POLICY_RATE_EU",      "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB deposit rate"),
        ("M1_EU",               "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB monetary aggregate"),
        ("M2_EU",               "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB monetary aggregate"),
        ("M3_EU",               "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB monetary aggregate"),
        ("M3_GROWTH_EU",        "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB M3 YoY"),
        ("EURUSD",              "monthly_lag",      {"lag_months": 1, "day": 1, "tolerance_days": 10},  "monthly","",  "",           "pattern",     "ECB reference rate"),
        ("EER_EU",              "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
        ("CLI_EU",              "monthly_lag",      {"lag_months": 2, "day": 10, "tolerance_days": 15}, "monthly","",  "",           "approximate", "OECD CLI ~2 month lag"),
        # ── UK ────────────────────────────────────────────────────────
        ("POLICY_RATE_GB",      "approximate_window", {"month_offset": 6, "window_days": 30}, "quarterly","",    "",                 "approximate", "BIS quarterly lag"),
    ]

    def seed_release_schedules(self) -> None:
        """Populate the release_schedule table from built-in definitions."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            for (concept_id, rule_type, rule_json_dict, frequency,
                 release_time_utc, tz, confidence, notes) in self._RELEASE_SCHEDULE_DEFS:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO release_schedule
                        (concept_id, rule_type, rule_json, frequency,
                         release_time_utc, timezone, source_authority, confidence,
                         next_expected, last_released, last_checked,
                         is_active, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'manual', ?, '', '', '', 1, ?, ?, ?)
                    """,
                    (concept_id, rule_type,
                     json.dumps(rule_json_dict, sort_keys=True),
                     frequency, release_time_utc, tz, confidence, notes, now, now),
                )

    def upsert_release_schedule(self, record: ReleaseScheduleRecord) -> None:
        """INSERT OR REPLACE a release schedule record."""
        now = utc_now().isoformat()
        rule_json_str = (
            json.dumps(record.rule_json, sort_keys=True)
            if isinstance(record.rule_json, dict)
            else record.rule_json
        )
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO release_schedule
                    (concept_id, rule_type, rule_json, frequency,
                     release_time_utc, timezone, source_authority, confidence,
                     next_expected, last_released, last_checked,
                     is_active, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.concept_id, record.rule_type, rule_json_str,
                 record.frequency, record.release_time_utc, record.timezone,
                 record.source_authority, record.confidence,
                 record.next_expected, record.last_released, record.last_checked,
                 int(record.is_active), record.notes,
                 record.created_at or now, record.updated_at or now),
            )

    def get_release_schedule(self, concept_id: str) -> ReleaseScheduleRecord | None:
        """Lookup a single release schedule by concept_id."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM release_schedule WHERE concept_id = ?",
                (concept_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_release_schedule(row)

    def list_release_schedules(
        self,
        *,
        is_active: bool | None = None,
        due_before: str | None = None,
    ) -> list[ReleaseScheduleRecord]:
        """List release schedules, optionally filtered."""
        clauses: list[str] = []
        params: list[Any] = []
        if is_active is not None:
            clauses.append("is_active = ?")
            params.append(int(is_active))
        if due_before is not None:
            clauses.append("next_expected <= ? AND next_expected != ''")
            params.append(due_before)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM release_schedule{where} ORDER BY next_expected",
                params,
            ).fetchall()
            return [self._row_to_release_schedule(r) for r in rows]

    def update_release_timestamps(
        self,
        concept_id: str,
        *,
        next_expected: str = "",
        last_released: str = "",
        last_checked: str = "",
    ) -> None:
        """Lightweight partial update for scheduler loop."""
        sets: list[str] = []
        params: list[Any] = []
        if next_expected:
            sets.append("next_expected = ?")
            params.append(next_expected)
        if last_released:
            sets.append("last_released = ?")
            params.append(last_released)
        if last_checked:
            sets.append("last_checked = ?")
            params.append(last_checked)
        if not sets:
            return
        sets.append("updated_at = ?")
        params.append(utc_now().isoformat())
        params.append(concept_id)
        with self._connection(commit=True) as connection:
            connection.execute(
                f"UPDATE release_schedule SET {', '.join(sets)} WHERE concept_id = ?",
                params,
            )

    def _row_to_release_schedule(self, row: sqlite3.Row) -> ReleaseScheduleRecord:
        rj = row["rule_json"]
        try:
            rule_json = json.loads(rj) if isinstance(rj, str) else rj
        except (json.JSONDecodeError, TypeError):
            rule_json = {}
        return ReleaseScheduleRecord(
            concept_id=row["concept_id"],
            rule_type=row["rule_type"],
            rule_json=rule_json,
            frequency=row["frequency"],
            release_time_utc=row["release_time_utc"],
            timezone=row["timezone"],
            source_authority=row["source_authority"],
            confidence=row["confidence"],
            next_expected=row["next_expected"],
            last_released=row["last_released"],
            last_checked=row["last_checked"],
            is_active=bool(row["is_active"]),
            notes=row["notes"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_release_status(self, record: ReleaseStatusRecord) -> None:
        """INSERT OR REPLACE a release status tracking record."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO release_status
                    (concept_id, release_date, status, attempt_count,
                     next_retry, last_attempt, source_used, data_date,
                     expected_period, provisional, error, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (record.concept_id, record.release_date, record.status,
                 record.attempt_count, record.next_retry, record.last_attempt,
                 record.source_used, record.data_date, record.expected_period,
                 int(record.provisional), record.error,
                 record.created_at or now, record.updated_at or now),
            )

    def get_release_status(
        self, concept_id: str, release_date: str,
    ) -> ReleaseStatusRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM release_status WHERE concept_id = ? AND release_date = ?",
                (concept_id, release_date),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_release_status(row)

    def get_latest_release_status(
        self, concept_id: str,
    ) -> ReleaseStatusRecord | None:
        """Return the most recent release_status row for a concept."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM release_status WHERE concept_id = ? "
                "ORDER BY release_date DESC LIMIT 1",
                (concept_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_release_status(row)

    def list_release_statuses(
        self,
        *,
        status: str | None = None,
        pending_retry_before: str | None = None,
    ) -> list[ReleaseStatusRecord]:
        """List release status records, with optional filters."""
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if pending_retry_before is not None:
            clauses.append("next_retry != '' AND next_retry <= ?")
            params.append(pending_retry_before)
            clauses.append("status IN ('PENDING','WAITING')")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM release_status{where} "
                "ORDER BY release_date DESC, concept_id",
                params,
            ).fetchall()
            return [self._row_to_release_status(r) for r in rows]

    def list_all_latest_release_statuses(self) -> list[ReleaseStatusRecord]:
        """Return the most recent release_status row per concept_id."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT rs.* FROM release_status rs
                INNER JOIN (
                    SELECT concept_id, MAX(release_date) AS max_rd
                    FROM release_status GROUP BY concept_id
                ) latest ON rs.concept_id = latest.concept_id
                    AND rs.release_date = latest.max_rd
                ORDER BY rs.concept_id
                """
            ).fetchall()
            return [self._row_to_release_status(r) for r in rows]

    def update_release_status(
        self,
        concept_id: str,
        release_date: str,
        *,
        status: str = "",
        attempt_count: int | None = None,
        next_retry: str = "",
        last_attempt: str = "",
        source_used: str = "",
        data_date: str = "",
        provisional: bool | None = None,
        error: str | None = None,
    ) -> None:
        """Partial update of a release_status row."""
        sets: list[str] = []
        params: list[Any] = []
        if status:
            sets.append("status = ?")
            params.append(status)
        if attempt_count is not None:
            sets.append("attempt_count = ?")
            params.append(attempt_count)
        if next_retry:
            sets.append("next_retry = ?")
            params.append(next_retry)
        if last_attempt:
            sets.append("last_attempt = ?")
            params.append(last_attempt)
        if source_used:
            sets.append("source_used = ?")
            params.append(source_used)
        if data_date:
            sets.append("data_date = ?")
            params.append(data_date)
        if provisional is not None:
            sets.append("provisional = ?")
            params.append(int(provisional))
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if not sets:
            return
        sets.append("updated_at = ?")
        params.append(utc_now().isoformat())
        params.extend([concept_id, release_date])
        with self._connection(commit=True) as connection:
            connection.execute(
                f"UPDATE release_status SET {', '.join(sets)} "
                "WHERE concept_id = ? AND release_date = ?",
                params,
            )

    def _row_to_release_status(self, row: sqlite3.Row) -> ReleaseStatusRecord:
        return ReleaseStatusRecord(
            concept_id=row["concept_id"],
            release_date=row["release_date"],
            status=row["status"],
            attempt_count=row["attempt_count"],
            next_retry=row["next_retry"],
            last_attempt=row["last_attempt"],
            source_used=row["source_used"],
            data_date=row["data_date"],
            expected_period=row["expected_period"],
            provisional=bool(row["provisional"]),
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
