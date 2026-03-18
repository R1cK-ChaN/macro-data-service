from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from contracts import (
    epoch_to_datetime,
    format_epoch_iso,
    format_epoch_iso_in_timezone,
    normalize_utc_iso,
    to_epoch_ms,
    utc_now,
)


def default_engine_db_path(root: Path | None = None) -> Path:
    base = root or Path.cwd()
    return base / ".macro-data" / "engine.db"


def _matches_scope_tags(text: str, tags: list[str]) -> bool:
    lowered = text.lower()
    return any(re.search(rf"\b{re.escape(tag.lower())}\b", lowered) for tag in tags)


@dataclass(frozen=True)
class StoredEventRecord:
    source: str
    event_id: str
    timestamp: int
    country: str
    indicator: str
    category: str
    importance: str
    actual: str | None = None
    forecast: str | None = None
    previous: str | None = None
    revised_previous: str | None = None
    surprise: float | None = None
    currency: str = ""
    unit: str = ""
    raw_json: dict[str, Any] = field(default_factory=dict)
    indicator_id: str | None = None


@dataclass(frozen=True)
class CalendarIndicatorRecord:
    indicator_id: str               # 'us.inflation.cpi_mom'
    canonical_name: str             # 'CPI MoM'
    topic: str
    country_code: str
    frequency: str                  # monthly, quarterly, etc.
    unit: str
    obs_family_id: str | None = None
    is_active: bool = True
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class CalendarIndicatorAliasRecord:
    alias_normalized: str           # lowercased, stripped
    indicator_id: str               # FK → calendar_indicator
    source: str                     # 'investing'|'forexfactory'|'tradingeconomics'
    country_code: str
    alias_original: str = ""
    created_at: str = ""


@dataclass(frozen=True)
class MarketPriceRecord:
    symbol: str
    asset_class: str
    price: float
    change_pct: float | None
    timestamp: int
    name: str = ""


@dataclass(frozen=True)
class CentralBankCommunicationRecord:
    source: str
    title: str
    url: str
    timestamp: int
    content_type: str
    speaker: str = ""
    summary: str = ""
    full_text: str = ""


@dataclass(frozen=True)
class IndicatorObservationRecord:
    series_id: str
    source: str
    date: str
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)
    obs_family_id: str | None = None


@dataclass(frozen=True)
class IndicatorVintageRecord:
    series_id: str
    source: str
    observation_date: str   # the date being measured
    vintage_date: str       # when this measurement was published
    value: float
    metadata: dict[str, Any] = field(default_factory=dict)
    obs_family_id: str | None = None


@dataclass(frozen=True)
class ObsSourceRecord:
    source_id: str          # 'fred', 'eia', 'treasury_fiscal'
    source_code: str
    source_name: str
    source_type: str        # data_aggregator, government_agency, central_bank, exchange, market_data
    country_code: str
    homepage_url: str
    api_base_url: str
    is_active: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ObsFamilyRecord:
    family_id: str                  # 'us.inflation.cpi_all'
    source_id: str                  # 'fred'
    provider_series_id: str         # 'CPIAUCSL' (matches indicators.series_id)
    canonical_name: str             # 'CPI All Urban Consumers'
    short_name: str
    unit: str                       # 'index', 'percent', 'billions_usd'
    frequency: str                  # daily, weekly, monthly, quarterly, annual, irregular
    seasonal_adjustment: str        # sa, nsa, saar, none
    country_code: str
    topic_code: str                 # inflation, employment, rates, energy, fiscal
    category: str                   # consumer_prices, treasury_yields
    is_active: bool
    has_vintages: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class ObsFamilyDocumentRecord:
    family_id: str
    release_family_id: str
    relationship: str           # produced_by, derived_from, related_to
    created_at: str


@dataclass(frozen=True)
class ConceptMapRecord:
    """Cross-source indicator mapping — groups equivalent series under one concept."""
    concept_id: str             # 'CPI_US', 'GDP_REAL_US'
    source_id: str              # 'fred', 'bls', 'imf'
    provider_series_id: str     # 'CPIAUCSL', 'CUUR0000SA0'
    obs_family_id: str          # FK to obs_family.family_id (may be empty)
    priority: int = 0           # 1 = authoritative, 2 = secondary, 3 = tertiary
    role: str = "primary"       # primary, secondary, cross_check
    notes: str = ""
    created_at: str = ""


@dataclass(frozen=True)
class ResolvedObservation:
    """A single resolved value for a concept on a given date, with provenance."""
    concept_id: str
    date: str
    value: float
    source_id: str
    provider_series_id: str
    priority: int
    role: str
    alternates: int = 0         # how many other sources also had this date
    vintage: str = "initial"    # "initial", "revised", or "unknown"
    revision_count: int = 0     # number of vintage entries for this obs date


@dataclass(frozen=True)
class ReleaseScheduleRecord:
    concept_id: str           # PK, FK to concept_map
    rule_type: str            # "day_of_month", "weekday_of_month", "quarter_lag",
                              # "daily", "weekly", "fixed_dates", "approximate_window"
    rule_json: dict[str, Any] # type-specific params
    frequency: str            # "daily", "weekly", "monthly", "quarterly", "annual"
    release_time_utc: str     # "12:30", "14:00", "" if unknown
    timezone: str             # "America/New_York", "" if unknown
    source_authority: str     # "manual", "fred_api", "calendar_events"
    confidence: str           # "exact", "pattern", "approximate"
    next_expected: str        # ISO datetime, precomputed
    last_released: str        # ISO datetime, updated after successful fetch
    last_checked: str         # ISO datetime, updated on every check
    is_active: bool = True
    notes: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class ReleaseStatusRecord:
    """Tracks per-release availability status with retry state."""
    concept_id: str
    release_date: str           # expected release date (ISO)
    status: str                 # PENDING, WAITING, FETCHED, CONFIRMED, STALE, FAILED
    attempt_count: int = 0
    next_retry: str = ""        # ISO datetime for next retry
    last_attempt: str = ""      # ISO datetime of last fetch attempt
    source_used: str = ""       # source_id that provided data
    data_date: str = ""         # latest observation date actually fetched
    expected_period: str = ""   # minimum expected observation date
    provisional: bool = False   # True if using fallback source
    error: str = ""
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True)
class NewsArticleRecord:
    url_hash: str
    source_feed: str
    feed_category: str
    title: str
    url: str
    timestamp: int
    description: str
    content_markdown: str
    impact_level: str
    finance_category: str
    confidence: float
    content_fetched: bool
    institution: str = ""
    country: str = ""
    market: str = ""
    asset_class: str = ""
    sector: str = ""
    document_type: str = ""
    event_type: str = ""
    subject: str = ""
    subject_id: str = ""
    data_period: str = ""
    contains_commentary: bool = False
    language: str = "en"
    authors: str = ""
    extraction_provider: str = "keyword"


@dataclass(frozen=True)
class TrendTopicRecord:
    trend_id: str
    provider: str
    provider_topic_id: str
    title_raw: str
    topic: str
    summary: str
    keywords: list[str] = field(default_factory=list)
    category: str = ""
    region: str = "global"
    popularity_score: float = 0.0
    provider_rank: int = 0
    engagement_score: float = 0.0
    comment_count: int = 0
    observed_at: int = 0
    expires_at: int = 0
    raw_json: dict[str, Any] = field(default_factory=dict)
    normalized_topic_hash: str = ""


@dataclass(frozen=True)
class RegimeSnapshotRecord:
    snapshot_id: int
    timestamp: str
    regime_json: dict[str, Any]
    trigger_event: str
    summary: str


@dataclass(frozen=True)
class GeneratedNoteRecord:
    note_id: int
    created_at: str
    note_type: str
    title: str
    summary: str
    body_markdown: str
    regime_json: dict[str, Any] | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class AnalyticalObservationRecord:
    observation_id: int
    observation_type: str
    summary: str
    detail: str
    source_kind: str
    source_id: int
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResearchArtifactRecord:
    artifact_id: int
    artifact_type: str
    title: str
    summary: str
    content_markdown: str
    source_kind: str
    source_id: int
    created_at: str
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradeSignalRecord:
    signal_id: int
    signal_type: str
    title: str
    summary: str
    rationale_markdown: str
    signal: dict[str, Any]
    confidence: float
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DecisionLogRecord:
    decision_id: int
    decision_type: str
    title: str
    summary: str
    rationale_markdown: str
    research_artifact_id: int | None
    signal_id: int | None
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PositionStateRecord:
    symbol: str
    exposure: float
    direction: str
    thesis: str
    updated_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PerformanceRecord:
    record_id: int
    metric_name: str
    metric_value: float
    period_label: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradingArtifactRecord:
    artifact_id: int
    artifact_type: str
    title: str
    summary: str
    rationale_markdown: str
    research_artifact_id: int
    decision_log_id: int | None
    signal: dict[str, Any]
    confidence: float
    created_at: str
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ClientProfileRecord:
    client_id: str
    preferred_language: str
    watchlist_topics: list[str]
    response_style: str
    risk_appetite: str
    investment_horizon: str
    institution_type: str
    risk_preference: str
    asset_focus: list[str]
    market_focus: list[str]
    expertise_level: str
    activity: str
    current_mood: str
    emotional_trend: str
    stress_level: str
    confidence: str
    notes: str
    personal_facts: list[str]
    last_active_at: str
    total_interactions: int
    updated_at: str


@dataclass(frozen=True)
class ConversationMessageRecord:
    message_id: int
    client_id: str
    channel: str
    thread_id: str
    role: str
    content: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DeliveryQueueRecord:
    delivery_id: int
    client_id: str
    channel: str
    thread_id: str
    source_type: str
    source_artifact_id: int | None
    content_rendered: str
    status: str
    delivered_at: str | None
    client_reaction: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GroupProfileRecord:
    group_id: str
    group_name: str
    group_topic: str
    group_notes: str
    member_count: int
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class GroupMemberRecord:
    group_id: str
    user_id: str
    display_name: str
    role_in_group: str
    personality_notes: str
    first_seen_at: str
    last_seen_at: str
    message_count: int


@dataclass(frozen=True)
class GroupMessageRecord:
    message_id: int
    group_id: str
    thread_id: str
    user_id: str
    display_name: str
    content: str
    created_at: str


@dataclass(frozen=True)
class DocSourceRecord:
    source_id: str
    source_code: str
    source_name: str
    source_type: str
    country_code: str
    default_language_code: str
    homepage_url: str
    is_active: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DocReleaseFamilyRecord:
    release_family_id: str
    source_id: str
    release_code: str
    release_name: str
    topic_code: str
    country_code: str
    frequency: str
    default_language_code: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DocumentRecord:
    document_id: str
    release_family_id: str
    source_id: str
    canonical_url: str
    title: str
    subtitle: str
    document_type: str
    mime_type: str
    language_code: str
    country_code: str
    topic_code: str
    published_date: str
    published_at: str
    status: str
    version_no: int
    parent_document_id: str
    hash_sha256: str
    created_at: str
    updated_at: str
    published_precision: str = ""
    published_epoch_ms: int = 0
    created_epoch_ms: int = 0
    updated_epoch_ms: int = 0


@dataclass(frozen=True)
class DocumentBlobRecord:
    document_blob_id: str
    document_id: str
    blob_role: str
    storage_path: str
    content_text: str
    content_bytes: bytes | None
    byte_size: int
    encoding: str
    parser_name: str
    parser_version: str
    extracted_at: str


@dataclass(frozen=True)
class DocumentExtraRecord:
    document_id: str
    extra_json: dict[str, Any]


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


# ── Observation family seed data ─────────────────────────────────────

_FRED_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "CPIAUCSL":     ("us.inflation.cpi_all",          "CPI All Urban Consumers",    "index",        "monthly",   "sa"),
    "CPILFESL":     ("us.inflation.cpi_core",          "Core CPI",                   "index",        "monthly",   "sa"),
    "PCEPILFE":     ("us.inflation.pce_core",          "Core PCE Price Index",        "index",        "monthly",   "sa"),
    "T5YIE":        ("us.inflation.breakeven_5y",      "5Y Breakeven Inflation",      "percent",      "daily",     "none"),
    "T10YIE":       ("us.inflation.breakeven_10y",     "10Y Breakeven Inflation",     "percent",      "daily",     "none"),
    "UNRATE":       ("us.employment.unemployment",     "Unemployment Rate",           "percent",      "monthly",   "sa"),
    "PAYEMS":       ("us.employment.nonfarm_payrolls", "Total Nonfarm Payrolls",      "thousands",    "monthly",   "sa"),
    "ICSA":         ("us.employment.initial_claims",   "Initial Jobless Claims",      "thousands",    "weekly",    "sa"),
    "CCSA":         ("us.employment.continuing_claims","Continuing Jobless Claims",   "thousands",    "weekly",    "sa"),
    "GDP":          ("us.growth.gdp_nominal",          "GDP",                         "billions_usd", "quarterly", "saar"),
    "GDPC1":        ("us.growth.gdp_real",             "Real GDP",                    "billions_usd", "quarterly", "saar"),
    "RSAFS":        ("us.growth.retail_sales",         "Retail Sales",                "millions_usd", "monthly",   "sa"),
    "INDPRO":       ("us.growth.industrial_production","Industrial Production",       "index",        "monthly",   "sa"),
    "DFF":          ("us.rates.fed_funds",             "Fed Funds Rate",              "percent",      "daily",     "none"),
    "DGS2":         ("us.rates.treasury_2y",           "2Y Treasury Yield",           "percent",      "daily",     "none"),
    "DGS10":        ("us.rates.treasury_10y",          "10Y Treasury Yield",          "percent",      "daily",     "none"),
    "DGS30":        ("us.rates.treasury_30y",          "30Y Treasury Yield",          "percent",      "daily",     "none"),
    "DFII10":       ("us.rates.real_yield_10y",        "10Y Real Yield",              "percent",      "daily",     "none"),
    "T10Y2Y":       ("us.rates.spread_10y2y",          "10Y-2Y Spread",               "percent",      "daily",     "none"),
    "WALCL":        ("us.liquidity.fed_balance_sheet", "Fed Balance Sheet",           "millions_usd", "weekly",    "none"),
    "M2SL":         ("us.liquidity.m2",                "M2 Money Supply",             "billions_usd", "monthly",   "sa"),
    "RRPONTSYD":    ("us.liquidity.reverse_repo",      "Reverse Repo",                "billions_usd", "daily",     "none"),
    "WTREGEN":      ("us.liquidity.tga",               "Treasury General Account",    "millions_usd", "weekly",    "none"),
    "DTWEXBGS":     ("us.fx.dollar_index_broad",       "Broad Dollar Index",          "index",        "daily",     "none"),
    "DEXCHUS":      ("us.fx.cny_usd",                  "CNY/USD Exchange Rate",       "ratio",        "daily",     "none"),
    "BAMLH0A0HYM2": ("us.credit.hy_oas",              "High Yield OAS",              "percent",      "daily",     "none"),
}

_EIA_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "EIA_BRENT":         ("us.energy.brent_spot",        "Brent Crude Spot Price",      "usd_per_barrel",           "daily",  "none"),
    "EIA_WTI":           ("us.energy.wti_spot",           "WTI Crude Spot Price",        "usd_per_barrel",           "daily",  "none"),
    "EIA_CRUDE_STOCKS":  ("us.energy.crude_stocks",       "Crude Oil Stocks",            "thousand_barrels",         "weekly", "none"),
    "EIA_NATGAS":        ("us.energy.natgas_futures",      "Natural Gas Futures",         "usd_per_mmbtu",           "daily",  "none"),
    "EIA_PETROL_SUPPLY": ("us.energy.petroleum_supply",    "Petroleum Supply",            "thousand_barrels_per_day", "weekly", "none"),
}

_TREASURY_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "TREAS_DEBT_TOTAL":  ("us.fiscal.debt_outstanding",   "Debt Outstanding",            "millions_usd", "daily",   "none"),
    "TREAS_TGA_BALANCE": ("us.fiscal.tga_balance",        "TGA Balance",                 "millions_usd", "daily",   "none"),
    "TREAS_AVG_RATE":    ("us.fiscal.avg_interest_rate",   "Average Interest Rate",       "percent",      "monthly", "none"),
}

_NYFED_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "NYFED_SOFR": ("us.rates.sofr", "Secured Overnight Financing Rate", "percent", "daily", "none"),
    "NYFED_EFFR": ("us.rates.effr", "Effective Federal Funds Rate",     "percent", "daily", "none"),
    "NYFED_OBFR": ("us.rates.obfr", "Overnight Bank Funding Rate",     "percent", "daily", "none"),
}

_IMF_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "IMF_CN_CPI":         ("cn.inflation.cpi",          "China CPI Index",              "index",        "monthly",    "none"),
    "IMF_CN_GDP":         ("cn.growth.gdp_real",         "China Real GDP (LCU)",         "lcu",          "quarterly",  "none"),
    "IMF_CN_FX_RESERVES": ("cn.reserves.fx",             "China FX Reserves (USD)",      "millions_usd", "monthly",    "none"),
    "IMF_JP_CPI":         ("jp.inflation.cpi",           "Japan CPI Index",              "index",        "monthly",    "none"),
    "IMF_JP_GDP":         ("jp.growth.gdp_real",          "Japan Real GDP (LCU)",         "lcu",          "quarterly",  "none"),
    "IMF_EU_CPI":         ("eu.inflation.cpi_imf",        "Euro Area CPI Index (IMF)",   "index",        "monthly",    "none"),
    "IMF_GLOBAL_TRADE":   ("us.trade.exports_fob",        "US Exports FOB (USD)",        "millions_usd", "monthly",    "none"),
}

_EUROSTAT_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "ESTAT_HICP":          ("eu.inflation.hicp",            "EA HICP YoY %",                     "percent",  "monthly",    "none"),
    "ESTAT_GDP":           ("eu.growth.gdp_qoq",            "EA GDP QoQ %",                      "percent",  "quarterly",  "sa"),
    "ESTAT_UNEMPLOYMENT":  ("eu.employment.unemployment",    "EA Unemployment Rate",              "percent",  "monthly",    "sa"),
    "ESTAT_INDPRO":        ("eu.growth.industrial_production", "EA Industrial Production MoM",    "percent",  "monthly",    "sa"),
    "ESTAT_ESI":           ("eu.sentiment.esi",              "EA Economic Sentiment Indicator",   "index",        "monthly", "sa"),
}

_BIS_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "BIS_POLICY_US": ("us.rates.policy_bis",     "US Policy Rate (BIS)",          "percent", "monthly",    "none"),
    "BIS_POLICY_EU": ("eu.rates.policy_bis",     "ECB Policy Rate (BIS)",         "percent", "monthly",    "none"),
    "BIS_POLICY_JP": ("jp.rates.policy_bis",     "BOJ Policy Rate (BIS)",         "percent", "monthly",    "none"),
    "BIS_POLICY_CN": ("cn.rates.policy_bis",     "PBOC Policy Rate (BIS)",        "percent", "monthly",    "none"),
    "BIS_POLICY_GB": ("gb.rates.policy_bis",     "BOE Policy Rate (BIS)",         "percent", "monthly",    "none"),
    "BIS_EER_US":    ("us.fx.eer_real",          "US Real Effective Exchange Rate",  "index", "monthly",    "none"),
    "BIS_EER_CN":    ("cn.fx.eer_real",          "CN Real Effective Exchange Rate",  "index", "monthly",    "none"),
    "BIS_EER_EU":    ("eu.fx.eer_real",          "EU Real Effective Exchange Rate",  "index", "monthly",    "none"),
    "BIS_CREDIT_GAP_US": ("us.credit.gap",       "US Credit-to-GDP Gap",           "percent", "quarterly", "none"),
    "BIS_CREDIT_GAP_CN": ("cn.credit.gap",       "CN Credit-to-GDP Gap",           "percent", "quarterly", "none"),
    "BIS_PROPERTY_US":   ("us.property.real",     "US Real Property Prices",        "index",   "quarterly", "none"),
    "BIS_PROPERTY_CN":   ("cn.property.real",     "CN Real Property Prices",        "index",   "quarterly", "none"),
}

_ECB_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "ECB_EA_M1":           ("eu.liquidity.m1",        "EA M1 Money Supply",        "millions_eur", "monthly", "sa"),
    "ECB_EA_M2":           ("eu.liquidity.m2",        "EA M2 Money Supply",        "millions_eur", "monthly", "sa"),
    "ECB_EA_M3":           ("eu.liquidity.m3",        "EA M3 Money Supply",        "millions_eur", "monthly", "sa"),
    "ECB_EA_M3_GROWTH":    ("eu.liquidity.m3_growth", "EA M3 Annual Growth Rate",  "percent",      "monthly", "none"),
    "ECB_EA_DEPOSIT_RATE": ("eu.rates.deposit_ecb",   "ECB Deposit Facility Rate", "percent",      "daily",   "none"),
    "ECB_EURUSD":          ("eu.fx.eurusd",           "EUR/USD Exchange Rate",     "ratio",        "monthly", "none"),
}

_OECD_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "OECD_CLI_US":           ("us.leading.cli",             "US Composite Leading Indicator",  "index",   "monthly", "none"),
    "OECD_CLI_CN":           ("cn.leading.cli",             "CN Composite Leading Indicator",  "index",   "monthly", "none"),
    "OECD_CLI_JP":           ("jp.leading.cli",             "JP Composite Leading Indicator",  "index",   "monthly", "none"),
    "OECD_CLI_EU":           ("eu.leading.cli",             "EA Composite Leading Indicator",  "index",   "monthly", "none"),
    "OECD_CONSUMER_CONF_US": ("us.sentiment.consumer_conf", "US Consumer Confidence (OECD)",   "index",   "monthly", "sa"),
    "OECD_BUSINESS_CONF_US": ("us.sentiment.business_conf", "US Business Confidence (OECD)",   "index",   "monthly", "sa"),
    "OECD_UNEMP_US":         ("us.employment.unemployment_oecd", "US Unemployment Rate (OECD)", "percent", "monthly", "sa"),
}

_WORLDBANK_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "WB_GDP_PCAP_US":   ("us.development.gdp_per_capita", "US GDP per Capita PPP",    "usd",     "annual", "none"),
    "WB_GDP_PCAP_CN":   ("cn.development.gdp_per_capita", "CN GDP per Capita PPP",    "usd",     "annual", "none"),
    "WB_GDP_GROWTH_US": ("us.growth.gdp_growth_wb",       "US GDP Growth % (WB)",     "percent", "annual", "none"),
    "WB_CA_GDP_US":     ("us.trade.current_account_gdp",   "US Current Account % GDP", "percent", "annual", "none"),
}

_BLS_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "CUUR0000SA0":              ("us.inflation.cpi_bls",               "CPI-U All Items (BLS)",                   "index",     "monthly",   "nsa"),
    "CUUR0000SA0L1E":           ("us.inflation.cpi_core_bls",          "Core CPI-U (BLS)",                        "index",     "monthly",   "nsa"),
    "CUUR0000SAF1":             ("us.inflation.cpi_food_bls",          "CPI-U Food (BLS)",                        "index",     "monthly",   "nsa"),
    "CUUR0000SA0E":             ("us.inflation.cpi_energy_bls",        "CPI-U Energy (BLS)",                      "index",     "monthly",   "nsa"),
    "CUUR0000SAH1":             ("us.inflation.cpi_shelter_bls",       "CPI-U Shelter (BLS)",                     "index",     "monthly",   "nsa"),
    "WPSFD4":                   ("us.inflation.ppi_final_demand_bls",  "PPI Final Demand (BLS)",                  "index",     "monthly",   "nsa"),
    "WPSFD49116":               ("us.inflation.ppi_core_bls",          "PPI Core (BLS)",                          "index",     "monthly",   "nsa"),
    "CES0000000001":            ("us.employment.nfp_bls",              "Total Nonfarm Payrolls (BLS CES)",        "thousands", "monthly",   "sa"),
    "CES0500000001":            ("us.employment.nfp_private_bls",      "Total Private Employment (BLS CES)",      "thousands", "monthly",   "sa"),
    "CES0500000003":            ("us.employment.avg_hourly_earnings_bls", "Avg Hourly Earnings Private (BLS CES)", "usd",     "monthly",   "sa"),
    "CES0500000002":            ("us.employment.avg_weekly_hours_bls", "Avg Weekly Hours Private (BLS CES)",      "hours",     "monthly",   "sa"),
    "LNS14000000":              ("us.employment.unemployment_bls",     "Unemployment Rate (BLS CPS)",             "percent",   "monthly",   "sa"),
    "LNS11300000":              ("us.employment.lfpr_bls",             "Labor Force Participation Rate (BLS CPS)","percent",   "monthly",   "sa"),
    "JTS000000000000000JOL":    ("us.employment.jolts_openings_bls",   "JOLTS Job Openings (BLS)",                "thousands", "monthly",   "sa"),
    "JTS000000000000000HIL":    ("us.employment.jolts_hires_bls",      "JOLTS Hires (BLS)",                       "thousands", "monthly",   "sa"),
    "JTS000000000000000QUL":    ("us.employment.jolts_quits_bls",      "JOLTS Quits (BLS)",                       "thousands", "monthly",   "sa"),
    "CIU1010000000000A":        ("us.employment.eci_total_bls",        "Employment Cost Index Total (BLS)",       "index",     "quarterly", "sa"),
    "PRS85006092":              ("us.productivity.nfb_productivity_bls","NFB Labor Productivity (BLS)",            "index",     "quarterly", "sa"),
    "PRS85006112":              ("us.productivity.nfb_ulc_bls",        "NFB Unit Labor Costs (BLS)",              "index",     "quarterly", "sa"),
}

_VINTAGE_FAMILY_IDS = {"GDP", "GDPC1", "CPIAUCSL", "PAYEMS", "UNRATE", "INDPRO", "RSAFS", "IMF_CN_GDP", "IMF_JP_GDP"}

_OBS_DOC_LINKS: list[tuple[str, str, str]] = [
    ("us.inflation.cpi_all",           "us.bls.cpi",       "produced_by"),
    ("us.inflation.cpi_core",          "us.bls.cpi",       "produced_by"),
    ("us.inflation.pce_core",          "us.bea.pce",       "produced_by"),
    ("us.employment.nonfarm_payrolls", "us.bls.nfp",       "produced_by"),
    ("us.employment.unemployment",     "us.bls.nfp",       "produced_by"),
    ("us.growth.gdp_nominal",          "us.bea.gdp",       "produced_by"),
    ("us.growth.gdp_real",             "us.bea.gdp",       "produced_by"),
    ("us.growth.retail_sales",         "us.census.retail",  "produced_by"),
    ("us.growth.industrial_production","us.fed.ip",         "produced_by"),
    ("us.fiscal.debt_outstanding",     "us.treasury.debt",  "produced_by"),
    # Eurostat numeric ↔ Eurostat publications
    ("eu.inflation.hicp",             "eu.eurostat.cpi",        "produced_by"),
    ("eu.growth.gdp_qoq",            "eu.eurostat.gdp",        "produced_by"),
    ("eu.employment.unemployment",    "eu.eurostat.employment",  "produced_by"),
]

_OBS_SOURCE_DEFS: list[tuple[str, str, str, str, str, str, str]] = [
    # source_id, source_code, source_name, source_type, country_code, homepage_url, api_base_url
    ("fred",            "fred",            "Federal Reserve Economic Data",     "data_aggregator",   "US", "https://fred.stlouisfed.org",                                    "https://api.stlouisfed.org/fred"),
    ("eia",             "eia",             "Energy Information Administration", "government_agency", "US", "https://www.eia.gov",                                            "https://api.eia.gov/v2"),
    ("treasury_fiscal", "treasury_fiscal", "Treasury Fiscal Data",             "government_agency", "US", "https://fiscaldata.treasury.gov",                                "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"),
    ("nyfed",           "nyfed",           "Federal Reserve Bank of New York", "central_bank",      "US", "https://www.newyorkfed.org",                                     "https://markets.newyorkfed.org/api"),
    ("rateprobability", "rateprobability", "rateprobability.com",              "market_data",       "US", "https://rateprobability.com",                                    "https://rateprobability.com/api"),
    ("imf",             "imf",             "International Monetary Fund",      "data_aggregator",   "US", "https://www.imf.org",                                           "https://api.imf.org/external/sdmx/3.0"),
    ("eurostat",        "eurostat",        "Eurostat",                         "government_agency", "EU", "https://ec.europa.eu/eurostat",                                  "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"),
    ("bis",             "bis",             "Bank for International Settlements","data_aggregator",  "CH", "https://www.bis.org",                                           "https://stats.bis.org/api/v2"),
    ("ecb",             "ecb",             "European Central Bank",             "central_bank",     "EU", "https://www.ecb.europa.eu",                                      "https://data-api.ecb.europa.eu/service/data"),
    ("oecd",            "oecd",            "Organisation for Economic Co-operation", "data_aggregator", "XX", "https://www.oecd.org",                                      "https://sdmx.oecd.org/public/rest/v2"),
    ("worldbank",       "worldbank",       "World Bank",                        "data_aggregator",  "XX", "https://www.worldbank.org",                                      "https://api.worldbank.org/v2"),
    ("bls",             "bls",             "Bureau of Labor Statistics",         "government_agency", "US", "https://www.bls.gov",                                            "https://api.bls.gov/publicAPI/v2"),
]

# ── Calendar indicator seed data ──────────────────────────────────────

# (indicator_id, canonical_name, topic, country_code, frequency, unit, obs_family_id)
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

# (alias_original, indicator_id, source, country_code)
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


class SQLiteEngineStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or default_engine_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def get_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def _connection(self, *, commit: bool) -> Iterator[sqlite3.Connection]:
        connection = self.get_connection()
        try:
            yield connection
            if commit:
                connection.commit()
        except Exception:
            if commit:
                connection.rollback()
            raise
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    country TEXT NOT NULL,
                    indicator TEXT NOT NULL,
                    category TEXT NOT NULL,
                    importance TEXT NOT NULL,
                    actual TEXT,
                    forecast TEXT,
                    previous TEXT,
                    revised_previous TEXT,
                    surprise REAL,
                    unit TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    UNIQUE(source, event_id)
                )
                """
            )
            try:
                connection.execute("ALTER TABLE calendar_events ADD COLUMN currency TEXT NOT NULL DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # column already exists
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS market_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    name TEXT NOT NULL,
                    price REAL NOT NULL,
                    change_pct REAL,
                    timestamp INTEGER NOT NULL,
                    scraped_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS central_bank_comms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    timestamp INTEGER NOT NULL,
                    content_type TEXT NOT NULL,
                    speaker TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    full_text TEXT NOT NULL,
                    scraped_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    date TEXT NOT NULL,
                    value REAL NOT NULL,
                    metadata_json TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    UNIQUE(series_id, source, date)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS indicator_vintages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    observation_date TEXT NOT NULL,
                    vintage_date TEXT NOT NULL,
                    value REAL NOT NULL,
                    metadata_json TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    UNIQUE(series_id, source, observation_date, vintage_date)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS news_articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_hash TEXT NOT NULL UNIQUE,
                    source_feed TEXT NOT NULL,
                    feed_category TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    description TEXT NOT NULL,
                    content_markdown TEXT NOT NULL,
                    impact_level TEXT NOT NULL,
                    finance_category TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    content_fetched INTEGER NOT NULL DEFAULT 0,
                    scraped_at TEXT NOT NULL
                )
                """
            )
            # -- news_articles new columns for LLM extraction -----------
            _news_new_cols = [
                ("institution", "TEXT NOT NULL DEFAULT ''"),
                ("country", "TEXT NOT NULL DEFAULT ''"),
                ("market", "TEXT NOT NULL DEFAULT ''"),
                ("asset_class", "TEXT NOT NULL DEFAULT ''"),
                ("sector", "TEXT NOT NULL DEFAULT ''"),
                ("document_type", "TEXT NOT NULL DEFAULT ''"),
                ("event_type", "TEXT NOT NULL DEFAULT ''"),
                ("subject", "TEXT NOT NULL DEFAULT ''"),
                ("subject_id", "TEXT NOT NULL DEFAULT ''"),
                ("data_period", "TEXT NOT NULL DEFAULT ''"),
                ("contains_commentary", "INTEGER NOT NULL DEFAULT 0"),
                ("language", "TEXT NOT NULL DEFAULT 'en'"),
                ("authors", "TEXT NOT NULL DEFAULT ''"),
                ("extraction_provider", "TEXT NOT NULL DEFAULT 'keyword'"),
            ]
            for col_name, col_def in _news_new_cols:
                try:
                    connection.execute(f"ALTER TABLE news_articles ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError:
                    pass
            # -- FTS5 full-text search for news articles ----------------
            # Guarded: SQLite builds without FTS5 skip this block;
            # search_news() falls back to LIKE queries.
            try:
                connection.execute(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
                        title, description, subject,
                        content='news_articles',
                        content_rowid='id'
                    )
                    """
                )
                for trigger_name in ("news_fts_ai", "news_fts_ad", "news_fts_au"):
                    connection.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
                connection.execute(
                    """
                    CREATE TRIGGER news_fts_ai AFTER INSERT ON news_articles BEGIN
                        INSERT INTO news_fts(rowid, title, description, subject)
                        VALUES (new.id, new.title, new.description, new.subject);
                    END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER news_fts_ad AFTER DELETE ON news_articles BEGIN
                        INSERT INTO news_fts(news_fts, rowid, title, description, subject)
                        VALUES ('delete', old.id, old.title, old.description, old.subject);
                    END
                    """
                )
                connection.execute(
                    """
                    CREATE TRIGGER news_fts_au AFTER UPDATE ON news_articles BEGIN
                        INSERT INTO news_fts(news_fts, rowid, title, description, subject)
                        VALUES ('delete', old.id, old.title, old.description, old.subject);
                        INSERT INTO news_fts(rowid, title, description, subject)
                        VALUES (new.id, new.title, new.description, new.subject);
                    END
                    """
                )
                connection.execute("INSERT INTO news_fts(news_fts) VALUES('rebuild')")
            except sqlite3.OperationalError:
                pass  # FTS5 not available; search_news() will use LIKE fallback
            # -- Article fingerprint table for multi-layer dedup ----------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS article_fingerprint (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url_hash TEXT NOT NULL,
                    title_hash TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    raw_url TEXT NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    source_feed TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_fp_url ON article_fingerprint(url_hash)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_fp_title ON article_fingerprint(title_hash)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trend_topics (
                    trend_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    provider_topic_id TEXT NOT NULL,
                    title_raw TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    keywords_json TEXT NOT NULL DEFAULT '[]',
                    category TEXT NOT NULL,
                    region TEXT NOT NULL,
                    popularity_score REAL NOT NULL,
                    provider_rank INTEGER NOT NULL DEFAULT 0,
                    engagement_score REAL NOT NULL DEFAULT 0,
                    comment_count INTEGER NOT NULL DEFAULT 0,
                    observed_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    raw_json TEXT NOT NULL DEFAULT '{}',
                    normalized_topic_hash TEXT NOT NULL,
                    scraped_at TEXT NOT NULL,
                    UNIQUE(provider, provider_topic_id)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_trend_topics_active "
                "ON trend_topics(expires_at, observed_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_trend_topics_scope "
                "ON trend_topics(category, region)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_trend_topics_popularity "
                "ON trend_topics(popularity_score DESC, observed_at DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_trend_topics_normalized "
                "ON trend_topics(normalized_topic_hash)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS regime_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    regime_json TEXT NOT NULL,
                    trigger_event TEXT NOT NULL,
                    summary TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS generated_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    note_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    body_markdown TEXT NOT NULL,
                    regime_json TEXT,
                    metadata_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS analytical_observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_type TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS research_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content_markdown TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    tags_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    rationale_markdown TEXT NOT NULL,
                    signal_json TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS decision_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    rationale_markdown TEXT NOT NULL,
                    research_artifact_id INTEGER,
                    signal_id INTEGER,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (research_artifact_id) REFERENCES research_artifacts(id),
                    FOREIGN KEY (signal_id) REFERENCES trade_signals(id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS position_state (
                    symbol TEXT PRIMARY KEY,
                    exposure REAL NOT NULL,
                    direction TEXT NOT NULL,
                    thesis TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS performance_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    period_label TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trading_artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    rationale_markdown TEXT NOT NULL,
                    research_artifact_id INTEGER NOT NULL,
                    decision_log_id INTEGER,
                    signal_json TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    tags_json TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (research_artifact_id) REFERENCES research_artifacts(id),
                    FOREIGN KEY (decision_log_id) REFERENCES decision_log(id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS client_profiles (
                    client_id TEXT PRIMARY KEY,
                    preferred_language TEXT NOT NULL DEFAULT '',
                    watchlist_topics_json TEXT NOT NULL DEFAULT '[]',
                    response_style TEXT NOT NULL DEFAULT '',
                    risk_appetite TEXT NOT NULL DEFAULT '',
                    investment_horizon TEXT NOT NULL DEFAULT '',
                    institution_type TEXT NOT NULL DEFAULT '',
                    risk_preference TEXT NOT NULL DEFAULT '',
                    asset_focus_json TEXT NOT NULL DEFAULT '[]',
                    market_focus_json TEXT NOT NULL DEFAULT '[]',
                    expertise_level TEXT NOT NULL DEFAULT '',
                    activity TEXT NOT NULL DEFAULT '',
                    current_mood TEXT NOT NULL DEFAULT '',
                    confidence TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    last_active_at TEXT NOT NULL DEFAULT '',
                    total_interactions INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._ensure_table_columns(
                connection,
                table_name="client_profiles",
                columns={
                    "institution_type": "TEXT NOT NULL DEFAULT ''",
                    "risk_preference": "TEXT NOT NULL DEFAULT ''",
                    "asset_focus_json": "TEXT NOT NULL DEFAULT '[]'",
                    "market_focus_json": "TEXT NOT NULL DEFAULT '[]'",
                    "expertise_level": "TEXT NOT NULL DEFAULT ''",
                    "activity": "TEXT NOT NULL DEFAULT ''",
                    "current_mood": "TEXT NOT NULL DEFAULT ''",
                    "emotional_trend": "TEXT NOT NULL DEFAULT ''",
                    "stress_level": "TEXT NOT NULL DEFAULT ''",
                    "confidence": "TEXT NOT NULL DEFAULT ''",
                    "notes": "TEXT NOT NULL DEFAULT ''",
                    "personal_facts_json": "TEXT NOT NULL DEFAULT '[]'",
                },
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_threads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    last_active_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    UNIQUE(client_id, channel, thread_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (client_id, channel, thread_id)
                        REFERENCES conversation_threads(client_id, channel, thread_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS delivery_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_artifact_id INTEGER,
                    content_rendered TEXT NOT NULL,
                    status TEXT NOT NULL,
                    delivered_at TEXT,
                    client_reaction TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_analytical_observations_created ON analytical_observations(id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_artifacts_type_created ON research_artifacts(artifact_type, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_trading_artifacts_research_created ON trading_artifacts(research_artifact_id, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_decision_log_research_created ON decision_log(research_artifact_id, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_performance_records_metric_created ON performance_records(metric_name, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_conversation_messages_thread_created ON conversation_messages(client_id, channel, thread_id, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_delivery_queue_client_created ON delivery_queue(client_id, channel, thread_id, id DESC)"
            )
            # -- Portfolio volatility management tables ---------------------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id TEXT NOT NULL DEFAULT 'default',
                    symbol TEXT NOT NULL,
                    name TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    weight REAL NOT NULL,
                    notional REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(portfolio_id, symbol)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_vol_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id TEXT NOT NULL DEFAULT 'default',
                    snapshot_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_portfolio_vol_snapshots_portfolio ON portfolio_vol_snapshots(portfolio_id, id DESC)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id TEXT NOT NULL DEFAULT 'default',
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    acknowledged INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS subagent_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    parent_agent TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    scope_tags_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary TEXT NOT NULL DEFAULT '',
                    elapsed_seconds REAL NOT NULL DEFAULT 0.0,
                    created_at TEXT NOT NULL
                )
                """
            )
            # -- Three-layer memory: group tables --------------------------------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS group_profiles (
                    group_id TEXT PRIMARY KEY,
                    group_name TEXT NOT NULL DEFAULT '',
                    group_topic TEXT NOT NULL DEFAULT '',
                    group_notes TEXT NOT NULL DEFAULT '',
                    member_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS group_members (
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    role_in_group TEXT NOT NULL DEFAULT '',
                    personality_notes TEXT NOT NULL DEFAULT '',
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    message_count INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (group_id, user_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS group_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL DEFAULT 'main',
                    user_id TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_group_messages_group_thread "
                "ON group_messages(group_id, thread_id, id DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_group_members_group "
                "ON group_members(group_id, last_seen_at DESC)"
            )
            # -- Document storage: 5-table normalized schema --------------------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS doc_source (
                    source_id TEXT PRIMARY KEY,
                    source_code TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_type TEXT NOT NULL
                        CHECK (source_type IN (
                            'government_agency', 'central_bank', 'intl_org',
                            'statistics_bureau', 'news_agency'
                        )),
                    country_code TEXT NOT NULL CHECK (length(country_code) = 2),
                    default_language_code TEXT CHECK (length(default_language_code) IN (2, 5)),
                    homepage_url TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS doc_release_family (
                    release_family_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    release_code TEXT NOT NULL,
                    release_name TEXT NOT NULL,
                    topic_code TEXT NOT NULL,
                    country_code TEXT NOT NULL CHECK (length(country_code) = 2),
                    frequency TEXT,
                    default_language_code TEXT CHECK (default_language_code IS NULL OR length(default_language_code) IN (2, 5)),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (source_id) REFERENCES doc_source(source_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS document (
                    document_id TEXT PRIMARY KEY,
                    release_family_id TEXT,
                    source_id TEXT NOT NULL,
                    canonical_url TEXT NOT NULL,
                    title TEXT NOT NULL,
                    subtitle TEXT NOT NULL DEFAULT '',
                    document_type TEXT NOT NULL
                        CHECK (document_type IN (
                            'release', 'bulletin', 'speech', 'methodology',
                            'revision_notice', 'minutes', 'statement',
                            'press_release', 'report', 'outlook'
                        )),
                    mime_type TEXT NOT NULL DEFAULT 'text/html',
                    language_code TEXT NOT NULL CHECK (length(language_code) IN (2, 5)),
                    country_code TEXT NOT NULL CHECK (length(country_code) = 2),
                    topic_code TEXT NOT NULL,
                    published_date TEXT NOT NULL,
                    published_at TEXT,
                    published_precision TEXT NOT NULL DEFAULT 'date_only',
                    published_epoch_ms INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'published'
                        CHECK (status IN ('published', 'revised', 'superseded', 'withdrawn')),
                    version_no INTEGER NOT NULL DEFAULT 1,
                    parent_document_id TEXT,
                    hash_sha256 TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    created_epoch_ms INTEGER NOT NULL DEFAULT 0,
                    updated_epoch_ms INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (release_family_id) REFERENCES doc_release_family(release_family_id),
                    FOREIGN KEY (source_id) REFERENCES doc_source(source_id),
                    FOREIGN KEY (parent_document_id) REFERENCES document(document_id)
                )
                """
            )
            self._ensure_table_columns(
                connection,
                table_name="document",
                columns={
                    "published_precision": "TEXT NOT NULL DEFAULT 'date_only'",
                    "published_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
                    "created_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
                    "updated_epoch_ms": "INTEGER NOT NULL DEFAULT 0",
                },
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS document_blob (
                    document_blob_id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL,
                    blob_role TEXT NOT NULL
                        CHECK (blob_role IN (
                            'raw_pdf', 'raw_html', 'clean_html',
                            'plain_text', 'markdown'
                        )),
                    storage_path TEXT,
                    content_text TEXT,
                    content_bytes BLOB,
                    byte_size INTEGER,
                    encoding TEXT,
                    parser_name TEXT,
                    parser_version TEXT,
                    extracted_at TEXT,
                    FOREIGN KEY (document_id) REFERENCES document(document_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS document_extra (
                    document_id TEXT PRIMARY KEY,
                    extra_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY (document_id) REFERENCES document(document_id)
                )
                """
            )
            # -- RAG sync watermarks ----------------------------------------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS rag_sync_watermarks (
                    source_type TEXT PRIMARY KEY,
                    last_synced_id INTEGER NOT NULL DEFAULT 0,
                    last_synced_at TEXT NOT NULL
                )
                """
            )
            # -- Document storage indexes ----------------------------------------
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_document_url "
                "ON document(canonical_url)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_source_date "
                "ON document(source_id, published_date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_release_date "
                "ON document(release_family_id, published_date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_country_topic_date "
                "ON document(country_code, topic_code, published_date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_published_epoch "
                "ON document(published_epoch_ms)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_document_status "
                "ON document(status)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_blob_document_role "
                "ON document_blob(document_id, blob_role)"
            )
            # -- Observation family: 3-table hierarchy --------------------------
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS obs_source (
                    source_id TEXT PRIMARY KEY,
                    source_code TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    source_type TEXT NOT NULL
                        CHECK (source_type IN (
                            'data_aggregator', 'government_agency', 'central_bank',
                            'exchange', 'market_data'
                        )),
                    country_code TEXT NOT NULL CHECK (length(country_code) = 2),
                    homepage_url TEXT,
                    api_base_url TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS obs_family (
                    family_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    provider_series_id TEXT NOT NULL,
                    canonical_name TEXT NOT NULL,
                    short_name TEXT NOT NULL DEFAULT '',
                    unit TEXT NOT NULL DEFAULT '',
                    frequency TEXT NOT NULL DEFAULT 'irregular'
                        CHECK (frequency IN (
                            'daily','weekly','monthly','quarterly','annual','irregular'
                        )),
                    seasonal_adjustment TEXT NOT NULL DEFAULT 'none'
                        CHECK (seasonal_adjustment IN ('sa','nsa','saar','none')),
                    country_code TEXT NOT NULL CHECK (length(country_code) = 2),
                    topic_code TEXT NOT NULL DEFAULT '',
                    category TEXT NOT NULL DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    has_vintages INTEGER NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (source_id) REFERENCES obs_source(source_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS obs_family_document (
                    family_id TEXT NOT NULL,
                    release_family_id TEXT NOT NULL,
                    relationship TEXT NOT NULL DEFAULT 'produced_by'
                        CHECK (relationship IN (
                            'produced_by','derived_from','related_to'
                        )),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (family_id, release_family_id),
                    FOREIGN KEY (family_id) REFERENCES obs_family(family_id),
                    FOREIGN KEY (release_family_id) REFERENCES doc_release_family(release_family_id)
                )
                """
            )
            # ALTER TABLE migrations for obs_family_id
            try:
                connection.execute(
                    "ALTER TABLE indicators ADD COLUMN obs_family_id TEXT DEFAULT NULL"
                )
            except sqlite3.OperationalError:
                pass  # column already exists
            try:
                connection.execute(
                    "ALTER TABLE indicator_vintages ADD COLUMN obs_family_id TEXT DEFAULT NULL"
                )
            except sqlite3.OperationalError:
                pass  # column already exists
            # -- Observation family indexes --------------------------------------
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_family_source "
                "ON obs_family(source_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_family_country_topic "
                "ON obs_family(country_code, topic_code)"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_family_provider_series "
                "ON obs_family(source_id, provider_series_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_indicators_family_date "
                "ON indicators(obs_family_id, date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_vintages_family_date "
                "ON indicator_vintages(obs_family_id, observation_date)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_obs_family_doc_release "
                "ON obs_family_document(release_family_id)"
            )

            # ── Cross-source concept map ───────────────────────────
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS concept_map (
                    concept_id         TEXT NOT NULL,
                    source_id          TEXT NOT NULL,
                    provider_series_id TEXT NOT NULL,
                    obs_family_id      TEXT NOT NULL DEFAULT '',
                    role               TEXT NOT NULL DEFAULT 'primary'
                        CHECK (role IN ('primary','secondary','cross_check')),
                    notes              TEXT NOT NULL DEFAULT '',
                    created_at         TEXT NOT NULL,
                    PRIMARY KEY (concept_id, source_id, provider_series_id)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_concept_map_concept "
                "ON concept_map(concept_id)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_concept_map_series "
                "ON concept_map(source_id, provider_series_id)"
            )
            try:
                connection.execute("ALTER TABLE concept_map ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # column already exists

            # ── Release schedule ──────────────────────────────────────
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS release_schedule (
                    concept_id        TEXT PRIMARY KEY,
                    rule_type         TEXT NOT NULL,
                    rule_json         TEXT NOT NULL DEFAULT '{}',
                    frequency         TEXT NOT NULL DEFAULT 'monthly',
                    release_time_utc  TEXT NOT NULL DEFAULT '',
                    timezone          TEXT NOT NULL DEFAULT '',
                    source_authority  TEXT NOT NULL DEFAULT 'manual',
                    confidence        TEXT NOT NULL DEFAULT 'pattern'
                        CHECK (confidence IN ('exact','pattern','approximate')),
                    next_expected     TEXT NOT NULL DEFAULT '',
                    last_released     TEXT NOT NULL DEFAULT '',
                    last_checked      TEXT NOT NULL DEFAULT '',
                    is_active         INTEGER NOT NULL DEFAULT 1,
                    notes             TEXT NOT NULL DEFAULT '',
                    created_at        TEXT NOT NULL,
                    updated_at        TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_release_schedule_next "
                "ON release_schedule(next_expected) WHERE is_active = 1"
            )

            # ── Release status (availability tracking) ────────────────
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS release_status (
                    concept_id      TEXT NOT NULL,
                    release_date    TEXT NOT NULL,
                    status          TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN (
                            'PENDING','WAITING','FETCHED','CONFIRMED','STALE','FAILED'
                        )),
                    attempt_count   INTEGER NOT NULL DEFAULT 0,
                    next_retry      TEXT NOT NULL DEFAULT '',
                    last_attempt    TEXT NOT NULL DEFAULT '',
                    source_used     TEXT NOT NULL DEFAULT '',
                    data_date       TEXT NOT NULL DEFAULT '',
                    expected_period TEXT NOT NULL DEFAULT '',
                    provisional     INTEGER NOT NULL DEFAULT 0,
                    error           TEXT NOT NULL DEFAULT '',
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL,
                    PRIMARY KEY (concept_id, release_date)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_release_status_retry "
                "ON release_status(next_retry) "
                "WHERE status IN ('PENDING','WAITING')"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_release_status_concept "
                "ON release_status(concept_id, status)"
            )

            # ── Calendar indicator normalization tables ───────────────
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_indicator (
                    indicator_id   TEXT PRIMARY KEY,
                    canonical_name TEXT NOT NULL,
                    topic          TEXT NOT NULL DEFAULT '',
                    country_code   TEXT NOT NULL,
                    frequency      TEXT NOT NULL DEFAULT 'monthly',
                    unit           TEXT NOT NULL DEFAULT '',
                    obs_family_id  TEXT DEFAULT NULL,
                    is_active      INTEGER NOT NULL DEFAULT 1,
                    created_at     TEXT NOT NULL,
                    updated_at     TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_cal_indicator_country_topic "
                "ON calendar_indicator(country_code, topic)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS calendar_indicator_alias (
                    alias_normalized TEXT NOT NULL,
                    indicator_id     TEXT NOT NULL,
                    source           TEXT NOT NULL,
                    country_code     TEXT NOT NULL,
                    alias_original   TEXT NOT NULL DEFAULT '',
                    created_at       TEXT NOT NULL,
                    PRIMARY KEY (alias_normalized, source, country_code)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_cal_alias_indicator "
                "ON calendar_indicator_alias(indicator_id)"
            )
            self._ensure_table_columns(
                connection,
                table_name="calendar_events",
                columns={"indicator_id": "TEXT DEFAULT NULL"},
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_calendar_events_indicator_id "
                "ON calendar_events(indicator_id)"
            )

    def _ensure_table_columns(
        self,
        connection: sqlite3.Connection,
        *,
        table_name: str,
        columns: dict[str, str],
    ) -> None:
        existing = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_def in columns.items():
            if column_name in existing:
                continue
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    def upsert_calendar_event(self, event: StoredEventRecord) -> None:
        with self._connection(commit=True) as connection:
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

    def insert_market_price(self, price: MarketPriceRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO market_prices (
                    symbol,
                    asset_class,
                    name,
                    price,
                    change_pct,
                    timestamp,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    price.symbol,
                    price.asset_class,
                    price.name,
                    price.price,
                    price.change_pct,
                    price.timestamp,
                    utc_now().isoformat(),
                ),
            )

    def upsert_central_bank_comm(self, communication: CentralBankCommunicationRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO central_bank_comms (
                    source,
                    title,
                    url,
                    timestamp,
                    content_type,
                    speaker,
                    summary,
                    full_text,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    communication.source,
                    communication.title,
                    communication.url,
                    communication.timestamp,
                    communication.content_type,
                    communication.speaker,
                    communication.summary,
                    communication.full_text,
                    utc_now().isoformat(),
                ),
            )

    def upsert_indicator_observation(self, observation: IndicatorObservationRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO indicators (
                    series_id,
                    source,
                    date,
                    value,
                    metadata_json,
                    obs_family_id,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation.series_id,
                    observation.source,
                    observation.date,
                    observation.value,
                    json.dumps(observation.metadata, ensure_ascii=True, sort_keys=True),
                    observation.obs_family_id,
                    utc_now().isoformat(),
                ),
            )

    def save_regime_snapshot(self, regime_json: dict[str, Any], trigger_event: str, summary: str) -> RegimeSnapshotRecord:
        timestamp = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO regime_snapshots (
                    timestamp,
                    regime_json,
                    trigger_event,
                    summary
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    timestamp,
                    json.dumps(regime_json, ensure_ascii=False, sort_keys=True),
                    trigger_event,
                    summary,
                ),
            )
            snapshot_id = int(cursor.lastrowid)
        return RegimeSnapshotRecord(
            snapshot_id=snapshot_id,
            timestamp=timestamp,
            regime_json=regime_json,
            trigger_event=trigger_event,
            summary=summary,
        )

    def save_generated_note(
        self,
        note_type: str,
        title: str,
        summary: str,
        body_markdown: str,
        regime_json: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> GeneratedNoteRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO generated_notes (
                    created_at,
                    note_type,
                    title,
                    summary,
                    body_markdown,
                    regime_json,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    note_type,
                    title,
                    summary,
                    body_markdown,
                    json.dumps(regime_json, ensure_ascii=False, sort_keys=True) if regime_json else None,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                ),
            )
            note_id = int(cursor.lastrowid)
        return GeneratedNoteRecord(
            note_id=note_id,
            created_at=created_at,
            note_type=note_type,
            title=title,
            summary=summary,
            body_markdown=body_markdown,
            regime_json=regime_json,
            metadata=metadata or {},
        )

    def list_recent_regime_snapshots(self, *, limit: int = 3) -> list[RegimeSnapshotRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            RegimeSnapshotRecord(
                snapshot_id=int(row["id"]),
                timestamp=row["timestamp"],
                regime_json=json.loads(row["regime_json"]),
                trigger_event=row["trigger_event"],
                summary=row["summary"],
            )
            for row in rows
        ]

    def list_recent_generated_notes(
        self,
        *,
        limit: int = 5,
        note_type: str | None = None,
    ) -> list[GeneratedNoteRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if note_type:
            conditions.append("note_type = ?")
            params.append(note_type)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM generated_notes
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """.format(where_clause=where_clause),
                [*params, limit],
            ).fetchall()
        return [
            GeneratedNoteRecord(
                note_id=int(row["id"]),
                created_at=row["created_at"],
                note_type=row["note_type"],
                title=row["title"],
                summary=row["summary"],
                body_markdown=row["body_markdown"],
                regime_json=json.loads(row["regime_json"]) if row["regime_json"] else None,
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def add_analytical_observation(
        self,
        *,
        observation_type: str,
        summary: str,
        detail: str,
        source_kind: str,
        source_id: int,
        metadata: dict[str, Any] | None = None,
    ) -> AnalyticalObservationRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO analytical_observations (
                    observation_type,
                    summary,
                    detail,
                    source_kind,
                    source_id,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_type,
                    summary,
                    detail,
                    source_kind,
                    source_id,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            observation_id = int(cursor.lastrowid)
        return AnalyticalObservationRecord(
            observation_id=observation_id,
            observation_type=observation_type,
            summary=summary,
            detail=detail,
            source_kind=source_kind,
            source_id=source_id,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_analytical_observations(self, *, limit: int = 5) -> list[AnalyticalObservationRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM analytical_observations
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            AnalyticalObservationRecord(
                observation_id=int(row["id"]),
                observation_type=row["observation_type"],
                summary=row["summary"],
                detail=row["detail"],
                source_kind=row["source_kind"],
                source_id=int(row["source_id"]),
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def list_tagged_observations(self, *, tags: list[str], limit: int = 4) -> list[AnalyticalObservationRecord]:
        if not tags:
            return self.list_recent_analytical_observations(limit=limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM analytical_observations
                ORDER BY id DESC
                """,
            ).fetchall()
        matched: list[AnalyticalObservationRecord] = []
        for row in rows:
            if not _matches_scope_tags(row["summary"], tags):
                continue
            matched.append(
                AnalyticalObservationRecord(
                    observation_id=int(row["id"]),
                    observation_type=row["observation_type"],
                    summary=row["summary"],
                    detail=row["detail"],
                    source_kind=row["source_kind"],
                    source_id=int(row["source_id"]),
                    created_at=row["created_at"],
                    metadata=json.loads(row["metadata_json"]),
                )
            )
            if len(matched) >= limit:
                break
        return matched

    def list_tagged_regime_snapshots(self, *, tags: list[str], limit: int = 2) -> list[RegimeSnapshotRecord]:
        if not tags:
            return self.list_recent_regime_snapshots(limit=limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                """,
            ).fetchall()
        matched: list[RegimeSnapshotRecord] = []
        for row in rows:
            if not _matches_scope_tags(row["summary"], tags):
                continue
            matched.append(
                RegimeSnapshotRecord(
                    snapshot_id=int(row["id"]),
                    timestamp=row["timestamp"],
                    regime_json=json.loads(row["regime_json"]),
                    trigger_event=row["trigger_event"],
                    summary=row["summary"],
                )
            )
            if len(matched) >= limit:
                break
        return matched

    def save_subagent_run(
        self,
        *,
        task_id: str,
        parent_agent: str,
        task_type: str,
        objective: str,
        scope_tags: list[str],
        status: str,
        summary: str,
        elapsed_seconds: float,
    ) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO subagent_runs (
                    task_id, parent_agent, task_type, objective,
                    scope_tags_json, status, summary, elapsed_seconds, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    parent_agent,
                    task_type,
                    objective,
                    json.dumps(scope_tags, ensure_ascii=False),
                    status,
                    summary,
                    elapsed_seconds,
                    utc_now().isoformat(),
                ),
            )

    def list_recent_subagent_runs(
        self,
        *,
        parent_agent: str | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        with self._connection(commit=False) as connection:
            if parent_agent:
                rows = connection.execute(
                    "SELECT * FROM subagent_runs WHERE parent_agent = ? ORDER BY id DESC LIMIT ?",
                    (parent_agent, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM subagent_runs ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "parent_agent": row["parent_agent"],
                "task_type": row["task_type"],
                "objective": row["objective"],
                "scope_tags": json.loads(row["scope_tags_json"]),
                "status": row["status"],
                "summary": row["summary"],
                "elapsed_seconds": row["elapsed_seconds"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def publish_research_artifact(
        self,
        *,
        artifact_type: str,
        title: str,
        summary: str,
        content_markdown: str,
        source_kind: str,
        source_id: int,
        tags: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> ResearchArtifactRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO research_artifacts (
                    artifact_type,
                    title,
                    summary,
                    content_markdown,
                    source_kind,
                    source_id,
                    tags_json,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_type,
                    title,
                    summary,
                    content_markdown,
                    source_kind,
                    source_id,
                    json.dumps(tags, ensure_ascii=False, sort_keys=True),
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            artifact_id = int(cursor.lastrowid)
        return ResearchArtifactRecord(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            title=title,
            summary=summary,
            content_markdown=content_markdown,
            source_kind=source_kind,
            source_id=source_id,
            created_at=created_at,
            tags=tags,
            metadata=metadata or {},
        )

    def list_recent_research_artifacts(
        self,
        *,
        limit: int = 5,
        artifact_types: tuple[str, ...] = (),
    ) -> list[ResearchArtifactRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if artifact_types:
            conditions.append("artifact_type IN (" + ",".join("?" for _ in artifact_types) + ")")
            params.extend(artifact_types)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM research_artifacts
                {where_clause}
                ORDER BY id DESC
                LIMIT ?
                """.format(where_clause=where_clause),
                [*params, limit],
            ).fetchall()
        return [
            ResearchArtifactRecord(
                artifact_id=int(row["id"]),
                artifact_type=row["artifact_type"],
                title=row["title"],
                summary=row["summary"],
                content_markdown=row["content_markdown"],
                source_kind=row["source_kind"],
                source_id=int(row["source_id"]),
                created_at=row["created_at"],
                tags=json.loads(row["tags_json"]),
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def search_research_artifacts(
        self,
        *,
        query: str,
        limit: int = 5,
        artifact_types: tuple[str, ...] = (),
    ) -> list[ResearchArtifactRecord]:
        terms = self._search_terms(query)
        candidates = self.list_recent_research_artifacts(limit=max(limit * 20, 100), artifact_types=artifact_types)
        scored: list[tuple[float, ResearchArtifactRecord]] = []
        for artifact in candidates:
            haystack = " ".join([artifact.title, artifact.summary, artifact.content_markdown])
            score = self._score_text_match(haystack, terms)
            if score <= 0:
                continue
            scored.append((score, artifact))
        scored.sort(key=lambda item: (item[0], item[1].created_at), reverse=True)
        return [record for _, record in scored[:limit]]

    def save_trade_signal(
        self,
        *,
        signal_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        signal: dict[str, Any],
        confidence: float,
        metadata: dict[str, Any] | None = None,
    ) -> TradeSignalRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO trade_signals (
                    signal_type,
                    title,
                    summary,
                    rationale_markdown,
                    signal_json,
                    confidence,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_type,
                    title,
                    summary,
                    rationale_markdown,
                    json.dumps(signal, ensure_ascii=False, sort_keys=True),
                    confidence,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            signal_id = int(cursor.lastrowid)
        return TradeSignalRecord(
            signal_id=signal_id,
            signal_type=signal_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            signal=signal,
            confidence=confidence,
            created_at=created_at,
            metadata=metadata or {},
        )

    def log_trading_decision(
        self,
        *,
        decision_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        research_artifact_id: int | None,
        signal_id: int | None,
        metadata: dict[str, Any] | None = None,
    ) -> DecisionLogRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO decision_log (
                    decision_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    signal_id,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    signal_id,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            decision_id = int(cursor.lastrowid)
        return DecisionLogRecord(
            decision_id=decision_id,
            decision_type=decision_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            research_artifact_id=research_artifact_id,
            signal_id=signal_id,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_decisions(self, *, limit: int = 5) -> list[DecisionLogRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM decision_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            DecisionLogRecord(
                decision_id=int(row["id"]),
                decision_type=row["decision_type"],
                title=row["title"],
                summary=row["summary"],
                rationale_markdown=row["rationale_markdown"],
                research_artifact_id=int(row["research_artifact_id"]) if row["research_artifact_id"] is not None else None,
                signal_id=int(row["signal_id"]) if row["signal_id"] is not None else None,
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def upsert_position_state(
        self,
        *,
        symbol: str,
        exposure: float,
        direction: str,
        thesis: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO position_state (
                    symbol,
                    exposure,
                    direction,
                    thesis,
                    metadata_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    exposure = excluded.exposure,
                    direction = excluded.direction,
                    thesis = excluded.thesis,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    symbol,
                    exposure,
                    direction,
                    thesis,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    utc_now().isoformat(),
                ),
            )

    def list_position_state(self, *, limit: int = 10) -> list[PositionStateRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM position_state
                ORDER BY updated_at DESC, symbol ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            PositionStateRecord(
                symbol=row["symbol"],
                exposure=float(row["exposure"]),
                direction=row["direction"],
                thesis=row["thesis"],
                updated_at=row["updated_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def record_performance(
        self,
        *,
        metric_name: str,
        metric_value: float,
        period_label: str,
        metadata: dict[str, Any] | None = None,
    ) -> PerformanceRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO performance_records (
                    metric_name,
                    metric_value,
                    period_label,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    metric_name,
                    metric_value,
                    period_label,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            record_id = int(cursor.lastrowid)
        return PerformanceRecord(
            record_id=record_id,
            metric_name=metric_name,
            metric_value=metric_value,
            period_label=period_label,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_performance_records(self, *, limit: int = 5) -> list[PerformanceRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM performance_records
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            PerformanceRecord(
                record_id=int(row["id"]),
                metric_name=row["metric_name"],
                metric_value=float(row["metric_value"]),
                period_label=row["period_label"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    def publish_trading_artifact(
        self,
        *,
        artifact_type: str,
        title: str,
        summary: str,
        rationale_markdown: str,
        research_artifact_id: int,
        signal: dict[str, Any],
        confidence: float,
        decision_log_id: int | None = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TradingArtifactRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO trading_artifacts (
                    artifact_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    decision_log_id,
                    signal_json,
                    confidence,
                    tags_json,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_type,
                    title,
                    summary,
                    rationale_markdown,
                    research_artifact_id,
                    decision_log_id,
                    json.dumps(signal, ensure_ascii=False, sort_keys=True),
                    confidence,
                    json.dumps(tags or [], ensure_ascii=False, sort_keys=True),
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            artifact_id = int(cursor.lastrowid)
        return TradingArtifactRecord(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            title=title,
            summary=summary,
            rationale_markdown=rationale_markdown,
            research_artifact_id=research_artifact_id,
            decision_log_id=decision_log_id,
            signal=signal,
            confidence=confidence,
            created_at=created_at,
            tags=tags or [],
            metadata=metadata or {},
        )

    def list_recent_trading_artifacts(self, *, limit: int = 5) -> list[TradingArtifactRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM trading_artifacts
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            TradingArtifactRecord(
                artifact_id=int(row["id"]),
                artifact_type=row["artifact_type"],
                title=row["title"],
                summary=row["summary"],
                rationale_markdown=row["rationale_markdown"],
                research_artifact_id=int(row["research_artifact_id"]),
                decision_log_id=int(row["decision_log_id"]) if row["decision_log_id"] is not None else None,
                signal=json.loads(row["signal_json"]),
                confidence=float(row["confidence"]),
                created_at=row["created_at"],
                tags=json.loads(row["tags_json"]),
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

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
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if released_only:
            conditions.append("actual IS NOT NULL")
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        # Only fixed SQL fragments are appended here; user input stays parameterized.
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM calendar_events
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def list_upcoming_events(
        self,
        *,
        limit: int = 10,
        importance: str | None = None,
        country: str | None = None,
        category: str | None = None,
    ) -> list[StoredEventRecord]:
        now_epoch = int(utc_now().timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [now_epoch]
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM calendar_events
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp ASC, id ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

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
        conditions = ["timestamp >= ?", "timestamp <= ?"]
        params: list[Any] = [date_from, date_to]
        if released_only:
            conditions.append("actual IS NOT NULL")
        if importance:
            conditions.append("importance = ?")
            params.append(importance)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if category:
            conditions.append("category = ?")
            params.append(category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM calendar_events
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp ASC, id ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

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

    def list_indicator_releases(
        self,
        *,
        indicator_keyword: str,
        limit: int = 12,
    ) -> list[StoredEventRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM calendar_events
                WHERE LOWER(indicator) LIKE ? AND actual IS NOT NULL
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                (f"%{indicator_keyword.lower()}%", limit),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def latest_market_prices(self) -> list[MarketPriceRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT latest.* FROM market_prices latest
                INNER JOIN (
                    SELECT symbol, MAX(id) AS max_id
                    FROM market_prices
                    GROUP BY symbol
                ) grouped ON latest.id = grouped.max_id
                ORDER BY latest.asset_class ASC, latest.symbol ASC
                """
            ).fetchall()
        return [self._row_to_market_price(row) for row in rows]

    def list_recent_central_bank_comms(
        self,
        *,
        source: str = "fed",
        limit: int = 5,
        days: int = 14,
        speaker: str | None = None,
        content_type: str | None = None,
    ) -> list[CentralBankCommunicationRecord]:
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["source = ?", "timestamp >= ?"]
        params: list[Any] = [source, cutoff]
        if speaker:
            conditions.append("LOWER(speaker) LIKE ?")
            params.append(f"%{speaker.lower()}%")
        if content_type:
            conditions.append("content_type = ?")
            params.append(content_type)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM central_bank_comms
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
        return [self._row_to_comm(row) for row in rows]

    def get_indicator_history(self, series_id: str, *, limit: int = 12) -> list[IndicatorObservationRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM indicators
                WHERE series_id = ?
                ORDER BY date DESC, id DESC
                LIMIT ?
                """,
                (series_id, limit),
            ).fetchall()
        return [self._row_to_indicator(row) for row in rows]

    def latest_regime_snapshot(self) -> RegimeSnapshotRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT * FROM regime_snapshots
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        return RegimeSnapshotRecord(
            snapshot_id=int(row["id"]),
            timestamp=row["timestamp"],
            regime_json=json.loads(row["regime_json"]),
            trigger_event=row["trigger_event"],
            summary=row["summary"],
        )

    def latest_released_event(self, *, indicator_keyword: str | None = None) -> StoredEventRecord | None:
        params: list[Any] = []
        conditions = ["actual IS NOT NULL"]
        if indicator_keyword:
            conditions.append("LOWER(indicator) LIKE ?")
            params.append(f"%{indicator_keyword.lower()}%")
        # Only fixed SQL fragments are appended here; user input stays parameterized.
        with self._connection(commit=False) as connection:
            row = connection.execute(
                f"""
                SELECT * FROM calendar_events
                WHERE {' AND '.join(conditions)}
                ORDER BY importance DESC, timestamp DESC, id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        return self._row_to_event(row) if row is not None else None

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

    def _row_to_market_price(self, row: sqlite3.Row) -> MarketPriceRecord:
        return MarketPriceRecord(
            symbol=row["symbol"],
            asset_class=row["asset_class"],
            name=row["name"],
            price=float(row["price"]),
            change_pct=float(row["change_pct"]) if row["change_pct"] is not None else None,
            timestamp=int(row["timestamp"]),
        )

    def _row_to_comm(self, row: sqlite3.Row) -> CentralBankCommunicationRecord:
        return CentralBankCommunicationRecord(
            source=row["source"],
            title=row["title"],
            url=row["url"],
            timestamp=int(row["timestamp"]),
            content_type=row["content_type"],
            speaker=row["speaker"],
            summary=row["summary"],
            full_text=row["full_text"],
        )

    def _row_to_indicator(self, row: sqlite3.Row) -> IndicatorObservationRecord:
        return IndicatorObservationRecord(
            series_id=row["series_id"],
            source=row["source"],
            date=row["date"],
            value=float(row["value"]),
            metadata=json.loads(row["metadata_json"]),
        )

    # -- Indicator vintages --------------------------------------------------

    def upsert_indicator_vintage(self, vintage: IndicatorVintageRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO indicator_vintages (
                    series_id,
                    source,
                    observation_date,
                    vintage_date,
                    value,
                    metadata_json,
                    obs_family_id,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    vintage.series_id,
                    vintage.source,
                    vintage.observation_date,
                    vintage.vintage_date,
                    vintage.value,
                    json.dumps(vintage.metadata, ensure_ascii=True, sort_keys=True),
                    vintage.obs_family_id,
                    utc_now().isoformat(),
                ),
            )

    def get_vintage_history(
        self, series_id: str, observation_date: str,
    ) -> list[IndicatorVintageRecord]:
        """Return all vintages for a given series_id + observation_date."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM indicator_vintages
                WHERE series_id = ? AND observation_date = ?
                ORDER BY vintage_date ASC
                """,
                (series_id, observation_date),
            ).fetchall()
        return [self._row_to_vintage(row) for row in rows]

    def get_vintages_for_series(
        self, series_id: str, *, limit: int = 50,
    ) -> list[IndicatorVintageRecord]:
        """Return the most recent vintage records for a series."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM indicator_vintages
                WHERE series_id = ?
                ORDER BY vintage_date DESC, observation_date DESC
                LIMIT ?
                """,
                (series_id, limit),
            ).fetchall()
        return [self._row_to_vintage(row) for row in rows]

    def _row_to_vintage(self, row: sqlite3.Row) -> IndicatorVintageRecord:
        return IndicatorVintageRecord(
            series_id=row["series_id"],
            source=row["source"],
            observation_date=row["observation_date"],
            vintage_date=row["vintage_date"],
            value=float(row["value"]),
            metadata=json.loads(row["metadata_json"]),
        )

    # -- News articles -------------------------------------------------------

    # Time-decay constants for news retrieval scoring
    _IMPACT_HALF_LIFE = {"critical": 7, "high": 5, "medium": 3, "low": 2, "info": 1}
    _IMPACT_WEIGHT = {"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.6, "info": 0.3}
    _TIME_DECAY_MAX_BOOST = 1.5
    _TIME_DECAY_MIN_BOOST = 0.1

    def upsert_news_article(self, article: NewsArticleRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO news_articles (
                    url_hash, source_feed, feed_category, title, url,
                    timestamp, description, content_markdown,
                    impact_level, finance_category, confidence,
                    content_fetched, institution, country, market,
                    asset_class, sector, document_type, event_type,
                    subject, subject_id, data_period,
                    contains_commentary, language, authors,
                    extraction_provider, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article.url_hash,
                    article.source_feed,
                    article.feed_category,
                    article.title,
                    article.url,
                    article.timestamp,
                    article.description,
                    article.content_markdown,
                    article.impact_level,
                    article.finance_category,
                    article.confidence,
                    int(article.content_fetched),
                    article.institution,
                    article.country,
                    article.market,
                    article.asset_class,
                    article.sector,
                    article.document_type,
                    article.event_type,
                    article.subject,
                    article.subject_id,
                    article.data_period,
                    int(article.contains_commentary),
                    article.language,
                    article.authors,
                    article.extraction_provider,
                    utc_now().isoformat(),
                ),
            )

    def upsert_trend_topic(self, trend: TrendTopicRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO trend_topics (
                    trend_id, provider, provider_topic_id, title_raw, topic,
                    summary, keywords_json, category, region, popularity_score,
                    provider_rank, engagement_score, comment_count,
                    observed_at, expires_at, raw_json, normalized_topic_hash,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trend.trend_id,
                    trend.provider,
                    trend.provider_topic_id,
                    trend.title_raw,
                    trend.topic,
                    trend.summary,
                    json.dumps(trend.keywords, ensure_ascii=True),
                    trend.category,
                    trend.region,
                    float(trend.popularity_score),
                    int(trend.provider_rank),
                    float(trend.engagement_score),
                    int(trend.comment_count),
                    int(trend.observed_at),
                    int(trend.expires_at),
                    json.dumps(trend.raw_json, ensure_ascii=True, sort_keys=True),
                    trend.normalized_topic_hash,
                    utc_now().isoformat(),
                ),
            )

    def list_active_trends(
        self,
        *,
        limit: int = 10,
        hours: int = 48,
        category: str | None = None,
        region: str | None = None,
    ) -> list[TrendTopicRecord]:
        now_ts = int(utc_now().timestamp())
        cutoff = int((utc_now() - timedelta(hours=hours)).timestamp())
        conditions = ["expires_at >= ?", "observed_at >= ?"]
        params: list[Any] = [now_ts, cutoff]
        if category:
            conditions.append("category = ?")
            params.append(category)
        if region:
            conditions.append("region = ?")
            params.append(region)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM trend_topics
                WHERE {' AND '.join(conditions)}
                ORDER BY popularity_score DESC, observed_at DESC, trend_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_trend_topic(row) for row in rows]

    def list_recent_news(
        self,
        *,
        limit: int = 20,
        days: int = 7,
        impact_level: str | None = None,
        feed_category: str | None = None,
        finance_category: str | None = None,
        country: str | None = None,
        asset_class: str | None = None,
    ) -> list[NewsArticleRecord]:
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if impact_level:
            conditions.append("impact_level = ?")
            params.append(impact_level)
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)
        if finance_category:
            conditions.append("finance_category = ?")
            params.append(finance_category)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if asset_class:
            conditions.append("asset_class = ?")
            params.append(asset_class)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM news_articles
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_news_article(row) for row in rows]

    def search_news(self, query: str, *, limit: int = 20) -> list[NewsArticleRecord]:
        with self._connection(commit=False) as connection:
            try:
                rows = connection.execute(
                    """
                    SELECT n.* FROM news_articles n
                    JOIN news_fts ON news_fts.rowid = n.id
                    WHERE news_fts MATCH ?
                    ORDER BY n.timestamp DESC, n.id DESC
                    LIMIT ?
                    """,
                    (query, limit),
                ).fetchall()
            except sqlite3.OperationalError:
                pattern = f"%{query}%"
                rows = connection.execute(
                    """
                    SELECT * FROM news_articles
                    WHERE title LIKE ? OR description LIKE ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, limit),
                ).fetchall()
        return [self._row_to_news_article(row) for row in rows]

    def get_news_context(
        self,
        *,
        query: str | None = None,
        days: int = 7,
        limit: int = 15,
        impact_level: str | None = None,
        feed_category: str | None = None,
        finance_category: str | None = None,
        country: str | None = None,
        asset_class: str | None = None,
        display_timezone: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve news with time-decay + impact-weight composite scoring."""
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if impact_level:
            conditions.append("impact_level = ?")
            params.append(impact_level)
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)
        if finance_category:
            conditions.append("finance_category = ?")
            params.append(finance_category)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if asset_class:
            conditions.append("asset_class = ?")
            params.append(asset_class)

        with self._connection(commit=False) as connection:
            if query:
                try:
                    rows = connection.execute(
                        f"""
                        SELECT n.* FROM news_articles n
                        JOIN news_fts ON news_fts.rowid = n.id
                        WHERE news_fts MATCH ? AND {' AND '.join(conditions)}
                        """,
                        [query] + params,
                    ).fetchall()
                except sqlite3.OperationalError:
                    pattern = f"%{query}%"
                    conditions.append("(title LIKE ? OR description LIKE ?)")
                    params.extend([pattern, pattern])
                    rows = connection.execute(
                        f"""
                        SELECT * FROM news_articles
                        WHERE {' AND '.join(conditions)}
                        """,
                        params,
                    ).fetchall()
            else:
                rows = connection.execute(
                    f"""
                    SELECT * FROM news_articles
                    WHERE {' AND '.join(conditions)}
                    """,
                    params,
                ).fetchall()

        now = utc_now()
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            article = self._row_to_news_article(row)
            pub = epoch_to_datetime(article.timestamp)
            age_days = max((now - pub).total_seconds() / 86400, 0.0)
            half_life = self._IMPACT_HALF_LIFE.get(article.impact_level, 2)
            time_decay = self._TIME_DECAY_MIN_BOOST + (
                (self._TIME_DECAY_MAX_BOOST - self._TIME_DECAY_MIN_BOOST)
                * math.pow(2, -age_days / half_life)
            )
            impact_w = self._IMPACT_WEIGHT.get(article.impact_level, 0.5)
            composite = time_decay * impact_w

            desc = article.description
            if len(desc) > 500:
                desc = desc[:500] + "..."
            payload = {
                "source_feed": article.source_feed,
                "title": article.title,
                "url": article.url,
                "timestamp": article.timestamp,
                "published_at": format_epoch_iso(article.timestamp),
                "description": desc,
                "impact_level": article.impact_level,
                "finance_category": article.finance_category,
                "country": article.country,
                "asset_class": article.asset_class,
                "subject": article.subject,
                "event_type": article.event_type,
                "score": round(composite, 4),
            }
            if display_timezone:
                try:
                    payload["published_at_local"] = format_epoch_iso_in_timezone(
                        article.timestamp,
                        display_timezone,
                    )
                    payload["published_timezone"] = display_timezone
                except ValueError:
                    pass
            scored.append((composite, payload))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for _, item in scored[:limit]]

    def get_recent_news_titles(self, *, hours: int = 24) -> list[str]:
        cutoff = (utc_now() - timedelta(hours=hours)).isoformat()
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT title FROM news_articles
                WHERE scraped_at >= ?
                ORDER BY id DESC
                """,
                (cutoff,),
            ).fetchall()
        return [row["title"] for row in rows]

    def news_article_exists(self, url_hash: str) -> bool:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM news_articles WHERE url_hash = ? LIMIT 1",
                (url_hash,),
            ).fetchone()
        return row is not None

    # -- Article fingerprint dedup methods --------------------------------

    def fingerprint_exists(self, *, url_hash: str | None = None, title_hash: str | None = None) -> bool:
        """Return True if a fingerprint with the given url_hash OR title_hash exists."""
        if not url_hash and not title_hash:
            return False
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM article_fingerprint WHERE url_hash = ? OR title_hash = ? LIMIT 1",
                (url_hash or "", title_hash or ""),
            ).fetchone()
        return row is not None

    def insert_fingerprint(
        self,
        url_hash: str,
        title_hash: str,
        canonical_url: str,
        raw_url: str,
        title: str = "",
        source_feed: str = "",
    ) -> None:
        """Insert a fingerprint record. Silently ignores duplicates."""
        now_iso = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO article_fingerprint
                    (url_hash, title_hash, canonical_url, raw_url, title, source_feed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (url_hash, title_hash, canonical_url, raw_url, title, source_feed, now_iso),
            )

    def backfill_fingerprints(self) -> int:
        """One-time migration: compute fingerprints for all existing news_articles."""
        from ingestion.url_canon import canonicalize_url, content_hash

        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT url_hash, url, title, timestamp FROM news_articles"
            ).fetchall()

        count = 0
        now_iso = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            for row in rows:
                canonical = canonicalize_url(row["url"])
                u_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                t_hash = content_hash(row["title"], int(row["timestamp"]))
                try:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO article_fingerprint
                            (url_hash, title_hash, canonical_url, raw_url, title, source_feed, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (u_hash, t_hash, canonical, row["url"], row["title"], "", now_iso),
                    )
                    count += 1
                except sqlite3.IntegrityError:
                    pass
        return count

    def _row_to_news_article(self, row: sqlite3.Row) -> NewsArticleRecord:
        return NewsArticleRecord(
            url_hash=row["url_hash"],
            source_feed=row["source_feed"],
            feed_category=row["feed_category"],
            title=row["title"],
            url=row["url"],
            timestamp=int(row["timestamp"]),
            description=row["description"],
            content_markdown=row["content_markdown"],
            impact_level=row["impact_level"],
            finance_category=row["finance_category"],
            confidence=float(row["confidence"]),
            content_fetched=bool(row["content_fetched"]),
            institution=row["institution"] or "",
            country=row["country"] or "",
            market=row["market"] or "",
            asset_class=row["asset_class"] or "",
            sector=row["sector"] or "",
            document_type=row["document_type"] or "",
            event_type=row["event_type"] or "",
            subject=row["subject"] or "",
            subject_id=row["subject_id"] or "",
            data_period=row["data_period"] or "",
            contains_commentary=bool(row["contains_commentary"]),
            language=row["language"] or "en",
            authors=row["authors"] or "",
            extraction_provider=row["extraction_provider"] or "keyword",
        )

    def _row_to_trend_topic(self, row: sqlite3.Row) -> TrendTopicRecord:
        try:
            keywords = json.loads(row["keywords_json"] or "[]")
        except json.JSONDecodeError:
            keywords = []
        if not isinstance(keywords, list):
            keywords = []
        try:
            raw_json = json.loads(row["raw_json"] or "{}")
        except json.JSONDecodeError:
            raw_json = {}
        if not isinstance(raw_json, dict):
            raw_json = {}
        return TrendTopicRecord(
            trend_id=row["trend_id"],
            provider=row["provider"],
            provider_topic_id=row["provider_topic_id"],
            title_raw=row["title_raw"],
            topic=row["topic"],
            summary=row["summary"],
            keywords=[str(item) for item in keywords if str(item).strip()],
            category=row["category"] or "",
            region=row["region"] or "global",
            popularity_score=float(row["popularity_score"]),
            provider_rank=int(row["provider_rank"]),
            engagement_score=float(row["engagement_score"]),
            comment_count=int(row["comment_count"]),
            observed_at=int(row["observed_at"]),
            expires_at=int(row["expires_at"]),
            raw_json=raw_json,
            normalized_topic_hash=row["normalized_topic_hash"] or "",
        )

    # -- Sales memory and delivery pipeline --------------------------------

    def get_client_profile(self, client_id: str) -> ClientProfileRecord:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT * FROM client_profiles
                WHERE client_id = ?
                LIMIT 1
                """,
                (client_id,),
            ).fetchone()
        return self._row_to_client_profile(row, client_id=client_id)

    def upsert_client_profile(
        self,
        client_id: str,
        *,
        preferred_language: str | None = None,
        watchlist_topics: list[str] | None = None,
        response_style: str | None = None,
        risk_appetite: str | None = None,
        investment_horizon: str | None = None,
        institution_type: str | None = None,
        risk_preference: str | None = None,
        asset_focus: list[str] | None = None,
        market_focus: list[str] | None = None,
        expertise_level: str | None = None,
        activity: str | None = None,
        current_mood: str | None = None,
        emotional_trend: str | None = None,
        stress_level: str | None = None,
        confidence: str | None = None,
        notes: str | None = None,
        personal_facts: list[str] | None = None,
        last_active_at: str | None = None,
        interaction_increment: int = 0,
    ) -> ClientProfileRecord:
        with self._connection(commit=True) as connection:
            return self._upsert_client_profile_in_connection(
                connection,
                client_id=client_id,
                preferred_language=preferred_language,
                watchlist_topics=watchlist_topics,
                response_style=response_style,
                risk_appetite=risk_appetite,
                investment_horizon=investment_horizon,
                institution_type=institution_type,
                risk_preference=risk_preference,
                asset_focus=asset_focus,
                market_focus=market_focus,
                expertise_level=expertise_level,
                activity=activity,
                current_mood=current_mood,
                emotional_trend=emotional_trend,
                stress_level=stress_level,
                confidence=confidence,
                notes=notes,
                personal_facts=personal_facts,
                last_active_at=last_active_at,
                interaction_increment=interaction_increment,
            )

    def ensure_conversation_thread(self, *, client_id: str, channel: str, thread_id: str) -> None:
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
            )

    def append_conversation_message(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationMessageRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=created_at,
            )
            cursor = connection.execute(
                """
                INSERT INTO conversation_messages (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            message_id = int(cursor.lastrowid)
            connection.execute(
                """
                UPDATE conversation_threads
                SET last_active_at = ?
                WHERE client_id = ? AND channel = ? AND thread_id = ?
                """,
                (created_at, client_id, channel, thread_id),
            )
        return ConversationMessageRecord(
            message_id=message_id,
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            role=role,
            content=content,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_conversation_messages(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        limit: int = 12,
    ) -> list[ConversationMessageRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT * FROM conversation_messages
                WHERE client_id = ? AND channel = ? AND thread_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (client_id, channel, thread_id, limit),
            ).fetchall()
        records = [
            ConversationMessageRecord(
                message_id=int(row["id"]),
                client_id=row["client_id"],
                channel=row["channel"],
                thread_id=row["thread_id"],
                role=row["role"],
                content=row["content"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]
        records.reverse()
        return records

    def enqueue_delivery(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        source_type: str,
        content_rendered: str,
        source_artifact_id: int | None = None,
        status: str = "delivered",
        delivered_at: str | None = None,
        client_reaction: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> DeliveryQueueRecord:
        created_at = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=created_at,
            )
            cursor = connection.execute(
                """
                INSERT INTO delivery_queue (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
                    created_at,
                ),
            )
            delivery_id = int(cursor.lastrowid)
        return DeliveryQueueRecord(
            delivery_id=delivery_id,
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            source_type=source_type,
            source_artifact_id=source_artifact_id,
            content_rendered=content_rendered,
            status=status,
            delivered_at=delivered_at,
            client_reaction=client_reaction,
            created_at=created_at,
            metadata=metadata or {},
        )

    def list_recent_deliveries(
        self,
        *,
        client_id: str,
        channel: str | None = None,
        thread_id: str | None = None,
        limit: int = 5,
    ) -> list[DeliveryQueueRecord]:
        conditions = ["client_id = ?"]
        params: list[Any] = [client_id]
        if channel is not None:
            conditions.append("channel = ?")
            params.append(channel)
        if thread_id is not None:
            conditions.append("thread_id = ?")
            params.append(thread_id)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM delivery_queue
                WHERE {' AND '.join(conditions)}
                ORDER BY id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [
            DeliveryQueueRecord(
                delivery_id=int(row["id"]),
                client_id=row["client_id"],
                channel=row["channel"],
                thread_id=row["thread_id"],
                source_type=row["source_type"],
                source_artifact_id=int(row["source_artifact_id"]) if row["source_artifact_id"] is not None else None,
                content_rendered=row["content_rendered"],
                status=row["status"],
                delivered_at=row["delivered_at"],
                client_reaction=row["client_reaction"],
                created_at=row["created_at"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in rows
        ]

    @staticmethod
    def _recency_decay(created_at: str, *, half_life_hours: float = 24.0) -> float:
        """Exponential decay factor: 1.0 for now, 0.5 at half_life_hours ago, etc."""
        try:
            created = datetime.fromisoformat(created_at)
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = max((utc_now() - created).total_seconds() / 3600.0, 0.0)
            return math.pow(0.5, age_hours / half_life_hours)
        except (ValueError, TypeError):
            return 0.5

    def search_delivery_queue(
        self,
        *,
        client_id: str,
        query: str,
        channel: str | None = None,
        thread_id: str | None = None,
        limit: int = 3,
    ) -> list[DeliveryQueueRecord]:
        terms = self._search_terms(query)
        candidates = self.list_recent_deliveries(
            client_id=client_id,
            channel=channel,
            thread_id=thread_id,
            limit=max(limit * 12, 50),
        )
        scored: list[tuple[float, DeliveryQueueRecord]] = []
        for item in candidates:
            score = self._score_text_match(item.content_rendered, terms)
            if score <= 0:
                continue
            score *= self._recency_decay(item.created_at)
            scored.append((score, item))
        scored.sort(key=lambda pair: (pair[0], pair[1].created_at), reverse=True)
        return [record for _, record in scored[:limit]]

    def record_sales_interaction(
        self,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        user_text: str,
        assistant_text: str,
        tool_audit: list[dict[str, Any]],
        profile_updates: dict[str, Any],
    ) -> None:
        user_timestamp = utc_now().isoformat()
        assistant_timestamp = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            self._upsert_client_profile_in_connection(
                connection,
                client_id=client_id,
                preferred_language=profile_updates.get("preferred_language"),
                watchlist_topics=profile_updates.get("watchlist_topics"),
                response_style=profile_updates.get("response_style"),
                risk_appetite=profile_updates.get("risk_appetite"),
                investment_horizon=profile_updates.get("investment_horizon"),
                institution_type=profile_updates.get("institution_type"),
                risk_preference=profile_updates.get("risk_preference"),
                asset_focus=profile_updates.get("asset_focus"),
                market_focus=profile_updates.get("market_focus"),
                expertise_level=profile_updates.get("expertise_level"),
                activity=profile_updates.get("activity"),
                current_mood=profile_updates.get("current_mood"),
                emotional_trend=profile_updates.get("emotional_trend"),
                stress_level=profile_updates.get("stress_level"),
                confidence=profile_updates.get("confidence"),
                notes=profile_updates.get("notes"),
                personal_facts=profile_updates.get("personal_facts"),
                last_active_at=assistant_timestamp,
                interaction_increment=1,
            )
            self._ensure_conversation_thread_in_connection(
                connection,
                client_id=client_id,
                channel=channel,
                thread_id=thread_id,
                timestamp=assistant_timestamp,
            )
            connection.executemany(
                """
                INSERT INTO conversation_messages (
                    client_id,
                    channel,
                    thread_id,
                    role,
                    content,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        client_id,
                        channel,
                        thread_id,
                        "user",
                        user_text,
                        json.dumps({"channel": channel}, ensure_ascii=False, sort_keys=True),
                        user_timestamp,
                    ),
                    (
                        client_id,
                        channel,
                        thread_id,
                        "assistant",
                        assistant_text,
                        json.dumps(
                            {"channel": channel, "tool_audit": tool_audit},
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        assistant_timestamp,
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO delivery_queue (
                    client_id,
                    channel,
                    thread_id,
                    source_type,
                    source_artifact_id,
                    content_rendered,
                    status,
                    delivered_at,
                    client_reaction,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    channel,
                    thread_id,
                    "sales_reply",
                    None,
                    assistant_text,
                    "delivered",
                    assistant_timestamp,
                    "",
                    json.dumps(
                        {"user_text": user_text, "tool_audit": tool_audit},
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    assistant_timestamp,
                ),
            )

    def _row_to_client_profile(self, row: sqlite3.Row | None, *, client_id: str) -> ClientProfileRecord:
        if row is None:
            return ClientProfileRecord(
                client_id=client_id,
                preferred_language="",
                watchlist_topics=[],
                response_style="",
                risk_appetite="",
                investment_horizon="",
                institution_type="",
                risk_preference="",
                asset_focus=[],
                market_focus=[],
                expertise_level="",
                activity="",
                current_mood="",
                emotional_trend="",
                stress_level="",
                confidence="",
                notes="",
                personal_facts=[],
                last_active_at="",
                total_interactions=0,
                updated_at="",
            )
        return ClientProfileRecord(
            client_id=row["client_id"],
            preferred_language=row["preferred_language"],
            watchlist_topics=json.loads(row["watchlist_topics_json"]),
            response_style=row["response_style"],
            risk_appetite=row["risk_appetite"],
            investment_horizon=row["investment_horizon"],
            institution_type=row["institution_type"],
            risk_preference=row["risk_preference"],
            asset_focus=json.loads(row["asset_focus_json"]),
            market_focus=json.loads(row["market_focus_json"]),
            expertise_level=row["expertise_level"],
            activity=row["activity"],
            current_mood=row["current_mood"],
            emotional_trend=row["emotional_trend"],
            stress_level=row["stress_level"],
            confidence=row["confidence"],
            notes=row["notes"],
            personal_facts=json.loads(row["personal_facts_json"]),
            last_active_at=row["last_active_at"],
            total_interactions=int(row["total_interactions"]),
            updated_at=row["updated_at"],
        )

    def _get_client_profile_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
    ) -> ClientProfileRecord:
        row = connection.execute(
            """
            SELECT * FROM client_profiles
            WHERE client_id = ?
            LIMIT 1
            """,
            (client_id,),
        ).fetchone()
        return self._row_to_client_profile(row, client_id=client_id)

    def _upsert_client_profile_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
        preferred_language: str | None = None,
        watchlist_topics: list[str] | None = None,
        response_style: str | None = None,
        risk_appetite: str | None = None,
        investment_horizon: str | None = None,
        institution_type: str | None = None,
        risk_preference: str | None = None,
        asset_focus: list[str] | None = None,
        market_focus: list[str] | None = None,
        expertise_level: str | None = None,
        activity: str | None = None,
        current_mood: str | None = None,
        emotional_trend: str | None = None,
        stress_level: str | None = None,
        confidence: str | None = None,
        notes: str | None = None,
        personal_facts: list[str] | None = None,
        last_active_at: str | None = None,
        interaction_increment: int = 0,
    ) -> ClientProfileRecord:
        current = self._get_client_profile_in_connection(connection, client_id=client_id)
        merged_topics = current.watchlist_topics
        if watchlist_topics:
            merged_topics = sorted(set(current.watchlist_topics).union(watchlist_topics))
        merged_asset_focus = current.asset_focus
        if asset_focus:
            merged_asset_focus = sorted(set(current.asset_focus).union(asset_focus))
        merged_market_focus = current.market_focus
        if market_focus:
            merged_market_focus = sorted(set(current.market_focus).union(market_focus))
        merged_personal_facts = current.personal_facts
        if personal_facts:
            # Dedup by last occurrence so re-mentioned facts refresh recency.
            combined = [*current.personal_facts, *personal_facts]
            seen: set[str] = set()
            deduped: list[str] = []
            for item in reversed(combined):
                if item not in seen:
                    seen.add(item)
                    deduped.append(item)
            deduped.reverse()
            merged_personal_facts = deduped[-20:]
        next_language = preferred_language if preferred_language is not None else current.preferred_language
        next_response_style = response_style if response_style is not None else current.response_style
        next_risk_appetite = risk_appetite if risk_appetite is not None else current.risk_appetite
        next_investment_horizon = (
            investment_horizon if investment_horizon is not None else current.investment_horizon
        )
        next_institution_type = institution_type if institution_type is not None else current.institution_type
        next_risk_preference = risk_preference if risk_preference is not None else current.risk_preference
        next_expertise_level = expertise_level if expertise_level is not None else current.expertise_level
        next_activity = activity if activity is not None else current.activity
        next_current_mood = current_mood if current_mood is not None else current.current_mood
        next_emotional_trend = emotional_trend if emotional_trend is not None else current.emotional_trend
        next_stress_level = stress_level if stress_level is not None else current.stress_level
        next_confidence = confidence if confidence is not None else current.confidence
        next_notes = notes if notes is not None else current.notes
        next_last_active = last_active_at if last_active_at is not None else current.last_active_at
        updated_at = utc_now().isoformat()
        total_interactions = current.total_interactions + interaction_increment
        connection.execute(
            """
            INSERT INTO client_profiles (
                client_id,
                preferred_language,
                watchlist_topics_json,
                response_style,
                risk_appetite,
                investment_horizon,
                institution_type,
                risk_preference,
                asset_focus_json,
                market_focus_json,
                expertise_level,
                activity,
                current_mood,
                emotional_trend,
                stress_level,
                confidence,
                notes,
                personal_facts_json,
                last_active_at,
                total_interactions,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_id) DO UPDATE SET
                preferred_language = excluded.preferred_language,
                watchlist_topics_json = excluded.watchlist_topics_json,
                response_style = excluded.response_style,
                risk_appetite = excluded.risk_appetite,
                investment_horizon = excluded.investment_horizon,
                institution_type = excluded.institution_type,
                risk_preference = excluded.risk_preference,
                asset_focus_json = excluded.asset_focus_json,
                market_focus_json = excluded.market_focus_json,
                expertise_level = excluded.expertise_level,
                activity = excluded.activity,
                current_mood = excluded.current_mood,
                emotional_trend = excluded.emotional_trend,
                stress_level = excluded.stress_level,
                confidence = excluded.confidence,
                notes = excluded.notes,
                personal_facts_json = excluded.personal_facts_json,
                last_active_at = excluded.last_active_at,
                total_interactions = excluded.total_interactions,
                updated_at = excluded.updated_at
            """,
            (
                client_id,
                next_language,
                json.dumps(merged_topics, ensure_ascii=False, sort_keys=True),
                next_response_style,
                next_risk_appetite,
                next_investment_horizon,
                next_institution_type,
                next_risk_preference,
                json.dumps(merged_asset_focus, ensure_ascii=False, sort_keys=True),
                json.dumps(merged_market_focus, ensure_ascii=False, sort_keys=True),
                next_expertise_level,
                next_activity,
                next_current_mood,
                next_emotional_trend,
                next_stress_level,
                next_confidence,
                next_notes,
                json.dumps(merged_personal_facts, ensure_ascii=False, sort_keys=True),
                next_last_active,
                total_interactions,
                updated_at,
            ),
        )
        return ClientProfileRecord(
            client_id=client_id,
            preferred_language=next_language,
            watchlist_topics=merged_topics,
            response_style=next_response_style,
            risk_appetite=next_risk_appetite,
            investment_horizon=next_investment_horizon,
            institution_type=next_institution_type,
            risk_preference=next_risk_preference,
            asset_focus=merged_asset_focus,
            market_focus=merged_market_focus,
            expertise_level=next_expertise_level,
            activity=next_activity,
            current_mood=next_current_mood,
            emotional_trend=next_emotional_trend,
            stress_level=next_stress_level,
            confidence=next_confidence,
            notes=next_notes,
            personal_facts=merged_personal_facts,
            last_active_at=next_last_active,
            total_interactions=total_interactions,
            updated_at=updated_at,
        )

    def _ensure_conversation_thread_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        client_id: str,
        channel: str,
        thread_id: str,
        timestamp: str | None = None,
    ) -> None:
        active_at = timestamp or utc_now().isoformat()
        connection.execute(
            """
            INSERT INTO conversation_threads (
                client_id,
                channel,
                thread_id,
                opened_at,
                last_active_at,
                status
            ) VALUES (?, ?, ?, ?, ?, 'active')
            ON CONFLICT(client_id, channel, thread_id) DO UPDATE SET
                last_active_at = excluded.last_active_at,
                status = 'active'
            """,
            (client_id, channel, thread_id, active_at, active_at),
        )

    def _search_terms(self, query: str) -> list[str]:
        terms: list[str] = []
        for token in re.findall(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]+", query):
            cleaned = token.strip()
            if len(cleaned) < 2:
                continue
            normalized = cleaned.casefold()
            terms.append(normalized)
            if re.fullmatch(r"[\u4e00-\u9fff]+", cleaned) and len(cleaned) > 2:
                terms.extend(cleaned[index : index + 2] for index in range(len(cleaned) - 1))
        if not terms and query.strip():
            fallback = query.casefold().strip()
            if len(fallback) >= 2:
                terms.append(fallback)
        return list(dict.fromkeys(terms))

    def _score_text_match(self, haystack: str, terms: list[str]) -> float:
        if not terms:
            return 0.0
        normalized = haystack.casefold()
        score = 0.0
        for term in terms:
            score += float(normalized.count(term))
        return score

    # ------------------------------------------------------------------ #
    #  Portfolio holdings                                                  #
    # ------------------------------------------------------------------ #

    def replace_portfolio_holdings(
        self,
        holdings: list[dict[str, Any]],
        portfolio_id: str = "default",
    ) -> None:
        """Replace all holdings for a portfolio (atomic swap)."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                "DELETE FROM portfolio_holdings WHERE portfolio_id = ?",
                (portfolio_id,),
            )
            for h in holdings:
                connection.execute(
                    """
                    INSERT INTO portfolio_holdings
                        (portfolio_id, symbol, name, asset_class, weight, notional, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        portfolio_id,
                        h["symbol"],
                        h["name"],
                        h["asset_class"],
                        h["weight"],
                        h["notional"],
                        now,
                    ),
                )

    def list_portfolio_holdings(
        self, portfolio_id: str = "default",
    ) -> list[dict[str, Any]]:
        """Return holdings for a portfolio as list of dicts."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT symbol, name, asset_class, weight, notional, updated_at
                FROM portfolio_holdings
                WHERE portfolio_id = ?
                ORDER BY weight DESC
                """,
                (portfolio_id,),
            ).fetchall()
        return [
            {
                "symbol": row["symbol"],
                "name": row["name"],
                "asset_class": row["asset_class"],
                "weight": row["weight"],
                "notional": row["notional"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------ #
    #  Portfolio volatility snapshots                                       #
    # ------------------------------------------------------------------ #

    def save_vol_snapshot(
        self,
        portfolio_id: str,
        snapshot_json: dict[str, Any],
    ) -> int:
        """Persist a volatility snapshot, return its id."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO portfolio_vol_snapshots (portfolio_id, snapshot_json, created_at)
                VALUES (?, ?, ?)
                """,
                (
                    portfolio_id,
                    json.dumps(snapshot_json, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def latest_vol_snapshot(
        self, portfolio_id: str = "default",
    ) -> dict[str, Any] | None:
        """Return the most recent snapshot dict, or None."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT snapshot_json FROM portfolio_vol_snapshots
                WHERE portfolio_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (portfolio_id,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["snapshot_json"])

    def list_vol_snapshots(
        self, portfolio_id: str = "default", *, limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Return recent snapshots newest-first."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT snapshot_json, created_at FROM portfolio_vol_snapshots
                WHERE portfolio_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (portfolio_id, limit),
            ).fetchall()
        return [
            {**json.loads(row["snapshot_json"]), "stored_at": row["created_at"]}
            for row in rows
        ]

    # ------------------------------------------------------------------ #
    #  Portfolio alerts                                                     #
    # ------------------------------------------------------------------ #

    def save_portfolio_alert(
        self,
        portfolio_id: str,
        alert_type: str,
        severity: str,
        message: str,
    ) -> int:
        """Persist an alert, return its id."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO portfolio_alerts
                    (portfolio_id, alert_type, severity, message, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (portfolio_id, alert_type, severity, message, now),
            )
            return int(cursor.lastrowid)

    def list_portfolio_alerts(
        self,
        portfolio_id: str = "default",
        *,
        limit: int = 20,
        unacknowledged_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return recent portfolio alerts."""
        conditions = ["portfolio_id = ?"]
        params: list[Any] = [portfolio_id]
        if unacknowledged_only:
            conditions.append("acknowledged = 0")
        where = " AND ".join(conditions)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT id, alert_type, severity, message, acknowledged, created_at
                FROM portfolio_alerts
                WHERE {where}
                ORDER BY id DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
        return [
            {
                "id": row["id"],
                "alert_type": row["alert_type"],
                "severity": row["severity"],
                "message": row["message"],
                "acknowledged": bool(row["acknowledged"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    # -- Three-layer memory: group methods -----------------------------------

    def upsert_group_profile(
        self,
        *,
        group_id: str,
        group_name: str = "",
        group_topic: str = "",
        group_notes: str = "",
        member_count: int = 0,
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_profiles (group_id, group_name, group_topic, group_notes, member_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    group_name = CASE WHEN excluded.group_name != '' THEN excluded.group_name ELSE group_profiles.group_name END,
                    group_topic = CASE WHEN excluded.group_topic != '' THEN excluded.group_topic ELSE group_profiles.group_topic END,
                    group_notes = CASE WHEN excluded.group_notes != '' THEN excluded.group_notes ELSE group_profiles.group_notes END,
                    member_count = CASE WHEN excluded.member_count > 0 THEN excluded.member_count ELSE group_profiles.member_count END,
                    updated_at = excluded.updated_at
                """,
                (group_id, group_name, group_topic, group_notes, member_count, now, now),
            )

    def get_group_profile(self, group_id: str) -> GroupProfileRecord:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM group_profiles WHERE group_id = ? LIMIT 1",
                (group_id,),
            ).fetchone()
        if row is None:
            now = utc_now().isoformat()
            return GroupProfileRecord(
                group_id=group_id,
                group_name="",
                group_topic="",
                group_notes="",
                member_count=0,
                created_at=now,
                updated_at=now,
            )
        return GroupProfileRecord(
            group_id=row["group_id"],
            group_name=row["group_name"],
            group_topic=row["group_topic"],
            group_notes=row["group_notes"],
            member_count=row["member_count"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_group_member(
        self,
        *,
        group_id: str,
        user_id: str,
        display_name: str = "",
        role_in_group: str = "",
        personality_notes: str = "",
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_members (group_id, user_id, display_name, role_in_group, personality_notes, first_seen_at, last_seen_at, message_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(group_id, user_id) DO UPDATE SET
                    display_name = CASE WHEN excluded.display_name != '' THEN excluded.display_name ELSE group_members.display_name END,
                    role_in_group = CASE WHEN excluded.role_in_group != '' THEN excluded.role_in_group ELSE group_members.role_in_group END,
                    personality_notes = CASE WHEN excluded.personality_notes != '' THEN excluded.personality_notes ELSE group_members.personality_notes END,
                    last_seen_at = excluded.last_seen_at,
                    message_count = group_members.message_count + 1
                """,
                (group_id, user_id, display_name, role_in_group, personality_notes, now, now),
            )

    def list_group_members(self, group_id: str, *, limit: int = 20) -> list[GroupMemberRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM group_members WHERE group_id = ? ORDER BY last_seen_at DESC LIMIT ?",
                (group_id, limit),
            ).fetchall()
        return [
            GroupMemberRecord(
                group_id=row["group_id"],
                user_id=row["user_id"],
                display_name=row["display_name"],
                role_in_group=row["role_in_group"],
                personality_notes=row["personality_notes"],
                first_seen_at=row["first_seen_at"],
                last_seen_at=row["last_seen_at"],
                message_count=row["message_count"],
            )
            for row in rows
        ]

    def append_group_message(
        self,
        *,
        group_id: str,
        thread_id: str = "main",
        user_id: str,
        display_name: str,
        content: str,
    ) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO group_messages (group_id, thread_id, user_id, display_name, content, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (group_id, thread_id, user_id, display_name, content, now),
            )

    def list_group_messages(
        self,
        group_id: str,
        thread_id: str = "main",
        *,
        limit: int = 30,
    ) -> list[GroupMessageRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT id, group_id, thread_id, user_id, display_name, content, created_at
                FROM group_messages
                WHERE group_id = ? AND thread_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (group_id, thread_id, limit),
            ).fetchall()
        records = [
            GroupMessageRecord(
                message_id=row["id"],
                group_id=row["group_id"],
                thread_id=row["thread_id"],
                user_id=row["user_id"],
                display_name=row["display_name"],
                content=row["content"],
                created_at=row["created_at"],
            )
            for row in rows
        ]
        records.reverse()  # chronological order
        return records

    # ── Document storage CRUD ──────────────────────────────────────────

    def upsert_doc_source(self, record: DocSourceRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO doc_source (
                    source_id, source_code, source_name, source_type,
                    country_code, default_language_code, homepage_url,
                    is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.source_id,
                    record.source_code,
                    record.source_name,
                    record.source_type,
                    record.country_code,
                    record.default_language_code,
                    record.homepage_url,
                    int(record.is_active),
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_doc_source(self, source_id: str) -> DocSourceRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM doc_source WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_doc_source(row)

    def list_doc_sources(self, *, active_only: bool = True) -> list[DocSourceRecord]:
        query = "SELECT * FROM doc_source"
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY source_id"
        with self._connection(commit=False) as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_doc_source(row) for row in rows]

    def _row_to_doc_source(self, row: sqlite3.Row) -> DocSourceRecord:
        return DocSourceRecord(
            source_id=row["source_id"],
            source_code=row["source_code"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            country_code=row["country_code"],
            default_language_code=row["default_language_code"] or "",
            homepage_url=row["homepage_url"] or "",
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_doc_release_family(self, record: DocReleaseFamilyRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO doc_release_family (
                    release_family_id, source_id, release_code, release_name,
                    topic_code, country_code, frequency, default_language_code,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.release_family_id,
                    record.source_id,
                    record.release_code,
                    record.release_name,
                    record.topic_code,
                    record.country_code,
                    record.frequency,
                    record.default_language_code,
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_doc_release_family(self, release_family_id: str) -> DocReleaseFamilyRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM doc_release_family WHERE release_family_id = ?",
                (release_family_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_doc_release_family(row)

    def list_doc_release_families(
        self,
        *,
        source_id: str | None = None,
        country_code: str | None = None,
        topic_code: str | None = None,
    ) -> list[DocReleaseFamilyRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic_code:
            conditions.append("topic_code = ?")
            params.append(topic_code)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM doc_release_family {where} ORDER BY release_family_id",
                params,
            ).fetchall()
        return [self._row_to_doc_release_family(row) for row in rows]

    def _row_to_doc_release_family(self, row: sqlite3.Row) -> DocReleaseFamilyRecord:
        return DocReleaseFamilyRecord(
            release_family_id=row["release_family_id"],
            source_id=row["source_id"],
            release_code=row["release_code"],
            release_name=row["release_name"],
            topic_code=row["topic_code"],
            country_code=row["country_code"],
            frequency=row["frequency"] or "",
            default_language_code=row["default_language_code"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_document(self, record: DocumentRecord) -> None:
        published_precision = record.published_precision or _infer_timestamp_precision(
            record.published_at or record.published_date
        )
        if record.published_at:
            if published_precision == "exact":
                published_at = _safe_utc_iso(record.published_at)
            else:
                published_at = record.published_at[:10]
        elif record.published_date:
            if published_precision == "exact":
                published_at = _safe_utc_iso(record.published_date)
            else:
                published_at = record.published_date
        else:
            published_at = ""
        published_epoch_ms = record.published_epoch_ms or _safe_epoch_ms(published_at or record.published_date)
        created_epoch_ms = record.created_epoch_ms or _safe_epoch_ms(record.created_at)
        updated_epoch_ms = record.updated_epoch_ms or _safe_epoch_ms(record.updated_at)
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document (
                    document_id, release_family_id, source_id, canonical_url,
                    title, subtitle, document_type, mime_type,
                    language_code, country_code, topic_code,
                    published_date, published_at, published_precision, published_epoch_ms, status, version_no,
                    parent_document_id, hash_sha256,
                    created_at, updated_at, created_epoch_ms, updated_epoch_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.document_id,
                    record.release_family_id or None,
                    record.source_id,
                    record.canonical_url,
                    record.title,
                    record.subtitle,
                    record.document_type,
                    record.mime_type,
                    record.language_code,
                    record.country_code,
                    record.topic_code,
                    record.published_date,
                    published_at or None,
                    published_precision,
                    published_epoch_ms,
                    record.status,
                    record.version_no,
                    record.parent_document_id or None,
                    record.hash_sha256 or None,
                    record.created_at,
                    record.updated_at,
                    created_epoch_ms,
                    updated_epoch_ms,
                ),
            )

    def get_document(self, document_id: str) -> DocumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document WHERE document_id = ?",
                (document_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document(row)

    def get_document_by_url(self, canonical_url: str) -> DocumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document WHERE canonical_url = ?",
                (canonical_url,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document(row)

    def document_exists(self, canonical_url: str) -> bool:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM document WHERE canonical_url = ? LIMIT 1",
                (canonical_url,),
            ).fetchone()
        return row is not None

    def list_documents(
        self,
        *,
        source_id: str | None = None,
        release_family_id: str | None = None,
        country_code: str | None = None,
        topic_code: str | None = None,
        status: str | None = None,
        document_type: str | None = None,
        limit: int = 50,
        days: int | None = None,
    ) -> list[DocumentRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if release_family_id:
            conditions.append("release_family_id = ?")
            params.append(release_family_id)
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic_code:
            conditions.append("topic_code = ?")
            params.append(topic_code)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if document_type:
            conditions.append("document_type = ?")
            params.append(document_type)
        if days is not None:
            cutoff = (date.today() - timedelta(days=days)).isoformat()
            cutoff_dt = datetime.fromisoformat(cutoff).replace(tzinfo=timezone.utc)
            cutoff_epoch_ms = int(cutoff_dt.timestamp() * 1000)
            conditions.append("(published_epoch_ms >= ? OR published_date >= ?)")
            params.extend([cutoff_epoch_ms, cutoff])
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM document
                {where}
                ORDER BY published_epoch_ms DESC, published_date DESC, document_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_document(row) for row in rows]

    def _row_to_document(self, row: sqlite3.Row) -> DocumentRecord:
        published_precision = row["published_precision"] or _infer_timestamp_precision(
            row["published_at"] or row["published_date"]
        )
        published_at = row["published_at"] or (
            _safe_utc_iso(row["published_date"]) if published_precision == "exact" else row["published_date"]
        )
        return DocumentRecord(
            document_id=row["document_id"],
            release_family_id=row["release_family_id"] or "",
            source_id=row["source_id"],
            canonical_url=row["canonical_url"],
            title=row["title"],
            subtitle=row["subtitle"] or "",
            document_type=row["document_type"],
            mime_type=row["mime_type"],
            language_code=row["language_code"],
            country_code=row["country_code"],
            topic_code=row["topic_code"],
            published_date=row["published_date"],
            published_at=published_at,
            published_precision=published_precision,
            status=row["status"],
            version_no=int(row["version_no"]),
            parent_document_id=row["parent_document_id"] or "",
            hash_sha256=row["hash_sha256"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            published_epoch_ms=(
                int(row["published_epoch_ms"])
                if row["published_epoch_ms"]
                else _safe_epoch_ms(row["published_at"] or row["published_date"])
            ),
            created_epoch_ms=(
                int(row["created_epoch_ms"])
                if row["created_epoch_ms"]
                else _safe_epoch_ms(row["created_at"])
            ),
            updated_epoch_ms=(
                int(row["updated_epoch_ms"])
                if row["updated_epoch_ms"]
                else _safe_epoch_ms(row["updated_at"])
            ),
        )

    def upsert_document_blob(self, record: DocumentBlobRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document_blob (
                    document_blob_id, document_id, blob_role,
                    storage_path, content_text, content_bytes,
                    byte_size, encoding, parser_name, parser_version,
                    extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.document_blob_id,
                    record.document_id,
                    record.blob_role,
                    record.storage_path or None,
                    record.content_text or None,
                    record.content_bytes,
                    record.byte_size,
                    record.encoding or None,
                    record.parser_name or None,
                    record.parser_version or None,
                    record.extracted_at or None,
                ),
            )

    def get_document_blob(
        self,
        document_id: str,
        blob_role: str,
    ) -> DocumentBlobRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document_blob WHERE document_id = ? AND blob_role = ?",
                (document_id, blob_role),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document_blob(row)

    def list_document_blobs(self, document_id: str) -> list[DocumentBlobRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM document_blob WHERE document_id = ? ORDER BY blob_role",
                (document_id,),
            ).fetchall()
        return [self._row_to_document_blob(row) for row in rows]

    def _row_to_document_blob(self, row: sqlite3.Row) -> DocumentBlobRecord:
        return DocumentBlobRecord(
            document_blob_id=row["document_blob_id"],
            document_id=row["document_id"],
            blob_role=row["blob_role"],
            storage_path=row["storage_path"] or "",
            content_text=row["content_text"] or "",
            content_bytes=row["content_bytes"],
            byte_size=int(row["byte_size"]) if row["byte_size"] is not None else 0,
            encoding=row["encoding"] or "",
            parser_name=row["parser_name"] or "",
            parser_version=row["parser_version"] or "",
            extracted_at=row["extracted_at"] or "",
        )

    def upsert_document_extra(self, record: DocumentExtraRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document_extra (
                    document_id, extra_json
                ) VALUES (?, ?)
                """,
                (
                    record.document_id,
                    json.dumps(record.extra_json, ensure_ascii=False, sort_keys=True),
                ),
            )

    def get_document_extra(self, document_id: str) -> DocumentExtraRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document_extra WHERE document_id = ?",
                (document_id,),
            ).fetchone()
        if row is None:
            return None
        return DocumentExtraRecord(
            document_id=row["document_id"],
            extra_json=json.loads(row["extra_json"]),
        )

    def seed_doc_sources_and_families(self, source_configs: dict[str, dict[str, dict[str, Any]]]) -> None:
        """Populate doc_source and doc_release_family from scraper config dicts.

        Args:
            source_configs: Mapping of region label to source_id→config dicts,
                e.g. {"us": {"us_bls_cpi": {...}, ...}, "cn": {...}}.
        """
        now = utc_now().isoformat()
        seen_sources: dict[str, DocSourceRecord] = {}

        for _region, sources in source_configs.items():
            for source_id, cfg in sources.items():
                institution = cfg.get("institution", "")
                country = cfg.get("country", "")
                language = cfg.get("language", "en")

                # Derive source-level key: e.g. "us.bls" from "us_bls_cpi"
                parts = source_id.split("_")
                source_key = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else source_id

                if source_key not in seen_sources:
                    source_type = self._infer_source_type(institution)
                    homepage = cfg.get("url", "")
                    seen_sources[source_key] = DocSourceRecord(
                        source_id=source_key,
                        source_code=parts[1] if len(parts) >= 2 else source_id,
                        source_name=institution,
                        source_type=source_type,
                        country_code=country,
                        default_language_code=language,
                        homepage_url=homepage,
                        is_active=True,
                        created_at=now,
                        updated_at=now,
                    )
                    self.upsert_doc_source(seen_sources[source_key])

                # Release family
                release_code = "_".join(parts[2:]) if len(parts) > 2 else parts[-1]
                data_category = cfg.get("data_category", "")
                frequency = self._infer_frequency(data_category)

                family = DocReleaseFamilyRecord(
                    release_family_id=source_id.replace("_", "."),
                    source_id=source_key,
                    release_code=release_code,
                    release_name=cfg.get("data_category", release_code).replace("_", " ").title(),
                    topic_code=data_category,
                    country_code=country,
                    frequency=frequency,
                    default_language_code=language,
                    created_at=now,
                    updated_at=now,
                )
                self.upsert_doc_release_family(family)

    @staticmethod
    def _infer_source_type(institution: str) -> str:
        lower = institution.lower()
        central_banks = [
            "federal reserve", "pboc", "人民银行", "bank of japan", "boj",
            "ecb", "bank of england",
        ]
        if any(cb in lower for cb in central_banks):
            return "central_bank"
        stats = ["统计局", "eurostat", "census", "cabinet office"]
        if any(s in lower for s in stats):
            return "statistics_bureau"
        intl = ["imf", "world bank", "oecd", "s&p global", "caixin"]
        if any(i in lower for i in intl):
            return "intl_org"
        return "government_agency"

    @staticmethod
    def _infer_frequency(data_category: str) -> str:
        monthly = [
            "inflation", "employment", "consumption", "trade",
            "industrial_production", "monetary", "interest_rate",
            "money_supply", "fx_reserves", "fiscal_policy",
            "bond_issuance", "capital_flows", "housing",
            "consumer_sentiment", "manufacturing",
        ]
        if data_category in monthly:
            return "monthly"
        if data_category in ("gdp", "investment"):
            return "quarterly"
        if data_category in ("monetary_policy", "economic_conditions"):
            return "irregular"
        return "irregular"

    # ── Observation family CRUD ────────────────────────────────────────

    def upsert_obs_source(self, record: ObsSourceRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO obs_source (
                    source_id, source_code, source_name, source_type,
                    country_code, homepage_url, api_base_url,
                    is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.source_id,
                    record.source_code,
                    record.source_name,
                    record.source_type,
                    record.country_code,
                    record.homepage_url,
                    record.api_base_url,
                    int(record.is_active),
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_obs_source(self, source_id: str) -> ObsSourceRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM obs_source WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_obs_source(row)

    def list_obs_sources(self, *, active_only: bool = True) -> list[ObsSourceRecord]:
        query = "SELECT * FROM obs_source"
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY source_id"
        with self._connection(commit=False) as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_obs_source(row) for row in rows]

    def _row_to_obs_source(self, row: sqlite3.Row) -> ObsSourceRecord:
        return ObsSourceRecord(
            source_id=row["source_id"],
            source_code=row["source_code"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            country_code=row["country_code"],
            homepage_url=row["homepage_url"] or "",
            api_base_url=row["api_base_url"] or "",
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_obs_family(self, record: ObsFamilyRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO obs_family (
                    family_id, source_id, provider_series_id, canonical_name,
                    short_name, unit, frequency, seasonal_adjustment,
                    country_code, topic_code, category,
                    is_active, has_vintages, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.family_id,
                    record.source_id,
                    record.provider_series_id,
                    record.canonical_name,
                    record.short_name,
                    record.unit,
                    record.frequency,
                    record.seasonal_adjustment,
                    record.country_code,
                    record.topic_code,
                    record.category,
                    int(record.is_active),
                    int(record.has_vintages),
                    json.dumps(record.metadata, ensure_ascii=False, sort_keys=True),
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_obs_family(self, family_id: str) -> ObsFamilyRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM obs_family WHERE family_id = ?",
                (family_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_obs_family(row)

    def get_obs_family_by_series(
        self, source_id: str, provider_series_id: str,
    ) -> ObsFamilyRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM obs_family WHERE source_id = ? AND provider_series_id = ?",
                (source_id, provider_series_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_obs_family(row)

    def list_obs_families(
        self,
        *,
        source_id: str | None = None,
        country_code: str | None = None,
        topic_code: str | None = None,
        frequency: str | None = None,
        active_only: bool = True,
    ) -> list[ObsFamilyRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if active_only:
            conditions.append("is_active = 1")
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic_code:
            conditions.append("topic_code = ?")
            params.append(topic_code)
        if frequency:
            conditions.append("frequency = ?")
            params.append(frequency)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM obs_family {where} ORDER BY family_id",
                params,
            ).fetchall()
        return [self._row_to_obs_family(row) for row in rows]

    def _row_to_obs_family(self, row: sqlite3.Row) -> ObsFamilyRecord:
        return ObsFamilyRecord(
            family_id=row["family_id"],
            source_id=row["source_id"],
            provider_series_id=row["provider_series_id"],
            canonical_name=row["canonical_name"],
            short_name=row["short_name"] or "",
            unit=row["unit"] or "",
            frequency=row["frequency"] or "irregular",
            seasonal_adjustment=row["seasonal_adjustment"] or "none",
            country_code=row["country_code"],
            topic_code=row["topic_code"] or "",
            category=row["category"] or "",
            is_active=bool(row["is_active"]),
            has_vintages=bool(row["has_vintages"]),
            metadata=json.loads(row["metadata_json"]) if row["metadata_json"] else {},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_obs_family_document(self, record: ObsFamilyDocumentRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO obs_family_document (
                    family_id, release_family_id, relationship, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    record.family_id,
                    record.release_family_id,
                    record.relationship,
                    record.created_at,
                ),
            )

    def list_obs_families_for_release(
        self, release_family_id: str,
    ) -> list[ObsFamilyRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT f.* FROM obs_family f
                JOIN obs_family_document d ON f.family_id = d.family_id
                WHERE d.release_family_id = ?
                ORDER BY f.family_id
                """,
                (release_family_id,),
            ).fetchall()
        return [self._row_to_obs_family(row) for row in rows]

    def list_releases_for_obs_family(
        self, family_id: str,
    ) -> list[DocReleaseFamilyRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT r.* FROM doc_release_family r
                JOIN obs_family_document d ON r.release_family_id = d.release_family_id
                WHERE d.family_id = ?
                ORDER BY r.release_family_id
                """,
                (family_id,),
            ).fetchall()
        return [self._row_to_doc_release_family(row) for row in rows]

    def list_release_families_for_indicator(
        self, indicator_id: str,
    ) -> list[DocReleaseFamilyRecord]:
        indicator = self.get_calendar_indicator(indicator_id)
        if indicator is None or not indicator.obs_family_id:
            return []
        return self.list_releases_for_obs_family(indicator.obs_family_id)

    # ── Observation family seed & backfill ─────────────────────────────

    def seed_obs_sources_and_families(self) -> None:
        """Populate obs_source, obs_family, and obs_family_document tables
        from the module-level seed data constants."""
        now = utc_now().isoformat()

        # 1. Seed obs_source entries
        for src_id, code, name, stype, country, homepage, api_url in _OBS_SOURCE_DEFS:
            self.upsert_obs_source(ObsSourceRecord(
                source_id=src_id,
                source_code=code,
                source_name=name,
                source_type=stype,
                country_code=country,
                homepage_url=homepage,
                api_base_url=api_url,
                is_active=True,
                created_at=now,
                updated_at=now,
            ))

        # 2. Seed obs_family entries from all maps
        source_maps: list[tuple[str, dict[str, tuple[str, str, str, str, str]]]] = [
            ("fred", _FRED_FAMILY_MAP),
            ("eia", _EIA_FAMILY_MAP),
            ("treasury_fiscal", _TREASURY_FAMILY_MAP),
            ("nyfed", _NYFED_FAMILY_MAP),
            ("imf", _IMF_FAMILY_MAP),
            ("eurostat", _EUROSTAT_FAMILY_MAP),
            ("bis", _BIS_FAMILY_MAP),
            ("ecb", _ECB_FAMILY_MAP),
            ("oecd", _OECD_FAMILY_MAP),
            ("worldbank", _WORLDBANK_FAMILY_MAP),
            ("bls", _BLS_FAMILY_MAP),
        ]
        for source_id, family_map in source_maps:
            for series_id, (fam_id, canon_name, unit, freq, sa) in family_map.items():
                parts = fam_id.split(".")
                topic = parts[1] if len(parts) > 1 else ""
                category = parts[2] if len(parts) > 2 else ""
                self.upsert_obs_family(ObsFamilyRecord(
                    family_id=fam_id,
                    source_id=source_id,
                    provider_series_id=series_id,
                    canonical_name=canon_name,
                    short_name="",
                    unit=unit,
                    frequency=freq,
                    seasonal_adjustment=sa,
                    country_code=parts[0].upper() if parts else "US",
                    topic_code=topic,
                    category=category,
                    is_active=True,
                    has_vintages=series_id in _VINTAGE_FAMILY_IDS,
                    created_at=now,
                    updated_at=now,
                ))

        # 3. Seed obs_family_document links (only if both sides exist)
        for fam_id, rel_fam_id, relationship in _OBS_DOC_LINKS:
            if self.get_obs_family(fam_id) and self.get_doc_release_family(rel_fam_id):
                self.upsert_obs_family_document(ObsFamilyDocumentRecord(
                    family_id=fam_id,
                    release_family_id=rel_fam_id,
                    relationship=relationship,
                    created_at=now,
                ))

    def seed_structural_ontology(self) -> None:
        """Populate deterministic macro structure tables needed for ontology queries."""
        from ingestion.scrapers.gov_report import (
            _CN_SOURCES,
            _EU_SOURCES,
            _JP_SOURCES,
            _US_SOURCES,
        )

        self.seed_doc_sources_and_families({
            "us": _US_SOURCES,
            "cn": _CN_SOURCES,
            "jp": _JP_SOURCES,
            "eu": _EU_SOURCES,
        })
        self.seed_obs_sources_and_families()
        self.seed_calendar_indicators()

    def backfill_obs_family_ids(self) -> int:
        """Set obs_family_id on existing indicators/vintages rows from obs_family table.
        Returns total number of rows updated."""
        with self._connection(commit=True) as connection:
            cur1 = connection.execute(
                """
                UPDATE indicators SET obs_family_id = (
                    SELECT family_id FROM obs_family
                    WHERE obs_family.provider_series_id = indicators.series_id
                      AND obs_family.source_id = indicators.source
                ) WHERE obs_family_id IS NULL
                """
            )
            cur2 = connection.execute(
                """
                UPDATE indicator_vintages SET obs_family_id = (
                    SELECT family_id FROM obs_family
                    WHERE obs_family.provider_series_id = indicator_vintages.series_id
                      AND obs_family.source_id = indicator_vintages.source
                ) WHERE obs_family_id IS NULL
                """
            )
        return (cur1.rowcount or 0) + (cur2.rowcount or 0)

    def build_obs_family_lookup(self) -> dict[tuple[str, str], str]:
        """Build a lookup dict mapping (source_id, provider_series_id) -> family_id."""
        families = self.list_obs_families(active_only=False)
        return {(f.source_id, f.provider_series_id): f.family_id for f in families}

    # ── Cross-source concept map ─────────────────────────────────────

    _CONCEPT_MAP_DEFS: list[tuple[str, str, str, str, int, str, str]] = [
        # (concept_id, source_id, provider_series_id, obs_family_id, priority, role, notes)
        #
        # ── US Inflation ─────────────────────────────────────────────
        ("CPI_US",              "bls",            "CUUR0000SA0",    "us.inflation.cpi_bls",          1, "primary",     "NSA, all urban"),
        ("CPI_US",              "fred",           "CPIAUCSL",       "us.inflation.cpi_all",          2, "secondary",   "SA, all urban"),
        ("CORE_CPI_US",         "bls",            "CUUR0000SA0L1E", "us.inflation.cpi_core_bls",     1, "primary",     "NSA, less food & energy"),
        ("CORE_CPI_US",         "fred",           "CPILFESL",       "us.inflation.cpi_core",         2, "secondary",   "SA, less food & energy"),
        ("CORE_PCE_US",         "fred",           "PCEPILFE",       "us.inflation.pce_core",         1, "primary",     "SA, Fed preferred gauge"),
        ("BREAKEVEN_5Y_US",     "fred",           "T5YIE",          "us.inflation.breakeven_5y",     1, "primary",     "TIPS-derived 5Y"),
        ("BREAKEVEN_10Y_US",    "fred",           "T10YIE",         "us.inflation.breakeven_10y",    1, "primary",     "TIPS-derived 10Y"),
        ("CPI_FOOD_US",         "bls",            "CUUR0000SAF1",   "us.inflation.cpi_food_bls",     1, "primary",     "NSA, food"),
        ("CPI_ENERGY_US",       "bls",            "CUUR0000SA0E",   "us.inflation.cpi_energy_bls",   1, "primary",     "NSA, energy"),
        ("CPI_SHELTER_US",      "bls",            "CUUR0000SAH1",   "us.inflation.cpi_shelter_bls",  1, "primary",     "NSA, shelter"),
        ("PPI_US",              "bls",            "WPSFD4",         "us.inflation.ppi_final_demand_bls", 1, "primary",  "NSA, final demand"),
        ("PPI_CORE_US",         "bls",            "WPSFD49116",     "us.inflation.ppi_core_bls",     1, "primary",     "NSA, core"),
        #
        # ── US Employment ────────────────────────────────────────────
        ("UNEMP_US",            "bls",            "LNS14000000",    "us.employment.unemployment_bls",1, "primary",     "SA, BLS CPS"),
        ("UNEMP_US",            "fred",           "UNRATE",         "us.employment.unemployment",    2, "secondary",   "SA, BLS CPS"),
        ("UNEMP_US",            "oecd",           "OECD_UNEMP_US",  "us.employment.unemployment_oecd",3,"cross_check","OECD KEI"),
        ("NFP_US",              "bls",            "CES0000000001",  "us.employment.nfp_bls",         1, "primary",     "SA, BLS CES"),
        ("NFP_US",              "fred",           "PAYEMS",         "us.employment.nonfarm_payrolls",2, "secondary",   "SA, BLS CES"),
        ("NFP_PRIVATE_US",      "bls",            "CES0500000001",  "us.employment.nfp_private_bls", 1, "primary",     "SA, private sector"),
        ("AVG_HOURLY_EARN_US",  "bls",            "CES0500000003",  "us.employment.avg_hourly_earnings_bls", 1, "primary", "SA, private"),
        ("AVG_WEEKLY_HOURS_US", "bls",            "CES0500000002",  "us.employment.avg_weekly_hours_bls", 1, "primary", "SA, private"),
        ("LFPR_US",             "bls",            "LNS11300000",    "us.employment.lfpr_bls",        1, "primary",     "SA, BLS CPS"),
        ("JOLTS_OPENINGS_US",   "bls",            "JTS000000000000000JOL", "us.employment.jolts_openings_bls", 1, "primary", "SA"),
        ("JOLTS_HIRES_US",      "bls",            "JTS000000000000000HIL", "us.employment.jolts_hires_bls",    1, "primary", "SA"),
        ("JOLTS_QUITS_US",      "bls",            "JTS000000000000000QUL", "us.employment.jolts_quits_bls",    1, "primary", "SA"),
        ("ECI_US",              "bls",            "CIU1010000000000A",     "us.employment.eci_total_bls",      1, "primary", "SA, quarterly"),
        ("INITIAL_CLAIMS_US",   "fred",           "ICSA",           "us.employment.initial_claims",  1, "primary",     "SA, weekly"),
        ("CONTINUING_CLAIMS_US","fred",           "CCSA",           "us.employment.continuing_claims",1,"primary",    "SA, weekly"),
        #
        # ── US Productivity ──────────────────────────────────────────
        ("PRODUCTIVITY_US",     "bls",            "PRS85006092",    "us.productivity.nfb_productivity_bls", 1, "primary", "SA, NFB"),
        ("UNIT_LABOR_COST_US",  "bls",            "PRS85006112",    "us.productivity.nfb_ulc_bls",   1, "primary",     "SA, NFB"),
        #
        # ── US Growth ────────────────────────────────────────────────
        ("GDP_NOMINAL_US",      "fred",           "GDP",            "us.growth.gdp_nominal",         1, "primary",     "SAAR"),
        ("GDP_REAL_US",         "fred",           "GDPC1",          "us.growth.gdp_real",            1, "primary",     "SAAR, chained 2017$"),
        ("RETAIL_SALES_US",     "fred",           "RSAFS",          "us.growth.retail_sales",        1, "primary",     "SA"),
        ("INDPRO_US",           "fred",           "INDPRO",         "us.growth.industrial_production",1,"primary",    "SA, index"),
        ("GDP_GROWTH_WB_US",    "worldbank",      "WB_GDP_GROWTH_US","us.growth.gdp_growth_wb",      1, "primary",     "Annual % growth"),
        #
        # ── US Rates ─────────────────────────────────────────────────
        ("POLICY_RATE_US",      "nyfed",          "NYFED_EFFR",     "us.rates.effr",                 1, "primary",     "NY Fed EFFR"),
        ("POLICY_RATE_US",      "fred",           "DFF",            "us.rates.fed_funds",            2, "secondary",   "Daily effective rate"),
        ("POLICY_RATE_US",      "bis",            "BIS_POLICY_US",  "us.rates.policy_bis",           3, "cross_check", "BIS central bank policy"),
        ("SOFR_US",             "nyfed",          "NYFED_SOFR",     "us.rates.sofr",                 1, "primary",     "Secured overnight"),
        ("OBFR_US",             "nyfed",          "NYFED_OBFR",     "us.rates.obfr",                 1, "primary",     "Overnight bank funding"),
        ("TREASURY_2Y_US",      "fred",           "DGS2",           "us.rates.treasury_2y",          1, "primary",     "Daily constant maturity"),
        ("TREASURY_10Y_US",     "fred",           "DGS10",          "us.rates.treasury_10y",         1, "primary",     "Daily constant maturity"),
        ("TREASURY_30Y_US",     "fred",           "DGS30",          "us.rates.treasury_30y",         1, "primary",     "Daily constant maturity"),
        ("REAL_YIELD_10Y_US",   "fred",           "DFII10",         "us.rates.real_yield_10y",       1, "primary",     "TIPS-derived"),
        ("SPREAD_10Y2Y_US",     "fred",           "T10Y2Y",         "us.rates.spread_10y2y",         1, "primary",     "Yield curve slope"),
        #
        # ── US Liquidity ─────────────────────────────────────────────
        ("FED_BALANCE_SHEET_US","fred",           "WALCL",          "us.liquidity.fed_balance_sheet",1,"primary",     "Weekly total assets"),
        ("M2_US",               "fred",           "M2SL",           "us.liquidity.m2",               1, "primary",     "SA"),
        ("REVERSE_REPO_US",     "fred",           "RRPONTSYD",      "us.liquidity.reverse_repo",     1, "primary",     "Daily ON RRP"),
        ("TGA_US",              "fred",           "WTREGEN",        "us.liquidity.tga",              1, "primary",     "Weekly TGA balance"),
        ("TGA_US",              "treasury_fiscal","TREAS_TGA_BALANCE","us.fiscal.tga_balance",        2, "cross_check", "Treasury daily TGA"),
        #
        # ── US FX ────────────────────────────────────────────────────
        ("DOLLAR_INDEX_US",     "fred",           "DTWEXBGS",       "us.fx.dollar_index_broad",      1, "primary",     "Broad trade-weighted"),
        ("DOLLAR_INDEX_US",     "bis",            "BIS_EER_US",     "us.fx.eer_real",                2, "cross_check", "BIS real EER"),
        ("CNYUSD",              "fred",           "DEXCHUS",        "us.fx.cny_usd",                 1, "primary",     "Daily spot"),
        #
        # ── US Credit ────────────────────────────────────────────────
        ("HY_OAS_US",           "fred",           "BAMLH0A0HYM2",  "us.credit.hy_oas",              1, "primary",     "ICE BofA HY OAS"),
        ("CREDIT_GAP_US",       "bis",            "BIS_CREDIT_GAP_US","us.credit.gap",               1, "primary",     "Credit-to-GDP gap"),
        #
        # ── US Property ──────────────────────────────────────────────
        ("PROPERTY_US",         "bis",            "BIS_PROPERTY_US","us.property.real",              1, "primary",     "Real property prices"),
        #
        # ── US Fiscal ────────────────────────────────────────────────
        ("DEBT_US",             "treasury_fiscal","TREAS_DEBT_TOTAL","us.fiscal.debt_outstanding",   1, "primary",     "Daily total debt"),
        ("AVG_INTEREST_RATE_US","treasury_fiscal","TREAS_AVG_RATE", "us.fiscal.avg_interest_rate",   1, "primary",     "Monthly avg rate"),
        #
        # ── US Energy ────────────────────────────────────────────────
        ("BRENT_CRUDE",         "eia",            "EIA_BRENT",      "us.energy.brent_spot",          1, "primary",     "Daily spot"),
        ("WTI_CRUDE",           "eia",            "EIA_WTI",        "us.energy.wti_spot",            1, "primary",     "Daily spot"),
        ("CRUDE_STOCKS_US",     "eia",            "EIA_CRUDE_STOCKS","us.energy.crude_stocks",       1, "primary",     "Weekly stocks"),
        ("NATGAS_US",           "eia",            "EIA_NATGAS",     "us.energy.natgas_futures",      1, "primary",     "Henry Hub futures"),
        ("PETROLEUM_SUPPLY_US", "eia",            "EIA_PETROL_SUPPLY","us.energy.petroleum_supply",  1, "primary",     "Weekly supply"),
        #
        # ── US Trade ─────────────────────────────────────────────────
        ("EXPORTS_US",          "imf",            "IMF_GLOBAL_TRADE","us.trade.exports_fob",         1, "primary",     "Exports FOB"),
        ("CURRENT_ACCOUNT_US",  "worldbank",      "WB_CA_GDP_US",   "us.trade.current_account_gdp",  1, "primary",     "CA % of GDP, annual"),
        #
        # ── US Sentiment ─────────────────────────────────────────────
        ("CONSUMER_CONF_US",    "oecd",           "OECD_CONSUMER_CONF_US","us.sentiment.consumer_conf",1,"primary",   "OECD consumer confidence"),
        ("BUSINESS_CONF_US",    "oecd",           "OECD_BUSINESS_CONF_US","us.sentiment.business_conf",1,"primary",   "OECD business confidence"),
        ("CLI_US",              "oecd",           "OECD_CLI_US",    "us.leading.cli",                1, "primary",     "Composite leading indicator"),
        #
        # ── US Development ───────────────────────────────────────────
        ("GDP_PER_CAPITA_US",   "worldbank",      "WB_GDP_PCAP_US", "us.development.gdp_per_capita", 1, "primary",     "PPP, annual"),
        #
        # ── China ────────────────────────────────────────────────────
        ("CPI_CN",              "imf",            "IMF_CN_CPI",     "cn.inflation.cpi",              1, "primary",     "IMF SDMX CPI index"),
        ("GDP_REAL_CN",         "imf",            "IMF_CN_GDP",     "cn.growth.gdp_real",            1, "primary",     "Real GDP LCU"),
        ("FX_RESERVES_CN",      "imf",            "IMF_CN_FX_RESERVES","cn.reserves.fx",             1, "primary",     "FX reserves USD"),
        ("POLICY_RATE_CN",      "bis",            "BIS_POLICY_CN",  "cn.rates.policy_bis",           1, "primary",     "PBOC policy rate"),
        ("CREDIT_GAP_CN",       "bis",            "BIS_CREDIT_GAP_CN","cn.credit.gap",               1, "primary",     "Credit-to-GDP gap"),
        ("PROPERTY_CN",         "bis",            "BIS_PROPERTY_CN","cn.property.real",              1, "primary",     "Real property prices"),
        ("EER_CN",              "bis",            "BIS_EER_CN",     "cn.fx.eer_real",                1, "primary",     "Real effective exchange rate"),
        ("CLI_CN",              "oecd",           "OECD_CLI_CN",    "cn.leading.cli",                1, "primary",     "Composite leading indicator"),
        ("GDP_PER_CAPITA_CN",   "worldbank",      "WB_GDP_PCAP_CN", "cn.development.gdp_per_capita", 1, "primary",     "PPP, annual"),
        #
        # ── Japan ────────────────────────────────────────────────────
        ("CPI_JP",              "imf",            "IMF_JP_CPI",     "jp.inflation.cpi",              1, "primary",     "IMF SDMX CPI index"),
        ("GDP_REAL_JP",         "imf",            "IMF_JP_GDP",     "jp.growth.gdp_real",            1, "primary",     "Real GDP LCU"),
        ("POLICY_RATE_JP",      "bis",            "BIS_POLICY_JP",  "jp.rates.policy_bis",           1, "primary",     "BOJ policy rate"),
        ("CLI_JP",              "oecd",           "OECD_CLI_JP",    "jp.leading.cli",                1, "primary",     "Composite leading indicator"),
        #
        # ── Euro Area ────────────────────────────────────────────────
        ("CPI_EU",              "eurostat",       "ESTAT_HICP",     "eu.inflation.hicp",             1, "primary",     "Eurostat HICP YoY"),
        ("CPI_EU",              "imf",            "IMF_EU_CPI",     "eu.inflation.cpi_imf",          2, "secondary",   "IMF SDMX HICP"),
        ("GDP_EU",              "eurostat",       "ESTAT_GDP",      "eu.growth.gdp_qoq",            1, "primary",     "GDP QoQ SA"),
        ("UNEMP_EU",            "eurostat",       "ESTAT_UNEMPLOYMENT","eu.employment.unemployment", 1, "primary",     "SA"),
        ("INDPRO_EU",           "eurostat",       "ESTAT_INDPRO",   "eu.growth.industrial_production",1,"primary",    "MoM SA"),
        ("ESI_EU",              "eurostat",       "ESTAT_ESI",      "eu.sentiment.esi",              1, "primary",     "Economic sentiment"),
        ("POLICY_RATE_EU",      "ecb",            "ECB_EA_DEPOSIT_RATE","eu.rates.deposit_ecb",      1, "primary",     "Deposit facility rate"),
        ("POLICY_RATE_EU",      "bis",            "BIS_POLICY_EU",  "eu.rates.policy_bis",           2, "cross_check", "BIS ECB policy rate"),
        ("M1_EU",               "ecb",            "ECB_EA_M1",      "eu.liquidity.m1",               1, "primary",     "SA"),
        ("M2_EU",               "ecb",            "ECB_EA_M2",      "eu.liquidity.m2",               1, "primary",     "SA"),
        ("M3_EU",               "ecb",            "ECB_EA_M3",      "eu.liquidity.m3",               1, "primary",     "SA"),
        ("M3_GROWTH_EU",        "ecb",            "ECB_EA_M3_GROWTH","eu.liquidity.m3_growth",       1, "primary",     "YoY growth rate"),
        ("EURUSD",              "ecb",            "ECB_EURUSD",     "eu.fx.eurusd",                  1, "primary",     "EUR/USD"),
        ("EER_EU",              "bis",            "BIS_EER_EU",     "eu.fx.eer_real",                1, "primary",     "Real effective exchange rate"),
        ("CLI_EU",              "oecd",           "OECD_CLI_EU",    "eu.leading.cli",                1, "primary",     "Composite leading indicator"),
        #
        # ── UK ───────────────────────────────────────────────────────
        ("POLICY_RATE_GB",      "bis",            "BIS_POLICY_GB",  "gb.rates.policy_bis",           1, "primary",     "BOE bank rate"),
    ]

    def seed_concept_map(self) -> None:
        """Populate the concept_map table from the built-in definitions."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            for concept_id, source_id, series_id, fam_id, priority, role, notes in self._CONCEPT_MAP_DEFS:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO concept_map
                        (concept_id, source_id, provider_series_id,
                         obs_family_id, priority, role, notes, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (concept_id, source_id, series_id, fam_id, priority, role, notes, now),
                )
                # Update existing rows from prior seeds that have priority=0
                connection.execute(
                    """
                    UPDATE concept_map SET priority = ?, role = ?
                    WHERE concept_id = ? AND source_id = ? AND provider_series_id = ?
                      AND priority = 0
                    """,
                    (priority, role, concept_id, source_id, series_id),
                )

    def get_concept_series(self, concept_id: str) -> list[ConceptMapRecord]:
        """Return all source mappings for a given concept, ordered by priority."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM concept_map WHERE concept_id = ? ORDER BY priority, source_id",
                (concept_id,),
            ).fetchall()
            return [
                ConceptMapRecord(
                    concept_id=r["concept_id"],
                    source_id=r["source_id"],
                    provider_series_id=r["provider_series_id"],
                    obs_family_id=r["obs_family_id"],
                    priority=r["priority"],
                    role=r["role"],
                    notes=r["notes"],
                    created_at=r["created_at"],
                )
                for r in rows
            ]

    def list_concepts(self, *, country_code: str | None = None) -> list[str]:
        """Return distinct concept_ids, optionally filtered by country suffix."""
        with self._connection(commit=False) as connection:
            if country_code:
                suffix = f"_{country_code.upper()}"
                rows = connection.execute(
                    "SELECT DISTINCT concept_id FROM concept_map "
                    "WHERE concept_id LIKE ? ORDER BY concept_id",
                    (f"%{suffix}",),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT DISTINCT concept_id FROM concept_map ORDER BY concept_id"
                ).fetchall()
            return [r["concept_id"] for r in rows]

    def get_concept_observations(
        self,
        concept_id: str,
        *,
        start_date: str | None = None,
    ) -> list[tuple[str, str, str, float]]:
        """Return (source, series_id, date, value) tuples across all sources for a concept."""
        mappings = self.get_concept_series(concept_id)
        if not mappings:
            return []
        results: list[tuple[str, str, str, float]] = []
        with self._connection(commit=False) as connection:
            for m in mappings:
                sql = (
                    "SELECT source, series_id, date, value FROM indicators "
                    "WHERE source = ? AND series_id = ?"
                )
                params: list[Any] = [m.source_id, m.provider_series_id]
                if start_date:
                    sql += " AND date >= ?"
                    params.append(start_date)
                sql += " ORDER BY date"
                for row in connection.execute(sql, params).fetchall():
                    results.append((row["source"], row["series_id"], row["date"], row["value"]))
        return results

    def get_series_stats(self, source: str, series_id: str) -> dict[str, Any]:
        """Return {count, min_date, max_date, latest_value} for a series."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS cnt,
                       MIN(date) AS min_date,
                       MAX(date) AS max_date
                FROM indicators
                WHERE source = ? AND series_id = ?
                """,
                (source, series_id),
            ).fetchone()
            count = row["cnt"] if row else 0
            if count == 0:
                return {"count": 0, "min_date": None, "max_date": None, "latest_value": None}
            latest = connection.execute(
                """
                SELECT value FROM indicators
                WHERE source = ? AND series_id = ?
                ORDER BY date DESC LIMIT 1
                """,
                (source, series_id),
            ).fetchone()
            return {
                "count": count,
                "min_date": row["min_date"],
                "max_date": row["max_date"],
                "latest_value": latest["value"] if latest else None,
            }

    def get_concept_stats(self, concept_id: str) -> list[dict[str, Any]]:
        """Return per-source stats for all series in a concept."""
        mappings = self.get_concept_series(concept_id)
        results: list[dict[str, Any]] = []
        for m in mappings:
            stats = self.get_series_stats(m.source_id, m.provider_series_id)
            stats["concept_id"] = concept_id
            stats["source"] = m.source_id
            stats["series_id"] = m.provider_series_id
            stats["obs_family_id"] = m.obs_family_id
            stats["role"] = m.role
            results.append(stats)
        return results

    # ── Indicator resolution ─────────────────────────────────────────

    def resolve_indicator(
        self,
        concept_id: str,
        *,
        date: str | None = None,
    ) -> ResolvedObservation | None:
        """Return the highest-priority observation for a concept on a given date.

        If *date* is None, returns the most recent observation across all sources.
        """
        mappings = self.get_concept_series(concept_id)
        if not mappings:
            return None
        with self._connection(commit=False) as connection:
            # Count how many sources have data for the target date (for alternates)
            best: ResolvedObservation | None = None
            alternates = 0
            for m in mappings:
                if date is not None:
                    row = connection.execute(
                        "SELECT date, value FROM indicators "
                        "WHERE source = ? AND series_id = ? AND date = ? "
                        "LIMIT 1",
                        (m.source_id, m.provider_series_id, date),
                    ).fetchone()
                else:
                    row = connection.execute(
                        "SELECT date, value FROM indicators "
                        "WHERE source = ? AND series_id = ? "
                        "ORDER BY date DESC LIMIT 1",
                        (m.source_id, m.provider_series_id),
                    ).fetchone()
                if row is None:
                    continue
                alternates += 1
                if best is None:
                    best = ResolvedObservation(
                        concept_id=concept_id,
                        date=row["date"],
                        value=row["value"],
                        source_id=m.source_id,
                        provider_series_id=m.provider_series_id,
                        priority=m.priority,
                        role=m.role,
                    )
            if best is not None:
                # Check vintage status from indicator_vintages table
                vintage = "initial"
                revision_count = 0
                try:
                    vrow = connection.execute(
                        "SELECT COUNT(*) FROM indicator_vintages "
                        "WHERE series_id = ? AND source = ? AND observation_date = ?",
                        (best.provider_series_id, best.source_id, best.date),
                    ).fetchone()
                    revision_count = vrow[0] if vrow else 0
                    if revision_count > 1:
                        vintage = "revised"
                    elif revision_count == 1:
                        vintage = "initial"
                    # 0 vintages means no vintage tracking for this series
                except Exception:
                    pass

                best = ResolvedObservation(
                    concept_id=best.concept_id,
                    date=best.date,
                    value=best.value,
                    source_id=best.source_id,
                    provider_series_id=best.provider_series_id,
                    priority=best.priority,
                    role=best.role,
                    alternates=alternates - 1,
                    vintage=vintage,
                    revision_count=revision_count,
                )
            return best

    def resolve_indicator_history(
        self,
        concept_id: str,
        *,
        limit: int = 12,
    ) -> list[ResolvedObservation]:
        """Return a resolved time series, picking the highest-priority source per date."""
        mappings = self.get_concept_series(concept_id)
        if not mappings:
            return []

        # Collect all distinct dates across all sources
        all_dates: set[str] = set()
        # source_data[i] = {date: value} for mapping i
        source_data: list[dict[str, float]] = []
        with self._connection(commit=False) as connection:
            for m in mappings:
                rows = connection.execute(
                    "SELECT date, value FROM indicators "
                    "WHERE source = ? AND series_id = ? "
                    "ORDER BY date DESC",
                    (m.source_id, m.provider_series_id),
                ).fetchall()
                data = {r["date"]: r["value"] for r in rows}
                source_data.append(data)
                all_dates.update(data.keys())

        # Sort dates descending and limit
        sorted_dates = sorted(all_dates, reverse=True)[:limit]
        results: list[ResolvedObservation] = []
        for d in sorted_dates:
            winner: ResolvedObservation | None = None
            alternates = 0
            for i, m in enumerate(mappings):
                if d in source_data[i]:
                    alternates += 1
                    if winner is None:
                        winner = ResolvedObservation(
                            concept_id=concept_id,
                            date=d,
                            value=source_data[i][d],
                            source_id=m.source_id,
                            provider_series_id=m.provider_series_id,
                            priority=m.priority,
                            role=m.role,
                        )
            if winner is not None:
                results.append(
                    ResolvedObservation(
                        concept_id=winner.concept_id,
                        date=winner.date,
                        value=winner.value,
                        source_id=winner.source_id,
                        provider_series_id=winner.provider_series_id,
                        priority=winner.priority,
                        role=winner.role,
                        alternates=alternates - 1,
                    )
                )
        return results

    # ── Calendar indicator normalization ──────────────────────────────

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
                SELECT * FROM calendar_events
                WHERE indicator_id = ? AND actual IS NOT NULL
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                (indicator_id, limit),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

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

    # ── Release schedule CRUD ─────────────────────────────────────────

    # (concept_id, rule_type, rule_json_dict, frequency, release_time_utc, timezone, confidence, notes)
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

    # ── Release status CRUD ───────────────────────────────────────────

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
