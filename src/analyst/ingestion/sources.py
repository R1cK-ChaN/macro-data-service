from __future__ import annotations

import concurrent.futures
import dataclasses
import hashlib
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Callable, Iterable

import feedparser
import logging
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from analyst.contracts import format_epoch_iso, normalize_utc_iso, to_epoch_ms
from analyst.env import get_env_value
from analyst.ingestion.news_classify import Deduplicator
from analyst.ingestion.news_extract import extract_news_metadata
from analyst.ingestion.news_feeds import get_feeds
from analyst.ingestion.news_fetcher import ArticleFetcher
from analyst.ingestion.url_canon import canonicalize_url, content_hash
from analyst.ingestion.scrapers import (
    ForexFactoryCalendarClient,
    InvestingCalendarClient,
    TradingEconomicsCalendarClient,
)
from analyst.ingestion.scrapers.gov_report import GovReportClient, GovReportItem
from analyst.ingestion.scrapers.bis import BISClient
from analyst.ingestion.scrapers.ecb import ECBClient
from analyst.ingestion.scrapers.eia import EIAClient
from analyst.ingestion.scrapers.eurostat import EurostatClient
from analyst.ingestion.scrapers.fred import FredClient
from analyst.ingestion.scrapers.imf import IMFClient
from analyst.ingestion.scrapers.oecd import OECDClient, OECDRateLimitError
from analyst.ingestion.scrapers.reddit import RedditTrendClient, RedditTrendPost
from analyst.ingestion.scrapers.weibo import WeiboTrendClient, WeiboTrendItem
from analyst.ingestion.scrapers.worldbank import WorldBankClient, WorldBankRateLimitError
from analyst.ingestion.scrapers.nyfed import NYFedRatesClient
from analyst.ingestion.scrapers.rateprobability import RateProbabilityClient
from analyst.ingestion.scrapers.treasury_fiscal import TreasuryFiscalClient

logger = logging.getLogger(__name__)
from analyst.storage import (
    CentralBankCommunicationRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
    DocumentRecord,
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    MarketPriceRecord,
    NewsArticleRecord,
    SQLiteEngineStore,
    StoredEventRecord,
    TrendTopicRecord,
)


def _infer_publish_precision(value: str | None) -> str:
    if not value:
        return "estimated"
    if re.search(r"[T ]\d{1,2}:\d{2}", value):
        return "exact"
    return "date_only"

FED_FEEDS = {
    "press_releases": {
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "content_type": "statement",
    },
    "speeches": {
        "url": "https://www.federalreserve.gov/feeds/speeches.xml",
        "content_type": "speech",
    },
    "testimony": {
        "url": "https://www.federalreserve.gov/feeds/testimony.xml",
        "content_type": "testimony",
    },
}

FED_SPEAKERS = [
    "Powell",
    "Waller",
    "Bowman",
    "Williams",
    "Barr",
    "Cook",
    "Jefferson",
    "Kugler",
    "Musalem",
    "Goolsbee",
    "Bostic",
    "Daly",
    "Collins",
    "Harker",
    "Kashkari",
    "Logan",
    "Barkin",
    "Hammack",
    "Schmid",
]

MACRO_SERIES = {
    "CPIAUCSL": {"name": "CPI All Urban", "category": "inflation", "freq": "monthly"},
    "CPILFESL": {"name": "Core CPI", "category": "inflation", "freq": "monthly"},
    "PCEPILFE": {"name": "Core PCE Price Index", "category": "inflation", "freq": "monthly"},
    "T5YIE": {"name": "5Y Breakeven Inflation", "category": "inflation", "freq": "daily"},
    "T10YIE": {"name": "10Y Breakeven Inflation", "category": "inflation", "freq": "daily"},
    "UNRATE": {"name": "Unemployment Rate", "category": "employment", "freq": "monthly"},
    "PAYEMS": {"name": "Total Nonfarm Payrolls", "category": "employment", "freq": "monthly"},
    "ICSA": {"name": "Initial Jobless Claims", "category": "employment", "freq": "weekly"},
    "CCSA": {"name": "Continuing Jobless Claims", "category": "employment", "freq": "weekly"},
    "GDP": {"name": "GDP", "category": "growth", "freq": "quarterly"},
    "GDPC1": {"name": "Real GDP", "category": "growth", "freq": "quarterly"},
    "RSAFS": {"name": "Retail Sales", "category": "growth", "freq": "monthly"},
    "INDPRO": {"name": "Industrial Production", "category": "growth", "freq": "monthly"},
    "DFF": {"name": "Fed Funds Rate", "category": "rates", "freq": "daily"},
    "DGS2": {"name": "2Y Treasury Yield", "category": "rates", "freq": "daily"},
    "DGS10": {"name": "10Y Treasury Yield", "category": "rates", "freq": "daily"},
    "DGS30": {"name": "30Y Treasury Yield", "category": "rates", "freq": "daily"},
    "DFII10": {"name": "10Y Real Yield", "category": "rates", "freq": "daily"},
    "T10Y2Y": {"name": "10Y-2Y Spread", "category": "rates", "freq": "daily"},
    "WALCL": {"name": "Fed Balance Sheet", "category": "liquidity", "freq": "weekly"},
    "M2SL": {"name": "M2 Money Supply", "category": "liquidity", "freq": "monthly"},
    "RRPONTSYD": {"name": "Reverse Repo", "category": "liquidity", "freq": "daily"},
    "WTREGEN": {"name": "Treasury General Account", "category": "liquidity", "freq": "weekly"},
    "DTWEXBGS": {"name": "Broad Dollar Index", "category": "fx", "freq": "daily"},
    "DEXCHUS": {"name": "CNY/USD Exchange Rate", "category": "fx", "freq": "daily"},
    "BAMLH0A0HYM2": {"name": "High Yield OAS", "category": "credit", "freq": "daily"},
}

MACRO_WATCHLIST = {
    "equity": {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ",
        "^DJI": "Dow Jones",
        "^VIX": "VIX",
    },
    "global_equity": {
        "^STOXX50E": "Euro Stoxx 50",
        "^N225": "Nikkei 225",
        "^HSI": "Hang Seng",
        "000001.SS": "Shanghai Composite",
    },
    "fx": {
        "DX-Y.NYB": "Dollar Index",
        "USDJPY=X": "USD/JPY",
        "USDCNY=X": "USD/CNY",
    },
    "bond": {
        "^TNX": "10Y Treasury Yield",
        "^TYX": "30Y Treasury Yield",
        "^FVX": "5Y Treasury Yield",
    },
    "commodity": {
        "GC=F": "Gold",
        "CL=F": "WTI Crude Oil",
        "HG=F": "Copper",
    },
    "crypto": {
        "BTC-USD": "Bitcoin",
        "ETH-USD": "Ethereum",
    },
}


def extract_speaker(title: str) -> str:
    for speaker in FED_SPEAKERS:
        if speaker.lower() in title.lower():
            return speaker
    return ""


@dataclass(frozen=True)
class RefreshStats:
    source: str
    count: int


@dataclass(frozen=True)
class IngestionRunReport:
    source: str
    stored: int
    fetched: int | None = None
    normalized: int | None = None
    validated: int | None = None
    deduplicated: int | None = None
    duration_ms: int = 0
    retries: int = 0
    error: str = ""
    validation_report: Any = None

    def to_counts(self) -> dict[str, int]:
        return {self.source: self.stored}

    def to_dict(self) -> dict[str, Any]:
        payload = dataclasses.asdict(self)
        payload["ok"] = not self.error
        if self.validation_report is not None:
            payload["validation"] = self.validation_report.to_dict()
        else:
            payload.pop("validation_report", None)
        return payload


@dataclass(frozen=True)
class IngestionSourceDefinition:
    name: str
    interval_seconds: int | None = None
    prepare: Callable[[], None] | None = None
    fetch: Callable[[], Iterable[Any]] | None = None
    normalize: Callable[[list[Any]], Iterable[Any]] | None = None
    validate: Callable[[list[Any]], Iterable[Any]] | None = None
    deduplicate: Callable[[list[Any]], Iterable[Any]] | None = None
    store: Callable[[list[Any]], int | None] | None = None
    execute: Callable[[], int] | None = None
    max_retries: int = 0
    retry_backoff_seconds: float = 0.0


VINTAGE_SERIES = ["GDP", "GDPC1", "CPIAUCSL", "PAYEMS", "UNRATE", "INDPRO", "RSAFS"]

EIA_SERIES = {
    "petroleum_brent": {
        "route": "petroleum/pri/spt/data",
        "params": {"data[]": "value", "facets[product][]": "EPCBRENT", "frequency": "daily"},
        "series_id": "EIA_BRENT",
        "category": "energy",
    },
    "petroleum_wti": {
        "route": "petroleum/pri/spt/data",
        "params": {"data[]": "value", "facets[product][]": "EPCWTI", "frequency": "daily"},
        "series_id": "EIA_WTI",
        "category": "energy",
    },
    "petroleum_stocks": {
        "route": "petroleum/stoc/wstk/data",
        "params": {"data[]": "value", "facets[product][]": "EPC0", "frequency": "weekly"},
        "series_id": "EIA_CRUDE_STOCKS",
        "category": "energy",
    },
    "natgas_futures": {
        "route": "natural-gas/pri/fut/data",
        "params": {"data[]": "value", "frequency": "daily"},
        "series_id": "EIA_NATGAS",
        "category": "energy",
    },
    "petroleum_supply": {
        "route": "petroleum/sum/snd/data",
        "params": {"data[]": "value", "frequency": "weekly"},
        "series_id": "EIA_PETROL_SUPPLY",
        "category": "energy",
    },
}

TREASURY_DATASETS = {
    "debt_outstanding": {
        "endpoint": "v2/accounting/od/debt_to_penny",
        "series_id": "TREAS_DEBT_TOTAL",
        "category": "fiscal",
    },
    "dts_operating_cash": {
        "endpoint": "v1/accounting/dts/deposits_withdrawals_operating_cash",
        "series_id": "TREAS_TGA_BALANCE",
        "category": "fiscal",
    },
    "avg_interest_rates": {
        "endpoint": "v2/accounting/od/avg_interest_rates",
        "series_id": "TREAS_AVG_RATE",
        "category": "fiscal",
    },
}

IMF_SERIES = {
    "cn_cpi": {
        "dataflow": "CPI", "version": "5.0.0", "key": "CHN.CPI._T.IX.M",
        "series_id": "IMF_CN_CPI", "category": "inflation",
    },
    "cn_gdp": {
        "dataflow": "QNEA", "version": "7.0.0", "key": "CHN.B1GQ.V.NSA.XDC.Q",
        "series_id": "IMF_CN_GDP", "category": "growth",
    },
    "cn_fx_reserves": {
        "dataflow": "IRFCL", "version": "11.0.0", "key": "CHN.IRFCLDT1_IRFCL54_USD",
        "series_id": "IMF_CN_FX_RESERVES", "category": "reserves",
    },
    "jp_cpi": {
        "dataflow": "CPI", "version": "5.0.0", "key": "JPN.CPI._T.IX.M",
        "series_id": "IMF_JP_CPI", "category": "inflation",
    },
    "jp_gdp": {
        "dataflow": "QNEA", "version": "7.0.0", "key": "JPN.B1GQ.V.SA.XDC.Q",
        "series_id": "IMF_JP_GDP", "category": "growth",
    },
    "eu_cpi": {
        "dataflow": "CPI", "version": "5.0.0", "key": "G163.HICP._T.IX.M",
        "series_id": "IMF_EU_CPI", "category": "inflation",
    },
    "global_trade": {
        "dataflow": "ITG", "version": "4.0.0", "key": "USA.XG.FOB_USD.M",
        "series_id": "IMF_GLOBAL_TRADE", "category": "trade",
    },
}

IMF_VINTAGE_SERIES = ["cn_gdp", "jp_gdp"]

EUROSTAT_SERIES = {
    "hicp": {
        "dataset": "prc_hicp_manr",
        "params": {"coicop": "CP00", "geo": "EA20"},
        "series_id": "ESTAT_HICP", "category": "inflation",
    },
    "gdp": {
        "dataset": "namq_10_gdp",
        "params": {"na_item": "B1GQ", "geo": "EA20", "unit": "CLV_PCH_PRE", "s_adj": "SCA"},
        "series_id": "ESTAT_GDP", "category": "growth",
    },
    "unemployment": {
        "dataset": "une_rt_m",
        "params": {"age": "TOTAL", "sex": "T", "geo": "EA20", "s_adj": "SA", "unit": "PC_ACT"},
        "series_id": "ESTAT_UNEMPLOYMENT", "category": "employment",
    },
    "indpro": {
        "dataset": "sts_inpr_m",
        "params": {"nace_r2": "B-D", "geo": "EA20", "s_adj": "SCA", "unit": "PCH_PRE"},
        "series_id": "ESTAT_INDPRO", "category": "growth",
    },
    "esi": {
        "dataset": "teibs010",
        "params": {"geo": "EA20", "indic": "BS-ESI-I", "s_adj": "SA"},
        "series_id": "ESTAT_ESI", "category": "sentiment",
    },
}

BIS_SERIES = {
    "policy_us": {"dataflow": "WS_CBPOL", "key": "M.US", "series_id": "BIS_POLICY_US", "category": "rates"},
    "policy_eu": {"dataflow": "WS_CBPOL", "key": "M.XM", "series_id": "BIS_POLICY_EU", "category": "rates"},
    "policy_jp": {"dataflow": "WS_CBPOL", "key": "M.JP", "series_id": "BIS_POLICY_JP", "category": "rates"},
    "policy_cn": {"dataflow": "WS_CBPOL", "key": "M.CN", "series_id": "BIS_POLICY_CN", "category": "rates"},
    "policy_gb": {"dataflow": "WS_CBPOL", "key": "M.GB", "series_id": "BIS_POLICY_GB", "category": "rates"},
    "eer_us":    {"dataflow": "WS_EER",    "key": "M.R.B.US", "series_id": "BIS_EER_US", "category": "fx"},
    "eer_cn":    {"dataflow": "WS_EER",    "key": "M.R.B.CN", "series_id": "BIS_EER_CN", "category": "fx"},
    "eer_eu":    {"dataflow": "WS_EER",    "key": "M.R.B.XM", "series_id": "BIS_EER_EU", "category": "fx"},
    "credit_gap_us": {"dataflow": "WS_CREDIT_GAP", "key": "Q.US.P", "series_id": "BIS_CREDIT_GAP_US", "category": "credit"},
    "credit_gap_cn": {"dataflow": "WS_CREDIT_GAP", "key": "Q.CN.P", "series_id": "BIS_CREDIT_GAP_CN", "category": "credit"},
    "property_us":   {"dataflow": "WS_SPP",  "key": "Q.US.R", "series_id": "BIS_PROPERTY_US", "category": "property"},
    "property_cn":   {"dataflow": "WS_SPP",  "key": "Q.CN.R", "series_id": "BIS_PROPERTY_CN", "category": "property"},
}

ECB_SERIES = {
    "m1":            {"dataflow": "BSI", "key": "M.U2.Y.V.M10.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M1",           "category": "liquidity"},
    "m2":            {"dataflow": "BSI", "key": "M.U2.Y.V.M20.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M2",           "category": "liquidity"},
    "m3":            {"dataflow": "BSI", "key": "M.U2.Y.V.M30.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M3",           "category": "liquidity"},
    "m3_growth":     {"dataflow": "BSI", "key": "M.U2.N.V.M30.X.I.U2.2300.Z01.A", "series_id": "ECB_EA_M3_GROWTH",     "category": "liquidity"},
    "deposit_rate":  {"dataflow": "FM",  "key": "B.U2.EUR.4F.KR.DFR.LEV",        "series_id": "ECB_EA_DEPOSIT_RATE",  "category": "rates"},
    "eurusd":        {"dataflow": "EXR", "key": "M.USD.EUR.SP00.A",              "series_id": "ECB_EURUSD",           "category": "fx"},
}

@dataclass(frozen=True)
class OECDSeriesConfig:
    dataflow: str
    series_id: str
    category: str
    agency_id: str = OECDClient.DEFAULT_AGENCY_ID
    version: str = "latest"
    key: str | None = None
    filters: dict[str, str] = field(default_factory=dict)


def _slugify_oecd_token(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "oecd"


def _generated_oecd_series_id(agency_id: str, dataflow_id: str, key: str) -> str:
    slug = _slugify_oecd_token(dataflow_id)[:48]
    digest = hashlib.sha1(f"{agency_id}|{dataflow_id}|{key}".encode("utf-8")).hexdigest()[:12].upper()
    return f"OECD_AUTO_{slug.upper()}_{digest}"


def _generated_oecd_config_key(dataflow_id: str, key: str) -> str:
    slug = _slugify_oecd_token(dataflow_id)[:48]
    digest = hashlib.sha1(f"{dataflow_id}|{key}".encode("utf-8")).hexdigest()[:10]
    return f"auto_{slug}_{digest}"


def render_oecd_series_configs(series_configs: dict[str, OECDSeriesConfig]) -> str:
    lines = ["generated_oecd_series = {"]
    for config_key, cfg in sorted(series_configs.items()):
        lines.append(f'    "{config_key}": OECDSeriesConfig(')
        lines.append(f'        dataflow="{cfg.dataflow}",')
        lines.append(f'        series_id="{cfg.series_id}",')
        lines.append(f'        category="{cfg.category}",')
        lines.append(f'        agency_id="{cfg.agency_id}",')
        lines.append(f'        version="{cfg.version}",')
        if cfg.key is not None:
            lines.append(f'        key="{cfg.key}",')
        if cfg.filters:
            lines.append("        filters={")
            for dim_id, dim_value in sorted(cfg.filters.items()):
                lines.append(f'            "{dim_id}": "{dim_value}",')
            lines.append("        },")
        lines.append("    ),")
    lines.append("}")
    return "\n".join(lines)


OECD_SERIES = {
    "cli_us": OECDSeriesConfig(
        dataflow="DSD_STES@DF_CLI",
        series_id="OECD_CLI_US",
        category="leading",
        filters={
            "REF_AREA": "USA",
            "FREQ": "M",
            "MEASURE": "LI",
            "UNIT_MEASURE": "IX",
            "ACTIVITY": "_Z",
            "ADJUSTMENT": "NOR",
            "TRANSFORMATION": "IX",
            "TIME_HORIZ": "_Z",
            "METHODOLOGY": "H",
        },
    ),
    "cli_cn": OECDSeriesConfig(
        dataflow="DSD_STES@DF_CLI",
        series_id="OECD_CLI_CN",
        category="leading",
        filters={
            "REF_AREA": "CHN",
            "FREQ": "M",
            "MEASURE": "LI",
            "UNIT_MEASURE": "IX",
            "ACTIVITY": "_Z",
            "ADJUSTMENT": "NOR",
            "TRANSFORMATION": "IX",
            "TIME_HORIZ": "_Z",
            "METHODOLOGY": "H",
        },
    ),
    "cli_jp": OECDSeriesConfig(
        dataflow="DSD_STES@DF_CLI",
        series_id="OECD_CLI_JP",
        category="leading",
        filters={
            "REF_AREA": "JPN",
            "FREQ": "M",
            "MEASURE": "LI",
            "UNIT_MEASURE": "IX",
            "ACTIVITY": "_Z",
            "ADJUSTMENT": "NOR",
            "TRANSFORMATION": "IX",
            "TIME_HORIZ": "_Z",
            "METHODOLOGY": "H",
        },
    ),
    "cli_eu": OECDSeriesConfig(
        dataflow="DSD_STES@DF_CLI",
        series_id="OECD_CLI_EU",
        category="leading",
        filters={
            "REF_AREA": "G4E",
            "FREQ": "M",
            "MEASURE": "LI",
            "UNIT_MEASURE": "IX",
            "ACTIVITY": "_Z",
            "ADJUSTMENT": "NOR",
            "TRANSFORMATION": "IX",
            "TIME_HORIZ": "_Z",
            "METHODOLOGY": "H",
        },
    ),
    "consumer_conf": OECDSeriesConfig(
        dataflow="DSD_STES@DF_CS",
        series_id="OECD_CONSUMER_CONF_US",
        category="sentiment",
        key="USA.M.CCICP.*.*.*.*.*.*",
    ),
    "business_conf": OECDSeriesConfig(
        dataflow="DSD_STES@DF_BTS",
        series_id="OECD_BUSINESS_CONF_US",
        category="sentiment",
        key="USA.M.BCICP.*.*.*.*.*.*",
    ),
    "unemployment_us": OECDSeriesConfig(
        dataflow="DSD_KEI@DF_KEI",
        series_id="OECD_UNEMP_US",
        category="employment",
        filters={
            "REF_AREA": "USA",
            "FREQ": "M",
            "MEASURE": "UNEMP",
            "UNIT_MEASURE": "PT_LF",
            "ACTIVITY": "_T",
            "ADJUSTMENT": "Y",
            "TRANSFORMATION": "_Z",
        },
    ),
}

@dataclass(frozen=True)
class WorldBankSeriesConfig:
    """Configuration for a single World Bank indicator series."""

    indicator: str
    series_id: str
    category: str
    country: str = "all"
    source_id: str = ""
    topic_id: str = ""
    start_year: int | None = None
    limit: int = 30


def _slugify_wb_token(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "worldbank"


def _generated_wb_series_id(indicator_code: str, country: str) -> str:
    slug = _slugify_wb_token(indicator_code)[:48]
    digest = hashlib.sha1(f"{indicator_code}|{country}".encode("utf-8")).hexdigest()[:12].upper()
    return f"WB_AUTO_{slug.upper()}_{digest}"


def _generated_wb_config_key(indicator_code: str, country: str) -> str:
    slug = _slugify_wb_token(indicator_code)[:48]
    digest = hashlib.sha1(f"{indicator_code}|{country}".encode("utf-8")).hexdigest()[:10]
    return f"auto_{slug}_{digest}"


def render_wb_series_configs(series_configs: dict[str, WorldBankSeriesConfig]) -> str:
    """Render Python source code for generated World Bank series configs."""
    lines = ["generated_wb_series = {"]
    for config_key, cfg in sorted(series_configs.items()):
        lines.append(f'    "{config_key}": WorldBankSeriesConfig(')
        lines.append(f'        indicator="{cfg.indicator}",')
        lines.append(f'        series_id="{cfg.series_id}",')
        lines.append(f'        category="{cfg.category}",')
        lines.append(f'        country="{cfg.country}",')
        if cfg.source_id:
            lines.append(f'        source_id="{cfg.source_id}",')
        if cfg.topic_id:
            lines.append(f'        topic_id="{cfg.topic_id}",')
        if cfg.start_year is not None:
            lines.append(f'        start_year={cfg.start_year},')
        if cfg.limit != 30:
            lines.append(f'        limit={cfg.limit},')
        lines.append("    ),")
    lines.append("}")
    return "\n".join(lines)


WORLDBANK_SERIES: dict[str, WorldBankSeriesConfig] = {
    "gdp_pcap_us":   WorldBankSeriesConfig(indicator="NY.GDP.PCAP.PP.CD",  country="USA", series_id="WB_GDP_PCAP_US",   category="development"),
    "gdp_pcap_cn":   WorldBankSeriesConfig(indicator="NY.GDP.PCAP.PP.CD",  country="CHN", series_id="WB_GDP_PCAP_CN",   category="development"),
    "gdp_growth_us": WorldBankSeriesConfig(indicator="NY.GDP.MKTP.KD.ZG",  country="USA", series_id="WB_GDP_GROWTH_US", category="growth"),
    "ca_gdp_us":     WorldBankSeriesConfig(indicator="BN.CAB.XOKA.GD.ZS",  country="USA", series_id="WB_CA_GDP_US",     category="trade"),
}


class FREDIngestionClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.client = FredClient(api_key=api_key)

    @property
    def api_key(self) -> str:
        return self.client.api_key

    def refresh_daily_series(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        daily_series = {sid: meta for sid, meta in MACRO_SERIES.items() if meta["freq"] == "daily"}
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
        for series_id, meta in daily_series.items():
            count += self._store_series(store, series_id, meta, start_date=start_date, limit=5, family_lookup=family_lookup)
            time.sleep(0.2)
        return RefreshStats(source="fred_daily", count=count)

    def refresh_all_series(
        self,
        store: SQLiteEngineStore,
        *,
        lookback_days: int = 365,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        for series_id, meta in MACRO_SERIES.items():
            count += self._store_series(store, series_id, meta, start_date=start_date, limit=100, family_lookup=family_lookup)
            time.sleep(0.2)
        return RefreshStats(source="fred_all", count=count)

    def refresh_vintages(
        self,
        store: SQLiteEngineStore,
        vintage_series: list[str] | None = None,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        series_list = vintage_series or VINTAGE_SERIES
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=365)).strftime("%Y-%m-%d")
        for series_id in series_list:
            try:
                vintages = self.client.get_vintages(series_id, start_date=start_date)
                fam_id = family_lookup.get(("fred", series_id)) if family_lookup else None
                for v in vintages:
                    store.upsert_indicator_vintage(
                        IndicatorVintageRecord(
                            series_id=v.series_id,
                            source="fred",
                            observation_date=v.date,
                            vintage_date=v.vintage_date,
                            value=v.value,
                            metadata={"name": MACRO_SERIES.get(series_id, {}).get("name", series_id)},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("FRED vintage refresh failed for %s", series_id, exc_info=True)
            time.sleep(0.3)
        return RefreshStats(source="fred_vintages", count=count)

    def _store_series(
        self,
        store: SQLiteEngineStore,
        series_id: str,
        meta: dict[str, str],
        *,
        start_date: str,
        limit: int,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> int:
        stored = 0
        fam_id = family_lookup.get(("fred", series_id)) if family_lookup else None
        for obs in self.client.get_series(series_id, start_date=start_date, limit=limit):
            store.upsert_indicator_observation(
                IndicatorObservationRecord(
                    series_id=series_id,
                    source="fred",
                    date=obs.date,
                    value=obs.value,
                    metadata={"name": meta["name"], "category": meta["category"]},
                    obs_family_id=fam_id,
                )
            )
            stored += 1
        return stored


class EIAIngestionClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.client = EIAClient(api_key=api_key)

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in EIA_SERIES.items():
            try:
                observations = self.client.get_series(
                    cfg["route"],
                    params=dict(cfg["params"]),
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("eia", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eia",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "unit": obs.unit},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("EIA refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="eia", count=count)


class TreasuryFiscalIngestionClient:
    def __init__(self) -> None:
        self.client = TreasuryFiscalClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        fetchers = {
            "debt_outstanding": self.client.fetch_debt_outstanding,
            "dts_operating_cash": self.client.fetch_tga_balance,
            "avg_interest_rates": self.client.fetch_avg_interest_rates,
        }
        for key, fetch_fn in fetchers.items():
            cfg = TREASURY_DATASETS[key]
            try:
                observations = fetch_fn(limit=30)
                fam_id = family_lookup.get(("treasury_fiscal", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="treasury_fiscal",
                            date=obs.date,
                            value=obs.value,
                            metadata={**obs.metadata, "category": cfg["category"]},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Treasury fiscal refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="treasury_fiscal", count=count)


class IMFIngestionClient:
    def __init__(self) -> None:
        self.client = IMFClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in IMF_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    version=cfg["version"],
                    limit=30,
                )
                fam_id = family_lookup.get(("imf", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="imf",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("IMF refresh failed for %s", key, exc_info=True)
            time.sleep(1.0)
        return RefreshStats(source="imf", count=count)

    def refresh_vintages(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        now = datetime.now(UTC)
        as_of_dates = [
            (now - timedelta(days=30 * i)).strftime("%Y-%m-%d")
            for i in range(12)
        ]
        for series_key in IMF_VINTAGE_SERIES:
            cfg = IMF_SERIES[series_key]
            try:
                vintages = self.client.get_vintages(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    version=cfg["version"],
                    as_of_dates=as_of_dates,
                    start_period=str(now.year - 2),
                    limit=30,
                )
                fam_id = family_lookup.get(("imf", cfg["series_id"])) if family_lookup else None
                for v in vintages:
                    store.upsert_indicator_vintage(
                        IndicatorVintageRecord(
                            series_id=v.series_id,
                            source="imf",
                            observation_date=v.date,
                            vintage_date=v.vintage_date,
                            value=v.value,
                            metadata={"category": cfg["category"], "dataflow": v.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("IMF vintage refresh failed for %s", series_key, exc_info=True)
        return RefreshStats(source="imf_vintages", count=count)


class EurostatIngestionClient:
    def __init__(self) -> None:
        self.client = EurostatClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in EUROSTAT_SERIES.items():
            try:
                observations = self.client.get_dataset(
                    cfg["dataset"],
                    params=dict(cfg["params"]),
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("eurostat", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eurostat",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataset": obs.dataset},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Eurostat refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="eurostat", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows()
        if query:
            needle = query.lower().strip()
            dataflows = [
                df for df in dataflows
                if needle in df.id.lower()
                or needle in df.name.lower()
                or needle in df.description.lower()
            ]
        dataflows.sort(key=lambda item: item.id)
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            allowed = set(dataflow_ids)
            return [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ][:limit] if limit is not None else [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ]
        return self.list_catalog_dataflows(query=query, limit=limit)

    def get_structure_summary(self, dataflow_id: str) -> Any:
        return self.client.summarize_structure(dataflow_id)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info("Skipping %s (estimated %d series — too large)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataset": dataflow.id,
                "params": {},
                "series_id": f"ESTAT_AUTO_{dataflow.id.upper()}",
                "category": category,
            }
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    ".",
                    series_id=f"ESTAT_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("eurostat", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eurostat",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataset": obs.dataset,
                                "dataflow_name": dataflow.name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Eurostat catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="eurostat", count=count)


class BISIngestionClient:
    def __init__(self) -> None:
        self.client = BISClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in BIS_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("bis", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="bis",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("BIS refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="bis", count=count)


class ECBIngestionClient:
    def __init__(self) -> None:
        self.client = ECBClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in ECB_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("ecb", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ecb",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ECB refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="ecb", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows()
        if query:
            needle = query.lower().strip()
            dataflows = [
                df for df in dataflows
                if needle in df.id.lower()
                or needle in df.name.lower()
                or needle in df.description.lower()
            ]
        dataflows.sort(key=lambda item: item.id)
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            allowed = set(dataflow_ids)
            matches = [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ]
            return matches[:limit] if limit is not None else matches
        return self.list_catalog_dataflows(query=query, limit=limit)

    def get_structure_summary(self, dataflow_id: str) -> Any:
        return self.client.summarize_structure(dataflow_id)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info("Skipping ECB %s (estimated %d series)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "key": ".",
                "series_id": f"ECB_AUTO_{dataflow.id.upper()}",
                "category": category,
            }
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    ".",
                    series_id=f"ECB_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("ecb", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ecb",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ECB catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="ecb", count=count)


class _OECDRateLimiter:
    """Thread-safe rate limiter for OECD API requests.

    OECD enforces a hard cap of 60 data downloads per hour (per IP).
    This limiter enforces both a minimum interval between requests and
    an hourly budget to stay under that cap.
    """

    HOURLY_BUDGET = 60

    def __init__(self, min_interval: float = 2.0) -> None:
        self._lock = threading.Lock()
        self._last_request = 0.0
        self._min_interval = min_interval
        self._hour_start = time.monotonic()
        self._hour_count = 0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            # Reset hourly window if >3600s elapsed
            if now - self._hour_start >= 3600.0:
                self._hour_start = now
                self._hour_count = 0
            # If we've hit the hourly budget, sleep until the window resets
            if self._hour_count >= self.HOURLY_BUDGET:
                sleep_for = 3600.0 - (now - self._hour_start)
                if sleep_for > 0:
                    logger.warning(
                        "OECD hourly budget (%d/%d) exhausted, sleeping %.0fs",
                        self._hour_count, self.HOURLY_BUDGET, sleep_for,
                    )
                    time.sleep(sleep_for)
                self._hour_start = time.monotonic()
                self._hour_count = 0
                now = time.monotonic()
            # Enforce minimum interval
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()
            self._hour_count += 1

    def backoff(self, seconds: float) -> None:
        """Push the next allowed request time forward (called on 429)."""
        with self._lock:
            self._last_request = time.monotonic() + seconds - self._min_interval


class _WorldBankRateLimiter:
    """Thread-safe rate limiter for World Bank API requests.

    World Bank has no documented hard rate limit, but we enforce
    respectful defaults: 0.3s min interval, 500 requests/hour budget.
    """

    HOURLY_BUDGET = 500

    def __init__(self, min_interval: float = 0.3) -> None:
        self._lock = threading.Lock()
        self._last_request = 0.0
        self._min_interval = min_interval
        self._hour_start = time.monotonic()
        self._hour_count = 0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now - self._hour_start >= 3600.0:
                self._hour_start = now
                self._hour_count = 0
            if self._hour_count >= self.HOURLY_BUDGET:
                sleep_for = 3600.0 - (now - self._hour_start)
                if sleep_for > 0:
                    logger.warning(
                        "World Bank hourly budget (%d/%d) exhausted, sleeping %.0fs",
                        self._hour_count, self.HOURLY_BUDGET, sleep_for,
                    )
                    time.sleep(sleep_for)
                self._hour_start = time.monotonic()
                self._hour_count = 0
                now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()
            self._hour_count += 1

    def backoff(self, seconds: float) -> None:
        """Push the next allowed request time forward (called on 429)."""
        with self._lock:
            self._last_request = time.monotonic() + seconds - self._min_interval


class OECDIngestionClient:
    def __init__(
        self,
        client: OECDClient | None = None,
        *,
        series_configs: dict[str, OECDSeriesConfig] | None = None,
    ) -> None:
        self.client = client or OECDClient()
        self.series_configs = series_configs or OECD_SERIES
        self._resolved_configs: dict[str, tuple[str, str]] = {}

    def _resolve_request(self, cfg: OECDSeriesConfig) -> tuple[str, str]:
        cached = self._resolved_configs.get(cfg.series_id)
        if cached is not None:
            return cached

        dataflow = self.client.get_dataflow(
            cfg.dataflow,
            agency_id=cfg.agency_id,
            version=cfg.version,
        )
        if cfg.key:
            resolved = (dataflow.version, cfg.key)
        else:
            resolved = (
                dataflow.version,
                self.client.build_key(
                    cfg.dataflow,
                    cfg.filters,
                    agency_id=cfg.agency_id,
                    version=dataflow.version,
                ),
            )
        self._resolved_configs[cfg.series_id] = resolved
        return resolved

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in self.series_configs.items():
            try:
                resolved_version, resolved_key = self._resolve_request(cfg)
                observations = self.client.fetch_data(
                    cfg.dataflow,
                    agency_id=cfg.agency_id,
                    version=resolved_version,
                    key=resolved_key,
                    series_id=cfg.series_id,
                    limit=30,
                )
                fam_id = family_lookup.get(("oecd", cfg.series_id)) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="oecd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": cfg.category,
                                "dataflow": obs.dataflow,
                                "agency_id": obs.agency_id,
                                "series_key": obs.series_key or resolved_key,
                                "dimensions": obs.dimensions,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("OECD refresh failed for %s", key, exc_info=True)
            time.sleep(2.0)
        return RefreshStats(source="oecd", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        agency_prefix: str = "OECD",
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows(agency_id="all")
        if agency_prefix:
            dataflows = [dataflow for dataflow in dataflows if dataflow.agency_id.startswith(agency_prefix)]
        if query:
            needle = query.lower().strip()
            dataflows = [
                dataflow for dataflow in dataflows
                if needle in dataflow.id.lower()
                or needle in dataflow.name.lower()
                or needle in dataflow.description.lower()
            ]
        dataflows.sort(key=lambda item: (item.agency_id, item.id, item.version))
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            if agency_id:
                return [
                    self.client.get_dataflow(dataflow_id, agency_id=agency_id, version="latest")
                    for dataflow_id in dataflow_ids
                ]
            allowed = set(dataflow_ids)
            matches = [
                dataflow for dataflow in self.list_catalog_dataflows(
                    agency_prefix=agency_prefix,
                    limit=None,
                )
                if dataflow.id in allowed
            ]
            ordered: list[Any] = []
            seen: set[tuple[str, str]] = set()
            for dataflow_id in dataflow_ids:
                for dataflow in matches:
                    marker = (dataflow.agency_id, dataflow.id)
                    if dataflow.id == dataflow_id and marker not in seen:
                        ordered.append(dataflow)
                        seen.add(marker)
            return ordered[:limit] if limit is not None else ordered
        return self.list_catalog_dataflows(query=query, agency_prefix=agency_prefix, limit=limit)

    def get_structure_summary(
        self,
        dataflow_id: str,
        *,
        agency_id: str = OECDClient.DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> Any:
        return self.client.summarize_structure(dataflow_id, agency_id=agency_id, version=version)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, OECDSeriesConfig]:
        generated: dict[str, OECDSeriesConfig] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_id=agency_id,
            query=query,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        ):
            series_list = self.client.enumerate_series(
                dataflow.id,
                agency_id=dataflow.agency_id,
                version=dataflow.version,
                key="all",
                observation_limit=1,
                max_series=series_per_dataflow,
            )
            for series in series_list:
                filters = self.client.series_to_filters(
                    dataflow.id,
                    series,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                )
                if not filters:
                    continue
                series_key = self.client.build_key(
                    dataflow.id,
                    filters,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                )
                config = OECDSeriesConfig(
                    dataflow=dataflow.id,
                    series_id=_generated_oecd_series_id(dataflow.agency_id, dataflow.id, series_key),
                    category=category,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    filters=filters,
                )
                generated[_generated_oecd_config_key(dataflow.id, series_key)] = config
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 3.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_id=agency_id,
            query=query,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        ):
            try:
                observations = self.client.fetch_data(
                    dataflow.id,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    key="all",
                    series_id=None,
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("oecd", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="oecd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                                "dataflow_description": dataflow.description,
                                "agency_id": obs.agency_id or dataflow.agency_id,
                                "series_key": obs.series_key,
                                "raw_series_key": obs.raw_series_key,
                                "dimensions": obs.dimensions,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("OECD catalog refresh failed for %s/%s", dataflow.agency_id, dataflow.id, exc_info=True)
            time.sleep(max(sleep_seconds, 0.0))
        return RefreshStats(source="oecd_catalog", count=count)

    def refresh_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
        max_workers: int = 3,
        request_delay: float = 2.0,
    ) -> RefreshStats:
        """Fetch all configured series using a thread pool.

        OECD enforces 60 data downloads/hour per IP.  The rate limiter
        enforces both ``request_delay`` spacing and the hourly budget.
        Defaults are conservative: 3 workers, 2 s between requests.
        """
        limiter = _OECDRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []
        errors: list[str] = []

        def _fetch_one(key: str, cfg: OECDSeriesConfig) -> list[IndicatorObservationRecord]:
            max_retries = 5
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    resolved_version, resolved_key = self._resolve_request(cfg)
                    observations = self.client.fetch_data(
                        cfg.dataflow,
                        agency_id=cfg.agency_id,
                        version=resolved_version,
                        key=resolved_key,
                        series_id=cfg.series_id,
                        limit=30,
                    )
                    break
                except OECDRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(10.0, request_delay) * (2 ** attempt)
                        logger.warning("OECD 429 for %s, retry %d backing off %.1fs", key, attempt + 1, backoff)
                        limiter.backoff(backoff)
                        continue
                    raise
            fam_id = family_lookup.get(("oecd", cfg.series_id)) if family_lookup else None
            return [
                IndicatorObservationRecord(
                    series_id=obs.series_id,
                    source="oecd",
                    date=obs.date,
                    value=obs.value,
                    metadata={
                        "category": cfg.category,
                        "dataflow": obs.dataflow,
                        "agency_id": obs.agency_id,
                        "series_key": obs.series_key or resolved_key,
                        "dimensions": obs.dimensions,
                    },
                    obs_family_id=fam_id,
                )
                for obs in observations
            ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for key, cfg in self.series_configs.items():
                future = executor.submit(_fetch_one, key, cfg)
                future_to_key[future] = key

            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    results.append(future.result())
                except Exception:
                    errors.append(key)
                    logger.warning("OECD parallel refresh failed for %s", key, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="oecd", count=count)

    def refresh_catalog_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = None,
        max_workers: int = 2,
        request_delay: float = 3.0,
        latest_observations: int = 1,
        updated_after: str | None = None,
        family_lookup: dict[tuple[str, str], str] | None = None,
        chunked: bool = True,
        obs_threshold: int = 1_000_000,
    ) -> RefreshStats:
        """Fetch catalog data from multiple dataflows in parallel.

        OECD enforces 60 data downloads/hour per IP.  Catalog fetches
        are heavier so defaults are more conservative: 2 workers, 3 s
        between requests.

        Pass ``updated_after`` (ISO-8601 datetime, e.g. '2026-03-01T00:00:00Z')
        to only fetch data that changed since your last sync.  Most OECD
        datasets update annually or monthly, so this dramatically reduces
        request volume for recurring ingestion.

        When ``chunked=True`` (default), datasets estimated to exceed
        ``obs_threshold`` observations are automatically split into
        decade-sized time-range queries to avoid timeouts and memory
        exhaustion on large cubes (e.g. SNA_TABLE1, ICIO).
        """
        dataflows = self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        )
        limiter = _OECDRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_with_retry(
            fetch_fn: Callable[[], list[Any]],
            label: str,
        ) -> list[Any]:
            max_retries = 5
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    return fetch_fn()
                except OECDRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(10.0, request_delay) * (2 ** attempt)
                        logger.warning(
                            "OECD 429 for %s, retry %d backing off %.1fs",
                            label, attempt + 1, backoff,
                        )
                        limiter.backoff(backoff)
                        continue
                    raise
            return []  # unreachable, but satisfies type checker

        def _fetch_dataflow(dataflow: Any) -> list[IndicatorObservationRecord]:
            label = f"{dataflow.agency_id}/{dataflow.id}"
            if chunked:
                observations = self.client.fetch_dataset_chunked(
                    dataflow.id,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    key="all",
                    series_id=None,
                    limit=latest_observations,
                    obs_threshold=obs_threshold,
                )
            else:
                observations = _fetch_with_retry(
                    lambda: self.client.fetch_data(
                        dataflow.id,
                        agency_id=dataflow.agency_id,
                        version=dataflow.version,
                        key="all",
                        series_id=None,
                        updated_after=updated_after,
                        limit=latest_observations,
                    ),
                    label,
                )
            records: list[IndicatorObservationRecord] = []
            for obs in observations:
                fam_id = family_lookup.get(("oecd", obs.series_id)) if family_lookup else None
                records.append(
                    IndicatorObservationRecord(
                        series_id=obs.series_id,
                        source="oecd",
                        date=obs.date,
                        value=obs.value,
                        metadata={
                            "category": "catalog",
                            "dataflow": obs.dataflow,
                            "dataflow_name": dataflow.name,
                            "dataflow_description": dataflow.description,
                            "agency_id": obs.agency_id or dataflow.agency_id,
                            "series_key": obs.series_key,
                            "raw_series_key": obs.raw_series_key,
                            "dimensions": obs.dimensions,
                        },
                        obs_family_id=fam_id,
                    )
                )
            return records

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for dataflow in dataflows:
                future = executor.submit(_fetch_dataflow, dataflow)
                future_to_id[future] = f"{dataflow.agency_id}/{dataflow.id}"

            for future in concurrent.futures.as_completed(future_to_id):
                label = future_to_id[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("OECD catalog parallel refresh failed for %s", label, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="oecd_catalog", count=count)


UNSD_SERIES: dict[str, dict[str, Any]] = {
    # Initial series — exact dataflow IDs to be confirmed by live API probing
    # The UNSD SDMX catalog uses agency-qualified dataflows
}


class UNSDIngestionClient:
    def __init__(self) -> None:
        from analyst.ingestion.scrapers.unsd import UNSDClient
        self.client = UNSDClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in UNSD_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg.get("key", "all"),
                    series_id=cfg["series_id"],
                    agency_id=cfg.get("agency_id", ""),
                    limit=30,
                )
                fam_id = family_lookup.get(("unsd", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="unsd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": cfg.get("category", ""),
                                "dataflow": obs.dataflow,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("UNSD refresh failed for %s", key, exc_info=True)
            time.sleep(1.0)
        return RefreshStats(source="unsd", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows()
        if query:
            needle = query.lower().strip()
            dataflows = [
                df for df in dataflows
                if needle in df.id.lower()
                or needle in df.name.lower()
                or needle in df.description.lower()
            ]
        dataflows.sort(key=lambda item: (item.agency_id, item.id))
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            allowed = set(dataflow_ids)
            matches = [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ]
            return matches[:limit] if limit is not None else matches
        return self.list_catalog_dataflows(query=query, limit=limit)

    def get_structure_summary(self, dataflow_id: str) -> Any:
        return self.client.summarize_structure(dataflow_id)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, agency_id=dataflow.agency_id)
                if est.total_series > 10_000_000:
                    logger.info("Skipping UNSD %s (estimated %d series)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.agency_id}_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "agency_id": dataflow.agency_id,
                "key": "all",
                "series_id": f"UNSD_AUTO_{dataflow.id.upper()}",
                "category": category,
            }
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.5,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    "all",
                    series_id=f"UNSD_{dataflow.id.upper()}",
                    agency_id=dataflow.agency_id,
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("unsd", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="unsd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                                "agency_id": dataflow.agency_id,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("UNSD catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="unsd", count=count)


class WorldBankIngestionClient:
    def __init__(
        self,
        client: WorldBankClient | None = None,
        *,
        series_configs: dict[str, WorldBankSeriesConfig] | None = None,
    ) -> None:
        self.client = client or WorldBankClient()
        self.series_configs = series_configs or WORLDBANK_SERIES

    # -- configured series refresh (backward compatible) ---------------------

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in self.series_configs.items():
            try:
                observations = self.client.get_indicator(
                    cfg.indicator,
                    cfg.country,
                    series_id=cfg.series_id,
                    limit=cfg.limit,
                )
                fam_id = family_lookup.get(("worldbank", cfg.series_id)) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="worldbank",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg.category, "indicator": obs.indicator},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("World Bank refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="worldbank", count=count)

    # -- parallel configured series refresh ----------------------------------

    def refresh_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
        max_workers: int = 4,
        request_delay: float = 0.3,
    ) -> RefreshStats:
        """Fetch all configured series using a thread pool."""
        limiter = _WorldBankRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_one(key: str, cfg: WorldBankSeriesConfig) -> list[IndicatorObservationRecord]:
            max_retries = 3
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    observations = self.client.get_indicator(
                        cfg.indicator,
                        cfg.country,
                        series_id=cfg.series_id,
                        start_year=cfg.start_year,
                        limit=cfg.limit,
                    )
                    break
                except WorldBankRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(5.0, request_delay) * (2 ** attempt)
                        logger.warning("World Bank 429 for %s, retry %d backing off %.1fs", key, attempt + 1, backoff)
                        limiter.backoff(backoff)
                        continue
                    raise
            fam_id = family_lookup.get(("worldbank", cfg.series_id)) if family_lookup else None
            return [
                IndicatorObservationRecord(
                    series_id=obs.series_id,
                    source="worldbank",
                    date=obs.date,
                    value=obs.value,
                    metadata={"category": cfg.category, "indicator": obs.indicator},
                    obs_family_id=fam_id,
                )
                for obs in observations
            ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for key, cfg in self.series_configs.items():
                future = executor.submit(_fetch_one, key, cfg)
                future_to_key[future] = key

            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("World Bank parallel refresh failed for %s", key, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="worldbank", count=count)

    # -- catalog discovery proxies -------------------------------------------

    def list_catalog_sources(self) -> list[Any]:
        return self.client.list_sources()

    def list_catalog_topics(self) -> list[Any]:
        return self.client.list_topics()

    def list_catalog_countries(self) -> list[Any]:
        return self.client.list_countries()

    def list_catalog_indicators(
        self,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if query:
            indicators = self.client.search_indicators(
                query, source_id=source_id, topic_id=topic_id, limit=limit or 50,
            )
        else:
            indicators = self.client.list_indicators(source_id=source_id, topic_id=topic_id)
            if limit:
                indicators = indicators[:limit]
        return indicators

    # -- config generation ---------------------------------------------------

    def generate_catalog_series_configs(
        self,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        category: str = "catalog",
    ) -> dict[str, WorldBankSeriesConfig]:
        """Auto-discover indicators and generate series configs."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        generated: dict[str, WorldBankSeriesConfig] = {}
        for ind in indicators:
            country = ";".join(countries) if countries else "all"
            config = WorldBankSeriesConfig(
                indicator=ind.id,
                series_id=_generated_wb_series_id(ind.id, country),
                category=category,
                country=country,
                source_id=ind.source_id,
            )
            generated[_generated_wb_config_key(ind.id, country)] = config
        return generated

    # -- catalog refresh (sequential) ----------------------------------------

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        latest_observations: int = 5,
        sleep_seconds: float = 0.3,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        """Discover indicators from catalog and fetch their latest data."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        country_str = ";".join(countries) if countries else "all"
        count = 0
        for ind in indicators:
            try:
                observations = self.client.get_indicator(
                    ind.id,
                    country_str,
                    series_id=f"WB_{ind.id}",
                    limit=latest_observations * 300 if country_str == "all" else latest_observations,
                    per_page=1000,
                    fetch_all_pages=country_str == "all",
                )
                for obs in observations:
                    sid = f"WB_{ind.id}_{obs.country_code}" if obs.country_code else f"WB_{ind.id}"
                    fam_id = family_lookup.get(("worldbank", sid)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=sid,
                            source="worldbank",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "indicator": ind.id,
                                "indicator_name": ind.name,
                                "source_name": ind.source_name,
                                "country_code": obs.country_code,
                                "country_name": obs.country_name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("World Bank catalog refresh failed for %s", ind.id, exc_info=True)
            time.sleep(max(sleep_seconds, 0.0))
        return RefreshStats(source="worldbank_catalog", count=count)

    # -- catalog refresh (parallel) ------------------------------------------

    def refresh_catalog_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        max_workers: int = 3,
        request_delay: float = 0.3,
        latest_observations: int = 5,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        """Fetch catalog data from multiple indicators in parallel."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        country_str = ";".join(countries) if countries else "all"
        limiter = _WorldBankRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_indicator(ind: Any) -> list[IndicatorObservationRecord]:
            max_retries = 3
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    observations = self.client.get_indicator(
                        ind.id,
                        country_str,
                        series_id=f"WB_{ind.id}",
                        limit=latest_observations * 300 if country_str == "all" else latest_observations,
                        per_page=1000,
                        fetch_all_pages=country_str == "all",
                    )
                    break
                except WorldBankRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(5.0, request_delay) * (2 ** attempt)
                        logger.warning(
                            "World Bank 429 for %s, retry %d backing off %.1fs",
                            ind.id, attempt + 1, backoff,
                        )
                        limiter.backoff(backoff)
                        continue
                    raise
            records: list[IndicatorObservationRecord] = []
            for obs in observations:
                sid = f"WB_{ind.id}_{obs.country_code}" if obs.country_code else f"WB_{ind.id}"
                fam_id = family_lookup.get(("worldbank", sid)) if family_lookup else None
                records.append(
                    IndicatorObservationRecord(
                        series_id=sid,
                        source="worldbank",
                        date=obs.date,
                        value=obs.value,
                        metadata={
                            "category": "catalog",
                            "indicator": ind.id,
                            "indicator_name": ind.name,
                            "source_name": ind.source_name,
                            "country_code": obs.country_code,
                            "country_name": obs.country_name,
                        },
                        obs_family_id=fam_id,
                    )
                )
            return records

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for ind in indicators:
                future = executor.submit(_fetch_indicator, ind)
                future_to_id[future] = ind.id

            for future in concurrent.futures.as_completed(future_to_id):
                label = future_to_id[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("World Bank catalog parallel refresh failed for %s", label, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="worldbank_catalog", count=count)


class FedIngestionClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "AnalystEngine/1.0"})

    def refresh(self, store: SQLiteEngineStore, *, fetch_full_text: bool = False) -> RefreshStats:
        communications = self.fetch_communications(fetch_full_text=fetch_full_text)
        return RefreshStats(source="fed", count=self.store_communications(store, communications))

    def fetch_communications(self, *, fetch_full_text: bool = False) -> list[CentralBankCommunicationRecord]:
        communications: list[CentralBankCommunicationRecord] = []
        for feed in FED_FEEDS.values():
            communications.extend(
                self._parse_feed(feed["url"], feed["content_type"], fetch_full_text=fetch_full_text)
            )
            time.sleep(0.5)
        return communications

    def store_communications(
        self,
        store: SQLiteEngineStore,
        communications: list[CentralBankCommunicationRecord],
    ) -> int:
        for communication in communications:
            store.upsert_central_bank_comm(communication)
        return len(communications)

    def _parse_feed(
        self,
        feed_url: str,
        content_type: str,
        *,
        fetch_full_text: bool,
    ) -> list[CentralBankCommunicationRecord]:
        communications: list[CentralBankCommunicationRecord] = []
        parsed = feedparser.parse(feed_url)
        for entry in parsed.entries:
            ts = 0
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                ts = int(datetime(*entry.published_parsed[:6], tzinfo=UTC).timestamp())
            title = entry.get("title", "")
            url = entry.get("link", "")
            summary = BeautifulSoup(entry.get("summary", ""), "html.parser").get_text(" ", strip=True)
            full_text = summary
            if fetch_full_text and url:
                full_text = self.fetch_full_text(url) or summary
            communications.append(
                CentralBankCommunicationRecord(
                    source="fed",
                    title=title,
                    url=url,
                    timestamp=ts or int(datetime.now(UTC).timestamp()),
                    content_type=self._detect_content_type(title, content_type),
                    speaker=extract_speaker(title),
                    summary=summary,
                    full_text=full_text,
                )
            )
        return communications

    def fetch_full_text(self, url: str) -> str:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        content_div = (
            soup.find("div", {"id": "article"})
            or soup.find("div", {"class": "col-xs-12 col-sm-8 col-md-8"})
            or soup.find("div", {"class": "row"})
        )
        if content_div is None:
            return ""
        for tag in content_div.find_all(["script", "style", "nav"]):
            tag.decompose()
        text = content_div.get_text(separator="\n", strip=True)
        return re.sub(r"\n{3,}", "\n\n", text)[:50_000]

    def _detect_content_type(self, title: str, fallback: str) -> str:
        lowered = title.lower()
        if "minutes" in lowered:
            return "minutes"
        if "statement" in lowered or "fomc" in lowered:
            return "statement"
        if "beige book" in lowered:
            return "beige_book"
        if "testimony" in lowered:
            return "testimony"
        return fallback


class MarketPriceClient:
    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        prices = self.fetch_prices()
        return RefreshStats(source="market", count=self.store_prices(store, prices))

    def fetch_prices(self) -> list[MarketPriceRecord]:
        prices: list[MarketPriceRecord] = []
        now_epoch = int(datetime.now(UTC).timestamp())
        for asset_class, symbols in MACRO_WATCHLIST.items():
            for symbol, name in symbols.items():
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.fast_info
                    price = info.get("lastPrice", info.get("previousClose"))
                    previous_close = info.get("previousClose")
                    if price is None:
                        history = ticker.history(period="2d")
                        if history.empty:
                            continue
                        price = float(history["Close"].iloc[-1])
                        previous_close = float(history["Close"].iloc[-2]) if len(history) > 1 else None
                    change_pct = None
                    if previous_close not in {None, 0}:
                        change_pct = round((float(price) - float(previous_close)) / float(previous_close) * 100, 2)
                    prices.append(
                        MarketPriceRecord(
                            symbol=symbol,
                            asset_class=asset_class,
                            name=name,
                            price=float(price),
                            change_pct=change_pct,
                            timestamp=now_epoch,
                        )
                    )
                except Exception:
                    continue
                time.sleep(0.1)
        return prices

    def store_prices(self, store: SQLiteEngineStore, prices: list[MarketPriceRecord]) -> int:
        for price in prices:
            store.insert_market_price(price)
        return len(prices)


@dataclass(frozen=True)
class RawNewsEntry:
    source_feed: str
    feed_category: str
    title: str
    url: str
    description: str
    timestamp: int


@dataclass(frozen=True)
class PreparedNewsRecord:
    source_feed: str
    feed_category: str
    description: str
    timestamp: int
    canonical_url: str
    raw_url: str
    raw_title: str
    url_hash: str
    title_hash: str


@dataclass(frozen=True)
class RedditTrendSourceConfig:
    subreddit: str
    category: str
    region: str = "global"


@dataclass(frozen=True)
class RawTrendEntry:
    subreddit: str
    category: str
    region: str
    provider_topic_id: str
    title_raw: str
    url: str
    score: int
    comment_count: int
    provider_rank: int
    observed_at: int
    is_stickied: bool
    is_nsfw: bool
    raw_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PreparedTrendRecord:
    trend_id: str
    provider: str
    provider_topic_id: str
    title_raw: str
    topic: str
    summary: str
    keywords: list[str]
    category: str
    region: str
    popularity_score: float
    provider_rank: int
    engagement_score: float
    comment_count: int
    observed_at: int
    expires_at: int
    raw_json: dict[str, Any]
    normalized_topic_hash: str


_REDDIT_TREND_SOURCES: tuple[RedditTrendSourceConfig, ...] = (
    RedditTrendSourceConfig(subreddit="technology", category="technology"),
    RedditTrendSourceConfig(subreddit="artificial", category="technology"),
    RedditTrendSourceConfig(subreddit="economics", category="business"),
    RedditTrendSourceConfig(subreddit="stocks", category="business"),
    RedditTrendSourceConfig(subreddit="investing", category="business"),
    RedditTrendSourceConfig(subreddit="worldnews", category="news"),
    RedditTrendSourceConfig(subreddit="news", category="news"),
    RedditTrendSourceConfig(subreddit="movies", category="entertainment"),
    RedditTrendSourceConfig(subreddit="television", category="entertainment"),
)

_TREND_KEYWORD_STOPWORDS = {
    "about",
    "after",
    "against",
    "also",
    "among",
    "because",
    "before",
    "being",
    "between",
    "could",
    "discussion",
    "from",
    "have",
    "into",
    "just",
    "more",
    "most",
    "news",
    "over",
    "post",
    "reddit",
    "should",
    "some",
    "still",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "today",
    "topic",
    "users",
    "what",
    "when",
    "where",
    "which",
    "while",
    "with",
    "would",
}

_WEIBO_CATEGORY_MAP = {
    "互联网": "technology",
    "体育": "sports",
    "作品衍生": "entertainment",
    "剧集": "entertainment",
    "国内时政": "news",
    "国际时政": "news",
    "幽默": "culture",
    "情感": "lifestyle",
    "民生新闻": "news",
    "汽车": "business",
    "突发/灾害": "news",
    "综艺": "entertainment",
    "美食": "lifestyle",
    "财经": "business",
    "音乐": "entertainment",
}


class RedditTrendIngestionClient:
    def __init__(
        self,
        *,
        client: RedditTrendClient | None = None,
        source_configs: tuple[RedditTrendSourceConfig, ...] = _REDDIT_TREND_SOURCES,
        max_items_per_subreddit: int = 25,
        ttl_hours: int = 48,
    ) -> None:
        self._client = client or RedditTrendClient()
        self._source_configs = source_configs
        self._max_items_per_subreddit = max_items_per_subreddit
        self._ttl_hours = ttl_hours

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        entries = self.fetch_entries()
        normalized = self.normalize_entries(entries)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(valid)
        return RefreshStats(source="reddit_trends", count=self.store_topics(store, deduplicated))

    def fetch_entries(self) -> list[RawTrendEntry]:
        entries: list[RawTrendEntry] = []
        for source in self._source_configs:
            posts = self._client.fetch_hot_posts(
                source.subreddit,
                limit=self._max_items_per_subreddit,
            )
            for provider_rank, post in enumerate(posts, start=1):
                if post.is_stickied or post.is_nsfw or not post.title.strip():
                    continue
                entries.append(self._to_raw_entry(post, source=source, provider_rank=provider_rank))
            time.sleep(0.2)
        return entries

    def normalize_entries(self, entries: list[RawTrendEntry]) -> list[PreparedTrendRecord]:
        normalized: list[PreparedTrendRecord] = []
        now_ts = int(datetime.now(timezone.utc).timestamp())
        ttl_seconds = max(self._ttl_hours, 1) * 3600
        for entry in entries:
            topic = self._clean_title(entry.title_raw)
            normalized_topic = self._normalize_topic_text(topic)
            normalized_hash = hashlib.sha256(normalized_topic.encode("utf-8")).hexdigest()
            observed_at = entry.observed_at if entry.observed_at > 0 else now_ts
            keywords = self._extract_keywords(normalized_topic)
            engagement_score = self._engagement_score(entry)
            popularity_score = self._popularity_score(
                entry,
                engagement_score=engagement_score,
                now_ts=now_ts,
            )
            normalized.append(
                PreparedTrendRecord(
                    trend_id=f"reddit:{normalized_hash}",
                    provider="reddit",
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=topic,
                    summary=self._build_summary(topic, category=entry.category, region=entry.region),
                    keywords=keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=round(popularity_score, 2),
                    provider_rank=entry.provider_rank,
                    engagement_score=round(engagement_score, 2),
                    comment_count=max(entry.comment_count, 0),
                    observed_at=observed_at,
                    expires_at=observed_at + ttl_seconds,
                    raw_json={
                        "subreddit": entry.subreddit,
                        "url": entry.url,
                        "score": entry.score,
                        "comment_count": entry.comment_count,
                        "provider_rank": entry.provider_rank,
                        **entry.raw_json,
                    },
                    normalized_topic_hash=normalized_hash,
                )
            )
        return normalized

    def validate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        valid: list[PreparedTrendRecord] = []
        for entry in entries:
            if len(entry.topic.strip()) < 12:
                continue
            if not entry.provider_topic_id.strip():
                continue
            if not entry.normalized_topic_hash.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        best_by_topic: dict[str, PreparedTrendRecord] = {}
        for entry in entries:
            existing = best_by_topic.get(entry.normalized_topic_hash)
            if existing is None or self._sort_key(entry) > self._sort_key(existing):
                best_by_topic[entry.normalized_topic_hash] = entry
        return sorted(best_by_topic.values(), key=self._sort_key, reverse=True)

    def store_topics(self, store: SQLiteEngineStore, entries: list[PreparedTrendRecord]) -> int:
        for entry in entries:
            store.upsert_trend_topic(
                TrendTopicRecord(
                    trend_id=entry.trend_id,
                    provider=entry.provider,
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=entry.topic,
                    summary=entry.summary,
                    keywords=entry.keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=entry.popularity_score,
                    provider_rank=entry.provider_rank,
                    engagement_score=entry.engagement_score,
                    comment_count=entry.comment_count,
                    observed_at=entry.observed_at,
                    expires_at=entry.expires_at,
                    raw_json=entry.raw_json,
                    normalized_topic_hash=entry.normalized_topic_hash,
                )
            )
        return len(entries)

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _to_raw_entry(
        post: RedditTrendPost,
        *,
        source: RedditTrendSourceConfig,
        provider_rank: int,
    ) -> RawTrendEntry:
        return RawTrendEntry(
            subreddit=source.subreddit,
            category=source.category,
            region=source.region,
            provider_topic_id=post.post_id,
            title_raw=post.title,
            url=post.permalink,
            score=post.score,
            comment_count=post.num_comments,
            provider_rank=provider_rank,
            observed_at=post.created_utc,
            is_stickied=post.is_stickied,
            is_nsfw=post.is_nsfw,
            raw_json=post.raw_json,
        )

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = title.strip()
        while cleaned:
            updated = re.sub(r"^\s*(?:\[[^\]]{1,40}\]|\([^)]{1,40}\)|\{[^}]{1,40}\})\s*", "", cleaned).strip()
            if updated == cleaned:
                break
            cleaned = updated
        cleaned = re.sub(r"([!?.,])\1+", r"\1", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip(" -|:")

    @staticmethod
    def _normalize_topic_text(title: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    @staticmethod
    def _extract_keywords(normalized_topic: str) -> list[str]:
        keywords: list[str] = []
        for token in re.findall(r"[a-z0-9]+", normalized_topic):
            if len(token) < 3 or token.isdigit():
                continue
            if token in _TREND_KEYWORD_STOPWORDS:
                continue
            if token in keywords:
                continue
            keywords.append(token)
            if len(keywords) == 5:
                break
        return keywords

    @staticmethod
    def _build_summary(topic: str, *, category: str, region: str) -> str:
        if region and region.lower() != "global":
            return f"{topic} is drawing heavy discussion in {region.lower()} {category} conversations."
        if category:
            return f"{topic} is drawing heavy discussion in {category} conversations."
        return f"{topic} is drawing heavy discussion right now."

    @staticmethod
    def _engagement_score(entry: RawTrendEntry) -> float:
        score_component = math.log1p(max(entry.score, 0)) * 10.0
        comments_component = math.log1p(max(entry.comment_count, 0)) * 8.0
        return min(55.0, score_component + comments_component)

    @staticmethod
    def _popularity_score(
        entry: RawTrendEntry,
        *,
        engagement_score: float,
        now_ts: int,
    ) -> float:
        rank_bonus = max(0.0, 30.0 - max(entry.provider_rank - 1, 0) * 1.2)
        age_hours = max((now_ts - max(entry.observed_at, 0)) / 3600.0, 0.0)
        recency_bonus = max(0.0, 15.0 - min(age_hours, 15.0))
        return min(100.0, engagement_score + rank_bonus + recency_bonus)

    @staticmethod
    def _sort_key(entry: PreparedTrendRecord) -> tuple[float, int, int, str]:
        return (
            entry.popularity_score,
            entry.observed_at,
            -entry.provider_rank,
            entry.topic.lower(),
        )


class WeiboTrendIngestionClient:
    def __init__(
        self,
        *,
        client: WeiboTrendClient | None = None,
        ttl_hours: int = 48,
    ) -> None:
        self._client = client or WeiboTrendClient()
        self._ttl_hours = ttl_hours

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        items = self.fetch_entries()
        normalized = self.normalize_entries(items)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(valid)
        return RefreshStats(source="weibo_trends", count=self.store_topics(store, deduplicated))

    def fetch_entries(self) -> list[WeiboTrendItem]:
        return [item for item in self._client.fetch_hot_band() if item.topic_flag != 0]

    def normalize_entries(self, items: list[WeiboTrendItem]) -> list[PreparedTrendRecord]:
        normalized: list[PreparedTrendRecord] = []
        now_ts = int(datetime.now(timezone.utc).timestamp())
        ttl_seconds = max(self._ttl_hours, 1) * 3600
        for item in items:
            topic = self._clean_title(item.note or item.word)
            normalized_topic = self._normalize_topic_text(topic)
            if not normalized_topic:
                continue
            normalized_hash = hashlib.sha256(normalized_topic.encode("utf-8")).hexdigest()
            observed_at = item.onboard_time if item.onboard_time > 0 else now_ts
            popularity_score = self._popularity_score(item, now_ts=now_ts)
            normalized.append(
                PreparedTrendRecord(
                    trend_id=f"weibo:{normalized_hash}",
                    provider="weibo",
                    provider_topic_id=item.word_scheme or item.word,
                    title_raw=item.note or item.word,
                    topic=topic,
                    summary=self._build_summary(topic, category=self._map_category(item.category)),
                    keywords=self._extract_keywords(topic),
                    category=self._map_category(item.category),
                    region="china",
                    popularity_score=round(popularity_score, 2),
                    provider_rank=max(item.realpos, 0),
                    engagement_score=round(self._engagement_score(item), 2),
                    comment_count=0,
                    observed_at=observed_at,
                    expires_at=observed_at + ttl_seconds,
                    raw_json=item.raw_json,
                    normalized_topic_hash=normalized_hash,
                )
            )
        return normalized

    def validate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        valid: list[PreparedTrendRecord] = []
        for entry in entries:
            if len(entry.topic.strip()) < 2:
                continue
            if not entry.provider_topic_id.strip():
                continue
            if not entry.normalized_topic_hash.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        best_by_topic: dict[str, PreparedTrendRecord] = {}
        for entry in entries:
            existing = best_by_topic.get(entry.normalized_topic_hash)
            if existing is None or self._sort_key(entry) > self._sort_key(existing):
                best_by_topic[entry.normalized_topic_hash] = entry
        return sorted(best_by_topic.values(), key=self._sort_key, reverse=True)

    def store_topics(self, store: SQLiteEngineStore, entries: list[PreparedTrendRecord]) -> int:
        for entry in entries:
            store.upsert_trend_topic(
                TrendTopicRecord(
                    trend_id=entry.trend_id,
                    provider=entry.provider,
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=entry.topic,
                    summary=entry.summary,
                    keywords=entry.keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=entry.popularity_score,
                    provider_rank=entry.provider_rank,
                    engagement_score=entry.engagement_score,
                    comment_count=entry.comment_count,
                    observed_at=entry.observed_at,
                    expires_at=entry.expires_at,
                    raw_json=entry.raw_json,
                    normalized_topic_hash=entry.normalized_topic_hash,
                )
            )
        return len(entries)

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = title.strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = re.sub(r"[#]+", "", cleaned)
        return cleaned.strip(" -|:")

    @staticmethod
    def _normalize_topic_text(title: str) -> str:
        normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", title)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.casefold().strip()

    @staticmethod
    def _extract_keywords(title: str) -> list[str]:
        keywords: list[str] = []
        for token in re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]{2,}", title):
            normalized = token.casefold()
            if normalized.isascii():
                if len(normalized) < 3 or normalized.isdigit():
                    continue
                if normalized in _TREND_KEYWORD_STOPWORDS:
                    continue
                value = normalized
            else:
                value = token.strip()
                if len(value) < 2:
                    continue
            if value in keywords:
                continue
            keywords.append(value)
            if len(keywords) == 5:
                break
        return keywords or [title[:16]]

    @staticmethod
    def _map_category(category: str) -> str:
        mapped = _WEIBO_CATEGORY_MAP.get(category.strip(), "")
        return mapped or "news"

    @staticmethod
    def _build_summary(topic: str, *, category: str) -> str:
        return f"{topic} is trending on Weibo in China's {category} conversations."

    @staticmethod
    def _engagement_score(item: WeiboTrendItem) -> float:
        hot_component = math.log1p(max(item.num, 0)) * 10.0
        raw_hot_component = math.log1p(max(item.raw_hot, 0)) * 6.0
        label_bonus = 4.0 if item.label_name in {"新", "热", "沸", "爆"} else 0.0
        return min(70.0, hot_component + raw_hot_component + label_bonus)

    @classmethod
    def _popularity_score(cls, item: WeiboTrendItem, *, now_ts: int) -> float:
        engagement_score = cls._engagement_score(item)
        rank_bonus = max(0.0, 28.0 - max(item.realpos - 1, 0) * 1.0)
        age_hours = max((now_ts - max(item.onboard_time, 0)) / 3600.0, 0.0) if item.onboard_time else 0.0
        recency_bonus = max(0.0, 12.0 - min(age_hours, 12.0))
        return min(100.0, engagement_score + rank_bonus + recency_bonus)

    @staticmethod
    def _sort_key(entry: PreparedTrendRecord) -> tuple[float, int, int, str]:
        return (
            entry.popularity_score,
            entry.observed_at,
            -entry.provider_rank,
            entry.topic.casefold(),
        )


class NewsIngestionClient:
    def __init__(
        self,
        *,
        timeout: int = 15,
        max_items_per_feed: int = 10,
        article_timeout: int = 20,
        max_content_chars: int = 15_000,
    ) -> None:
        self._timeout = timeout
        self._max_items_per_feed = max_items_per_feed
        self._article_fetcher = ArticleFetcher(
            timeout=article_timeout,
            max_content_chars=max_content_chars,
        )
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        })

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        category: str | None = None,
    ) -> RefreshStats:
        entries = self.fetch_entries(category=category)
        normalized = self.normalize_entries(entries)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(store, valid)
        return RefreshStats(source="news", count=self.store_articles(store, deduplicated))

    def fetch_entries(self, *, category: str | None = None) -> list[RawNewsEntry]:
        entries: list[RawNewsEntry] = []
        for feed in get_feeds(category):
            try:
                resp = self._session.get(feed.url, timeout=self._timeout)
                resp.raise_for_status()
                parsed = feedparser.parse(resp.text)
            except Exception:
                continue

            for entry in parsed.entries[: self._max_items_per_feed]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                raw_desc = entry.get("summary", "") or entry.get("description", "")
                description = BeautifulSoup(raw_desc, "html.parser").get_text(" ", strip=True)
                ts = 0
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    ts = int(datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).timestamp())
                if not ts:
                    ts = int(datetime.now(timezone.utc).timestamp())
                entries.append(
                    RawNewsEntry(
                        source_feed=feed.name,
                        feed_category=feed.category,
                        title=title,
                        url=link,
                        description=description,
                        timestamp=ts,
                    )
                )
            time.sleep(0.3)
        return entries

    def normalize_entries(self, entries: list[RawNewsEntry]) -> list[PreparedNewsRecord]:
        normalized: list[PreparedNewsRecord] = []
        for entry in entries:
            try:
                canonical = canonicalize_url(entry.url)
                url_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                title_hash = content_hash(entry.title, entry.timestamp)
                normalized.append(
                    PreparedNewsRecord(
                        source_feed=entry.source_feed,
                        feed_category=entry.feed_category,
                        description=entry.description,
                        timestamp=entry.timestamp,
                        canonical_url=canonical,
                        raw_url=entry.url,
                        raw_title=entry.title,
                        url_hash=url_hash,
                        title_hash=title_hash,
                    )
                )
            except Exception:
                continue
        return normalized

    def validate_entries(self, entries: list[PreparedNewsRecord]) -> list[PreparedNewsRecord]:
        valid: list[PreparedNewsRecord] = []
        for entry in entries:
            if not entry.raw_title.strip():
                continue
            if not entry.raw_url.strip():
                continue
            if not entry.canonical_url.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(
        self,
        store: SQLiteEngineStore,
        entries: list[PreparedNewsRecord],
    ) -> list[PreparedNewsRecord]:
        deduplicator = Deduplicator(threshold=0.6)
        deduplicator.seed(store.get_recent_news_titles(hours=24))
        unique: list[PreparedNewsRecord] = []
        for entry in entries:
            if store.fingerprint_exists(url_hash=entry.url_hash, title_hash=entry.title_hash):
                continue
            if deduplicator.is_duplicate(entry.raw_title):
                continue
            unique.append(entry)
        return unique

    def store_articles(self, store: SQLiteEngineStore, entries: list[PreparedNewsRecord]) -> int:
        count = 0
        for entry in entries:
            try:
                article = self._article_fetcher.fetch_article(entry.raw_url, entry.description)
                extraction = extract_news_metadata(
                    title=entry.raw_title,
                    description=entry.description,
                    content_markdown=article.content,
                    source_feed=entry.source_feed,
                    feed_category=entry.feed_category,
                    published_at=format_epoch_iso(entry.timestamp),
                )
                record = NewsArticleRecord(
                    url_hash=entry.url_hash,
                    source_feed=entry.source_feed,
                    feed_category=entry.feed_category,
                    title=extraction.title,
                    url=entry.raw_url,
                    timestamp=entry.timestamp,
                    description=entry.description,
                    content_markdown=article.content,
                    impact_level=extraction.impact_level,
                    finance_category=extraction.finance_category,
                    confidence=extraction.confidence,
                    content_fetched=article.fetched,
                    institution=extraction.institution,
                    country=extraction.country,
                    market=extraction.market,
                    asset_class=extraction.asset_class,
                    sector=extraction.sector,
                    document_type=extraction.document_type,
                    event_type=extraction.event_type,
                    subject=extraction.subject,
                    subject_id=extraction.subject_id,
                    data_period=extraction.data_period,
                    contains_commentary=extraction.contains_commentary,
                    language=extraction.language,
                    authors=extraction.authors,
                    extraction_provider=extraction.extraction_provider,
                )
                store.upsert_news_article(record)
                store.insert_fingerprint(
                    url_hash=entry.url_hash,
                    title_hash=entry.title_hash,
                    canonical_url=entry.canonical_url,
                    raw_url=entry.raw_url,
                    title=entry.raw_title,
                    source_feed=entry.source_feed,
                )
                count += 1
                time.sleep(0.5)
            except Exception:
                continue
        return count

    def close(self) -> None:
        self._article_fetcher.close()


class GovReportIngestionClient:
    """Fetches government reports and stores them into the normalized
    document tables (doc_source / doc_release_family / document /
    document_blob / document_extra) **and** the legacy news_articles
    table so existing consumers keep working."""

    def __init__(self) -> None:
        self._client = GovReportClient()
        self._seeded = False

    def _ensure_seed(self, store: SQLiteEngineStore) -> None:
        if self._seeded:
            return
        from analyst.ingestion.scrapers.gov_report import (
            _US_SOURCES,
            _CN_SOURCES,
            _JP_SOURCES,
            _EU_SOURCES,
        )
        store.seed_doc_sources_and_families({
            "us": _US_SOURCES,
            "cn": _CN_SOURCES,
            "jp": _JP_SOURCES,
            "eu": _EU_SOURCES,
        })
        self._seeded = True

    @staticmethod
    def _gov_document_type(data_category: str) -> str:
        """Map data_category to a valid document.document_type value."""
        mapping = {
            "monetary_policy": "statement",
            "economic_conditions": "bulletin",
            "speeches": "speech",
            "press_releases": "press_release",
        }
        return mapping.get(data_category, "release")

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        items = self.fetch_items()
        return RefreshStats(source="gov_reports", count=self.store_items(store, items))

    def fetch_items(self) -> list[GovReportItem]:
        return self._client.fetch_all()

    def store_items(self, store: SQLiteEngineStore, items: list[GovReportItem]) -> int:
        self._ensure_seed(store)
        count = 0
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat()
        now_epoch_ms = int(now_dt.timestamp() * 1000)
        for item in items:
            try:
                canonical = canonicalize_url(item.url)
                url_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                published_precision = item.published_precision or _infer_publish_precision(item.published_at)
                try:
                    if published_precision == "exact" and item.published_at:
                        published_at = normalize_utc_iso(item.published_at)
                        published_epoch_ms = to_epoch_ms(item.published_at)
                    elif published_precision == "date_only" and item.published_at:
                        published_at = item.published_at[:10]
                        published_epoch_ms = to_epoch_ms(published_at)
                    else:
                        published_at = now_iso
                        published_epoch_ms = now_epoch_ms
                        published_precision = "estimated"
                except ValueError:
                    published_at = now_iso
                    published_epoch_ms = now_epoch_ms
                    published_precision = "estimated"
                published_date = published_at[:10]

                # --- Normalized document storage ---
                if not store.document_exists(item.url):
                    doc_id = url_hash[:16]
                    release_family_id = item.source_id.replace("_", ".")
                    parts = item.source_id.split("_")
                    source_key = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else item.source_id

                    doc = DocumentRecord(
                        document_id=doc_id,
                        release_family_id=release_family_id,
                        source_id=source_key,
                        canonical_url=item.url,
                        title=item.title,
                        subtitle="",
                        document_type=self._gov_document_type(item.data_category),
                        mime_type="text/html",
                        language_code=item.language,
                        country_code=item.country,
                        topic_code=item.data_category,
                        published_date=published_date,
                        published_at=published_at,
                        published_precision=published_precision,
                        status="published",
                        version_no=1,
                        parent_document_id="",
                        hash_sha256=url_hash,
                        created_at=now_iso,
                        updated_at=now_iso,
                        published_epoch_ms=published_epoch_ms,
                        created_epoch_ms=now_epoch_ms,
                        updated_epoch_ms=now_epoch_ms,
                    )
                    store.upsert_document(doc)

                    if item.content_markdown:
                        blob = DocumentBlobRecord(
                            document_blob_id=f"{doc_id}_md",
                            document_id=doc_id,
                            blob_role="markdown",
                            storage_path="",
                            content_text=item.content_markdown,
                            content_bytes=None,
                            byte_size=len(item.content_markdown.encode("utf-8")),
                            encoding="utf-8",
                            parser_name="markdownify",
                            parser_version="",
                            extracted_at=now_iso,
                        )
                        store.upsert_document_blob(blob)

                    if item.raw_json or item.importance:
                        extra_data = dict(item.raw_json) if item.raw_json else {}
                        extra_data["importance"] = item.importance
                        extra_data["institution"] = item.institution
                        extra_data["description"] = item.description
                        extra_data["published_precision"] = published_precision
                        store.upsert_document_extra(DocumentExtraRecord(
                            document_id=doc_id,
                            extra_json=extra_data,
                        ))

                # --- Legacy news_articles storage ---
                if store.news_article_exists(url_hash):
                    continue
                ts = int(published_epoch_ms / 1000)
                record = NewsArticleRecord(
                    url_hash=url_hash,
                    source_feed=f"gov_{item.source_id}",
                    feed_category="government",
                    title=item.title,
                    url=item.url,
                    timestamp=ts,
                    description=item.description,
                    content_markdown=item.content_markdown,
                    impact_level=item.importance or "medium",
                    finance_category=item.data_category,
                    confidence=0.8,
                    content_fetched=bool(item.content_markdown),
                    institution=item.institution,
                    country=item.country,
                    document_type="government_report",
                    language=item.language,
                )
                store.upsert_news_article(record)
                count += 1
            except Exception:
                logger.warning("Gov report storage failed: %s", item.source_id, exc_info=True)
                continue
        return count


class IngestionOrchestrator:
    def __init__(
        self,
        store: SQLiteEngineStore,
        *,
        fred: FREDIngestionClient | None = None,
        investing: InvestingCalendarClient | None = None,
        forexfactory: ForexFactoryCalendarClient | None = None,
        tradingeconomics: TradingEconomicsCalendarClient | None = None,
        fed: FedIngestionClient | None = None,
        market: MarketPriceClient | None = None,
        news: NewsIngestionClient | None = None,
        reddit_trends: RedditTrendIngestionClient | None = None,
        weibo_trends: WeiboTrendIngestionClient | None = None,
        rate_probability: RateProbabilityClient | None = None,
        nyfed: NYFedRatesClient | None = None,
        gov_report: GovReportIngestionClient | None = None,
        eia: EIAIngestionClient | None = None,
        treasury_fiscal: TreasuryFiscalIngestionClient | None = None,
        imf: IMFIngestionClient | None = None,
        eurostat: EurostatIngestionClient | None = None,
        bis: BISIngestionClient | None = None,
        ecb: ECBIngestionClient | None = None,
        oecd: OECDIngestionClient | None = None,
        worldbank: WorldBankIngestionClient | None = None,
        validation_engine: Any | None = None,
    ) -> None:
        self.store = store
        self._validation = validation_engine
        self.fred = fred or FREDIngestionClient()
        self.investing = investing or InvestingCalendarClient()
        self.forexfactory = forexfactory or ForexFactoryCalendarClient()
        self.tradingeconomics = tradingeconomics or TradingEconomicsCalendarClient()
        self.fed = fed or FedIngestionClient()
        self.market = market or MarketPriceClient()
        self.news = news or NewsIngestionClient()
        self.reddit_trends = reddit_trends or RedditTrendIngestionClient()
        self.weibo_trends = weibo_trends or WeiboTrendIngestionClient()
        self.rate_probability = rate_probability or RateProbabilityClient()
        self.nyfed = nyfed or NYFedRatesClient()
        self.gov_report = gov_report or GovReportIngestionClient()
        self.eia = eia or EIAIngestionClient()
        self.treasury_fiscal = treasury_fiscal or TreasuryFiscalIngestionClient()
        self.imf = imf or IMFIngestionClient()
        self.eurostat = eurostat or EurostatIngestionClient()
        self.bis = bis or BISIngestionClient()
        self.ecb = ecb or ECBIngestionClient()
        self.oecd = oecd or OECDIngestionClient()
        self.worldbank = worldbank or WorldBankIngestionClient()
        self._obs_seeded = False
        self._cal_seeded = False
        self._family_lookup: dict[tuple[str, str], str] = {}
        self._sources: dict[str, IngestionSourceDefinition] = {}
        self._default_refresh_order: list[str] = []
        self._last_run_reports: dict[str, IngestionRunReport] = {}
        self._register_default_sources()

    def _ensure_obs_seed(self) -> None:
        """Seed observation sources/families once, then build lookup cache."""
        if self._obs_seeded:
            return
        self.store.seed_obs_sources_and_families()
        self.store.backfill_obs_family_ids()
        self._family_lookup = self.store.build_obs_family_lookup()
        self._obs_seeded = True

    def _ensure_calendar_indicator_seed(self) -> None:
        if self._cal_seeded:
            return
        self.store.seed_calendar_indicators()
        self._cal_seeded = True

    def _resolve_calendar_indicator(self, event: StoredEventRecord) -> StoredEventRecord:
        indicator_id = self.store.resolve_calendar_alias(
            event.indicator, event.source, event.country
        )
        if indicator_id:
            return dataclasses.replace(event, indicator_id=indicator_id)
        return event

    def register_source(self, definition: IngestionSourceDefinition) -> None:
        self._sources[definition.name] = definition

    def list_sources(self) -> list[str]:
        return list(self._sources)

    def last_run_report(self, source: str) -> IngestionRunReport | None:
        return self._last_run_reports.get(source)

    def run_source(self, source: str) -> IngestionRunReport:
        definition = self._sources.get(source)
        if definition is None:
            raise KeyError(f"unknown ingestion source: {source}")
        return self._run_definition(definition)

    def _register_default_sources(self) -> None:
        definitions = [
            self._build_calendar_source(),
            self._build_fed_source(),
            self._build_market_source(),
            self._build_fred_daily_source(),
            self._build_fred_full_source(),
            self._build_news_source(),
            self._build_reddit_trends_source(),
            self._build_weibo_trends_source(),
            self._build_rate_probability_source(),
            self._build_fred_vintages_source(),
            self._build_nyfed_rates_source(),
            self._build_gov_reports_source(),
            self._build_eia_source(),
            self._build_treasury_fiscal_source(),
            self._build_imf_source(),
            self._build_imf_vintages_source(),
            self._build_eurostat_source(),
            self._build_bis_source(),
            self._build_ecb_source(),
            self._build_oecd_source(),
            self._build_worldbank_source(),
            self._build_worldbank_catalog_source(),
        ]
        for definition in definitions:
            self.register_source(definition)
        self._default_refresh_order = [
            "calendar",
            "fed",
            "market",
            "fred_daily",
            "news",
            "reddit_trends",
            "weibo_trends",
            "rate_probability",
            "nyfed_rates",
            "gov_reports",
            "eia",
            "treasury_fiscal",
            "imf",
            "imf_vintages",
            "eurostat",
            "bis",
            "ecb",
            "oecd",
            "worldbank",
            "worldbank_catalog",
        ]

    @staticmethod
    def _materialize_items(items: Iterable[Any] | None) -> list[Any]:
        if items is None:
            return []
        if isinstance(items, list):
            return items
        return list(items)

    def _run_stage(
        self,
        stage: Callable[[list[Any]], Iterable[Any]] | None,
        items: list[Any],
    ) -> list[Any]:
        if stage is None:
            return items
        return self._materialize_items(stage(items))

    def _run_definition(self, definition: IngestionSourceDefinition) -> IngestionRunReport:
        attempt = 0
        while True:
            started = time.perf_counter()
            try:
                if definition.prepare is not None:
                    definition.prepare()

                if definition.execute is not None:
                    stored = int(definition.execute())
                    report = IngestionRunReport(
                        source=definition.name,
                        stored=stored,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        retries=attempt,
                    )
                    report = self._run_validation(definition.name, report)
                    self._last_run_reports[definition.name] = report
                    return report

                if definition.fetch is None or definition.store is None:
                    raise ValueError(f"source {definition.name} is missing pipeline stages")

                raw_items = self._materialize_items(definition.fetch())
                normalized_items = self._run_stage(definition.normalize, raw_items)
                validated_items = self._run_stage(definition.validate, normalized_items)
                deduplicated_items = self._run_stage(definition.deduplicate, validated_items)
                stored = definition.store(deduplicated_items)
                report = IngestionRunReport(
                    source=definition.name,
                    stored=int(stored if stored is not None else len(deduplicated_items)),
                    fetched=len(raw_items),
                    normalized=len(normalized_items),
                    validated=len(validated_items),
                    deduplicated=len(deduplicated_items),
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    retries=attempt,
                )
                report = self._run_validation(definition.name, report)
                self._last_run_reports[definition.name] = report
                return report
            except Exception as exc:
                if attempt < definition.max_retries:
                    attempt += 1
                    if definition.retry_backoff_seconds > 0:
                        time.sleep(definition.retry_backoff_seconds * attempt)
                    continue
                logger.warning("%s ingestion failed", definition.name, exc_info=True)
                report = IngestionRunReport(
                    source=definition.name,
                    stored=0,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    retries=attempt,
                    error=str(exc),
                )
                self._last_run_reports[definition.name] = report
                return report

    def _run_validation(self, source_name: str, report: IngestionRunReport) -> IngestionRunReport:
        if self._validation is None:
            return report
        try:
            val_report = self._validation.validate_post_store(source_name, report)
            return dataclasses.replace(report, validation_report=val_report)
        except Exception:
            logger.warning("%s post-store validation failed", source_name, exc_info=True)
            return report

    @staticmethod
    def _deduplicate_by_key(items: list[Any], key_fn: Callable[[Any], Any]) -> list[Any]:
        unique: list[Any] = []
        seen: set[Any] = set()
        for item in items:
            marker = key_fn(item)
            if marker in seen:
                continue
            seen.add(marker)
            unique.append(item)
        return unique

    def _build_calendar_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="calendar",
            interval_seconds=3600,
            prepare=self._ensure_calendar_indicator_seed,
            fetch=self._fetch_calendar_events,
            normalize=self._normalize_calendar_events,
            validate=self._validate_calendar_events,
            deduplicate=self._deduplicate_calendar_events,
            store=self._store_calendar_events,
        )

    def _fetch_calendar_events(self) -> list[StoredEventRecord]:
        events: list[StoredEventRecord] = []
        providers = [
            ("Investing.com", lambda: self.investing.fetch_range(days_back=1, days_forward=3)),
            ("ForexFactory", self.forexfactory.fetch),
            ("TradingEconomics", self.tradingeconomics.fetch),
        ]
        for label, fetch_fn in providers:
            try:
                events.extend(list(fetch_fn()))
            except Exception:
                logger.warning("%s calendar refresh failed", label, exc_info=True)
        return events

    def _normalize_calendar_events(self, events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return [self._resolve_calendar_indicator(event) for event in events]

    @staticmethod
    def _validate_calendar_events(events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return [
            event for event in events
            if event.event_id and event.indicator and event.country and event.timestamp > 0
        ]

    def _deduplicate_calendar_events(self, events: list[StoredEventRecord]) -> list[StoredEventRecord]:
        return self._deduplicate_by_key(events, lambda event: (event.source, event.event_id))

    def _store_calendar_events(self, events: list[StoredEventRecord]) -> int:
        for event in events:
            self.store.upsert_calendar_event(event)
        return len(events)

    def _build_fed_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fed",
            interval_seconds=14_400,
            fetch=self.fed.fetch_communications,
            validate=self._validate_fed_communications,
            deduplicate=self._deduplicate_fed_communications,
            store=lambda items: self.fed.store_communications(self.store, items),
        )

    @staticmethod
    def _validate_fed_communications(
        communications: list[CentralBankCommunicationRecord],
    ) -> list[CentralBankCommunicationRecord]:
        return [item for item in communications if item.title.strip() and item.url.strip()]

    def _deduplicate_fed_communications(
        self,
        communications: list[CentralBankCommunicationRecord],
    ) -> list[CentralBankCommunicationRecord]:
        return self._deduplicate_by_key(
            communications,
            lambda item: (item.url, item.timestamp, item.title),
        )

    def _build_market_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="market",
            interval_seconds=1800,
            fetch=self.market.fetch_prices,
            validate=self._validate_market_prices,
            deduplicate=self._deduplicate_market_prices,
            store=lambda items: self.market.store_prices(self.store, items),
        )

    @staticmethod
    def _validate_market_prices(prices: list[MarketPriceRecord]) -> list[MarketPriceRecord]:
        return [price for price in prices if price.symbol and price.price > 0]

    def _deduplicate_market_prices(self, prices: list[MarketPriceRecord]) -> list[MarketPriceRecord]:
        return self._deduplicate_by_key(prices, lambda price: price.symbol)

    def _build_news_source(self, *, category: str | None = None) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="news",
            interval_seconds=300 if category is None else None,
            fetch=lambda: self.news.fetch_entries(category=category),
            normalize=self.news.normalize_entries,
            validate=self.news.validate_entries,
            deduplicate=lambda items: self.news.deduplicate_entries(self.store, items),
            store=lambda items: self.news.store_articles(self.store, items),
            max_retries=1,
            retry_backoff_seconds=1.0,
        )

    def _build_reddit_trends_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="reddit_trends",
            interval_seconds=3600,
            fetch=self.reddit_trends.fetch_entries,
            normalize=self.reddit_trends.normalize_entries,
            validate=self.reddit_trends.validate_entries,
            deduplicate=self.reddit_trends.deduplicate_entries,
            store=lambda items: self.reddit_trends.store_topics(self.store, items),
        )

    def _build_weibo_trends_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="weibo_trends",
            interval_seconds=3600,
            fetch=self.weibo_trends.fetch_entries,
            normalize=self.weibo_trends.normalize_entries,
            validate=self.weibo_trends.validate_entries,
            deduplicate=self.weibo_trends.deduplicate_entries,
            store=lambda items: self.weibo_trends.store_topics(self.store, items),
        )

    def _build_rate_probability_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="rate_probability",
            interval_seconds=3600,
            fetch=self._fetch_rate_probability_observations,
            deduplicate=self._deduplicate_observations,
            store=self._store_indicator_observations,
        )

    def _fetch_rate_probability_observations(self) -> list[IndicatorObservationRecord]:
        prob = self.rate_probability.fetch_probabilities()
        observations: list[IndicatorObservationRecord] = []
        for meeting in prob.meetings:
            observations.append(
                IndicatorObservationRecord(
                    series_id=f"FEDPROB_{meeting.meeting_date}",
                    source="rateprobability",
                    date=prob.as_of[:10] if len(prob.as_of) >= 10 else prob.as_of,
                    value=meeting.implied_rate,
                    metadata={
                        "prob_move_pct": meeting.prob_move_pct,
                        "is_cut": meeting.is_cut,
                        "num_moves": meeting.num_moves,
                        "change_bps": meeting.change_bps,
                        "current_band": prob.current_band,
                    },
                )
            )
        return observations

    def _build_fred_daily_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_daily",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_daily_series(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_fred_full_source(self, *, lookback_days: int = 365) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_full",
            interval_seconds=None,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_all_series(
                self.store,
                lookback_days=lookback_days,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_fred_vintages_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="fred_vintages",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.fred.refresh_vintages(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_nyfed_rates_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="nyfed_rates",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            fetch=self._fetch_nyfed_rate_observations,
            deduplicate=self._deduplicate_observations,
            store=self._store_indicator_observations,
        )

    def _fetch_nyfed_rate_observations(self) -> list[IndicatorObservationRecord]:
        observations: list[IndicatorObservationRecord] = []
        for rate in self.nyfed.fetch_all_rates(last_n=5):
            metadata: dict[str, Any] = {}
            if rate.percentile_1 is not None:
                metadata["percentile_1"] = rate.percentile_1
            if rate.percentile_25 is not None:
                metadata["percentile_25"] = rate.percentile_25
            if rate.percentile_75 is not None:
                metadata["percentile_75"] = rate.percentile_75
            if rate.percentile_99 is not None:
                metadata["percentile_99"] = rate.percentile_99
            if rate.volume_billions is not None:
                metadata["volume_billions"] = rate.volume_billions
            if rate.target_rate_from is not None:
                metadata["target_range"] = f"{rate.target_rate_from}-{rate.target_rate_to}"
            series_id = f"NYFED_{rate.type}"
            fam_id = self._family_lookup.get(("nyfed", series_id)) if self._family_lookup else None
            observations.append(
                IndicatorObservationRecord(
                    series_id=series_id,
                    source="nyfed",
                    date=rate.date,
                    value=rate.rate,
                    metadata=metadata,
                    obs_family_id=fam_id,
                )
            )
        return observations

    def _build_gov_reports_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="gov_reports",
            interval_seconds=21_600,
            fetch=self.gov_report.fetch_items,
            validate=self._validate_gov_report_items,
            deduplicate=self._deduplicate_gov_report_items,
            store=lambda items: self.gov_report.store_items(self.store, items),
        )

    @staticmethod
    def _validate_gov_report_items(items: list[GovReportItem]) -> list[GovReportItem]:
        return [item for item in items if item.title.strip() and item.url.strip() and item.source_id.strip()]

    def _deduplicate_gov_report_items(self, items: list[GovReportItem]) -> list[GovReportItem]:
        return self._deduplicate_by_key(items, lambda item: canonicalize_url(item.url))

    def _build_eia_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="eia",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.eia.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_treasury_fiscal_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="treasury_fiscal",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.treasury_fiscal.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_imf_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="imf",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.imf.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_imf_vintages_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="imf_vintages",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.imf.refresh_vintages(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_eurostat_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="eurostat",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.eurostat.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_bis_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="bis",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.bis.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_ecb_source(self) -> IngestionSourceDefinition:
        return IngestionSourceDefinition(
            name="ecb",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=lambda: self.ecb.refresh(
                self.store,
                family_lookup=self._family_lookup or None,
            ).count,
        )

    def _build_oecd_source(self) -> IngestionSourceDefinition:
        def _execute_oecd() -> int:
            lookup = self._family_lookup or None
            if len(self.oecd.series_configs) > 20:
                return self.oecd.refresh_parallel(
                    self.store, family_lookup=lookup,
                ).count
            return self.oecd.refresh(
                self.store, family_lookup=lookup,
            ).count

        return IngestionSourceDefinition(
            name="oecd",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=_execute_oecd,
        )

    def _build_worldbank_source(self) -> IngestionSourceDefinition:
        def _execute_worldbank() -> int:
            lookup = self._family_lookup or None
            if len(self.worldbank.series_configs) > 10:
                return self.worldbank.refresh_parallel(
                    self.store, family_lookup=lookup,
                ).count
            return self.worldbank.refresh(
                self.store, family_lookup=lookup,
            ).count

        return IngestionSourceDefinition(
            name="worldbank",
            interval_seconds=86_400,
            prepare=self._ensure_obs_seed,
            execute=_execute_worldbank,
        )

    def _build_worldbank_catalog_source(self) -> IngestionSourceDefinition:
        def _execute() -> int:
            lookup = self._family_lookup or None
            return self.worldbank.refresh_catalog_parallel(
                self.store, family_lookup=lookup,
            ).count

        return IngestionSourceDefinition(
            name="worldbank_catalog",
            interval_seconds=86_400 * 7,
            prepare=self._ensure_obs_seed,
            execute=_execute,
        )

    def _deduplicate_observations(
        self,
        observations: list[IndicatorObservationRecord],
    ) -> list[IndicatorObservationRecord]:
        return self._deduplicate_by_key(
            observations,
            lambda observation: (observation.source, observation.series_id, observation.date),
        )

    def _store_indicator_observations(self, observations: list[IndicatorObservationRecord]) -> int:
        for observation in observations:
            self.store.upsert_indicator_observation(observation)
        return len(observations)

    def refresh_calendar(self) -> dict[str, int]:
        return self.run_source("calendar").to_counts()

    def refresh_market(self) -> dict[str, int]:
        return self.run_source("market").to_counts()

    def refresh_fed(self) -> dict[str, int]:
        return self.run_source("fed").to_counts()

    def refresh_fred_daily(self) -> dict[str, int]:
        return self.run_source("fred_daily").to_counts()

    def refresh_fred_full(self, *, lookback_days: int = 365) -> dict[str, int]:
        if lookback_days == 365:
            return self.run_source("fred_full").to_counts()
        return self._run_definition(self._build_fred_full_source(lookback_days=lookback_days)).to_counts()

    def refresh_news(self, *, category: str | None = None) -> dict[str, int]:
        if category is None:
            return self.run_source("news").to_counts()
        return self._run_definition(self._build_news_source(category=category)).to_counts()

    def refresh_rate_probability(self) -> dict[str, int]:
        return self.run_source("rate_probability").to_counts()

    def refresh_fred_vintages(self) -> dict[str, int]:
        return self.run_source("fred_vintages").to_counts()

    def refresh_eia(self) -> dict[str, int]:
        return self.run_source("eia").to_counts()

    def refresh_treasury_fiscal(self) -> dict[str, int]:
        return self.run_source("treasury_fiscal").to_counts()

    def refresh_imf(self) -> dict[str, int]:
        return self.run_source("imf").to_counts()

    def refresh_imf_vintages(self) -> dict[str, int]:
        return self.run_source("imf_vintages").to_counts()

    def refresh_eurostat(self) -> dict[str, int]:
        return self.run_source("eurostat").to_counts()

    def refresh_bis(self) -> dict[str, int]:
        return self.run_source("bis").to_counts()

    def refresh_ecb(self) -> dict[str, int]:
        return self.run_source("ecb").to_counts()

    def refresh_oecd(self) -> dict[str, int]:
        return self.run_source("oecd").to_counts()

    def refresh_worldbank(self) -> dict[str, int]:
        return self.run_source("worldbank").to_counts()

    def refresh_worldbank_catalog(self) -> dict[str, int]:
        return self.run_source("worldbank_catalog").to_counts()

    def refresh_gov_reports(self) -> dict[str, int]:
        return self.run_source("gov_reports").to_counts()

    def refresh_nyfed_rates(self) -> dict[str, int]:
        return self.run_source("nyfed_rates").to_counts()

    def refresh_all(self) -> dict[str, int]:
        results: dict[str, int] = {}
        for source in self._default_refresh_order:
            results.update(self.run_source(source).to_counts())
        return results

    def run_schedule(self, *, poll_interval_seconds: int = 60) -> None:
        jobs = {
            name: definition
            for name, definition in self._sources.items()
            if definition.interval_seconds is not None
        }
        next_run = {name: 0.0 for name in jobs}
        self.refresh_all()
        while True:
            now = time.time()
            for job_name, job in jobs.items():
                if now >= next_run[job_name]:
                    self.run_source(job_name)
                    next_run[job_name] = now + float(job.interval_seconds or 0)
            time.sleep(poll_interval_seconds)
