from __future__ import annotations

import dataclasses
import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

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
from analyst.ingestion.scrapers.oecd import OECDClient
from analyst.ingestion.scrapers.worldbank import WorldBankClient
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

WORLDBANK_SERIES = {
    "gdp_pcap_us":   {"indicator": "NY.GDP.PCAP.PP.CD",  "country": "USA", "series_id": "WB_GDP_PCAP_US",   "category": "development"},
    "gdp_pcap_cn":   {"indicator": "NY.GDP.PCAP.PP.CD",  "country": "CHN", "series_id": "WB_GDP_PCAP_CN",   "category": "development"},
    "gdp_growth_us": {"indicator": "NY.GDP.MKTP.KD.ZG",  "country": "USA", "series_id": "WB_GDP_GROWTH_US", "category": "growth"},
    "ca_gdp_us":     {"indicator": "BN.CAB.XOKA.GD.ZS",  "country": "USA", "series_id": "WB_CA_GDP_US",     "category": "trade"},
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
            time.sleep(1.0)
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
        sleep_seconds: float = 1.2,
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


class WorldBankIngestionClient:
    def __init__(self) -> None:
        self.client = WorldBankClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in WORLDBANK_SERIES.items():
            try:
                observations = self.client.get_indicator(
                    cfg["indicator"],
                    cfg["country"],
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("worldbank", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="worldbank",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "indicator": obs.indicator},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("World Bank refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="worldbank", count=count)


class FedIngestionClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "AnalystEngine/1.0"})

    def refresh(self, store: SQLiteEngineStore, *, fetch_full_text: bool = False) -> RefreshStats:
        count = 0
        for feed in FED_FEEDS.values():
            for communication in self._parse_feed(feed["url"], feed["content_type"], fetch_full_text=fetch_full_text):
                store.upsert_central_bank_comm(communication)
                count += 1
            time.sleep(0.5)
        return RefreshStats(source="fed", count=count)

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
        count = 0
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
                    store.insert_market_price(
                        MarketPriceRecord(
                            symbol=symbol,
                            asset_class=asset_class,
                            name=name,
                            price=float(price),
                            change_pct=change_pct,
                            timestamp=now_epoch,
                        )
                    )
                    count += 1
                except Exception:
                    continue
                time.sleep(0.1)
        return RefreshStats(source="market", count=count)


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
        """Fetch -> extract articles -> LLM/keyword metadata -> store."""
        feeds = get_feeds(category)

        # Seed fuzzy deduplicator with recent titles
        deduplicator = Deduplicator(threshold=0.6)
        recent_titles = store.get_recent_news_titles(hours=24)
        deduplicator.seed(recent_titles)

        stored_count = 0
        for feed in feeds:
            try:
                resp = self._session.get(feed.url, timeout=self._timeout)
                resp.raise_for_status()
                parsed = feedparser.parse(resp.text)
            except Exception:
                continue

            entries = parsed.entries[: self._max_items_per_feed]
            for entry in entries:
                try:
                    title = entry.get("title", "").strip()
                    if not title:
                        continue

                    link = entry.get("link", "")
                    if not link:
                        continue

                    # Compute timestamp early — needed for content_hash
                    ts = 0
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        ts = int(datetime(
                            *entry.published_parsed[:6], tzinfo=timezone.utc
                        ).timestamp())
                    if not ts:
                        ts = int(datetime.now(timezone.utc).timestamp())

                    # Layer 1+2: fingerprint check (cheap, before HTTP fetch)
                    canonical = canonicalize_url(link)
                    url_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                    title_hash = content_hash(title, ts)
                    if store.fingerprint_exists(url_hash=url_hash, title_hash=title_hash):
                        continue

                    # Layer 3: fuzzy title check
                    if deduplicator.is_duplicate(title):
                        continue

                    raw_desc = entry.get("summary", "") or entry.get("description", "")
                    from bs4 import BeautifulSoup as _BS
                    description = _BS(raw_desc, "html.parser").get_text(" ", strip=True)

                    article = self._article_fetcher.fetch_article(link, description)
                    extraction = extract_news_metadata(
                        title=title,
                        description=description,
                        content_markdown=article.content,
                        source_feed=feed.name,
                        feed_category=feed.category,
                        published_at=format_epoch_iso(ts),
                    )

                    record = NewsArticleRecord(
                        url_hash=url_hash,
                        source_feed=feed.name,
                        feed_category=feed.category,
                        title=extraction.title,
                        url=link,
                        timestamp=ts,
                        description=description,
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
                        url_hash=url_hash,
                        title_hash=title_hash,
                        canonical_url=canonical,
                        raw_url=link,
                        title=title,
                        source_feed=feed.name,
                    )
                    stored_count += 1

                    time.sleep(0.5)
                except Exception:
                    continue

            time.sleep(0.3)

        return RefreshStats(source="news", count=stored_count)

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
        self._ensure_seed(store)
        items = self._client.fetch_all()
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
        return RefreshStats(source="gov_reports", count=count)


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
    ) -> None:
        self.store = store
        self.fred = fred or FREDIngestionClient()
        self.investing = investing or InvestingCalendarClient()
        self.forexfactory = forexfactory or ForexFactoryCalendarClient()
        self.tradingeconomics = tradingeconomics or TradingEconomicsCalendarClient()
        self.fed = fed or FedIngestionClient()
        self.market = market or MarketPriceClient()
        self.news = news or NewsIngestionClient()
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

    def refresh_calendar(self) -> dict[str, int]:
        self._ensure_calendar_indicator_seed()
        total = 0
        for label, fetch_fn in [
            ("Investing.com", lambda: self.investing.fetch_range(days_back=1, days_forward=3)),
            ("ForexFactory", lambda: self.forexfactory.fetch()),
            ("TradingEconomics", lambda: self.tradingeconomics.fetch()),
        ]:
            try:
                for event in fetch_fn():
                    event = self._resolve_calendar_indicator(event)
                    self.store.upsert_calendar_event(event)
                    total += 1
            except Exception:
                logger.warning("%s calendar refresh failed", label, exc_info=True)
        return {"calendar": total}

    def refresh_market(self) -> dict[str, int]:
        stats = self.market.refresh(self.store)
        return {stats.source: stats.count}

    def refresh_fed(self) -> dict[str, int]:
        stats = self.fed.refresh(self.store)
        return {stats.source: stats.count}

    def refresh_fred_daily(self) -> dict[str, int]:
        stats = self.fred.refresh_daily_series(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_fred_full(self, *, lookback_days: int = 365) -> dict[str, int]:
        stats = self.fred.refresh_all_series(self.store, lookback_days=lookback_days, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_news(self, *, category: str | None = None) -> dict[str, int]:
        stats = self.news.refresh(self.store, category=category)
        return {stats.source: stats.count}

    def refresh_rate_probability(self) -> dict[str, int]:
        count = 0
        try:
            prob = self.rate_probability.fetch_probabilities()
            for m in prob.meetings:
                self.store.upsert_indicator_observation(
                    IndicatorObservationRecord(
                        series_id=f"FEDPROB_{m.meeting_date}",
                        source="rateprobability",
                        date=prob.as_of[:10] if len(prob.as_of) >= 10 else prob.as_of,
                        value=m.implied_rate,
                        metadata={
                            "prob_move_pct": m.prob_move_pct,
                            "is_cut": m.is_cut,
                            "num_moves": m.num_moves,
                            "change_bps": m.change_bps,
                            "current_band": prob.current_band,
                        },
                        # Dynamic series — no pre-registered family
                    )
                )
                count += 1
        except Exception:
            logger.warning("rateprobability.com refresh failed", exc_info=True)
        return {"rate_probability": count}

    def refresh_fred_vintages(self) -> dict[str, int]:
        stats = self.fred.refresh_vintages(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_eia(self) -> dict[str, int]:
        stats = self.eia.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_treasury_fiscal(self) -> dict[str, int]:
        stats = self.treasury_fiscal.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_imf(self) -> dict[str, int]:
        stats = self.imf.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_imf_vintages(self) -> dict[str, int]:
        stats = self.imf.refresh_vintages(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_eurostat(self) -> dict[str, int]:
        stats = self.eurostat.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_bis(self) -> dict[str, int]:
        stats = self.bis.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_ecb(self) -> dict[str, int]:
        stats = self.ecb.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_oecd(self) -> dict[str, int]:
        stats = self.oecd.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_worldbank(self) -> dict[str, int]:
        stats = self.worldbank.refresh(self.store, family_lookup=self._family_lookup or None)
        return {stats.source: stats.count}

    def refresh_gov_reports(self) -> dict[str, int]:
        try:
            stats = self.gov_report.refresh(self.store)
            return {stats.source: stats.count}
        except Exception:
            logger.warning("Gov reports refresh failed", exc_info=True)
            return {"gov_reports": 0}

    def refresh_nyfed_rates(self) -> dict[str, int]:
        count = 0
        try:
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
                self.store.upsert_indicator_observation(
                    IndicatorObservationRecord(
                        series_id=series_id,
                        source="nyfed",
                        date=rate.date,
                        value=rate.rate,
                        metadata=metadata,
                        obs_family_id=fam_id,
                    )
                )
                count += 1
        except Exception:
            logger.warning("NY Fed rates refresh failed", exc_info=True)
        return {"nyfed_rates": count}

    def refresh_all(self) -> dict[str, int]:
        self._ensure_obs_seed()
        results: dict[str, int] = {}
        for batch in (
            self.refresh_calendar(),
            self.refresh_fed(),
            self.refresh_market(),
            self.refresh_fred_daily(),
            self.refresh_news(),
            self.refresh_rate_probability(),
            self.refresh_nyfed_rates(),
            self.refresh_gov_reports(),
            self.refresh_eia(),
            self.refresh_treasury_fiscal(),
            self.refresh_imf(),
            self.refresh_imf_vintages(),
            self.refresh_eurostat(),
            self.refresh_bis(),
            self.refresh_ecb(),
            self.refresh_oecd(),
            self.refresh_worldbank(),
        ):
            results.update(batch)
        return results

    def run_schedule(self, *, poll_interval_seconds: int = 60) -> None:
        jobs = {
            "calendar": {"interval": 3600, "handler": self.refresh_calendar},
            "fed": {"interval": 14_400, "handler": self.refresh_fed},
            "market": {"interval": 1800, "handler": self.refresh_market},
            "fred_daily": {"interval": 86_400, "handler": self.refresh_fred_daily},
            "fred_vintages": {"interval": 86_400, "handler": self.refresh_fred_vintages},
            "news": {"interval": 900, "handler": self.refresh_news},
            "rate_probability": {"interval": 3600, "handler": self.refresh_rate_probability},
            "nyfed_rates": {"interval": 86_400, "handler": self.refresh_nyfed_rates},
            "gov_reports": {"interval": 21_600, "handler": self.refresh_gov_reports},
            "eia": {"interval": 86_400, "handler": self.refresh_eia},
            "treasury_fiscal": {"interval": 86_400, "handler": self.refresh_treasury_fiscal},
            "imf": {"interval": 86_400, "handler": self.refresh_imf},
            "imf_vintages": {"interval": 86_400, "handler": self.refresh_imf_vintages},
            "eurostat": {"interval": 86_400, "handler": self.refresh_eurostat},
            "bis": {"interval": 86_400, "handler": self.refresh_bis},
            "ecb": {"interval": 86_400, "handler": self.refresh_ecb},
            "oecd": {"interval": 86_400, "handler": self.refresh_oecd},
            "worldbank": {"interval": 86_400, "handler": self.refresh_worldbank},
        }
        next_run = {name: 0.0 for name in jobs}
        self.refresh_all()
        while True:
            now = time.time()
            for job_name, job in jobs.items():
                if now >= next_run[job_name]:
                    job["handler"]()
                    next_run[job_name] = now + float(job["interval"])
            time.sleep(poll_interval_seconds)
