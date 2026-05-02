"""Indicator-domain query helpers for SQLiteEngineStore.

Covers indicators + indicator_vintages + central_bank_comms +
obs_source / obs_family / obs_family_document + concept_map +
obs_enrichment + subjects (taxonomy table reads), plus the catalog/
source_capability surface (source_capability + catalog_entity +
catalog_sync_checkpoint + catalog_sync_run).

Owns the module-level seed data (_FRED_FAMILY_MAP, _EIA_FAMILY_MAP,
_OBS_SOURCE_DEFS, …) consumed by ``seed_obs_sources_and_families`` and
the cross-source ``_CONCEPT_MAP_DEFS`` class attribute consumed by
``seed_concept_map``.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from contracts import utc_now
from storage.models.calendar import StoredEventRecord
from storage.models.documents import DocReleaseFamilyRecord
from storage.models.indicator import (
    CentralBankCommunicationRecord,
    ConceptMapRecord,
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    ObsFamilyDocumentRecord,
    ObsFamilyRecord,
    ObsRawRecord,
    ObsSourceRecord,
    ResolvedObservation,
)
from storage.queries.calendar import _add_calendar_keyword_filter


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
    "WEI":          ("us.growth.weekly_economic_index","Weekly Economic Index",        "percent",      "weekly",    "nsa"),
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
    "RHORUSQ156N":  ("us.housing.homeownership_rate",  "Homeownership Rate",          "percent",      "quarterly", "nsa"),
    "RRVRUSQ156N":  ("us.housing.rental_vacancy_rate", "Rental Vacancy Rate",         "percent",      "quarterly", "nsa"),
    "RHVRUSQ156N":  ("us.housing.homeowner_vacancy_rate", "Homeowner Vacancy Rate",    "percent",      "quarterly", "nsa"),
    "VIXCLS":       ("us.markets.vix",                 "CBOE VIX",                    "index",        "daily",     "none"),
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
    "NYFED_GSCPI": ("us.supply_chain.gscpi", "Global Supply Chain Pressure Index", "index", "monthly", "none"),
}

_RATEPROBABILITY_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    # FedWatch midpoint (CME-equivalent forward rate expectations). Per-meeting
    # FEDPROB_<date> observations are also emitted for the forward curve but
    # aren't concept-mapped — the meeting set rolls over each FOMC cycle.
    "FEDWATCH_MIDPOINT": (
        "us.rates.fedwatch_midpoint",
        "FedWatch Midpoint (CME-equivalent)",
        "percent",
        "daily",
        "none",
    ),
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
    "BIS_TC_GOV_US":     ("us.credit.gov_leverage", "US General Government Leverage", "percent", "quarterly", "none"),
    "BIS_TC_HH_US":      ("us.credit.household_leverage", "US Household Leverage",   "percent", "quarterly", "none"),
    "BIS_TC_NFC_US":     ("us.credit.nfc_leverage", "US NFC Leverage",              "percent", "quarterly", "none"),
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

_BUNDESBANK_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    # series_id: (family_id, canonical_name, unit, frequency, seasonal_adjustment)
    "BUNDESBANK_DE_GOVT_2Y":  ("de.rates.govt_2y",  "Germany 2Y Federal Securities Yield",  "percent", "daily", "none"),
    "BUNDESBANK_DE_GOVT_5Y":  ("de.rates.govt_5y",  "Germany 5Y Federal Securities Yield",  "percent", "daily", "none"),
    "BUNDESBANK_DE_GOVT_7Y":  ("de.rates.govt_7y",  "Germany 7Y Federal Securities Yield",  "percent", "daily", "none"),
    "BUNDESBANK_DE_GOVT_10Y": ("de.rates.govt_10y", "Germany 10Y Federal Securities Yield", "percent", "daily", "none"),
    "BUNDESBANK_DE_GOVT_15Y": ("de.rates.govt_15y", "Germany 15Y Federal Securities Yield", "percent", "daily", "none"),
    "BUNDESBANK_DE_GOVT_30Y": ("de.rates.govt_30y", "Germany 30Y Federal Securities Yield", "percent", "daily", "none"),
}

_MOF_JGB_MATURITIES = (
    "1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y", "8Y", "9Y",
    "10Y", "15Y", "20Y", "25Y", "30Y", "40Y",
)

_MOF_JGB_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    f"MOF_JP_GOVT_{maturity}": (
        f"jp.rates.govt_{maturity.lower()}",
        f"Japan {maturity} Government Bond Yield",
        "percent",
        "daily",
        "none",
    )
    for maturity in _MOF_JGB_MATURITIES
}

_MOF_JGB_CONCEPT_MAP_DEFS: tuple[tuple[str, str, str, str, int, str, str], ...] = tuple(
    (
        f"JP_GOVT_{maturity}",
        "mof_jp",
        f"MOF_JP_GOVT_{maturity}",
        f"jp.rates.govt_{maturity.lower()}",
        1,
        "primary",
        "MOF constant-maturity JGB interest rate",
    )
    for maturity in _MOF_JGB_MATURITIES
)

_AISI_STEEL_FAMILY_MAP: dict[str, tuple[str, str, str, str, str]] = {
    "AISI_RAW_STEEL_PRODUCTION_US": (
        "us.industry.raw_steel_production",
        "US Weekly Raw Steel Production",
        "net_tons",
        "weekly",
        "none",
    ),
    "AISI_RAW_STEEL_WOW_US": (
        "us.industry.raw_steel_production_wow",
        "US Weekly Raw Steel Production WoW",
        "percent",
        "weekly",
        "none",
    ),
    "AISI_RAW_STEEL_YOY_US": (
        "us.industry.raw_steel_production_yoy",
        "US Weekly Raw Steel Production YoY",
        "percent",
        "weekly",
        "none",
    ),
}

_AISI_STEEL_CONCEPT_MAP_DEFS: tuple[tuple[str, str, str, str, int, str, str], ...] = (
    (
        "RAW_STEEL_PRODUCTION_US",
        "aisi",
        "AISI_RAW_STEEL_PRODUCTION_US",
        "us.industry.raw_steel_production",
        1,
        "primary",
        "AISI weekly raw steel production",
    ),
    (
        "RAW_STEEL_PRODUCTION_WOW_US",
        "aisi",
        "AISI_RAW_STEEL_WOW_US",
        "us.industry.raw_steel_production_wow",
        1,
        "primary",
        "AISI weekly raw steel production week-over-week change",
    ),
    (
        "RAW_STEEL_PRODUCTION_YOY_US",
        "aisi",
        "AISI_RAW_STEEL_YOY_US",
        "us.industry.raw_steel_production_yoy",
        1,
        "primary",
        "AISI weekly raw steel production year-over-year change",
    ),
)

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
    ("destatis",        "destatis",        "German Federal Statistical Office","government_agency", "DE", "https://www.destatis.de",                                         "https://www-genesis.destatis.de/genesisWS/rest/2020"),
    ("zew",             "zew",             "ZEW Leibniz Centre for European Economic Research","market_data", "DE", "https://www.zew.de",                         "https://www.zew.de/en/press/latest-press-releases"),
    ("ifo",             "ifo",             "ifo Institute",                    "market_data",       "DE", "https://www.ifo.de",                                              "https://www.ifo.de/en/press"),
    ("gfk",             "gfk",             "NIM Consumer Climate powered by GfK","market_data",     "DE", "https://www.nim.org",                                             "https://www.nim.org/en/consumer-climate"),
    ("hcob",            "hcob",            "HCOB Germany PMI (S&P Global)",    "market_data",       "DE", "https://www.pmi.spglobal.com",                                    "https://www.pmi.spglobal.com/Public/Release/ReleaseDates?language=en"),
    ("ec-bcs",          "ec-bcs",          "European Commission DG ECFIN — Business and Consumer Surveys", "government_agency", "EU", "https://economy-finance.ec.europa.eu", "https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys_en"),
    ("insee",           "insee",           "French National Institute of Statistics and Economic Studies","government_agency", "FR", "https://www.insee.fr",              "https://www.insee.fr/en/agenda-diffusion"),
    ("ine",             "ine",             "Instituto Nacional de Estadistica","government_agency", "ES", "https://www.ine.es",                                              "https://www.ine.es/dyngs/Prensa"),
    ("istat",           "istat",           "Italian National Institute of Statistics","government_agency", "IT", "https://www.istat.it",                                      "https://www.istat.it/en/press-release"),
    ("bis",             "bis",             "Bank for International Settlements","data_aggregator",  "CH", "https://www.bis.org",                                           "https://stats.bis.org/api/v2"),
    ("ecb",             "ecb",             "European Central Bank",             "central_bank",     "EU", "https://www.ecb.europa.eu",                                      "https://data-api.ecb.europa.eu/service/data"),
    ("bundesbank",      "bundesbank",      "Deutsche Bundesbank",               "central_bank",     "DE", "https://www.bundesbank.de",                                      "https://api.statistiken.bundesbank.de/rest"),
    ("mof_jp",          "mof_jp",          "Japan Ministry of Finance",         "government_agency", "JP", "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm", "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv"),
    ("aisi",            "aisi",            "American Iron and Steel Institute", "data_aggregator",  "US", "https://www.steel.org/industry-data/",                            "https://www.steel.org/industry-data/"),
    ("oecd",            "oecd",            "Organisation for Economic Co-operation", "data_aggregator", "XX", "https://www.oecd.org",                                      "https://sdmx.oecd.org/public/rest/v2"),
    ("worldbank",       "worldbank",       "World Bank",                        "data_aggregator",  "XX", "https://www.worldbank.org",                                      "https://api.worldbank.org/v2"),
    ("bls",             "bls",             "Bureau of Labor Statistics",         "government_agency", "US", "https://www.bls.gov",                                            "https://api.bls.gov/publicAPI/v2"),
]



class _IndicatorQueriesMixin:
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

    def insert_obs_raw(self, records: list[ObsRawRecord]) -> int:
        """Insert raw macro time-series snapshots; return number of new rows.

        ``INSERT OR IGNORE`` on the ``(source, series_id, content_hash)``
        PK matches the ``cal_econ_raw`` idempotency contract — same
        canonicalized payload = same row, no duplicate write. A revised
        observation flips the hash and lands as a new row, preserving the
        revision chain.

        Issue #69 slice 1 — see schema rationale comment on ``obs_raw``.
        """
        if not records:
            return 0
        rows = [
            (
                r.source,
                r.series_id,
                r.snapshot_epoch_ms,
                r.content_hash,
                r.payload_json,
                r.fetched_at,
                r.request_params_json,
            )
            for r in records
        ]
        with self._connection(commit=True) as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO obs_raw (
                    source, series_id, snapshot_epoch_ms,
                    content_hash, payload_json, fetched_at, request_params_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return connection.total_changes - before

    def latest_obs_raw_for_series(
        self, source: str, series_id: str,
    ) -> ObsRawRecord | None:
        """Latest snapshot for ``(source, series_id)``.

        Hits ``idx_obs_raw_latest`` (``source, series_id,
        snapshot_epoch_ms DESC``) so the lookup is O(1) regardless of
        revision-chain depth. Used by re-projection: replay the latest
        raw payload through the parser without re-fetching upstream.
        """
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT source, series_id, snapshot_epoch_ms, content_hash,
                       payload_json, fetched_at, request_params_json
                FROM obs_raw
                WHERE source = ? AND series_id = ?
                ORDER BY snapshot_epoch_ms DESC
                LIMIT 1
                """,
                (source, series_id),
            ).fetchone()
        if row is None:
            return None
        return ObsRawRecord(
            source=row["source"],
            series_id=row["series_id"],
            snapshot_epoch_ms=int(row["snapshot_epoch_ms"]),
            content_hash=row["content_hash"],
            payload_json=row["payload_json"],
            fetched_at=row["fetched_at"],
            request_params_json=row["request_params_json"] or "{}",
        )

    def list_indicator_releases(
        self,
        *,
        indicator_keyword: str,
        limit: int = 12,
    ) -> list[StoredEventRecord]:
        with self._connection(commit=False) as connection:
            conditions = ["actual IS NOT NULL"]
            params: list[Any] = []
            matched_keyword = _add_calendar_keyword_filter(
                conditions, params, indicator_keyword, connection=connection
            )
            if not matched_keyword:
                return []
            params.append(limit)
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
            ("rateprobability", _RATEPROBABILITY_FAMILY_MAP),
            ("imf", _IMF_FAMILY_MAP),
            ("eurostat", _EUROSTAT_FAMILY_MAP),
            ("bis", _BIS_FAMILY_MAP),
            ("ecb", _ECB_FAMILY_MAP),
            ("bundesbank", _BUNDESBANK_FAMILY_MAP),
            ("mof_jp", _MOF_JGB_FAMILY_MAP),
            ("aisi", _AISI_STEEL_FAMILY_MAP),
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
        ("WEI_US",              "fred",           "WEI",            "us.growth.weekly_economic_index",1,"primary",    "Weekly Economic Index, NSA"),
        ("GDP_GROWTH_WB_US",    "worldbank",      "WB_GDP_GROWTH_US","us.growth.gdp_growth_wb",      1, "primary",     "Annual % growth"),
        ("GSCPI_US",            "nyfed",          "NYFED_GSCPI",    "us.supply_chain.gscpi",          1, "primary",     "NY Fed Global Supply Chain Pressure Index"),
        *_AISI_STEEL_CONCEPT_MAP_DEFS,
        #
        # ── US Rates ─────────────────────────────────────────────────
        ("POLICY_RATE_US",      "nyfed",          "NYFED_EFFR",     "us.rates.effr",                 1, "primary",     "NY Fed EFFR"),
        ("POLICY_RATE_US",      "fred",           "DFF",            "us.rates.fed_funds",            2, "secondary",   "Daily effective rate"),
        ("POLICY_RATE_US",      "bis",            "BIS_POLICY_US",  "us.rates.policy_bis",           3, "cross_check", "BIS central bank policy"),
        ("FEDWATCH_US",         "rateprobability","FEDWATCH_MIDPOINT","us.rates.fedwatch_midpoint",  1, "primary",     "CME-equivalent midpoint, daily snapshot"),
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
        ("VIX_US",              "fred",           "VIXCLS",         "us.markets.vix",                1, "primary",     "CBOE VIX close, regime-classified via obs_enrichment"),
        ("CREDIT_GAP_US",       "bis",            "BIS_CREDIT_GAP_US","us.credit.gap",               1, "primary",     "Credit-to-GDP gap"),
        ("GOV_LEVERAGE_US",     "bis",            "BIS_TC_GOV_US",  "us.credit.gov_leverage",        1, "primary",     "BIS Total Credit, general government debt/GDP"),
        ("HOUSEHOLD_LEVERAGE_US","bis",           "BIS_TC_HH_US",   "us.credit.household_leverage",  1, "primary",     "BIS Total Credit, household debt/GDP"),
        ("NFC_LEVERAGE_US",     "bis",            "BIS_TC_NFC_US",  "us.credit.nfc_leverage",        1, "primary",     "BIS Total Credit, NFC debt/GDP"),
        #
        # ── US Property ──────────────────────────────────────────────
        ("PROPERTY_US",         "bis",            "BIS_PROPERTY_US","us.property.real",              1, "primary",     "Real property prices"),
        ("HOMEOWNERSHIP_RATE_US","fred",           "RHORUSQ156N",    "us.housing.homeownership_rate", 1, "primary",     "Census HVS, NSA"),
        ("RENTAL_VACANCY_RATE_US","fred",          "RRVRUSQ156N",    "us.housing.rental_vacancy_rate",1, "primary",     "Census HVS, NSA"),
        ("HOMEOWNER_VACANCY_RATE_US","fred",       "RHVRUSQ156N",    "us.housing.homeowner_vacancy_rate",1,"primary",   "Census HVS, NSA"),
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
        *_MOF_JGB_CONCEPT_MAP_DEFS,
        #
        # ── Germany ─────────────────────────────────────────────────
        ("DE_GOVT_2Y",          "bundesbank",     "BUNDESBANK_DE_GOVT_2Y", "de.rates.govt_2y",       1, "primary",     "Bundesbank current Federal securities yield"),
        ("DE_GOVT_5Y",          "bundesbank",     "BUNDESBANK_DE_GOVT_5Y", "de.rates.govt_5y",       1, "primary",     "Bundesbank current Federal securities yield"),
        ("DE_GOVT_7Y",          "bundesbank",     "BUNDESBANK_DE_GOVT_7Y", "de.rates.govt_7y",       1, "primary",     "Bundesbank current Federal securities yield"),
        ("DE_GOVT_10Y",         "bundesbank",     "BUNDESBANK_DE_GOVT_10Y", "de.rates.govt_10y",     1, "primary",     "Bundesbank current Federal securities yield"),
        ("DE_GOVT_15Y",         "bundesbank",     "BUNDESBANK_DE_GOVT_15Y", "de.rates.govt_15y",     1, "primary",     "Bundesbank current Federal securities yield"),
        ("DE_GOVT_30Y",         "bundesbank",     "BUNDESBANK_DE_GOVT_30Y", "de.rates.govt_30y",     1, "primary",     "Bundesbank current Federal securities yield"),
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

    def sync_subjects(self, subjects: list[dict]) -> None:
        """Upsert the subject vocabulary and its aliases.

        ``subjects`` is the list parsed from ``config/subjects.yaml`` — each
        dict has ``id``, ``display``, and an ``aliases`` mapping of
        alias_type → list of alias values. Existing subjects are replaced
        and their alias rows rebuilt; subjects not in the input are left
        alone so removal is always explicit.
        """
        with self._connection(commit=True) as connection:
            for sub in subjects:
                sid = sub["id"]
                connection.execute(
                    "INSERT OR REPLACE INTO subjects (subject_id, display_name) "
                    "VALUES (?, ?)",
                    (sid, sub["display"]),
                )
                connection.execute(
                    "DELETE FROM subject_aliases WHERE subject_id = ?",
                    (sid,),
                )
                alias_rows: list[tuple[str, str, str]] = []
                for alias_type, values in (sub.get("aliases") or {}).items():
                    for value in values or []:
                        alias_rows.append((sid, alias_type, value))
                if alias_rows:
                    connection.executemany(
                        "INSERT OR IGNORE INTO subject_aliases "
                        "(subject_id, alias_type, alias_value) VALUES (?, ?, ?)",
                        alias_rows,
                    )

    def list_subjects(self) -> list[dict[str, str]]:
        """Return all subjects with their display names, ordered by id."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT subject_id, display_name FROM subjects ORDER BY subject_id"
            ).fetchall()
            return [{"subject_id": r["subject_id"], "display_name": r["display_name"]}
                    for r in rows]

    def get_subject_aliases(
        self, subject_id: str, *, alias_type: str | None = None
    ) -> list[str]:
        """Return alias values for a subject, optionally filtered by type."""
        with self._connection(commit=False) as connection:
            if alias_type:
                rows = connection.execute(
                    "SELECT alias_value FROM subject_aliases "
                    "WHERE subject_id = ? AND alias_type = ?",
                    (subject_id, alias_type),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT alias_value FROM subject_aliases WHERE subject_id = ?",
                    (subject_id,),
                ).fetchall()
            return [r[0] for r in rows]

    def set_obs_enrichment(
        self, *, obs_family_id: str, date: str, key: str, value: str,
    ) -> None:
        """Upsert a single (family, date, key) enrichment row.

        ``value`` is stored as text so the same sidecar can hold regime
        labels, boolean-as-string flags, or numeric buckets without a
        type-specific column.
        """
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                "INSERT OR REPLACE INTO obs_enrichment "
                "(obs_family_id, date, key, value, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (obs_family_id, date, key, value, now),
            )

    def get_obs_enrichment(
        self, *, obs_family_id: str, date: str, key: str,
    ) -> str | None:
        """Return the enrichment value for one (family, date, key) tuple,
        or ``None`` if no row exists."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT value FROM obs_enrichment "
                "WHERE obs_family_id = ? AND date = ? AND key = ?",
                (obs_family_id, date, key),
            ).fetchone()
        return row["value"] if row else None

    def list_obs_enrichment_for_family(
        self, obs_family_id: str, *, key: str | None = None,
    ) -> list[tuple[str, str, str]]:
        """Return ``(date, key, value)`` rows for a family, optionally
        filtered by ``key``, ordered by date descending."""
        with self._connection(commit=False) as connection:
            if key is not None:
                rows = connection.execute(
                    "SELECT date, key, value FROM obs_enrichment "
                    "WHERE obs_family_id = ? AND key = ? "
                    "ORDER BY date DESC",
                    (obs_family_id, key),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT date, key, value FROM obs_enrichment "
                    "WHERE obs_family_id = ? ORDER BY date DESC, key",
                    (obs_family_id,),
                ).fetchall()
        return [(r["date"], r["key"], r["value"]) for r in rows]

    def refresh_vix_regime(
        self, *, source: str = "fred", series_id: str = "VIXCLS",
        obs_family_id: str = "us.markets.vix",
    ) -> int:
        """Compute regime labels for every VIX close stored so far and
        upsert them into obs_enrichment under key='regime'.

        Callers can invoke this after a FRED refresh (or on a schedule)
        so the latest snapshot always has a classification. Returns the
        number of rows written.
        """
        from ingestion.timeseries.regimes import classify_vix_regime
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT date, value FROM indicators "
                "WHERE series_id = ? AND source = ?",
                (series_id, source),
            ).fetchall()
        written = 0
        for row in rows:
            label = classify_vix_regime(row["value"])
            if label is None:
                continue
            self.set_obs_enrichment(
                obs_family_id=obs_family_id,
                date=row["date"],
                key="regime",
                value=label,
            )
            written += 1
        return written

    def resolve_subjects_for_concept(self, concept_id: str) -> list[str]:
        """Find subject_ids that alias any provider_series_id registered for
        ``concept_id`` in concept_map. Used at query time to pivot between
        the timeseries vocabulary (CPI_US) and the subject vocabulary
        (econ.cpi) without a dedicated bridge table.
        """
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT sa.subject_id
                FROM concept_map cm
                JOIN subject_aliases sa ON sa.alias_value = cm.provider_series_id
                WHERE cm.concept_id = ?
                """,
                (concept_id,),
            ).fetchall()
            return [r[0] for r in rows]

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
        """Return distinct concept_ids, optionally filtered by obs-family country."""
        with self._connection(commit=False) as connection:
            if country_code:
                country = country_code.upper()
                rows = connection.execute(
                    """
                    SELECT DISTINCT cm.concept_id
                    FROM concept_map cm
                    LEFT JOIN obs_family f ON f.family_id = cm.obs_family_id
                    WHERE f.country_code = ?
                       OR (
                           f.family_id IS NULL
                           AND (
                               lower(cm.obs_family_id) LIKE ?
                               OR cm.concept_id LIKE ? ESCAPE '\\'
                               OR cm.concept_id LIKE ? ESCAPE '\\'
                           )
                       )
                    ORDER BY cm.concept_id
                    """,
                    (country, f"{country.lower()}.%", f"{country}\\_%", f"%\\_{country}"),
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

    def get_source_storage_stats(self, source_id: str) -> dict[str, Any]:
        mapping = {
            "fred": ("indicators", "source = ?", ("fred",), "scraped_at"),
            "bls": ("indicators", "source = ?", ("bls",), "scraped_at"),
            "eia": ("indicators", "source = ?", ("eia",), "scraped_at"),
            "treasury_fiscal": ("indicators", "source = ?", ("treasury_fiscal",), "scraped_at"),
            "imf": ("indicators", "source = ?", ("imf",), "scraped_at"),
            "eurostat": ("indicators", "source = ?", ("eurostat",), "scraped_at"),
            "bis": ("indicators", "source = ?", ("bis",), "scraped_at"),
            "ecb": ("indicators", "source = ?", ("ecb",), "scraped_at"),
            "bundesbank": ("indicators", "source = ?", ("bundesbank",), "scraped_at"),
            "oecd": ("indicators", "source = ?", ("oecd",), "scraped_at"),
            "worldbank": ("indicators", "source = ?", ("worldbank",), "scraped_at"),
            "nyfed_rates": ("indicators", "source = ?", ("nyfed",), "scraped_at"),
            "rate_probability": ("indicators", "source = ?", ("rateprobability",), "scraped_at"),
            "census": ("indicators", "source = ?", ("census",), "scraped_at"),
            "ilo": ("indicators", "source = ?", ("ilo",), "scraped_at"),
            "unsd": ("indicators", "source = ?", ("unsd",), "scraped_at"),
            "fred_vintages": ("indicator_vintages", "source = ?", ("fred",), "scraped_at"),
            "imf_vintages": ("indicator_vintages", "source = ?", ("imf",), "scraped_at"),
            "market": ("market_prices", "1 = 1", tuple(), "scraped_at"),
            "fed": ("central_bank_comms", "source = ?", ("fed",), "scraped_at"),
            "calendar": (
                "v_calendar_item", "1 = 1", (),
                "strftime('%Y-%m-%dT%H:%M:%f+00:00', observed_at_epoch_ms / 1000.0, 'unixepoch')",
            ),
            "corp_calendar": (
                "cal_corp_event", "provider = ?", ("eodhd",),
                "strftime('%Y-%m-%dT%H:%M:%f+00:00', observed_at_epoch_ms / 1000.0, 'unixepoch')",
            ),
            "news": ("news_articles", "source_feed NOT LIKE 'gov_%'", tuple(), "scraped_at"),
            "gov_reports": ("document", "1 = 1", tuple(), "updated_at"),
            "reddit_trends": ("trend_topics", "provider = ?", ("reddit",), "scraped_at"),
            "weibo_trends": ("trend_topics", "provider = ?", ("weibo",), "scraped_at"),
        }
        table, where_clause, params, ts_col = mapping.get(
            source_id,
            ("catalog_entity", "source_id = ?", (source_id,), "updated_at"),
        )
        with self._connection(commit=False) as connection:
            row = connection.execute(
                f"""
                SELECT COUNT(*) AS cnt, MAX({ts_col}) AS latest_ts
                FROM {table}
                WHERE {where_clause}
                """,
                params,
            ).fetchone()
        count = int(row["cnt"]) if row is not None and row["cnt"] is not None else 0
        latest_ts = row["latest_ts"] if row is not None else ""
        return {"table": table, "count": count, "latest_ts": latest_ts or ""}

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

    def upsert_source_capability(self, payload: dict[str, Any]) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO source_capability (
                    source_id,
                    display_name,
                    source_type,
                    entity_type,
                    supports_discovery,
                    supports_structure,
                    supports_latest_sync,
                    supports_backfill,
                    is_default_scheduled,
                    description,
                    notes,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    source_type = excluded.source_type,
                    entity_type = excluded.entity_type,
                    supports_discovery = excluded.supports_discovery,
                    supports_structure = excluded.supports_structure,
                    supports_latest_sync = excluded.supports_latest_sync,
                    supports_backfill = excluded.supports_backfill,
                    is_default_scheduled = excluded.is_default_scheduled,
                    description = excluded.description,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["source_id"],
                    payload.get("display_name", payload["source_id"]),
                    payload.get("source_type", ""),
                    payload.get("entity_type", ""),
                    int(bool(payload.get("supports_discovery", False))),
                    int(bool(payload.get("supports_structure", False))),
                    int(bool(payload.get("supports_latest_sync", False))),
                    int(bool(payload.get("supports_backfill", False))),
                    int(bool(payload.get("is_default_scheduled", False))),
                    payload.get("description", ""),
                    payload.get("notes", ""),
                    payload.get("updated_at", now),
                ),
            )

    def get_source_capability(self, source_id: str) -> dict[str, Any] | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM source_capability WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        return self._row_to_source_capability(row) if row is not None else None

    def list_source_capabilities(
        self,
        *,
        source_type: str | None = None,
        default_scheduled: bool | None = None,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if default_scheduled is not None:
            conditions.append("is_default_scheduled = ?")
            params.append(int(default_scheduled))
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM source_capability{where} ORDER BY source_id",
                params,
            ).fetchall()
        return [self._row_to_source_capability(row) for row in rows]

    def upsert_catalog_entity(self, payload: dict[str, Any]) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO catalog_entity (
                    source_id,
                    entity_id,
                    entity_type,
                    display_name,
                    description,
                    metadata_json,
                    is_active,
                    discovered_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, entity_id) DO UPDATE SET
                    entity_type = excluded.entity_type,
                    display_name = excluded.display_name,
                    description = excluded.description,
                    metadata_json = excluded.metadata_json,
                    is_active = excluded.is_active,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["source_id"],
                    payload["entity_id"],
                    payload.get("entity_type", ""),
                    payload.get("display_name", payload["entity_id"]),
                    payload.get("description", ""),
                    json.dumps(payload.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    int(bool(payload.get("is_active", True))),
                    payload.get("discovered_at", now),
                    payload.get("updated_at", now),
                ),
            )

    def list_catalog_entities(
        self,
        source_id: str,
        *,
        query: str | None = None,
        entity_type: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        conditions = ["source_id = ?"]
        params: list[Any] = [source_id]
        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type)
        if query:
            pattern = f"%{query.lower()}%"
            conditions.append(
                "(LOWER(entity_id) LIKE ? OR LOWER(display_name) LIKE ? OR LOWER(description) LIKE ?)"
            )
            params.extend([pattern, pattern, pattern])
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM catalog_entity
                WHERE {' AND '.join(conditions)}
                ORDER BY display_name, entity_id
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_catalog_entity(row) for row in rows]

    def count_catalog_entities(self, source_id: str) -> int:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS cnt FROM catalog_entity WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        return int(row["cnt"]) if row is not None else 0

    def upsert_catalog_sync_checkpoint(self, payload: dict[str, Any]) -> None:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO catalog_sync_checkpoint (
                    source_id,
                    job_type,
                    cursor,
                    entities_total,
                    entities_synced,
                    observations_synced,
                    last_success_at,
                    last_error,
                    metadata_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id, job_type) DO UPDATE SET
                    cursor = excluded.cursor,
                    entities_total = excluded.entities_total,
                    entities_synced = excluded.entities_synced,
                    observations_synced = excluded.observations_synced,
                    last_success_at = excluded.last_success_at,
                    last_error = excluded.last_error,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["source_id"],
                    payload["job_type"],
                    payload.get("cursor", ""),
                    int(payload.get("entities_total", 0)),
                    int(payload.get("entities_synced", 0)),
                    int(payload.get("observations_synced", 0)),
                    payload.get("last_success_at", ""),
                    payload.get("last_error", ""),
                    json.dumps(payload.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                    payload.get("updated_at", now),
                ),
            )

    def get_catalog_sync_checkpoint(
        self,
        source_id: str,
        job_type: str,
    ) -> dict[str, Any] | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                """
                SELECT * FROM catalog_sync_checkpoint
                WHERE source_id = ? AND job_type = ?
                """,
                (source_id, job_type),
            ).fetchone()
        return self._row_to_catalog_sync_checkpoint(row) if row is not None else None

    def list_catalog_sync_checkpoints(
        self,
        *,
        source_id: str | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if source_id:
            where = " WHERE source_id = ?"
            params.append(source_id)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM catalog_sync_checkpoint{where} ORDER BY source_id, job_type",
                params,
            ).fetchall()
        return [self._row_to_catalog_sync_checkpoint(row) for row in rows]

    def insert_catalog_sync_run(self, payload: dict[str, Any]) -> int:
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            cursor = connection.execute(
                """
                INSERT INTO catalog_sync_run (
                    source_id,
                    job_type,
                    status,
                    entities_total,
                    entities_synced,
                    observations_synced,
                    started_at,
                    finished_at,
                    duration_ms,
                    error,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["source_id"],
                    payload["job_type"],
                    payload.get("status", "running"),
                    int(payload.get("entities_total", 0)),
                    int(payload.get("entities_synced", 0)),
                    int(payload.get("observations_synced", 0)),
                    payload.get("started_at", now),
                    payload.get("finished_at", ""),
                    int(payload.get("duration_ms", 0)),
                    payload.get("error", ""),
                    json.dumps(payload.get("metadata", {}), ensure_ascii=True, sort_keys=True),
                ),
            )
            return int(cursor.lastrowid)

    def update_catalog_sync_run(self, run_id: int, payload: dict[str, Any]) -> None:
        sets: list[str] = []
        params: list[Any] = []
        field_map = {
            "status": "status",
            "entities_total": "entities_total",
            "entities_synced": "entities_synced",
            "observations_synced": "observations_synced",
            "started_at": "started_at",
            "finished_at": "finished_at",
            "duration_ms": "duration_ms",
            "error": "error",
        }
        for key, column in field_map.items():
            if key in payload:
                sets.append(f"{column} = ?")
                params.append(payload[key])
        if "metadata" in payload:
            sets.append("metadata_json = ?")
            params.append(json.dumps(payload["metadata"], ensure_ascii=True, sort_keys=True))
        if not sets:
            return
        params.append(run_id)
        with self._connection(commit=True) as connection:
            connection.execute(
                f"UPDATE catalog_sync_run SET {', '.join(sets)} WHERE id = ?",
                params,
            )

    def list_catalog_sync_runs(
        self,
        *,
        source_id: str | None = None,
        job_type: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if job_type:
            conditions.append("job_type = ?")
            params.append(job_type)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM catalog_sync_run
                {where}
                ORDER BY started_at DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_catalog_sync_run(row) for row in rows]

    def _row_to_source_capability(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "source_id": row["source_id"],
            "display_name": row["display_name"],
            "source_type": row["source_type"],
            "entity_type": row["entity_type"],
            "supports_discovery": bool(row["supports_discovery"]),
            "supports_structure": bool(row["supports_structure"]),
            "supports_latest_sync": bool(row["supports_latest_sync"]),
            "supports_backfill": bool(row["supports_backfill"]),
            "is_default_scheduled": bool(row["is_default_scheduled"]),
            "description": row["description"],
            "notes": row["notes"],
            "updated_at": row["updated_at"],
        }

    def _row_to_catalog_entity(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return {
            "source_id": row["source_id"],
            "entity_id": row["entity_id"],
            "entity_type": row["entity_type"],
            "display_name": row["display_name"],
            "description": row["description"],
            "metadata": metadata,
            "is_active": bool(row["is_active"]),
            "discovered_at": row["discovered_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_catalog_sync_checkpoint(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return {
            "source_id": row["source_id"],
            "job_type": row["job_type"],
            "cursor": row["cursor"],
            "entities_total": int(row["entities_total"]),
            "entities_synced": int(row["entities_synced"]),
            "observations_synced": int(row["observations_synced"]),
            "last_success_at": row["last_success_at"],
            "last_error": row["last_error"],
            "metadata": metadata,
            "updated_at": row["updated_at"],
        }

    def _row_to_catalog_sync_run(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            metadata = {}
        return {
            "run_id": int(row["id"]),
            "source_id": row["source_id"],
            "job_type": row["job_type"],
            "status": row["status"],
            "entities_total": int(row["entities_total"]),
            "entities_synced": int(row["entities_synced"]),
            "observations_synced": int(row["observations_synced"]),
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "duration_ms": int(row["duration_ms"]),
            "error": row["error"],
            "metadata": metadata,
        }
