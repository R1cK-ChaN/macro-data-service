"""Centralised series configuration for every data source.

Extracted from ``sources.py`` so that fetcher adapters, tests, and CLI
commands can import configs without pulling in the orchestrator.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Federal Reserve
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# FRED / ALFRED
# ---------------------------------------------------------------------------

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

VINTAGE_SERIES = ["GDP", "GDPC1", "CPIAUCSL", "PAYEMS", "UNRATE", "INDPRO", "RSAFS"]

# ---------------------------------------------------------------------------
# EIA
# ---------------------------------------------------------------------------

EIA_SERIES = {
    # -- Petroleum --
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
    "petroleum_supply": {
        "route": "petroleum/sum/snd/data",
        "params": {"data[]": "value", "frequency": "monthly"},
        "series_id": "EIA_PETROL_SUPPLY",
        "category": "energy",
    },
    # -- Natural gas --
    "natgas_futures": {
        "route": "natural-gas/pri/fut/data",
        "params": {"data[]": "value", "frequency": "daily"},
        "series_id": "EIA_NATGAS",
        "category": "energy",
    },
    "natgas_production": {
        "route": "natural-gas/sum/lsum/data",
        "params": {"data[]": "value", "frequency": "monthly"},
        "series_id": "EIA_NATGAS_PROD",
        "category": "energy",
    },
    # -- Electricity --
    "elec_retail_sales": {
        "route": "electricity/retail-sales/data",
        "params": {"data[]": "revenue", "frequency": "monthly"},
        "series_id": "EIA_ELEC_RETAIL",
        "category": "energy",
    },
    "elec_generation": {
        "route": "electricity/electric-power-operational-data/data",
        "params": {"data[]": "generation", "frequency": "monthly"},
        "series_id": "EIA_ELEC_GEN",
        "category": "energy",
    },
    "elec_fuel_consumption": {
        "route": "electricity/electric-power-operational-data/data",
        "params": {"data[]": "consumption-for-eg", "frequency": "monthly"},
        "series_id": "EIA_ELEC_FUEL",
        "category": "energy",
    },
    # -- Coal --
    "coal_production": {
        "route": "coal/mine-production/data",
        "params": {"data[]": "production", "frequency": "annual"},
        "series_id": "EIA_COAL_PROD",
        "category": "energy",
    },
    "coal_consumption": {
        "route": "coal/consumption-and-quality/data",
        "params": {"data[]": "consumption", "frequency": "quarterly"},
        "series_id": "EIA_COAL_CONS",
        "category": "energy",
    },
    # -- Total energy --
    "total_energy_consumption": {
        "route": "total-energy/data",
        "params": {"data[]": "value", "facets[msn][]": "TETCBUS", "frequency": "monthly"},
        "series_id": "EIA_TOTAL_CONSUMPTION",
        "category": "energy",
    },
    "total_energy_production": {
        "route": "total-energy/data",
        "params": {"data[]": "value", "facets[msn][]": "TEPRBUS", "frequency": "monthly"},
        "series_id": "EIA_TOTAL_PRODUCTION",
        "category": "energy",
    },
    # -- STEO (Short-Term Energy Outlook) --
    "steo_crude_price": {
        "route": "steo/data",
        "params": {"data[]": "value", "facets[seriesId][]": "BREPUUS", "frequency": "monthly"},
        "series_id": "EIA_STEO_CRUDE",
        "category": "energy",
    },
    # -- International --
    "intl_crude_production": {
        "route": "international/data",
        "params": {"data[]": "value", "facets[productId][]": "57", "frequency": "monthly"},
        "series_id": "EIA_INTL_CRUDE_PROD",
        "category": "energy",
    },
}

EIA_KNOWN_ROUTES = {
    "petroleum": "Petroleum data (prices, stocks, supply, production)",
    "natural-gas": "Natural gas data (prices, production, storage)",
    "electricity": "Electricity data (generation, sales, fuel consumption)",
    "coal": "Coal data (production, consumption, quality)",
    "total-energy": "Total energy data (consumption, production by source)",
    "steo": "Short-Term Energy Outlook (forecasts)",
    "aeo": "Annual Energy Outlook (long-term projections)",
    "international": "International energy data",
    "nuclear-outages": "Nuclear power plant outages",
    "crude-oil-imports": "Crude oil imports by country",
    "densified-biomass": "Densified biomass fuel report",
}

# ---------------------------------------------------------------------------
# Treasury Fiscal
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# BLS
# ---------------------------------------------------------------------------

BLS_SERIES = {
    # CPI (Consumer Price Index) — Survey prefix: CU
    "cpi_all_urban": {
        "series_id": "CUUR0000SA0",
        "name": "CPI-U All Items",
        "category": "inflation",
        "survey": "CU",
        "freq": "monthly",
    },
    "cpi_core": {
        "series_id": "CUUR0000SA0L1E",
        "name": "CPI-U All Items Less Food and Energy",
        "category": "inflation",
        "survey": "CU",
        "freq": "monthly",
    },
    "cpi_food": {
        "series_id": "CUUR0000SAF1",
        "name": "CPI-U Food",
        "category": "inflation",
        "survey": "CU",
        "freq": "monthly",
    },
    "cpi_energy": {
        "series_id": "CUUR0000SA0E",
        "name": "CPI-U Energy",
        "category": "inflation",
        "survey": "CU",
        "freq": "monthly",
    },
    "cpi_shelter": {
        "series_id": "CUUR0000SAH1",
        "name": "CPI-U Shelter",
        "category": "inflation",
        "survey": "CU",
        "freq": "monthly",
    },
    # PPI (Producer Price Index) — Survey prefix: WP
    "ppi_final_demand": {
        "series_id": "WPSFD4",
        "name": "PPI Final Demand",
        "category": "inflation",
        "survey": "WP",
        "freq": "monthly",
    },
    "ppi_core": {
        "series_id": "WPSFD49116",
        "name": "PPI Final Demand Less Foods and Energy",
        "category": "inflation",
        "survey": "WP",
        "freq": "monthly",
    },
    # Employment Situation (CES) — Survey prefix: CE
    "nfp_total": {
        "series_id": "CES0000000001",
        "name": "Total Nonfarm Payrolls",
        "category": "employment",
        "survey": "CE",
        "freq": "monthly",
    },
    "nfp_private": {
        "series_id": "CES0500000001",
        "name": "Total Private Employment",
        "category": "employment",
        "survey": "CE",
        "freq": "monthly",
    },
    "avg_hourly_earnings": {
        "series_id": "CES0500000003",
        "name": "Average Hourly Earnings, Private",
        "category": "employment",
        "survey": "CE",
        "freq": "monthly",
    },
    "avg_weekly_hours": {
        "series_id": "CES0500000002",
        "name": "Average Weekly Hours, Private",
        "category": "employment",
        "survey": "CE",
        "freq": "monthly",
    },
    # Current Population Survey — Survey prefix: LN
    "unemployment_rate": {
        "series_id": "LNS14000000",
        "name": "Unemployment Rate",
        "category": "employment",
        "survey": "LN",
        "freq": "monthly",
    },
    "labor_force_participation": {
        "series_id": "LNS11300000",
        "name": "Labor Force Participation Rate",
        "category": "employment",
        "survey": "LN",
        "freq": "monthly",
    },
    # JOLTS — Survey prefix: JT
    "jolts_openings": {
        "series_id": "JTS000000000000000JOL",
        "name": "JOLTS Job Openings",
        "category": "employment",
        "survey": "JT",
        "freq": "monthly",
    },
    "jolts_hires": {
        "series_id": "JTS000000000000000HIL",
        "name": "JOLTS Hires",
        "category": "employment",
        "survey": "JT",
        "freq": "monthly",
    },
    "jolts_quits": {
        "series_id": "JTS000000000000000QUL",
        "name": "JOLTS Quits",
        "category": "employment",
        "survey": "JT",
        "freq": "monthly",
    },
    # Employment Cost Index — Survey prefix: CI
    "eci_total": {
        "series_id": "CIU1010000000000A",
        "name": "Employment Cost Index, Total Compensation",
        "category": "employment",
        "survey": "CI",
        "freq": "quarterly",
    },
    # Productivity — Survey prefix: PR
    "nfb_productivity": {
        "series_id": "PRS85006092",
        "name": "Nonfarm Business Labor Productivity",
        "category": "productivity",
        "survey": "PR",
        "freq": "quarterly",
    },
    "nfb_unit_labor_costs": {
        "series_id": "PRS85006112",
        "name": "Nonfarm Business Unit Labor Costs",
        "category": "productivity",
        "survey": "PR",
        "freq": "quarterly",
    },
}

BLS_SURVEY_PREFIXES = {
    "CU": "CPI-All Urban Consumers",
    "CW": "CPI-Urban Wage Earners",
    "CE": "Current Employment Statistics",
    "LN": "Current Population Survey",
    "WP": "PPI-Commodities",
    "PC": "PPI-Industry",
    "JT": "JOLTS",
    "AP": "Average Prices",
    "CI": "Employment Cost Index",
    "PR": "Productivity and Costs",
    "LA": "Local Area Unemployment Statistics",
    "SM": "State and Metro Area Employment",
}

# ---------------------------------------------------------------------------
# BEA
# ---------------------------------------------------------------------------

BEA_DATASETS = {
    # NIPA — National Income and Product Accounts
    "gdp_summary": {
        "dataset": "NIPA", "table": "T10101",
        "name": "GDP Summary", "category": "output", "freq": "Q",
    },
    "gdp_contributions": {
        "dataset": "NIPA", "table": "T10102",
        "name": "Contributions to GDP Growth", "category": "output", "freq": "Q",
    },
    "real_gdp": {
        "dataset": "NIPA", "table": "T10106",
        "name": "Real GDP", "category": "output", "freq": "Q",
    },
    "pce_price_index": {
        "dataset": "NIPA", "table": "T20301",
        "name": "PCE Price Index", "category": "inflation", "freq": "Q",
    },
    "pce_categories": {
        "dataset": "NIPA", "table": "T20305",
        "name": "PCE by Category", "category": "consumption", "freq": "Q",
    },
    "personal_income": {
        "dataset": "NIPA", "table": "T20100",
        "name": "Personal Income and Outlays", "category": "income", "freq": "Q",
    },
    "corporate_profits": {
        "dataset": "NIPA", "table": "T61600D",
        "name": "Corporate Profits by Industry", "category": "profits", "freq": "Q",
    },
    "savings_rate": {
        "dataset": "NIPA", "table": "T20600",
        "name": "Personal Income and Disposition (Monthly)", "category": "savings", "freq": "M",
    },
    "govt_consumption": {
        "dataset": "NIPA", "table": "T30100",
        "name": "Government Consumption Expenditures", "category": "government", "freq": "Q",
    },
    # GDPbyIndustry
    "gdp_value_added": {
        "dataset": "GDPbyIndustry", "table": "1",
        "name": "Value Added by Industry", "category": "output", "freq": "Q",
    },
    "gdp_gross_output": {
        "dataset": "GDPbyIndustry", "table": "15",
        "name": "Gross Output by Industry", "category": "output", "freq": "Q",
    },
    # Regional
    "state_gdp": {
        "dataset": "Regional", "table": "SAGDP2",
        "name": "State GDP by Industry", "category": "output", "freq": "A",
    },
    "state_income": {
        "dataset": "Regional", "table": "SAINC1",
        "name": "State Personal Income", "category": "income", "freq": "A",
    },
    # FixedAssets
    "fixed_assets_net_stock": {
        "dataset": "FixedAssets", "table": "FAAt101",
        "name": "Net Stock of Fixed Assets", "category": "investment", "freq": "A",
    },
    # ITA — International Transactions
    "trade_balance_goods": {
        "dataset": "ITA", "table": "BalGds",
        "name": "Balance on Goods", "category": "trade", "freq": "Q",
    },
    "current_account": {
        "dataset": "ITA", "table": "BalCurrAcct",
        "name": "Current Account Balance", "category": "trade", "freq": "Q",
    },
    # IIP — International Investment Position
    "net_intl_investment": {
        "dataset": "IIP", "table": "ALL",
        "name": "U.S. Net International Investment Position", "category": "trade", "freq": "Q",
    },
}

BEA_KNOWN_DATASETS = {
    "NIPA": "National Income and Product Accounts",
    "NIUnderlyingDetail": "NIPA Underlying Detail Tables",
    "FixedAssets": "Fixed Assets",
    "MNE": "Multinational Enterprises",
    "GDPbyIndustry": "GDP by Industry",
    "ITA": "International Transactions",
    "IIP": "International Investment Position",
    "Regional": "Regional Economic Accounts",
    "UnderlyingGDPbyIndustry": "Underlying GDP by Industry",
    "InputOutput": "Input-Output Statistics",
}

# ---------------------------------------------------------------------------
# Census
# ---------------------------------------------------------------------------

CENSUS_DATASETS = {
    # -- ACS 5-Year: Demographic and socioeconomic structure --
    "total_population": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B01001_001E",
        "name": "Total Population", "category": "demographics", "geo_for": "state:*",
    },
    "median_household_income": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B19013_001E",
        "name": "Median Household Income", "category": "income", "geo_for": "state:*",
    },
    "total_housing_units": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B25001_001E",
        "name": "Total Housing Units", "category": "housing", "geo_for": "state:*",
    },
    "owner_occupied_units": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B25003_002E",
        "name": "Owner-Occupied Housing Units", "category": "housing", "geo_for": "state:*",
    },
    "labor_force_population": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B23025_002E",
        "name": "In Labor Force (Population 16+)", "category": "employment", "geo_for": "state:*",
    },
    "median_home_value": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B25077_001E",
        "name": "Median Value of Owner-Occupied Units", "category": "housing", "geo_for": "state:*",
    },
    "poverty_population": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B17001_002E",
        "name": "Income Below Poverty Level", "category": "poverty", "geo_for": "state:*",
    },
    "gini_index": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B19083_001E",
        "name": "Gini Index of Income Inequality", "category": "income", "geo_for": "state:*",
    },
    "median_gross_rent": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B25064_001E",
        "name": "Median Gross Rent", "category": "housing", "geo_for": "state:*",
    },
    "health_insurance_coverage": {
        "dataset": "acs/acs5", "vintage": 2023, "variable": "B27001_001E",
        "name": "Total Health Insurance Universe", "category": "demographics", "geo_for": "state:*",
    },
    # -- ACS 1-Year: More timely versions --
    "national_population_1yr": {
        "dataset": "acs/acs1", "vintage": 2023, "variable": "B01001_001E",
        "name": "Total Population (1-Year)", "category": "demographics", "geo_for": "us:*",
    },
    "national_median_income_1yr": {
        "dataset": "acs/acs1", "vintage": 2023, "variable": "B19013_001E",
        "name": "Median Household Income (1-Year)", "category": "income", "geo_for": "us:*",
    },
    # -- County Business Patterns --
    "cbp_establishments": {
        "dataset": "cbp", "vintage": 2022, "variable": "ESTAB",
        "name": "Number of Establishments", "category": "business", "geo_for": "state:*",
    },
    "cbp_employment": {
        "dataset": "cbp", "vintage": 2022, "variable": "EMP",
        "name": "Paid Employees (mid-March)", "category": "employment", "geo_for": "state:*",
    },
    "cbp_annual_payroll": {
        "dataset": "cbp", "vintage": 2022, "variable": "PAYANN",
        "name": "Annual Payroll ($1,000)", "category": "income", "geo_for": "state:*",
    },
}

CENSUS_KNOWN_DATASETS = {
    "acs/acs5": "American Community Survey 5-Year Data",
    "acs/acs1": "American Community Survey 1-Year Data",
    "acs/acs5/profile": "ACS 5-Year Data Profiles",
    "acs/acs5/subject": "ACS 5-Year Subject Tables",
    "dec/pl": "Decennial Census Redistricting Data",
    "dec/dhc": "Decennial Census Demographic and Housing",
    "pep/population": "Population Estimates Program",
    "pep/charagegroups": "Population Estimates by Age Groups",
    "cbp": "County Business Patterns",
    "zbp": "ZIP Code Business Patterns",
    "ecnbasic": "Economic Census Basic Statistics",
    "nonemp": "Nonemployer Statistics",
    "abs/cb": "Annual Business Survey",
    "acs/flows": "ACS Migration Flows",
    "timeseries/poverty/histpov2": "Historical Poverty Tables",
}

# Baseline snapshot for catalog drift detection.
# Updated: 2026-03-16. Re-run with --update-baseline to refresh.
CENSUS_CATALOG_BASELINE = {
    "total_datasets": 1760,
    "distinct_paths": 564,
    "distinct_vintages": 41,
    "acs5_variables": 28295,
}

# ---------------------------------------------------------------------------
# IMF (SDMX)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Eurostat (SDMX)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# BIS (SDMX)
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# ECB (SDMX)
# ---------------------------------------------------------------------------

ECB_SERIES = {
    "m1":            {"dataflow": "BSI", "key": "M.U2.Y.V.M10.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M1",           "category": "liquidity"},
    "m2":            {"dataflow": "BSI", "key": "M.U2.Y.V.M20.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M2",           "category": "liquidity"},
    "m3":            {"dataflow": "BSI", "key": "M.U2.Y.V.M30.X.I.U2.2300.Z01.E", "series_id": "ECB_EA_M3",           "category": "liquidity"},
    "m3_growth":     {"dataflow": "BSI", "key": "M.U2.N.V.M30.X.I.U2.2300.Z01.A", "series_id": "ECB_EA_M3_GROWTH",     "category": "liquidity"},
    "deposit_rate":  {"dataflow": "FM",  "key": "B.U2.EUR.4F.KR.DFR.LEV",        "series_id": "ECB_EA_DEPOSIT_RATE",  "category": "rates"},
    "eurusd":        {"dataflow": "EXR", "key": "M.USD.EUR.SP00.A",              "series_id": "ECB_EURUSD",           "category": "fx"},
}

# ---------------------------------------------------------------------------
# OECD (SDMX)
# ---------------------------------------------------------------------------

OECD_DEFAULT_AGENCY_ID = "OECD.SDD.STES"


@dataclass(frozen=True)
class OECDSeriesConfig:
    dataflow: str
    series_id: str
    category: str
    agency_id: str = OECD_DEFAULT_AGENCY_ID
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

# ---------------------------------------------------------------------------
# World Bank
# ---------------------------------------------------------------------------


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

# ---------------------------------------------------------------------------
# ILO (SDMX) — series to be populated via catalog probing
# ---------------------------------------------------------------------------

ILO_SERIES: dict[str, dict[str, Any]] = {}

# ---------------------------------------------------------------------------
# UNSD (SDMX) — series to be populated via catalog probing
# ---------------------------------------------------------------------------

UNSD_SERIES: dict[str, dict[str, Any]] = {}
