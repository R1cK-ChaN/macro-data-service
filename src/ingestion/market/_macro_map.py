"""Macro → market-layer mapping for rates, FX, and commodities.

The issue #1 plan calls for surfacing rates / FX / commodity series from
FRED and EIA inside the ``market_*`` schema so a single
``get_market_history`` call can answer for "US 10Y yield" the same way it
does for "SPY". Each entry projects a macro observation into a synthetic
one-column daily bar: open=high=low=close=value.

No corporate actions exist for these series — ``has_missing_corp_acts``
stays False because there are no missing corp acts; ``split_factor`` stays
1.0 and ``dividend_cash`` stays 0.0.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MacroMarketEntry:
    """Link a macro time series to a synthetic market instrument."""

    instrument_id: str          # stable macro-market id, e.g. "MACRO_RATES_US_10Y"
    ticker: str                 # agent-facing ticker, e.g. "US10Y"
    name: str
    asset_class: str            # "rate" | "fx" | "commodity"
    market: str
    currency: str
    unit: str                   # "percent" | "usd_per_barrel" | "usd_per_eur" …
    source: str                 # storage.indicators.source — "fred" | "eia"
    series_id: str              # storage.indicators.series_id
    description_for_agent: str = ""


# Rates — FRED (US) + ECB (euro area) series. Values are percent.
RATES_UNIVERSE: tuple[MacroMarketEntry, ...] = (
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_FEDFUNDS",
        ticker="FEDFUNDS",
        name="Federal Funds Effective Rate",
        asset_class="rate",
        market="US policy rate",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="DFF",
        description_for_agent="Effective overnight US federal funds rate (percent).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_2Y",
        ticker="US2Y",
        name="US 2Y Treasury Yield",
        asset_class="rate",
        market="US Treasury yield curve",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="DGS2",
        description_for_agent="Constant-maturity 2-year Treasury yield (percent).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_10Y",
        ticker="US10Y",
        name="US 10Y Treasury Yield",
        asset_class="rate",
        market="US Treasury yield curve",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="DGS10",
        description_for_agent="Constant-maturity 10-year Treasury yield (percent).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_30Y",
        ticker="US30Y",
        name="US 30Y Treasury Yield",
        asset_class="rate",
        market="US Treasury yield curve",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="DGS30",
        description_for_agent="Constant-maturity 30-year Treasury yield (percent).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_10Y_REAL",
        ticker="US10Y_REAL",
        name="US 10Y Real Yield (TIPS)",
        asset_class="rate",
        market="US inflation-linked yield",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="DFII10",
        description_for_agent="Constant-maturity 10-year TIPS real yield (percent).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_US_10Y_2Y_SPREAD",
        ticker="US10Y2Y",
        name="US 10Y-2Y Treasury Spread",
        asset_class="rate",
        market="US Treasury yield curve",
        currency="USD",
        unit="percent",
        source="fred",
        series_id="T10Y2Y",
        description_for_agent="10Y minus 2Y Treasury yield spread (percentage points).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_RATES_EA_DEPOSIT",
        ticker="EADEPO",
        name="ECB Deposit Facility Rate",
        asset_class="rate",
        market="Euro area policy rate",
        currency="EUR",
        unit="percent",
        source="ecb",
        series_id="ECB_EA_DEPOSIT_RATE",
        description_for_agent="ECB deposit facility rate — the policy floor of the euro area corridor (percent).",
    ),
)


# FX — FRED (broad dollar, CNY) + ECB (EUR/USD). Values are spot exchange rates.
FX_UNIVERSE: tuple[MacroMarketEntry, ...] = (
    MacroMarketEntry(
        instrument_id="MACRO_FX_DXY_BROAD",
        ticker="DXYB",
        name="Broad Dollar Index",
        asset_class="fx",
        market="US dollar trade-weighted index",
        currency="USD",
        unit="index_jan_2006_100",
        source="fred",
        series_id="DTWEXBGS",
        description_for_agent="Fed broad trade-weighted US dollar index (Jan 2006 = 100).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_FX_USDCNY",
        ticker="USDCNY",
        name="USD/CNY Spot",
        asset_class="fx",
        market="US dollar / Chinese yuan spot",
        currency="CNY",
        unit="cny_per_usd",
        source="fred",
        series_id="DEXCHUS",
        description_for_agent="Chinese yuan per US dollar (spot).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_FX_EURUSD",
        ticker="EURUSD",
        name="EUR/USD Spot",
        asset_class="fx",
        market="Euro / US dollar spot",
        currency="USD",
        unit="usd_per_eur",
        source="ecb",
        # Daily EXR series — the monthly ECB_EURUSD average would otherwise
        # surface as 12 bars/year on a 1d interval.
        series_id="ECB_EURUSD_D",
        description_for_agent="US dollars per euro (ECB daily reference rate).",
    ),
)


# Commodities — EIA daily series, values are USD per unit.
COMMODITIES_UNIVERSE: tuple[MacroMarketEntry, ...] = (
    MacroMarketEntry(
        instrument_id="MACRO_COMMOD_WTI",
        ticker="WTI",
        name="WTI Crude Oil Spot",
        asset_class="commodity",
        market="Global crude oil benchmark",
        currency="USD",
        unit="usd_per_barrel",
        source="eia",
        series_id="EIA_WTI",
        description_for_agent="West Texas Intermediate crude oil spot price (USD/bbl).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_COMMOD_BRENT",
        ticker="BRENT",
        name="Brent Crude Oil Spot",
        asset_class="commodity",
        market="Global crude oil benchmark",
        currency="USD",
        unit="usd_per_barrel",
        source="eia",
        series_id="EIA_BRENT",
        description_for_agent="Brent crude oil spot price (USD/bbl).",
    ),
    MacroMarketEntry(
        instrument_id="MACRO_COMMOD_NATGAS",
        ticker="NATGAS",
        name="Henry Hub Natural Gas Futures Front Month",
        asset_class="commodity",
        market="US natural gas benchmark",
        currency="USD",
        unit="usd_per_mmbtu",
        source="eia",
        series_id="EIA_NATGAS",
        description_for_agent="Henry Hub natural-gas front-month futures price (USD/MMBtu).",
    ),
)


MACRO_MARKET_UNIVERSE: tuple[MacroMarketEntry, ...] = (
    *RATES_UNIVERSE,
    *FX_UNIVERSE,
    *COMMODITIES_UNIVERSE,
)

MACRO_MARKET_BY_INSTRUMENT_ID: dict[str, MacroMarketEntry] = {
    entry.instrument_id: entry for entry in MACRO_MARKET_UNIVERSE
}

MACRO_MARKET_BY_TICKER: dict[str, MacroMarketEntry] = {
    entry.ticker: entry for entry in MACRO_MARKET_UNIVERSE
}

MACRO_MARKET_BY_SERIES: dict[tuple[str, str], MacroMarketEntry] = {
    (entry.source, entry.series_id): entry for entry in MACRO_MARKET_UNIVERSE
}
