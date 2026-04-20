"""Seed universe of macro ETFs fetched from Tiingo.

Each entry defines the stable `instrument_id` that never moves across ticker
renames, plus identity fields used by OpenFIGI/ISIN repair in later phases.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TiingoUniverseEntry:
    instrument_id: str
    ticker: str
    name: str
    asset_class: str
    market: str
    exchange_code: str
    isin: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    description_for_agent: str = ""


TIINGO_MACRO_ETF_UNIVERSE: tuple[TiingoUniverseEntry, ...] = (
    TiingoUniverseEntry(
        instrument_id="US_SPY",
        ticker="SPY",
        name="SPDR S&P 500 ETF",
        asset_class="equity_etf",
        market="United States equity market",
        exchange_code="NYSEARCA",
        isin="US78462F1030",
        composite_figi="BBG000BDTBL9",
        description_for_agent="Flagship S&P 500 tracker — broad US large-cap equity proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_QQQ",
        ticker="QQQ",
        name="Invesco QQQ Trust",
        asset_class="equity_etf",
        market="United States equity market",
        exchange_code="NASDAQ",
        isin="US46090E1038",
        composite_figi="BBG000BSWKH7",
        description_for_agent="Nasdaq-100 tracker — US large-cap tech/growth proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_IWM",
        ticker="IWM",
        name="iShares Russell 2000 ETF",
        asset_class="equity_etf",
        market="United States equity market",
        exchange_code="NYSEARCA",
        isin="US4642876555",
        composite_figi="BBG000BM6LV5",
        description_for_agent="Russell 2000 tracker — US small-cap equity proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_DIA",
        ticker="DIA",
        name="SPDR Dow Jones Industrial Average ETF",
        asset_class="equity_etf",
        market="United States equity market",
        exchange_code="NYSEARCA",
        isin="US73935A1043",
        composite_figi="BBG000BT4HY4",
        description_for_agent="Dow Jones Industrial Average tracker — US blue-chip proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_TLT",
        ticker="TLT",
        name="iShares 20+ Year Treasury Bond ETF",
        asset_class="bond_etf",
        market="United States fixed income market",
        exchange_code="NASDAQ",
        isin="US4642874329",
        description_for_agent="Long-duration US Treasury tracker — proxy for long rates.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_IEF",
        ticker="IEF",
        name="iShares 7-10 Year Treasury Bond ETF",
        asset_class="bond_etf",
        market="United States fixed income market",
        exchange_code="NASDAQ",
        isin="US4642874402",
        description_for_agent="Intermediate US Treasury tracker — proxy for mid rates.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_HYG",
        ticker="HYG",
        name="iShares iBoxx $ High Yield Corporate Bond ETF",
        asset_class="bond_etf",
        market="United States fixed income market",
        exchange_code="NYSEARCA",
        isin="US4642885135",
        description_for_agent="US high-yield credit tracker — risk-appetite proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_LQD",
        ticker="LQD",
        name="iShares iBoxx $ Investment Grade Corporate Bond ETF",
        asset_class="bond_etf",
        market="United States fixed income market",
        exchange_code="NYSEARCA",
        isin="US4642872422",
        description_for_agent="US investment-grade credit tracker — IG spread proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_GLD",
        ticker="GLD",
        name="SPDR Gold Shares",
        asset_class="commodity_etf",
        market="United States commodity ETF market",
        exchange_code="NYSEARCA",
        isin="US78463V1070",
        description_for_agent="Physical gold tracker — safe-haven and real-rates proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_SLV",
        ticker="SLV",
        name="iShares Silver Trust",
        asset_class="commodity_etf",
        market="United States commodity ETF market",
        exchange_code="NYSEARCA",
        isin="US46428Q1094",
        description_for_agent="Physical silver tracker — industrial/precious metals proxy.",
    ),
    TiingoUniverseEntry(
        instrument_id="US_USO",
        ticker="USO",
        name="United States Oil Fund",
        asset_class="commodity_etf",
        market="United States commodity ETF market",
        exchange_code="NYSEARCA",
        isin="US91232N1081",
        description_for_agent="WTI crude oil front-month tracker — energy proxy.",
    ),
)


TIINGO_UNIVERSE_BY_TICKER: dict[str, TiingoUniverseEntry] = {
    entry.ticker: entry for entry in TIINGO_MACRO_ETF_UNIVERSE
}

TIINGO_UNIVERSE_BY_INSTRUMENT_ID: dict[str, TiingoUniverseEntry] = {
    entry.instrument_id: entry for entry in TIINGO_MACRO_ETF_UNIVERSE
}
