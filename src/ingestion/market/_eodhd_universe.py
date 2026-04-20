"""Seed universe of global (non-US) instruments fetched from EODHD.

Each entry pins the stable ``instrument_id`` used across provider ticker
renames, plus identity fields (ISIN/FIGI) so later OpenFIGI or EODHD
ID-mapping repair can rejoin segments without re-fetching history.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EODHDUniverseEntry:
    instrument_id: str              # e.g. "JP_NIKKEI225"
    eodhd_ticker: str               # full EODHD ticker, e.g. "N225.INDX"
    primary_ticker: str             # bare ticker shown to agents/humans
    exchange_code: str              # EODHD exchange suffix, e.g. "INDX"
    name: str
    asset_class: str                # equity, equity_etf, index
    market: str
    currency: str
    isin: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    description_for_agent: str = ""


EODHD_GLOBAL_UNIVERSE: tuple[EODHDUniverseEntry, ...] = (
    EODHDUniverseEntry(
        instrument_id="JP_NIKKEI225",
        eodhd_ticker="N225.INDX",
        primary_ticker="N225",
        exchange_code="INDX",
        name="Nikkei 225 Index",
        asset_class="index",
        market="Japan equity index",
        currency="JPY",
        description_for_agent="Flagship Japan equity benchmark.",
    ),
    EODHDUniverseEntry(
        instrument_id="DE_DAX",
        eodhd_ticker="GDAXI.INDX",
        primary_ticker="GDAXI",
        exchange_code="INDX",
        name="DAX 40 Performance Index",
        asset_class="index",
        market="Germany equity index",
        currency="EUR",
        description_for_agent="Germany blue-chip DAX 40 performance index.",
    ),
    EODHDUniverseEntry(
        instrument_id="HK_HSI",
        eodhd_ticker="HSI.INDX",
        primary_ticker="HSI",
        exchange_code="INDX",
        name="Hang Seng Index",
        asset_class="index",
        market="Hong Kong equity index",
        currency="HKD",
        description_for_agent="Hong Kong Hang Seng Index — HK equity proxy.",
    ),
    EODHDUniverseEntry(
        instrument_id="GLOBAL_VWRL_LSE",
        eodhd_ticker="VWRL.LSE",
        primary_ticker="VWRL",
        exchange_code="LSE",
        name="Vanguard FTSE All-World UCITS ETF",
        asset_class="equity_etf",
        market="Global equity ETF listed on London Stock Exchange",
        currency="USD",
        isin="IE00B3RBWM25",
        description_for_agent="Global developed + emerging equity tracker, LSE USD line.",
    ),
    EODHDUniverseEntry(
        instrument_id="DE_SAP",
        eodhd_ticker="SAP.XETRA",
        primary_ticker="SAP",
        exchange_code="XETRA",
        name="SAP SE",
        asset_class="equity",
        market="Germany equity market (XETRA)",
        currency="EUR",
        isin="DE0007164600",
        description_for_agent="SAP — Europe mega-cap enterprise software.",
    ),
    EODHDUniverseEntry(
        instrument_id="HK_TENCENT",
        eodhd_ticker="0700.HK",
        primary_ticker="0700",
        exchange_code="HK",
        name="Tencent Holdings Ltd",
        asset_class="equity",
        market="Hong Kong equity market",
        currency="HKD",
        isin="KYG875721634",
        description_for_agent="Tencent — China mega-cap tech listed in Hong Kong.",
    ),
)


EODHD_UNIVERSE_BY_TICKER: dict[str, EODHDUniverseEntry] = {
    entry.eodhd_ticker: entry for entry in EODHD_GLOBAL_UNIVERSE
}

EODHD_UNIVERSE_BY_INSTRUMENT_ID: dict[str, EODHDUniverseEntry] = {
    entry.instrument_id: entry for entry in EODHD_GLOBAL_UNIVERSE
}
