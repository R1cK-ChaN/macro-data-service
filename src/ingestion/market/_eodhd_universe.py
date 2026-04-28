"""Seed universe of global (non-US) instruments fetched from EODHD.

Each entry pins the stable ``instrument_id`` used across provider ticker
renames, plus identity fields (ISIN/FIGI) so later OpenFIGI or EODHD
ID-mapping repair can rejoin segments without re-fetching history.

Asset-class coverage: equity / ETF / index plus FX (``.FOREX``), crypto
(``.CC``), and spot metals (also ``.FOREX`` — EODHD lists ``XAUUSD`` /
``XAGUSD`` / ``XPTUSD`` / ``XPDUSD`` / ``XCUUSD`` as currency rows backed
by the LBMA / spot reference). Continuous-front-month futures (``.COMM``)
require a separate EODHD add-on subscription not present on the current
account; if that lands later, a follow-up can extend this universe without
touching the rest of the stack.

No-overlap-by-design vs ``ingestion.market._macro_map.MACRO_MARKET_UNIVERSE``:

* ``MACRO_FX_EURUSD`` (FRED H.10 noon rate) and the EODHD ``EURUSD.FOREX``
  spot below are **different price definitions of the same underlying**.
  The macro lane projects the official central-bank reference (`H.10`
  10:15 ET fix or ECB 14:15 CET fix), the EODHD lane projects an OTC
  spot continuous tape. Different ``instrument_id``, different time slice
  per day, both legitimate. Downstream chooses by ``instrument_id`` —
  this registry never auto-deduplicates.
* The same applies to commodity overlap: ``MACRO_COMMOD_WTI`` /
  ``_BRENT`` / ``_NATGAS`` are EIA spot reference series. EODHD has no
  spot-crude / spot-natgas equivalent on this plan, so there is no
  EODHD-side row to overlap with — the macro lane is the only path for
  those three series. Spot metals (XAUUSD … XCUUSD) currently have no
  macro-lane counterpart, so likewise no overlap to coordinate.

Roll convention / 24-7 caveats (probed against EODHD):

* `.CC` crypto bars are 24/7 calendar-day-keyed — EODHD aligns on
  UTC-day close, so a Sunday bar appears for every crypto symbol.
  ``market_price_bars`` PK includes ``date`` so duplicate-week handling
  is automatic; downstream rate-of-change calculators must decide
  whether to mask non-trading days for cross-asset comparisons.
* `.FOREX` spot bars (incl. metals) are five-day-week — EODHD skips
  Saturday and the closing UTC slice of Friday until Sunday's Asian
  open, matching standard interbank conventions.
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
    asset_class: str                # equity, equity_etf, index, fx, crypto, commodity
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
    # ── FX spot pairs (EODHD .FOREX) ─────────────────────────────────────
    # Currency labels follow market convention: pricing the BASE currency
    # in QUOTE units. ``EURUSD`` quotes USD per EUR → ``currency`` is USD.
    EODHDUniverseEntry(
        instrument_id="FX_EURUSD",
        eodhd_ticker="EURUSD.FOREX",
        primary_ticker="EURUSD",
        exchange_code="FOREX",
        name="EUR/USD Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="USD",
        description_for_agent="EUR/USD spot — most-traded G10 cross.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_USDJPY",
        eodhd_ticker="USDJPY.FOREX",
        primary_ticker="USDJPY",
        exchange_code="FOREX",
        name="USD/JPY Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="JPY",
        description_for_agent="USD/JPY spot — yen carry / Japan macro proxy.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_GBPUSD",
        eodhd_ticker="GBPUSD.FOREX",
        primary_ticker="GBPUSD",
        exchange_code="FOREX",
        name="GBP/USD Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="USD",
        description_for_agent="GBP/USD spot (cable).",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_USDCNH",
        eodhd_ticker="USDCNH.FOREX",
        primary_ticker="USDCNH",
        exchange_code="FOREX",
        name="USD/CNH Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="CNH",
        description_for_agent="USD/CNH spot — offshore yuan, the freely-traded China FX gauge.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_AUDUSD",
        eodhd_ticker="AUDUSD.FOREX",
        primary_ticker="AUDUSD",
        exchange_code="FOREX",
        name="AUD/USD Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="USD",
        description_for_agent="AUD/USD spot — commodity-linked G10.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_USDCAD",
        eodhd_ticker="USDCAD.FOREX",
        primary_ticker="USDCAD",
        exchange_code="FOREX",
        name="USD/CAD Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="CAD",
        description_for_agent="USD/CAD spot — North American oil-linked G10 cross.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_USDCHF",
        eodhd_ticker="USDCHF.FOREX",
        primary_ticker="USDCHF",
        exchange_code="FOREX",
        name="USD/CHF Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="CHF",
        description_for_agent="USD/CHF spot — Swiss safe-haven G10 cross.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_NZDUSD",
        eodhd_ticker="NZDUSD.FOREX",
        primary_ticker="NZDUSD",
        exchange_code="FOREX",
        name="NZD/USD Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="USD",
        description_for_agent="NZD/USD spot — antipodean G10 cross.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_EURGBP",
        eodhd_ticker="EURGBP.FOREX",
        primary_ticker="EURGBP",
        exchange_code="FOREX",
        name="EUR/GBP Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="GBP",
        description_for_agent="EUR/GBP spot — euro vs sterling.",
    ),
    EODHDUniverseEntry(
        instrument_id="FX_EURJPY",
        eodhd_ticker="EURJPY.FOREX",
        primary_ticker="EURJPY",
        exchange_code="FOREX",
        name="EUR/JPY Spot",
        asset_class="fx",
        market="Global FX spot",
        currency="JPY",
        description_for_agent="EUR/JPY spot — euro vs yen carry-sensitive cross.",
    ),
    # ── Spot metals (EODHD .FOREX, USD-denominated) ──────────────────────
    # `.COMM` continuous front-month futures are not available on the
    # current EODHD plan; spot metals via FOREX are the closest substitute
    # for backtesting and macro observation.
    EODHDUniverseEntry(
        instrument_id="COMMOD_GOLD_SPOT",
        eodhd_ticker="XAUUSD.FOREX",
        primary_ticker="XAUUSD",
        exchange_code="FOREX",
        name="Gold Spot (XAU/USD)",
        asset_class="commodity",
        market="Global spot metals",
        currency="USD",
        description_for_agent="Spot gold price (USD per troy oz).",
    ),
    EODHDUniverseEntry(
        instrument_id="COMMOD_SILVER_SPOT",
        eodhd_ticker="XAGUSD.FOREX",
        primary_ticker="XAGUSD",
        exchange_code="FOREX",
        name="Silver Spot (XAG/USD)",
        asset_class="commodity",
        market="Global spot metals",
        currency="USD",
        description_for_agent="Spot silver price (USD per troy oz).",
    ),
    EODHDUniverseEntry(
        instrument_id="COMMOD_PLATINUM_SPOT",
        eodhd_ticker="XPTUSD.FOREX",
        primary_ticker="XPTUSD",
        exchange_code="FOREX",
        name="Platinum Spot (XPT/USD)",
        asset_class="commodity",
        market="Global spot metals",
        currency="USD",
        description_for_agent="Spot platinum price (USD per troy oz).",
    ),
    EODHDUniverseEntry(
        instrument_id="COMMOD_PALLADIUM_SPOT",
        eodhd_ticker="XPDUSD.FOREX",
        primary_ticker="XPDUSD",
        exchange_code="FOREX",
        name="Palladium Spot (XPD/USD)",
        asset_class="commodity",
        market="Global spot metals",
        currency="USD",
        description_for_agent="Spot palladium price (USD per troy oz).",
    ),
    EODHDUniverseEntry(
        instrument_id="COMMOD_COPPER_SPOT",
        eodhd_ticker="XCUUSD.FOREX",
        primary_ticker="XCUUSD",
        exchange_code="FOREX",
        name="Copper Spot (XCU/USD)",
        asset_class="commodity",
        market="Global spot metals",
        currency="USD",
        description_for_agent="Spot copper price (USD per troy oz unit on EODHD's tape).",
    ),
    # ── Crypto (EODHD .CC, 24/7 calendar-day-keyed) ──────────────────────
    EODHDUniverseEntry(
        instrument_id="CRYPTO_BTC_USD",
        eodhd_ticker="BTC-USD.CC",
        primary_ticker="BTC-USD",
        exchange_code="CC",
        name="Bitcoin / US Dollar",
        asset_class="crypto",
        market="Cryptocurrency spot",
        currency="USD",
        description_for_agent="Bitcoin spot price (USD).",
    ),
    EODHDUniverseEntry(
        instrument_id="CRYPTO_ETH_USD",
        eodhd_ticker="ETH-USD.CC",
        primary_ticker="ETH-USD",
        exchange_code="CC",
        name="Ethereum / US Dollar",
        asset_class="crypto",
        market="Cryptocurrency spot",
        currency="USD",
        description_for_agent="Ethereum spot price (USD).",
    ),
    EODHDUniverseEntry(
        instrument_id="CRYPTO_SOL_USD",
        eodhd_ticker="SOL-USD.CC",
        primary_ticker="SOL-USD",
        exchange_code="CC",
        name="Solana / US Dollar",
        asset_class="crypto",
        market="Cryptocurrency spot",
        currency="USD",
        description_for_agent="Solana spot price (USD).",
    ),
    EODHDUniverseEntry(
        instrument_id="CRYPTO_BNB_USD",
        eodhd_ticker="BNB-USD.CC",
        primary_ticker="BNB-USD",
        exchange_code="CC",
        name="BNB / US Dollar",
        asset_class="crypto",
        market="Cryptocurrency spot",
        currency="USD",
        description_for_agent="BNB spot price (USD).",
    ),
    EODHDUniverseEntry(
        instrument_id="CRYPTO_XRP_USD",
        eodhd_ticker="XRP-USD.CC",
        primary_ticker="XRP-USD",
        exchange_code="CC",
        name="XRP / US Dollar",
        asset_class="crypto",
        market="Cryptocurrency spot",
        currency="USD",
        description_for_agent="XRP spot price (USD).",
    ),
)


EODHD_UNIVERSE_BY_TICKER: dict[str, EODHDUniverseEntry] = {
    entry.eodhd_ticker: entry for entry in EODHD_GLOBAL_UNIVERSE
}

EODHD_UNIVERSE_BY_INSTRUMENT_ID: dict[str, EODHDUniverseEntry] = {
    entry.instrument_id: entry for entry in EODHD_GLOBAL_UNIVERSE
}
