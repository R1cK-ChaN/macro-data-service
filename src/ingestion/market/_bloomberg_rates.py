"""Licensed Bloomberg rates universe for market-price ingestion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BloombergRateEntry:
    instrument_id: str
    ticker: str
    provider_symbol: str
    name: str
    curve: str
    tenor: str
    asset_class: str
    market: str
    currency: str
    unit: str
    provider: str
    source_name: str
    env_vars: tuple[str, ...]
    description_for_agent: str = ""


BLOOMBERG_RATE_UNIVERSE: tuple[BloombergRateEntry, ...] = (
    BloombergRateEntry(
        instrument_id="RATES_USD_OIS_3M_BLOOMBERG",
        ticker="USSOC",
        provider_symbol="USSOC BGN Curncy",
        name="USD 3M OIS Swap Rate",
        curve="USD OIS",
        tenor="3M",
        asset_class="rate",
        market="USD overnight-indexed swap curve",
        currency="USD",
        unit="percent",
        provider="bloomberg",
        source_name="Bloomberg BGN",
        env_vars=(
            "BLOOMBERG_USSOC_CSV",
            "BLOOMBERG_USSOC_CSV_PATH",
            "BLOOMBERG_USD_OIS_3M_CSV",
            "BLOOMBERG_USD_OIS_3M_CSV_PATH",
        ),
        description_for_agent=(
            "USD 3-month OIS swap quote matching Bloomberg USSOC BGN Curncy. "
            "Loaded from a licensed Bloomberg BGN or compatible vendor CSV export."
        ),
    ),
)


BLOOMBERG_RATE_BY_INSTRUMENT_ID: dict[str, BloombergRateEntry] = {
    entry.instrument_id: entry for entry in BLOOMBERG_RATE_UNIVERSE
}

BLOOMBERG_RATE_BY_TICKER: dict[str, BloombergRateEntry] = {
    entry.ticker: entry for entry in BLOOMBERG_RATE_UNIVERSE
}

BLOOMBERG_RATE_BY_PROVIDER_SYMBOL: dict[str, BloombergRateEntry] = {
    entry.provider_symbol: entry for entry in BLOOMBERG_RATE_UNIVERSE
}
