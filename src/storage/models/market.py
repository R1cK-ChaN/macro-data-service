"""Storage records — market data records (price + instrument + symbol-history + bar).

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class MarketPriceRecord:
    symbol: str
    asset_class: str
    price: float
    change_pct: float | None
    timestamp: int
    name: str = ""


@dataclass(frozen=True)
class MarketInstrumentRecord:
    instrument_id: str                      # e.g. "US_SPY"
    primary_ticker: str                     # current trading ticker
    name: str                               # "SPDR S&P 500 ETF"
    asset_class: str                        # equity_etf, equity, bond_etf, commodity_etf, rate
    market: str                             # "United States equity market"
    exchange_code: str = ""                 # e.g. "NYSEARCA", "NASDAQ"
    currency: str = "USD"
    isin: str = ""
    openfigi: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    cusip: str = ""
    lei: str = ""
    primary_provider: str = "eodhd"
    provider_symbols_json: dict[str, str] = field(default_factory=dict)
    history_status: str = "provider_continuous"  # provider_continuous|break_detected|stitched|manual_review
    description_for_agent: str = ""


@dataclass(frozen=True)
class MarketSymbolHistoryRecord:
    segment_id: str                         # stable, e.g. f"{instrument_id}:{valid_from}:{ticker}"
    instrument_id: str
    ticker: str
    provider_name: str
    valid_from: str                         # YYYY-MM-DD
    valid_to: str = ""                      # YYYY-MM-DD or "" for open-ended
    exchange_code: str = ""
    isin: str = ""
    figi: str = ""
    event_type: str = "listing_start"       # listing_start|ticker_rename|exchange_change|delisting|manual_link
    mapping_confidence: str = "provider_native"  # provider_native|auto_isin|auto_figi|name_match|manual
    source_name: str = ""
    raw_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketPriceBarRecord:
    instrument_id: str
    date: str                               # YYYY-MM-DD
    bar_interval: str                       # "1d"
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_name: str                        # provider display name
    source_symbol: str                      # ticker used at the provider
    source_segment_id: str = ""
    adjusted_open: float | None = None
    adjusted_high: float | None = None
    adjusted_low: float | None = None
    adjusted_close: float | None = None
    adjusted_volume: float | None = None
    dividend_cash: float = 0.0
    split_factor: float = 1.0
    has_break_detected: bool = False
    has_pre2018_delisted: bool = False
    has_missing_corp_acts: bool = False
    has_mapping_review_needed: bool = False
    quality_flags_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketCorpActionsRawRecord:
    """Audit-lane raw row for the market corporate-actions table.

    Mirrors ``CalendarCorpRawRecord`` in shape — separate types because
    the two lanes evolve independently (event-shaped vs ticker-shaped).
    See ``storage/schema.py`` for the rationale comment on the table.
    """

    provider: str           # "eodhd"
    ticker: str             # full provider ticker e.g. "AAPL.US"
    action_type: str        # "dividend" | "split"
    event_date: str         # YYYY-MM-DD — ex-date (div) or split-date
    snapshot_epoch_ms: int  # when WE fetched this snapshot
    content_hash: str       # sha256 over normalized mutable fields
    payload_json: str       # raw response row, verbatim
    fetched_at: str         # ISO-8601 UTC


@dataclass(frozen=True)
class MarketPriceBarsRawRecord:
    """Audit-lane raw row for the market price-bars table (issue #69 slice 2).

    Mirrors ``MarketCorpActionsRawRecord`` in shape but at the per-fetch
    granularity rather than per-event: one row per HTTP response per
    ``(provider, ticker)``. ``payload_json`` holds the full
    ``/api/eod/{ticker}`` body verbatim; ``content_hash`` is sha256 over the
    canonicalized bar array (sorted by date with envelope timestamps
    dropped) so an unchanged daily refresh dedupes via INSERT OR IGNORE.

    Same payoff as the calendar/corp-actions lanes: every value in
    ``market_price_bars`` is reproducible from raw, so projection logic
    can be re-run after a fix without re-spending provider quota.
    """

    provider: str           # "eodhd"
    ticker: str             # full provider ticker, e.g. "AAPL.US"
    snapshot_epoch_ms: int  # when WE fetched this snapshot, UTC ms
    content_hash: str       # sha256 over canonicalized bar array
    payload_json: str       # full HTTP response body, verbatim
    fetched_at: str         # ISO-8601 UTC convenience column
    request_params_json: str = "{}"  # from/to/etc — needed to interpret partial responses
