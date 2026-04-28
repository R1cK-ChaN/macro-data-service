"""EODHD global market-data provider.

Mirrors ``TiingoMarketDataProvider`` but targets EODHD's ``/api/eod`` for
global (non-US) equities, ETFs, and indices. Reuses the market-layer
schema introduced in issue #1 P0 and shares the quality-check helpers
defined alongside the Tiingo provider.

EODHD's EOD endpoint does not ship dividend/split metadata, so every bar
is flagged ``has_missing_corp_acts=True`` until the P2 corporate-action
flow (``/api/div``, ``/api/splits``) is wired in a follow-up.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.market._eodhd_universe import (
    EODHD_GLOBAL_UNIVERSE,
    EODHDUniverseEntry,
)
from ingestion.market.clients._tiingo import (
    DEFAULT_BREAK_THRESHOLD,
    PRE2018_CUTOFF,
    check_adjustment_applied,
    check_ohlc_sanity,
    detect_history_breaks,
)
from ingestion.market.scrapers._eodhd import (
    EODHDAPIError,
    EODHDClient,
    EODHDDailyBar,
    EODHDNotFoundError,
)
from storage import (
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketSymbolHistoryRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


# Asset classes that can carry corporate actions (dividends/splits) and
# can be "delisted". FX, crypto, and spot metals are continuous-tape
# instruments — there are no splits, no dividends, and the "pre-2018
# delisted" probe doesn't apply. Bars for these asset classes must NOT
# carry has_missing_corp_acts / has_pre2018_delisted flags, otherwise
# downstream agents will surface bogus quality warnings on every FX /
# crypto / spot-metal bar.
#
# bond_etf / commodity_etf are kept in the bearing set: bond ETFs make
# regular distributions, and commodity ETFs do split. Indices are kept
# even though indices themselves don't pay dividends because EODHD
# returns adjusted_close = close for indices and the missing-CA flag
# remains the right signal for "we don't have an underlying-basket
# total-return view yet".
_CORP_ACTION_BEARING_ASSET_CLASSES = frozenset({
    "equity", "equity_etf", "bond_etf", "commodity_etf", "index",
})


class EODHDMarketDataProvider:
    """High-level EODHD provider for global (non-US) coverage."""

    source_name = "eodhd"

    def __init__(
        self,
        client: EODHDClient | None = None,
        *,
        universe: tuple[EODHDUniverseEntry, ...] = EODHD_GLOBAL_UNIVERSE,
        break_threshold: float = DEFAULT_BREAK_THRESHOLD,
        request_sleep: float = 0.25,
    ) -> None:
        self.client = client or EODHDClient()
        self.universe = universe
        self.break_threshold = break_threshold
        self.request_sleep = request_sleep

    # -- seeding ------------------------------------------------------------

    def seed_universe(self, store: SQLiteEngineStore) -> int:
        for entry in self.universe:
            self._seed_single_entry(store, entry)
        return len(self.universe)

    @staticmethod
    def _seed_single_entry(
        store: SQLiteEngineStore, entry: EODHDUniverseEntry
    ) -> None:
        existing = store.get_market_instrument(entry.instrument_id)
        history_status = (
            existing.history_status if existing is not None else "provider_continuous"
        )
        store.upsert_market_instrument(
            MarketInstrumentRecord(
                instrument_id=entry.instrument_id,
                primary_ticker=entry.primary_ticker,
                name=entry.name,
                asset_class=entry.asset_class,
                market=entry.market,
                exchange_code=entry.exchange_code,
                currency=entry.currency,
                isin=entry.isin,
                composite_figi=entry.composite_figi,
                share_class_figi=entry.share_class_figi,
                primary_provider="eodhd",
                provider_symbols_json={"eodhd": entry.eodhd_ticker},
                history_status=history_status,
                description_for_agent=entry.description_for_agent,
            )
        )
        store.upsert_market_symbol_segment(
            MarketSymbolHistoryRecord(
                segment_id=f"{entry.instrument_id}:eodhd:{entry.eodhd_ticker}",
                instrument_id=entry.instrument_id,
                ticker=entry.eodhd_ticker,
                provider_name="eodhd",
                exchange_code=entry.exchange_code,
                isin=entry.isin,
                figi=entry.composite_figi,
                valid_from="1900-01-01",
                valid_to="",
                event_type="listing_start",
                mapping_confidence="provider_native",
                source_name="eodhd.eod.meta",
                raw_json={"seed": True},
            )
        )

    # -- provider API ------------------------------------------------------

    def get_daily_bars(
        self,
        ticker: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[EODHDDailyBar]:
        return self.client.get_daily_bars(ticker, start_date=start_date, end_date=end_date)

    def refresh_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        """Fetch EODHD bars and persist into ``market_price_bars``.

        ``symbol`` accepts either the full EODHD ticker (``N225.INDX``) or a
        bare primary ticker (``N225``) when it unambiguously resolves
        through the seeded universe.
        """
        entry = self._resolve_universe_entry(symbol)
        if entry is None:
            existing = store.find_market_instrument_by_ticker(symbol)
            if existing is None or existing.primary_provider != "eodhd":
                logger.warning("refresh_market_history(eodhd): %s not in seeded universe", symbol)
                return RefreshStats(source="eodhd", count=0)
            eodhd_ticker = existing.provider_symbols_json.get("eodhd", existing.primary_ticker)
            entry = EODHDUniverseEntry(
                instrument_id=existing.instrument_id,
                eodhd_ticker=eodhd_ticker,
                primary_ticker=existing.primary_ticker,
                exchange_code=existing.exchange_code,
                name=existing.name,
                asset_class=existing.asset_class,
                market=existing.market,
                currency=existing.currency,
                isin=existing.isin,
                composite_figi=existing.composite_figi,
                share_class_figi=existing.share_class_figi,
                description_for_agent=existing.description_for_agent,
            )
        elif store.get_market_instrument(entry.instrument_id) is None:
            self._seed_single_entry(store, entry)

        try:
            bars = self.get_daily_bars(entry.eodhd_ticker, start_date=start, end_date=end)
        except EODHDNotFoundError:
            logger.warning("EODHD ticker not found: %s", entry.eodhd_ticker)
            return RefreshStats(source="eodhd", count=0)
        except EODHDAPIError:
            logger.warning("EODHD fetch failed for %s", entry.eodhd_ticker, exc_info=True)
            return RefreshStats(source="eodhd", count=0)

        if not bars:
            return RefreshStats(source="eodhd", count=0)

        adjustment_applied = check_adjustment_applied(bars)
        # FX / crypto / spot-metal lines have no corporate actions and
        # don't "delist" in the equity sense — gate the equity-only flags
        # to corp-action-bearing asset classes so non-equity bars don't
        # surface bogus warnings to downstream agents.
        carries_corp_actions = entry.asset_class in _CORP_ACTION_BEARING_ASSET_CLASSES
        # Break detection: run when adjustments are present (so the series
        # is comparable across corporate actions) OR when the series has
        # no corporate actions to begin with — for FX/crypto/spot-metal
        # tapes a provider splice or scale jump still needs to surface as
        # has_break_detected, even though adjusted_close equals close
        # because there's nothing to adjust for.
        break_dates = (
            detect_history_breaks(bars, threshold=self.break_threshold)
            if adjustment_applied or not carries_corp_actions
            else []
        )
        break_set = set(break_dates)

        segment_id = f"{entry.instrument_id}:eodhd:{entry.eodhd_ticker}"
        count = 0
        for bar in bars:
            flags_json: dict[str, Any] = {}
            if not check_ohlc_sanity(bar):
                flags_json["ohlc_sanity"] = "failed"
            if not adjustment_applied:
                flags_json["adjustment_check"] = "raw_only"
            if carries_corp_actions:
                flags_json["corp_acts_missing"] = "eodhd_eod_endpoint_has_no_div_split"
            if bar.date in break_set:
                flags_json["break_detected"] = True

            store.upsert_market_price_bar(
                MarketPriceBarRecord(
                    instrument_id=entry.instrument_id,
                    source_segment_id=segment_id,
                    date=bar.date,
                    bar_interval="1d",
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    adjusted_open=bar.adj_open,
                    adjusted_high=bar.adj_high,
                    adjusted_low=bar.adj_low,
                    adjusted_close=bar.adj_close,
                    adjusted_volume=bar.adj_volume,
                    dividend_cash=bar.div_cash,
                    split_factor=bar.split_factor,
                    source_name="EODHD",
                    source_symbol=entry.eodhd_ticker,
                    has_break_detected=bar.date in break_set,
                    has_pre2018_delisted=(
                        carries_corp_actions
                        and bar.date < PRE2018_CUTOFF
                        and not adjustment_applied
                    ),
                    has_missing_corp_acts=carries_corp_actions,
                    has_mapping_review_needed=False,
                    quality_flags_json=flags_json,
                )
            )
            count += 1

        if break_dates:
            # Only upgrade history_status; never downgrade a prior alert
            # on a partial-window refresh.
            store.update_instrument_history_status(entry.instrument_id, "break_detected")
        return RefreshStats(source="eodhd", count=count)

    def refresh_universe(
        self,
        store: SQLiteEngineStore,
        *,
        lookback_days: int = 365,
    ) -> RefreshStats:
        self.seed_universe(store)
        start = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        total = 0
        for entry in self.universe:
            stats = self.refresh_market_history(store, entry.eodhd_ticker, start=start)
            total += stats.count
            if self.request_sleep:
                time.sleep(self.request_sleep)
        return RefreshStats(source="eodhd", count=total)

    # -- read API ----------------------------------------------------------

    def get_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
        adjusted: bool = True,
    ) -> list[dict[str, Any]]:
        instrument = self._resolve_instrument(store, symbol)
        if instrument is None:
            return []
        bars = store.list_market_price_bars(instrument.instrument_id, start=start, end=end)
        rows: list[dict[str, Any]] = []
        for bar in bars:
            flags: list[str] = []
            if bar.has_break_detected:
                flags.append("break_detected")
            if bar.has_missing_corp_acts:
                flags.append("missing_corp_acts")
            if bar.has_pre2018_delisted:
                flags.append("pre2018_delisted")
            if bar.has_mapping_review_needed:
                flags.append("mapping_review_needed")

            if adjusted:
                price_open = bar.adjusted_open if bar.adjusted_open is not None else bar.open
                price_high = bar.adjusted_high if bar.adjusted_high is not None else bar.high
                price_low = bar.adjusted_low if bar.adjusted_low is not None else bar.low
                price_close = bar.adjusted_close if bar.adjusted_close is not None else bar.close
                price_volume = bar.adjusted_volume if bar.adjusted_volume is not None else bar.volume
            else:
                price_open, price_high, price_low = bar.open, bar.high, bar.low
                price_close, price_volume = bar.close, bar.volume

            rows.append(
                {
                    "instrument_id": instrument.instrument_id,
                    "ticker": instrument.primary_ticker,
                    "name": instrument.name,
                    "market": instrument.market,
                    "isin": instrument.isin,
                    "openfigi": instrument.openfigi or instrument.composite_figi,
                    "history_status": instrument.history_status,
                    "date": bar.date,
                    "open": price_open,
                    "high": price_high,
                    "low": price_low,
                    "close": price_close,
                    "adjusted_close": bar.adjusted_close if bar.adjusted_close is not None else bar.close,
                    "volume": price_volume,
                    "quality_flags": flags,
                    "source": bar.source_name,
                    "agent_summary": _agent_summary(instrument, bar, price_close),
                }
            )
        return rows

    # -- helpers -----------------------------------------------------------

    def _resolve_universe_entry(self, symbol: str) -> EODHDUniverseEntry | None:
        # Look through the instance's own universe first so callers can pass
        # a custom tuple without falling through to the module-level default.
        for candidate in self.universe:
            if symbol in (candidate.instrument_id, candidate.eodhd_ticker):
                return candidate
        bare = symbol.upper()
        for candidate in self.universe:
            if candidate.primary_ticker == bare:
                return candidate
        return None

    def _resolve_instrument(
        self, store: SQLiteEngineStore, symbol: str
    ) -> MarketInstrumentRecord | None:
        entry = self._resolve_universe_entry(symbol)
        if entry is not None:
            return store.get_market_instrument(entry.instrument_id)
        return store.find_market_instrument_by_ticker(symbol)


def _agent_summary(
    instrument: MarketInstrumentRecord,
    bar: MarketPriceBarRecord,
    close: float,
) -> str:
    status_phrase = {
        "provider_continuous": "its history is provider-continuous through EODHD",
        "break_detected": "its history has an unresolved break (pending lazy repair)",
        "stitched": "its history was stitched from multiple ticker segments",
        "manual_review": "its history is flagged for manual review",
    }.get(instrument.history_status, f"history_status={instrument.history_status}")
    return (
        f"{instrument.primary_ticker} closed at {close:.2f} {instrument.currency} "
        f"on {bar.date}. {status_phrase[0].upper()}{status_phrase[1:]}."
    )
