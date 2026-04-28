"""Tiingo market-data provider.

Wraps ``TiingoClient`` to:

* seed the macro ETF universe into ``market_instruments``
* fetch Tiingo EOD bars and persist them to ``market_price_bars``
* run OHLC sanity / adjustment / break-detection quality checks
* expose ``refresh_market_history`` and ``get_market_history`` for callers
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ingestion.market._bars_canonicalize import bars_content_hash
from ingestion.market._tiingo_universe import (
    TIINGO_MACRO_ETF_UNIVERSE,
    TIINGO_UNIVERSE_BY_INSTRUMENT_ID,
    TIINGO_UNIVERSE_BY_TICKER,
    TiingoUniverseEntry,
)
from ingestion.market.scrapers._tiingo import (
    TiingoAPIError,
    TiingoClient,
    TiingoDailyBar,
)
from storage import (
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketPriceBarsRawRecord,
    MarketSymbolHistoryRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)

DEFAULT_BREAK_THRESHOLD = 0.5
PRE2018_CUTOFF = "2018-01-01"


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


def check_adjustment_applied(bars: list[TiingoDailyBar]) -> bool:
    """Return True if adjusted_close differs from close on enough rows.

    Mirrors the reference implementation in issue #1 but without pandas: if
    adjusted_close equals close on >=90% of rows the feed is treated as
    raw-only and downstream checks skip adjustment-dependent logic.
    """
    if not bars:
        return False
    comparable = 0
    same = 0
    for bar in bars:
        if bar.adj_close is None:
            continue
        comparable += 1
        if abs(bar.adj_close - bar.close) < 1e-9:
            same += 1
    if comparable == 0:
        return False
    return (same / comparable) < 0.9


def detect_history_breaks(
    bars: list[TiingoDailyBar],
    *,
    threshold: float = DEFAULT_BREAK_THRESHOLD,
) -> list[str]:
    """Detect adjusted-close breaks above threshold on non-corporate-action days.

    Returns the list of break dates (YYYY-MM-DD).
    """
    break_dates: list[str] = []
    prev_adj: float | None = None
    for bar in bars:
        if bar.adj_close is None:
            prev_adj = None
            continue
        if (bar.split_factor or 1.0) != 1.0 or (bar.div_cash or 0.0) > 0.0:
            # Skip corporate-action days as the docs specify.
            prev_adj = bar.adj_close
            continue
        if prev_adj is None or prev_adj == 0:
            prev_adj = bar.adj_close
            continue
        change = abs((bar.adj_close - prev_adj) / prev_adj)
        if change > threshold:
            break_dates.append(bar.date)
        prev_adj = bar.adj_close
    return break_dates


def check_ohlc_sanity(bar: TiingoDailyBar) -> bool:
    """Return True if the OHLC values are internally consistent and positive."""
    if min(bar.open, bar.high, bar.low, bar.close) <= 0:
        return False
    lo = bar.low
    hi = bar.high
    return lo <= bar.open <= hi and lo <= bar.close <= hi and lo <= hi


class TiingoMarketDataProvider:
    """High-level Tiingo provider used by the orchestrator and the service."""

    source_name = "tiingo"

    def __init__(
        self,
        client: TiingoClient | None = None,
        *,
        universe: tuple[TiingoUniverseEntry, ...] = TIINGO_MACRO_ETF_UNIVERSE,
        break_threshold: float = DEFAULT_BREAK_THRESHOLD,
        request_sleep: float = 0.2,
    ) -> None:
        self.client = client or TiingoClient()
        self.universe = universe
        self.break_threshold = break_threshold
        self.request_sleep = request_sleep

    # -- seeding ------------------------------------------------------------

    def seed_universe(self, store: SQLiteEngineStore) -> int:
        """Idempotently upsert the macro ETF universe into market_instruments.

        Preserves any prior ``history_status`` (e.g. ``break_detected``,
        ``stitched``, ``manual_review``) so re-seeding never clears alerts.
        """
        for entry in self.universe:
            self._seed_single_entry(store, entry)
        return len(self.universe)

    @staticmethod
    def _seed_single_entry(store: SQLiteEngineStore, entry: TiingoUniverseEntry) -> None:
        existing = store.get_market_instrument(entry.instrument_id)
        history_status = (
            existing.history_status if existing is not None else "provider_continuous"
        )
        store.upsert_market_instrument(
            MarketInstrumentRecord(
                instrument_id=entry.instrument_id,
                primary_ticker=entry.ticker,
                name=entry.name,
                asset_class=entry.asset_class,
                market=entry.market,
                exchange_code=entry.exchange_code,
                currency="USD",
                isin=entry.isin,
                composite_figi=entry.composite_figi,
                share_class_figi=entry.share_class_figi,
                primary_provider="tiingo",
                provider_symbols_json={"tiingo": entry.ticker},
                history_status=history_status,
                description_for_agent=entry.description_for_agent,
            )
        )
        store.upsert_market_symbol_segment(
            MarketSymbolHistoryRecord(
                segment_id=f"{entry.instrument_id}:tiingo:{entry.ticker}",
                instrument_id=entry.instrument_id,
                ticker=entry.ticker,
                provider_name="tiingo",
                exchange_code=entry.exchange_code,
                isin=entry.isin,
                figi=entry.composite_figi,
                valid_from="1900-01-01",
                valid_to="",
                event_type="listing_start",
                mapping_confidence="provider_native",
                source_name="tiingo.daily.meta",
                raw_json={"seed": True},
            )
        )

    # -- provider interface required by the issue --------------------------

    def get_daily_bars(
        self,
        symbol: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[TiingoDailyBar]:
        return self.client.get_daily_bars(symbol, start_date=start_date, end_date=end_date)

    def refresh_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        """Fetch Tiingo bars for ``symbol`` and persist into ``market_price_bars``.

        The symbol must be in the seeded universe (or already in
        ``market_instruments``). When the symbol is in the macro universe but
        the instrument row hasn't been seeded yet (e.g. when the caller skipped
        ``seed_universe``) we seed just that row before writing bars.
        """
        entry = TIINGO_UNIVERSE_BY_TICKER.get(symbol.upper())
        if entry is None:
            existing = store.find_market_instrument_by_ticker(symbol)
            if existing is None:
                logger.warning("refresh_market_history: %s not in seeded universe", symbol)
                return RefreshStats(source="tiingo", count=0)
            entry = TiingoUniverseEntry(
                instrument_id=existing.instrument_id,
                ticker=existing.primary_ticker,
                name=existing.name,
                asset_class=existing.asset_class,
                market=existing.market,
                exchange_code=existing.exchange_code,
                isin=existing.isin,
                composite_figi=existing.composite_figi,
                share_class_figi=existing.share_class_figi,
                description_for_agent=existing.description_for_agent,
            )
        elif store.get_market_instrument(entry.instrument_id) is None:
            self._seed_single_entry(store, entry)

        try:
            bars, raw_payload, request_params = self.client.get_daily_bars_with_raw(
                entry.ticker, start_date=start, end_date=end,
            )
        except TiingoAPIError:
            logger.warning("Tiingo fetch failed for %s", entry.ticker, exc_info=True)
            return RefreshStats(source="tiingo", count=0)

        if not bars:
            return RefreshStats(source="tiingo", count=0)

        # Issue #69 slice 2: capture raw payload before projecting bars.
        # Same idempotent INSERT OR IGNORE contract as the EODHD lane —
        # unchanged daily refresh dedupes on content_hash, a new bar
        # flips the hash and lands as a fresh row.
        if raw_payload:
            self._capture_bars_raw(
                store, entry.ticker, raw_payload, request_params,
            )

        adjustment_applied = check_adjustment_applied(bars)
        break_dates = detect_history_breaks(bars, threshold=self.break_threshold) if adjustment_applied else []
        break_set = set(break_dates)

        segment_id = f"{entry.instrument_id}:tiingo:{entry.ticker}"
        count = 0
        for bar in bars:
            flags_json: dict[str, Any] = {}
            ohlc_ok = check_ohlc_sanity(bar)
            if not ohlc_ok:
                flags_json["ohlc_sanity"] = "failed"
            if not adjustment_applied:
                flags_json["adjustment_check"] = "raw_only"
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
                    source_name="Tiingo",
                    source_symbol=entry.ticker,
                    has_break_detected=bar.date in break_set,
                    has_pre2018_delisted=bar.date < PRE2018_CUTOFF and not adjustment_applied,
                    has_missing_corp_acts=not adjustment_applied,
                    has_mapping_review_needed=False,
                    quality_flags_json=flags_json,
                )
            )
            count += 1

        if break_dates:
            # Only ever upgrade to break_detected. A partial-window refresh
            # cannot prove the full history is clean, so never downgrade an
            # existing alert back to provider_continuous here.
            store.update_instrument_history_status(entry.instrument_id, "break_detected")
        return RefreshStats(source="tiingo", count=count)

    @staticmethod
    def _capture_bars_raw(
        store: SQLiteEngineStore,
        ticker: str,
        payload: list[dict[str, Any]],
        request_params: dict[str, str],
    ) -> int:
        """Land one ``market_price_bars_raw`` row for the fetched payload.

        Idempotent — same canonicalized hash dedupes via INSERT OR
        IGNORE. Issue #69 slice 2.
        """
        snapshot_epoch_ms = int(datetime.now(UTC).timestamp() * 1000)
        record = MarketPriceBarsRawRecord(
            provider="tiingo",
            ticker=ticker,
            snapshot_epoch_ms=snapshot_epoch_ms,
            content_hash=bars_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True, ensure_ascii=False),
            fetched_at=datetime.now(UTC).isoformat(),
            request_params_json=json.dumps(request_params, sort_keys=True),
        )
        try:
            return store.insert_market_price_bars_raw([record])
        except Exception:
            logger.warning(
                "market_price_bars_raw write failed for %s", ticker, exc_info=True,
            )
            return 0

    def refresh_universe(
        self,
        store: SQLiteEngineStore,
        *,
        lookback_days: int = 365,
    ) -> RefreshStats:
        """Seed the universe (idempotent) and refresh every symbol in it."""
        self.seed_universe(store)
        start = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        total = 0
        for entry in self.universe:
            stats = self.refresh_market_history(store, entry.ticker, start=start)
            total += stats.count
            if self.request_sleep:
                time.sleep(self.request_sleep)
        return RefreshStats(source="tiingo", count=total)

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
        """Return agent-native rows for ``symbol`` between ``start`` and ``end``.

        ``adjusted=True`` returns rows keyed off adjusted_* fields with a
        fallback to raw values where the provider did not supply adjustments.
        """
        instrument = self._resolve_instrument(store, symbol)
        if instrument is None:
            return []
        bars = store.list_market_price_bars(
            instrument.instrument_id,
            start=start,
            end=end,
        )
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
                price_open = bar.open
                price_high = bar.high
                price_low = bar.low
                price_close = bar.close
                price_volume = bar.volume

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

    @staticmethod
    def _resolve_instrument(store: SQLiteEngineStore, symbol: str) -> MarketInstrumentRecord | None:
        entry = TIINGO_UNIVERSE_BY_INSTRUMENT_ID.get(symbol)
        if entry is not None:
            return store.get_market_instrument(entry.instrument_id)
        entry = TIINGO_UNIVERSE_BY_TICKER.get(symbol.upper())
        if entry is not None:
            return store.get_market_instrument(entry.instrument_id)
        return store.find_market_instrument_by_ticker(symbol)


def _agent_summary(
    instrument: MarketInstrumentRecord,
    bar: MarketPriceBarRecord,
    close: float,
) -> str:
    status_phrase = {
        "provider_continuous": "its history is provider-continuous through Tiingo",
        "break_detected": "its history has an unresolved break (pending lazy repair)",
        "stitched": "its history was stitched from multiple ticker segments",
        "manual_review": "its history is flagged for manual review",
    }.get(instrument.history_status, f"history_status={instrument.history_status}")
    return (
        f"{instrument.primary_ticker} closed at {close:.2f} {instrument.currency} "
        f"on {bar.date}. {status_phrase[0].upper()}{status_phrase[1:]}."
    )
