"""Macro → market layer provider.

Projects existing macro time series (FRED rates, FRED FX, EIA commodities)
stored in the ``indicators`` table into the ``market_*`` schema so the same
``get_market_history`` interface covers rates/FX/commodities alongside
equities and ETFs.

Each macro observation becomes a synthetic daily bar:
``open = high = low = close = value`` with ``split_factor = 1`` and
``dividend_cash = 0``. No corporate actions apply, so every bar carries
``has_missing_corp_acts = False``.
"""

from __future__ import annotations

import logging
from typing import Any

from ingestion.market._macro_map import (
    MACRO_MARKET_UNIVERSE,
    MacroMarketEntry,
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


class MacroMarketProvider:
    """Projects macro observations into ``market_price_bars``.

    Does not fetch anything from the network — the upstream FRED / EIA
    ingestion pipelines already refresh ``indicators``. This provider
    re-reads those rows and syncs them into the market layer.
    """

    source_name = "macro_market"

    def __init__(
        self,
        *,
        universe: tuple[MacroMarketEntry, ...] = MACRO_MARKET_UNIVERSE,
    ) -> None:
        self.universe = universe

    # -- seeding -----------------------------------------------------------

    def seed_universe(self, store: SQLiteEngineStore) -> int:
        for entry in self.universe:
            self._seed_single_entry(store, entry)
        return len(self.universe)

    @staticmethod
    def _seed_single_entry(
        store: SQLiteEngineStore, entry: MacroMarketEntry
    ) -> None:
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
                exchange_code="",
                currency=entry.currency,
                isin="",
                openfigi="",
                composite_figi="",
                share_class_figi="",
                cusip="",
                lei="",
                primary_provider=entry.source,
                provider_symbols_json={entry.source: entry.series_id},
                history_status=history_status,
                description_for_agent=entry.description_for_agent,
            )
        )
        store.upsert_market_symbol_segment(
            MarketSymbolHistoryRecord(
                segment_id=f"{entry.instrument_id}:{entry.source}:{entry.series_id}",
                instrument_id=entry.instrument_id,
                ticker=entry.ticker,
                provider_name=entry.source,
                exchange_code="",
                isin="",
                figi="",
                valid_from="1900-01-01",
                valid_to="",
                event_type="listing_start",
                mapping_confidence="provider_native",
                source_name=f"{entry.source}.indicators.meta",
                raw_json={"seed": True, "unit": entry.unit},
            )
        )

    # -- projection --------------------------------------------------------

    def refresh_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        """Project all ``indicators`` rows for ``symbol`` into market_price_bars.

        ``symbol`` accepts the ``instrument_id``, the primary ticker, or the
        underlying provider ``series_id``. Returns the number of projected
        bars.
        """
        entry = self._resolve_entry(symbol)
        if entry is None:
            logger.warning("MacroMarketProvider: %s not in macro universe", symbol)
            return RefreshStats(source="macro_market", count=0)
        if store.get_market_instrument(entry.instrument_id) is None:
            self._seed_single_entry(store, entry)

        observations = _read_indicator_rows(
            store,
            source=entry.source,
            series_id=entry.series_id,
            start=start,
            end=end,
        )
        segment_id = f"{entry.instrument_id}:{entry.source}:{entry.series_id}"
        count = 0
        for date, value in observations:
            store.upsert_market_price_bar(
                MarketPriceBarRecord(
                    instrument_id=entry.instrument_id,
                    source_segment_id=segment_id,
                    date=date,
                    bar_interval="1d",
                    open=value,
                    high=value,
                    low=value,
                    close=value,
                    volume=0.0,
                    adjusted_open=value,
                    adjusted_high=value,
                    adjusted_low=value,
                    adjusted_close=value,
                    adjusted_volume=0.0,
                    dividend_cash=0.0,
                    split_factor=1.0,
                    source_name=_source_display(entry.source),
                    source_symbol=entry.series_id,
                    has_break_detected=False,
                    has_pre2018_delisted=False,
                    has_missing_corp_acts=False,
                    has_mapping_review_needed=False,
                    quality_flags_json={
                        "asset_class": entry.asset_class,
                        "unit": entry.unit,
                        "projected_from": f"{entry.source}:{entry.series_id}",
                    },
                )
            )
            count += 1
        return RefreshStats(source="macro_market", count=count)

    def refresh_universe(self, store: SQLiteEngineStore) -> RefreshStats:
        """Seed (idempotent) then re-project every macro-market entry."""
        self.seed_universe(store)
        total = 0
        for entry in self.universe:
            stats = self.refresh_market_history(store, entry.instrument_id)
            total += stats.count
        return RefreshStats(source="macro_market", count=total)

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
            asset_class = bar.quality_flags_json.get("asset_class") or instrument.asset_class
            unit = bar.quality_flags_json.get("unit", "")
            close = bar.close
            adjusted_close = bar.adjusted_close if bar.adjusted_close is not None else close
            rows.append(
                {
                    "instrument_id": instrument.instrument_id,
                    "ticker": instrument.primary_ticker,
                    "name": instrument.name,
                    "market": instrument.market,
                    "asset_class": asset_class,
                    "unit": unit,
                    "history_status": instrument.history_status,
                    "date": bar.date,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": close,
                    "adjusted_close": adjusted_close,
                    "volume": bar.volume,
                    "quality_flags": [],
                    "source": bar.source_name,
                    "agent_summary": _agent_summary(instrument, asset_class, unit, bar.date, close),
                }
            )
        return rows

    # -- helpers -----------------------------------------------------------

    def _resolve_entry(self, symbol: str) -> MacroMarketEntry | None:
        # Resolve against the instance's own universe so callers can pass a
        # custom tuple without falling through to the module-level default.
        upper = symbol.upper()
        for candidate in self.universe:
            if symbol == candidate.instrument_id:
                return candidate
            if upper == candidate.ticker:
                return candidate
            if symbol == candidate.series_id:
                return candidate
        return None

    def _resolve_instrument(
        self, store: SQLiteEngineStore, symbol: str
    ) -> MarketInstrumentRecord | None:
        entry = self._resolve_entry(symbol)
        if entry is not None:
            return store.get_market_instrument(entry.instrument_id)
        return store.find_market_instrument_by_ticker(symbol)


def _read_indicator_rows(
    store: SQLiteEngineStore,
    *,
    source: str,
    series_id: str,
    start: str | None,
    end: str | None,
) -> list[tuple[str, float]]:
    sql = ["SELECT date, value FROM indicators WHERE source = ? AND series_id = ?"]
    params: list[Any] = [source, series_id]
    if start:
        sql.append("AND date >= ?")
        params.append(start)
    if end:
        sql.append("AND date <= ?")
        params.append(end)
    sql.append("ORDER BY date")
    with store._connection(commit=False) as connection:
        rows = connection.execute(" ".join(sql), params).fetchall()
    return [(row["date"], float(row["value"])) for row in rows]


def _source_display(source: str) -> str:
    return {"fred": "FRED", "eia": "EIA", "ecb": "ECB"}.get(source, source.upper())


def _agent_summary(
    instrument: MarketInstrumentRecord,
    asset_class: str,
    unit: str,
    date: str,
    close: float,
) -> str:
    if asset_class == "rate":
        return (
            f"{instrument.primary_ticker} yield closed at {close:.3f}% on {date} "
            f"(source {instrument.primary_provider.upper()} series "
            f"{instrument.provider_symbols_json.get(instrument.primary_provider, '')})."
        )
    if asset_class == "fx":
        return (
            f"{instrument.primary_ticker} closed at {close:.4f} ({unit}) on {date}."
        )
    if asset_class == "commodity":
        return (
            f"{instrument.primary_ticker} closed at {close:.2f} {instrument.currency} on "
            f"{date} ({unit})."
        )
    return f"{instrument.primary_ticker} closed at {close} on {date}."
