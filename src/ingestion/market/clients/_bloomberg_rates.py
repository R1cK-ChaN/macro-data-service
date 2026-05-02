"""Licensed Bloomberg-compatible rates provider.

This provider reads local CSV exports from Bloomberg BGN or a compatible
licensed rates vendor and projects them into the market bar schema.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from env import get_env_value
from ingestion.market._bars_canonicalize import bars_content_hash
from ingestion.market._bloomberg_rates import (
    BLOOMBERG_RATE_UNIVERSE,
    BloombergRateEntry,
)
from ingestion.market.scrapers._bloomberg_rates import parse_bloomberg_rate_csv
from storage import (
    MarketInstrumentRecord,
    MarketPriceBarRecord,
    MarketPriceBarsRawRecord,
    MarketSymbolHistoryRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class BloombergRatesFileProvider:
    """Loads licensed Bloomberg-compatible rates CSV files into market bars."""

    source_name = "bloomberg_rates"

    def __init__(
        self,
        *,
        universe: tuple[BloombergRateEntry, ...] = BLOOMBERG_RATE_UNIVERSE,
        file_map: dict[str, str | Path] | None = None,
    ) -> None:
        self.universe = universe
        self.file_map = {str(k): Path(v) for k, v in (file_map or {}).items()}

    def seed_universe(self, store: SQLiteEngineStore) -> int:
        for entry in self.universe:
            self._seed_single_entry(store, entry)
        return len(self.universe)

    @staticmethod
    def _seed_single_entry(
        store: SQLiteEngineStore, entry: BloombergRateEntry
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
                exchange_code="BGN",
                currency=entry.currency,
                primary_provider=entry.provider,
                provider_symbols_json={entry.provider: entry.provider_symbol},
                history_status=history_status,
                description_for_agent=entry.description_for_agent,
            )
        )
        store.upsert_market_symbol_segment(
            MarketSymbolHistoryRecord(
                segment_id=_segment_id(entry),
                instrument_id=entry.instrument_id,
                ticker=entry.provider_symbol,
                provider_name=entry.provider,
                exchange_code="BGN",
                valid_from="1900-01-01",
                valid_to="",
                event_type="listing_start",
                mapping_confidence="provider_native",
                source_name=f"{entry.provider}.licensed_csv.meta",
                raw_json={
                    "seed": True,
                    "ticker": entry.ticker,
                    "provider_symbol": entry.provider_symbol,
                    "curve": entry.curve,
                    "tenor": entry.tenor,
                    "unit": entry.unit,
                },
            )
        )

    def refresh_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> RefreshStats:
        entry = self._resolve_entry(symbol)
        if entry is None:
            logger.warning("BloombergRatesFileProvider: %s not in rates universe", symbol)
            return RefreshStats(source=self.source_name, count=0)
        if store.get_market_instrument(entry.instrument_id) is None:
            self._seed_single_entry(store, entry)

        path = self._resolve_file_path(entry)
        if path is None:
            logger.info("No licensed rates CSV configured for %s", entry.provider_symbol)
            return RefreshStats(source=self.source_name, count=0)
        if not path.exists():
            logger.warning("Licensed rates CSV missing for %s: %s", entry.provider_symbol, path)
            return RefreshStats(source=self.source_name, count=0)

        payload = path.read_text(encoding="utf-8-sig")
        observations, raw_rows = parse_bloomberg_rate_csv(payload, entry=entry)
        self._capture_bars_raw(
            store,
            entry,
            raw_rows=raw_rows,
            path=path,
            start=start,
            end=end,
        )
        observations = [
            obs for obs in observations
            if (start is None or obs.date >= start) and (end is None or obs.date <= end)
        ]
        segment_id = _segment_id(entry)
        count = 0
        for obs in observations:
            store.upsert_market_price_bar(
                MarketPriceBarRecord(
                    instrument_id=entry.instrument_id,
                    source_segment_id=segment_id,
                    date=obs.date,
                    bar_interval="1d",
                    open=obs.value,
                    high=obs.value,
                    low=obs.value,
                    close=obs.value,
                    volume=0.0,
                    adjusted_open=obs.value,
                    adjusted_high=obs.value,
                    adjusted_low=obs.value,
                    adjusted_close=obs.value,
                    adjusted_volume=0.0,
                    dividend_cash=0.0,
                    split_factor=1.0,
                    source_name=entry.source_name,
                    source_symbol=entry.provider_symbol,
                    has_break_detected=False,
                    has_pre2018_delisted=False,
                    has_missing_corp_acts=False,
                    has_mapping_review_needed=False,
                    quality_flags_json={
                        "asset_class": entry.asset_class,
                        "unit": entry.unit,
                        "curve": entry.curve,
                        "tenor": entry.tenor,
                        "provider": entry.provider,
                        "provider_symbol": entry.provider_symbol,
                        "licensed_source": True,
                        **obs.metadata,
                    },
                )
            )
            count += 1
        return RefreshStats(source=self.source_name, count=count)

    def refresh_universe(self, store: SQLiteEngineStore) -> RefreshStats:
        self.seed_universe(store)
        total = 0
        for entry in self.universe:
            total += self.refresh_market_history(store, entry.instrument_id).count
        return RefreshStats(source=self.source_name, count=total)

    def get_market_history(
        self,
        store: SQLiteEngineStore,
        symbol: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        entry = self._resolve_entry(symbol)
        instrument = (
            store.get_market_instrument(entry.instrument_id)
            if entry is not None else store.find_market_instrument_by_ticker(symbol)
        )
        if instrument is None:
            return []
        bars = store.list_market_price_bars(instrument.instrument_id, start=start, end=end)
        rows: list[dict[str, Any]] = []
        for bar in bars:
            unit = bar.quality_flags_json.get("unit", "percent")
            curve = bar.quality_flags_json.get("curve", "")
            tenor = bar.quality_flags_json.get("tenor", "")
            rows.append({
                "instrument_id": instrument.instrument_id,
                "ticker": instrument.primary_ticker,
                "name": instrument.name,
                "market": instrument.market,
                "asset_class": instrument.asset_class,
                "unit": unit,
                "curve": curve,
                "tenor": tenor,
                "history_status": instrument.history_status,
                "date": bar.date,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "adjusted_close": bar.adjusted_close,
                "volume": bar.volume,
                "quality_flags": [],
                "source": bar.source_name,
                "agent_summary": (
                    f"{instrument.primary_ticker} {tenor} {curve} closed at "
                    f"{bar.close:.4f}% on {bar.date}."
                ),
            })
        return rows

    def _resolve_entry(self, symbol: str) -> BloombergRateEntry | None:
        upper = symbol.upper()
        for entry in self.universe:
            if symbol == entry.instrument_id:
                return entry
            if upper == entry.ticker:
                return entry
            if upper == entry.provider_symbol.upper():
                return entry
        return None

    def _resolve_file_path(self, entry: BloombergRateEntry) -> Path | None:
        for key in (entry.instrument_id, entry.ticker, entry.provider_symbol):
            path = self.file_map.get(key)
            if path is not None:
                return path
        for env_var in entry.env_vars:
            value = get_env_value(env_var)
            if value:
                return Path(value)
        return None

    def _capture_bars_raw(
        self,
        store: SQLiteEngineStore,
        entry: BloombergRateEntry,
        *,
        raw_rows: list[dict[str, str]],
        path: Path,
        start: str | None,
        end: str | None,
    ) -> int:
        if not raw_rows:
            return 0
        now = datetime.now(UTC)
        payload = [
            {
                "date": _raw_row_date(row),
                "provider_symbol": entry.provider_symbol,
                "raw": row,
            }
            for row in raw_rows
        ]
        record = MarketPriceBarsRawRecord(
            provider=entry.provider,
            ticker=entry.provider_symbol,
            snapshot_epoch_ms=int(now.timestamp() * 1000),
            content_hash=bars_content_hash(payload),
            payload_json=json.dumps(payload, sort_keys=True, ensure_ascii=True),
            fetched_at=now.isoformat(),
            request_params_json=json.dumps(
                {
                    "path": str(path),
                    "instrument_id": entry.instrument_id,
                    "provider_symbol": entry.provider_symbol,
                    "start": start or "",
                    "end": end or "",
                },
                sort_keys=True,
                ensure_ascii=True,
            ),
        )
        return store.insert_market_price_bars_raw([record])


def _segment_id(entry: BloombergRateEntry) -> str:
    return f"{entry.instrument_id}:{entry.provider}:{entry.provider_symbol}"


def _raw_row_date(row: dict[str, str]) -> str:
    for key, value in row.items():
        normalized = "".join(ch for ch in key.lower() if ch.isalnum())
        if normalized in {
            "date",
            "dates",
            "pxdate",
            "pricedate",
            "asofdate",
            "datetime",
            "timestamp",
        }:
            return value
    return ""
