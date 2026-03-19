"""Market price ingestion client."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

import yfinance as yf

from ingestion.series_config import MACRO_WATCHLIST
from storage import MarketPriceRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class MarketPriceClient:
    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        prices = self.fetch_prices()
        return RefreshStats(source="market", count=self.store_prices(store, prices))

    def fetch_prices(self) -> list[MarketPriceRecord]:
        prices: list[MarketPriceRecord] = []
        now_epoch = int(datetime.now(UTC).timestamp())
        for asset_class, symbols in MACRO_WATCHLIST.items():
            for symbol, name in symbols.items():
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.fast_info
                    price = info.get("lastPrice", info.get("previousClose"))
                    previous_close = info.get("previousClose")
                    if price is None:
                        history = ticker.history(period="2d")
                        if history.empty:
                            continue
                        price = float(history["Close"].iloc[-1])
                        previous_close = float(history["Close"].iloc[-2]) if len(history) > 1 else None
                    change_pct = None
                    if previous_close not in {None, 0}:
                        change_pct = round((float(price) - float(previous_close)) / float(previous_close) * 100, 2)
                    prices.append(
                        MarketPriceRecord(
                            symbol=symbol,
                            asset_class=asset_class,
                            name=name,
                            price=float(price),
                            change_pct=change_pct,
                            timestamp=now_epoch,
                        )
                    )
                except Exception:
                    continue
                time.sleep(0.1)
        return prices

    def store_prices(self, store: SQLiteEngineStore, prices: list[MarketPriceRecord]) -> int:
        for price in prices:
            store.insert_market_price(price)
        return len(prices)
