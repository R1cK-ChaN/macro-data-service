from __future__ import annotations

from typing import Any

from contracts import format_epoch_iso

from .base import (
    LocalMacroDataServiceBase,
    _VALID_MARKET_ASSET_CLASSES,
    logger,
)


class MarketOpsMixin(LocalMacroDataServiceBase):
    def _op_get_market_snapshot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        prices = self._store.latest_market_prices()
        return {"prices": [self._price_to_dict(price) for price in prices]}

    def _op_fetch_live_markets(self, arguments: dict[str, Any]) -> dict[str, Any]:
        from ingestion.scrapers import TradingEconomicsMarketsClient

        asset_class = (arguments.get("asset_class") or "all").lower().strip()
        try:
            quotes = TradingEconomicsMarketsClient().fetch_markets()
        except Exception as exc:
            logger.warning("Live markets fetch failed: %s", exc)
            return {"error": str(exc), "quotes": []}
        items = [
            {
                "name": quote.name,
                "asset_class": quote.asset_class,
                "price": quote.price,
                "change": quote.change,
                "change_pct": quote.change_pct,
                "symbol": quote.symbol,
            }
            for quote in quotes
        ]
        if asset_class != "all" and asset_class in _VALID_MARKET_ASSET_CLASSES:
            items = [quote for quote in items if str(quote["asset_class"]).lower() == asset_class]
        return {"total": len(items), "asset_class_filter": asset_class, "quotes": items}

    def _price_to_dict(self, price: Any) -> dict[str, Any]:
        return {
            "symbol": price.symbol,
            "name": price.name,
            "asset_class": price.asset_class,
            "price": price.price,
            "change_pct": price.change_pct,
            "timestamp": price.timestamp,
            "datetime_utc": format_epoch_iso(price.timestamp),
        }
