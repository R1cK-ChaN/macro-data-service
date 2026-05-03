from __future__ import annotations

from typing import Any

from .base import LocalMacroDataServiceBase


class MarketOpsMixin(LocalMacroDataServiceBase):
    """Market-lane service ops.

    Reads route through ``self._market_store`` (the ClickHouse adapter
    landed in issue #118 P2). The cross-domain SQLite store is still
    available as ``self._store`` for ops that mix domains, but the
    market lane never reaches into ``engine.db`` after the bilingual
    split landed in #118.
    """

    def _op_get_market_history(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Daily-bar window for an instrument.

        ``arguments``:
        * ``instrument_id`` (required) — canonical id (e.g. ``US_AAPL``).
        * ``start`` / ``end`` — inclusive ``YYYY-MM-DD`` window bounds.
        * ``adjusted`` (default ``True``) — return adjusted OHLCV via
          column aliasing rather than raw.
        """
        if self._market_store is None:
            return {"error": "market store not configured", "rows": []}
        instrument_id = str(arguments.get("instrument_id") or "").strip()
        if not instrument_id:
            return {"error": "instrument_id is required", "rows": []}
        rows = self._market_store.get_market_history(
            instrument_id,
            start=arguments.get("start") or None,
            end=arguments.get("end") or None,
            adjusted=bool(arguments.get("adjusted", True)),
        )
        return {
            "instrument_id": instrument_id,
            "total": len(rows),
            "rows": rows,
        }

    def _op_get_market_snapshot(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        if self._market_store is None:
            return {"error": "market store not configured", "prices": []}
        rows = self._market_store.latest_market_snapshot()
        return {"total": len(rows), "prices": rows}

    def _op_fetch_live_markets(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        return {
            "error": "live TradingEconomics market scraper retired in issue #123",
            "quotes": [],
        }
