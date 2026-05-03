from __future__ import annotations

from typing import Any

from .base import LocalMacroDataServiceBase


class ManifestOpsMixin(LocalMacroDataServiceBase):
    def _op_get_data_manifest(self, arguments: dict[str, Any]) -> dict[str, Any]:
        del arguments
        market_stats = self._market_manifest_stats()
        return self._store.get_data_manifest(market_bars=market_stats)

    def _market_manifest_stats(self) -> dict[str, Any]:
        if self._market_store is None:
            return {
                "row_count": 0,
                "latest_timestamp": None,
                "latest_ingested_at": None,
                "notes": ["market store unavailable"],
            }
        get_stats = getattr(self._market_store, "get_manifest_stats", None)
        if callable(get_stats):
            try:
                stats = get_stats()
            except Exception as exc:
                return {
                    "row_count": 0,
                    "latest_timestamp": None,
                    "latest_ingested_at": None,
                    "error": str(exc),
                    "notes": ["market store unavailable"],
                }
            if isinstance(stats, dict):
                return stats
        snapshot = self._market_store.latest_market_snapshot()
        latest = max((str(row.get("time") or "") for row in snapshot), default="")
        return {
            "row_count": len(snapshot),
            "latest_timestamp": latest or None,
            "latest_ingested_at": None,
        }
