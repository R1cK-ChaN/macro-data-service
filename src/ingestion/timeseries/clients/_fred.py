"""FRED ingestion client — refresh daily, full, and vintage series."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.scrapers.fred import FredClient
from ingestion.series_config import MACRO_SERIES, VINTAGE_SERIES
from storage import IndicatorObservationRecord, IndicatorVintageRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    """Minimal refresh stats (re-imported from sources at runtime)."""
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class FREDIngestionClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.client = FredClient(api_key=api_key)

    @property
    def api_key(self) -> str:
        return self.client.api_key

    def refresh_daily_series(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        daily_series = {sid: meta for sid, meta in MACRO_SERIES.items() if meta["freq"] == "daily"}
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d")
        for series_id, meta in daily_series.items():
            count += self._store_series(store, series_id, meta, start_date=start_date, limit=5, family_lookup=family_lookup)
            time.sleep(0.2)
        return RefreshStats(source="fred_daily", count=count)

    def refresh_nondaily_series(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        nondaily = {sid: meta for sid, meta in MACRO_SERIES.items() if meta["freq"] != "daily"}
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=120)).strftime("%Y-%m-%d")
        for series_id, meta in nondaily.items():
            count += self._store_series(store, series_id, meta, start_date=start_date, limit=10, family_lookup=family_lookup)
            time.sleep(1.0)
        return RefreshStats(source="fred_nondaily", count=count)

    def refresh_all_series(
        self,
        store: SQLiteEngineStore,
        *,
        lookback_days: int = 365,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        for series_id, meta in MACRO_SERIES.items():
            count += self._store_series(store, series_id, meta, start_date=start_date, limit=100, family_lookup=family_lookup)
            time.sleep(0.2)
        return RefreshStats(source="fred_all", count=count)

    def refresh_vintages(
        self,
        store: SQLiteEngineStore,
        vintage_series: list[str] | None = None,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        series_list = vintage_series or VINTAGE_SERIES
        count = 0
        start_date = (datetime.now(UTC) - timedelta(days=365)).strftime("%Y-%m-%d")
        for series_id in series_list:
            try:
                vintages = self.client.get_vintages(series_id, start_date=start_date)
                fam_id = family_lookup.get(("fred", series_id)) if family_lookup else None
                for v in vintages:
                    store.upsert_indicator_vintage(
                        IndicatorVintageRecord(
                            series_id=v.series_id,
                            source="fred",
                            observation_date=v.date,
                            vintage_date=v.vintage_date,
                            value=v.value,
                            metadata={"name": MACRO_SERIES.get(series_id, {}).get("name", series_id)},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("FRED vintage refresh failed for %s", series_id, exc_info=True)
            time.sleep(0.3)
        return RefreshStats(source="fred_vintages", count=count)

    def _store_series(
        self,
        store: SQLiteEngineStore,
        series_id: str,
        meta: dict[str, str],
        *,
        start_date: str,
        limit: int,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> int:
        stored = 0
        fam_id = family_lookup.get(("fred", series_id)) if family_lookup else None
        for obs in self.client.get_series(series_id, start_date=start_date, limit=limit):
            store.upsert_indicator_observation(
                IndicatorObservationRecord(
                    series_id=series_id,
                    source="fred",
                    date=obs.date,
                    value=obs.value,
                    metadata={"name": meta["name"], "category": meta["category"]},
                    obs_family_id=fam_id,
                )
            )
            stored += 1
        return stored


