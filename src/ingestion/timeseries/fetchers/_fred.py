"""FRED fetcher adapter — FredClient → list[RawSeries]."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.scrapers.fred import FredClient
from ingestion.series_config import MACRO_SERIES
from ingestion.types import RawObservation, RawSeries


class FredFetcher:
    source_name = "fred"

    def __init__(
        self,
        client: FredClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or FredClient()
        self.series_config = series_config or MACRO_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        start = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        results: list[RawSeries] = []
        for series_id, meta in self.series_config.items():
            rs = self._fetch_one(series_id, meta, start)
            if rs is not None:
                results.append(rs)
            time.sleep(0.2)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        meta = self.series_config.get(series_id)
        if meta is None:
            return None
        start = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        return self._fetch_one(series_id, meta, start)

    def _fetch_one(
        self, series_id: str, meta: dict[str, Any], start_date: str
    ) -> RawSeries | None:
        try:
            obs_list = self.client.get_series(series_id, start_date=start_date, limit=100)
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(date=obs.date, value=obs.value)
            for obs in obs_list
        )
        return RawSeries(
            source="fred",
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata=meta,
        )
