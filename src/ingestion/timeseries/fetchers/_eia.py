"""EIA fetcher adapter — EIAClient → list[RawSeries]."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from ingestion.scrapers.eia import EIAClient
from ingestion.series_config import EIA_SERIES
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


class EIAFetcher:
    source_name = "eia"

    def __init__(
        self,
        client: EIAClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or EIAClient()
        self.series_config = series_config or EIA_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for _key, cfg in self.series_config.items():
            rs = self._fetch_one(cfg)
            if rs is not None:
                results.append(rs)
            time.sleep(0.3)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        cfg = next(
            (c for c in self.series_config.values() if c["series_id"] == series_id),
            None,
        )
        if cfg is None:
            return None
        return self._fetch_one(cfg)

    def _fetch_one(self, cfg: dict[str, Any]) -> RawSeries | None:
        try:
            obs_list = self.client.get_series(
                cfg["route"],
                params=cfg["params"],
                series_id=cfg["series_id"],
                limit=100,
            )
        except Exception as exc:
            logger.error("EIA fetch failed [%s route=%s]: %s", cfg.get("series_id", "?"), cfg.get("route", "?"), exc)
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={"unit": obs.unit} if obs.unit else {},
            )
            for obs in obs_list
        )
        return RawSeries(
            source="eia",
            series_id=cfg["series_id"],
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.get("category", "energy")},
        )
