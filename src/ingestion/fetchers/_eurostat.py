"""Eurostat fetcher adapter — EurostatClient.get_dataset() → list[RawSeries].

Uses the JSON-stat endpoint (not SDMX) because EUROSTAT_SERIES configs
are keyed by ``dataset`` + ``params`` which map to ``get_dataset()``.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from ingestion.sdmx.providers.eurostat import EurostatClient
from ingestion.series_config import EUROSTAT_SERIES
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


class EurostatFetcher:
    source_name = "eurostat"

    def __init__(
        self,
        client: EurostatClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or EurostatClient()
        self.series_config = series_config or EUROSTAT_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for _key, cfg in self.series_config.items():
            rs = self._fetch_one(cfg)
            if rs is not None:
                results.append(rs)
            time.sleep(0.5)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        cfg = next(
            (c for c in self.series_config.values() if c.get("series_id") == series_id),
            None,
        )
        if cfg is None:
            return None
        return self._fetch_one(cfg)

    def _fetch_one(self, cfg: dict[str, Any]) -> RawSeries | None:
        dataset = cfg.get("dataset", "")
        series_id = cfg.get("series_id", "")
        try:
            obs_list = self.client.get_dataset(
                dataset,
                params=dict(cfg.get("params", {})),
                series_id=series_id,
                limit=30,
            )
        except Exception as exc:
            logger.error("Eurostat fetch failed [%s]: %s", dataset, exc)
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={"dataset": obs.dataset} if obs.dataset else {},
            )
            for obs in obs_list
        )
        return RawSeries(
            source="eurostat",
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.get("category", ""), "dataset": dataset},
        )
