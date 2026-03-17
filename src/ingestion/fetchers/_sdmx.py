"""Generic SDMX fetcher adapter — any SDMXClient → list[RawSeries].

Works for IMF, BIS, ECB, Eurostat, ILO, and UNSD providers that share
the ``SDMXObservation`` output type.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ingestion.sdmx._base_client import SDMXClient
from ingestion.types import RawObservation, RawSeries


class SDMXFetcher:
    """Wraps any ``SDMXClient`` subclass and converts to ``RawSeries``."""

    def __init__(
        self,
        client: SDMXClient,
        source_name: str,
        series_config: dict[str, dict[str, Any]],
    ) -> None:
        self.client = client
        self.source_name = source_name
        self.series_config = series_config

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
        dataflow = cfg.get("dataflow", cfg.get("dataset", ""))
        key = cfg.get("key", ".")
        version = cfg.get("version", "")
        series_id = cfg.get("series_id", "")
        try:
            obs_list = self.client.get_data(
                dataflow, version, key, series_id=series_id, limit=30,
            )
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={
                    k: v
                    for k, v in {"dataflow": obs.dataflow, "dataset": obs.dataset}.items()
                    if v
                },
            )
            for obs in obs_list
        )
        return RawSeries(
            source=self.source_name,
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.get("category", "")},
        )
