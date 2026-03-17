"""OECD fetcher adapter — OECDClient → list[RawSeries].

Handles OECD-specific observation fields (dimensions, series_key)
and the ``OECDSeriesConfig`` dataclass.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from ingestion.sdmx.providers.oecd import OECDClient
from ingestion.series_config import OECD_SERIES, OECDSeriesConfig
from ingestion.types import RawObservation, RawSeries


class OECDFetcher:
    source_name = "oecd"

    def __init__(
        self,
        client: OECDClient | None = None,
        series_config: dict[str, OECDSeriesConfig] | None = None,
    ) -> None:
        self.client = client or OECDClient()
        self.series_config = series_config or OECD_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for _key, cfg in self.series_config.items():
            rs = self._fetch_one(cfg)
            if rs is not None:
                results.append(rs)
            time.sleep(1.0)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        cfg = next(
            (c for c in self.series_config.values() if c.series_id == series_id),
            None,
        )
        if cfg is None:
            return None
        return self._fetch_one(cfg)

    def _fetch_one(self, cfg: OECDSeriesConfig) -> RawSeries | None:
        try:
            if cfg.key is not None:
                obs_list = self.client.fetch_series(
                    cfg.dataflow, cfg.key,
                    agency_id=cfg.agency_id, version=cfg.version,
                    series_id=cfg.series_id, limit=100,
                )
            else:
                obs_list = self.client.fetch_data(
                    cfg.dataflow,
                    agency_id=cfg.agency_id, version=cfg.version,
                    filters=cfg.filters, series_id=cfg.series_id, limit=100,
                )
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={
                    k: v
                    for k, v in {
                        "dataflow": obs.dataflow,
                        "agency_id": obs.agency_id,
                        "series_key": obs.series_key,
                        "dimensions": obs.dimensions or None,
                    }.items()
                    if v
                },
            )
            for obs in obs_list
        )
        return RawSeries(
            source="oecd",
            series_id=cfg.series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.category, "dataflow": cfg.dataflow},
        )
