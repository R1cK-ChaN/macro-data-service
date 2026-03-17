"""World Bank fetcher adapter — WorldBankClient → list[RawSeries]."""

from __future__ import annotations

import time
from datetime import UTC, datetime

from ingestion.scrapers.worldbank import WorldBankClient
from ingestion.series_config import WORLDBANK_SERIES, WorldBankSeriesConfig
from ingestion.types import RawObservation, RawSeries


class WorldBankFetcher:
    source_name = "worldbank"

    def __init__(
        self,
        client: WorldBankClient | None = None,
        series_config: dict[str, WorldBankSeriesConfig] | None = None,
    ) -> None:
        self.client = client or WorldBankClient()
        self.series_config = series_config or WORLDBANK_SERIES

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
            (c for c in self.series_config.values() if c.series_id == series_id),
            None,
        )
        if cfg is None:
            return None
        return self._fetch_one(cfg)

    def _fetch_one(self, cfg: WorldBankSeriesConfig) -> RawSeries | None:
        try:
            obs_list = self.client.get_indicator(
                cfg.indicator,
                cfg.country,
                series_id=cfg.series_id,
                start_year=cfg.start_year,
                limit=cfg.limit,
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
                        "indicator": obs.indicator,
                        "country_code": obs.country_code,
                        "country_name": obs.country_name,
                    }.items()
                    if v
                },
            )
            for obs in obs_list
        )
        return RawSeries(
            source="worldbank",
            series_id=cfg.series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.category, "indicator": cfg.indicator},
        )
