"""World Bank fetcher adapter — WorldBankClient → list[RawSeries]."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime

from ingestion.scrapers.worldbank import WorldBankClient
from ingestion.series_config import WORLDBANK_SERIES, WorldBankSeriesConfig
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


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
            obs_list, payload, params = self.client.get_indicator_with_raw(
                cfg.indicator,
                cfg.country,
                series_id=cfg.series_id,
                start_year=cfg.start_year,
                limit=cfg.limit,
            )
        except Exception as exc:
            logger.error("WorldBank fetch failed [%s country=%s]: %s", cfg.indicator, cfg.country, exc)
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
        content_hash = content_hash_for_source("worldbank", payload) if payload else None
        return RawSeries(
            source="worldbank",
            series_id=cfg.series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.category, "indicator": cfg.indicator},
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
