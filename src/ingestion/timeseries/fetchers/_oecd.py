"""OECD fetcher adapter — OECDClient → list[RawSeries].

Handles OECD-specific observation fields (dimensions, series_key)
and the ``OECDSeriesConfig`` dataclass.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime

from ingestion.sdmx.providers.oecd import OECDClient

logger = logging.getLogger(__name__)
from ingestion.series_config import OECD_SERIES, OECDSeriesConfig
from ingestion.timeseries.canonicalize import content_hash_for_source
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
        # OECD SDMX REST API rejects "latest" as a version in the URL path;
        # pass None to let the server use its default version.
        version = cfg.version if cfg.version != "latest" else None
        try:
            if cfg.key is not None:
                obs_list, payload, params = self.client.fetch_series_with_raw(
                    cfg.dataflow, cfg.key,
                    agency_id=cfg.agency_id, version=version,
                    series_id=cfg.series_id, limit=100,
                )
            else:
                obs_list, payload, params = self.client.fetch_data_with_raw(
                    cfg.dataflow,
                    agency_id=cfg.agency_id, version=version,
                    filters=cfg.filters, series_id=cfg.series_id, limit=100,
                )
        except Exception as exc:
            logger.error("OECD fetch failed [%s dataflow=%s]: %s", cfg.series_id, cfg.dataflow, exc)
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
        content_hash = content_hash_for_source("oecd", payload) if payload else None
        return RawSeries(
            source="oecd",
            series_id=cfg.series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.category, "dataflow": cfg.dataflow},
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
