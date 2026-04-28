"""Generic SDMX fetcher adapter — any SDMXClient → list[RawSeries].

Works for IMF, BIS, ECB, Eurostat, ILO, and UNSD providers that share
the ``SDMXObservation`` output type.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from ingestion.sdmx._base_client import SDMXClient
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


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
            # OECD uses (dataflow, version, key) positional signature;
            # BIS/IMF/base use (dataflow, key) with version as kwarg.
            if version and self.source_name == "oecd":
                obs_list, payload, params = self.client.get_data_with_raw(
                    dataflow, version, key, series_id=series_id, limit=30,
                )
            elif version:
                obs_list, payload, params = self.client.get_data_with_raw(
                    dataflow, key, series_id=series_id, version=version, limit=30,
                )
            else:
                obs_list, payload, params = self.client.get_data_with_raw(
                    dataflow, key, series_id=series_id, limit=30,
                )
        except Exception as exc:
            logger.error("%s fetch failed [%s key=%s]: %s", self.source_name, dataflow, key, exc)
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
        content_hash = content_hash_for_source(self.source_name, payload) if payload else None
        return RawSeries(
            source=self.source_name,
            series_id=series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.get("category", "")},
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
