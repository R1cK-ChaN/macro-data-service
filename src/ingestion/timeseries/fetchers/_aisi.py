"""AISI fetcher adapter — AISIClient -> list[RawSeries]."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ingestion.series_config import AISI_WEEKLY_STEEL_SERIES
from ingestion.scrapers.aisi import AISIClient
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries


class AISIFetcher:
    source_name = "aisi"

    def __init__(
        self,
        client: AISIClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or AISIClient()
        self.series_config = series_config or AISI_WEEKLY_STEEL_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        metrics = tuple(cfg["metric"] for cfg in self.series_config.values())
        raw_by_metric = self.client.get_all_series_with_raw(metrics)
        rows: list[RawSeries] = []
        for cfg in self.series_config.values():
            metric = cfg["metric"]
            observations, payload, params = raw_by_metric.get(metric, ([], {}, {}))
            if observations:
                rows.append(self._raw_series(cfg, observations, payload, params))
        return rows

    def fetch_series(
        self,
        series_id: str,
        *,
        lookback_days: int = 365,
    ) -> RawSeries | None:
        cfg = next(
            (item for item in self.series_config.values() if item["series_id"] == series_id),
            None,
        )
        if cfg is None:
            return None
        observations, payload, params = self.client.get_series_with_raw(cfg["metric"])
        if not observations:
            return None
        return self._raw_series(cfg, observations, payload, params)

    def _raw_series(
        self,
        cfg: dict[str, Any],
        observations: list[Any],
        payload: dict,
        params: dict[str, str],
    ) -> RawSeries:
        metric = cfg["metric"]
        return RawSeries(
            source="aisi",
            series_id=cfg["series_id"],
            observations=tuple(
                RawObservation(
                    date=obs.date,
                    value=obs.value,
                    provider_metadata={"metric": metric},
                )
                for obs in observations
            ),
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": cfg.get("category", ""),
                "metric": metric,
                "name": cfg.get("name", ""),
                "unit": cfg.get("unit", ""),
            },
            raw_payload=payload,
            content_hash=content_hash_for_source("aisi", payload),
            request_params_json=json.dumps(params, sort_keys=True),
        )
