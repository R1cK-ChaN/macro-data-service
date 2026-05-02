"""ISM fetcher adapter — ISMClient -> list[RawSeries]."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ingestion.scrapers.ism import ISMClient
from ingestion.series_config import ISM_REPORT_SERIES
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries


class ISMFetcher:
    source_name = "ism"

    def __init__(
        self,
        client: ISMClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or ISMClient()
        self.series_config = series_config or ISM_REPORT_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        raw_by_series = self.client.get_all_series_with_raw(self.series_config)
        rows: list[RawSeries] = []
        for cfg in self.series_config.values():
            series_id = str(cfg["series_id"])
            observations, payload, params = raw_by_series.get(series_id, ([], {}, {}))
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
            (
                item
                for item in self.series_config.values()
                if item["series_id"] == series_id
            ),
            None,
        )
        if cfg is None:
            return None
        observations, payload, params = self.client.get_series_with_raw(cfg)
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
        survey = str(cfg["survey"])
        metric = str(cfg["metric"])
        measure = str(cfg["measure"])
        return RawSeries(
            source="ism",
            series_id=str(cfg["series_id"]),
            observations=tuple(
                RawObservation(
                    date=obs.date,
                    value=obs.value,
                    provider_metadata={
                        "survey": survey,
                        "metric": metric,
                        "measure": measure,
                    },
                )
                for obs in observations
            ),
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": cfg.get("category", ""),
                "survey": survey,
                "metric": metric,
                "measure": measure,
                "name": cfg.get("name", ""),
                "unit": cfg.get("unit", ""),
            },
            raw_payload=payload,
            content_hash=content_hash_for_source("ism", payload),
            request_params_json=json.dumps(params, sort_keys=True),
        )
