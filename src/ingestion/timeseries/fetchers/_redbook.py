"""Redbook fetcher adapter — RedbookClient -> list[RawSeries]."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ingestion.scrapers.redbook import RedbookClient
from ingestion.series_config import REDBOOK_SERIES
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries


class RedbookFetcher:
    source_name = "redbook"

    def __init__(
        self,
        client: RedbookClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or RedbookClient()
        self.series_config = series_config or REDBOOK_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        raw_by_series = self.client.get_all_series_with_raw(
            self.series_config,
            lookback_days=lookback_days,
        )
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
        observations, payload, params = self.client.get_series_with_raw(
            cfg,
            lookback_days=lookback_days,
        )
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
        return RawSeries(
            source="redbook",
            series_id=str(cfg["series_id"]),
            observations=tuple(
                RawObservation(
                    date=obs.date,
                    value=obs.value,
                    provider_metadata={
                        "country": cfg.get("country", ""),
                        "indicator": cfg.get("indicator", ""),
                        "source_symbol": cfg.get("source_symbol", ""),
                    },
                )
                for obs in observations
            ),
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": cfg.get("category", ""),
                "country": cfg.get("country", ""),
                "indicator": cfg.get("indicator", ""),
                "name": cfg.get("name", ""),
                "source_symbol": cfg.get("source_symbol", ""),
                "unit": cfg.get("unit", ""),
            },
            raw_payload=payload,
            content_hash=content_hash_for_source("redbook", payload),
            request_params_json=json.dumps(params, sort_keys=True),
        )
