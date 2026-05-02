"""MOF JGB fetcher adapter — MOFJGBClient -> list[RawSeries]."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ingestion.series_config import MOF_JGB_SERIES
from ingestion.scrapers.mof_jgb import MOFJGBClient
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries


class MOFJGBFetcher:
    source_name = "mof_jp"

    def __init__(
        self,
        client: MOFJGBClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or MOFJGBClient()
        self.series_config = series_config or MOF_JGB_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        maturities = tuple(cfg["maturity"] for cfg in self.series_config.values())
        raw_by_maturity = self.client.get_all_series_with_raw(maturities, limit=30)
        rows: list[RawSeries] = []
        for cfg in self.series_config.values():
            maturity = cfg["maturity"]
            observations, payload, params = raw_by_maturity.get(maturity, ([], {}, {}))
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
        observations, payload, params = self.client.get_series_with_raw(
            cfg["maturity"], limit=30,
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
        maturity = cfg["maturity"]
        return RawSeries(
            source="mof_jp",
            series_id=cfg["series_id"],
            observations=tuple(
                RawObservation(
                    date=obs.date,
                    value=obs.value,
                    provider_metadata={"maturity": maturity},
                )
                for obs in observations
            ),
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": cfg.get("category", ""),
                "maturity": maturity,
                "name": cfg.get("name", ""),
            },
            raw_payload=payload,
            content_hash=content_hash_for_source("mof_jp", payload),
            request_params_json=json.dumps(params, sort_keys=True),
        )
