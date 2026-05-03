"""Treasury Fiscal fetcher adapter — TreasuryFiscalClient → list[RawSeries]."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from ingestion.scrapers.treasury_fiscal import TreasuryFiscalClient
from ingestion.series_config import TREASURY_DATASETS
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)

_METHOD_MAP = {
    "debt_outstanding": "fetch_debt_outstanding_with_raw",
    "dts_operating_cash": "fetch_tga_balance_with_raw",
    "avg_interest_rates": "fetch_avg_interest_rates_with_raw",
}


class TreasuryFetcher:
    source_name = "treasury_fiscal"

    def __init__(
        self,
        client: TreasuryFiscalClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or TreasuryFiscalClient()
        self.series_config = series_config or TREASURY_DATASETS

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        results: list[RawSeries] = []
        for key, cfg in self.series_config.items():
            rs = self._fetch_one(key, cfg)
            if rs is not None:
                results.append(rs)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        for key, cfg in self.series_config.items():
            if cfg["series_id"] == series_id:
                return self._fetch_one(key, cfg)
        return None

    def _fetch_one(self, key: str, cfg: dict[str, Any]) -> RawSeries | None:
        method_name = _METHOD_MAP.get(key)
        if method_name is None:
            return None
        try:
            obs_list, payload, params = getattr(self.client, method_name)(limit=30)
        except Exception as exc:
            logger.error("Treasury fetch failed [%s method=%s]: %s", cfg.get("series_id", "?"), method_name, exc)
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata=obs.metadata if obs.metadata else {},
            )
            for obs in obs_list
        )
        content_hash = content_hash_for_source("treasury_fiscal", payload) if payload else None
        return RawSeries(
            source="treasury_fiscal",
            series_id=cfg["series_id"],
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.get("category", "fiscal")},
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
