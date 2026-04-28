"""BLS fetcher adapter — BLSClient → list[RawSeries]."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ingestion.scrapers.bls import BLSClient
from ingestion.series_config import BLS_SERIES
from ingestion.timeseries.canonicalize import bls_content_hash
from ingestion.types import RawObservation, RawSeries


class BLSFetcher:
    source_name = "bls"

    def __init__(
        self,
        client: BLSClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.client = client or BLSClient()
        self.series_config = series_config or BLS_SERIES

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        start_year = (datetime.now(UTC).year - max(1, lookback_days // 365))
        results: list[RawSeries] = []
        for _key, meta in self.series_config.items():
            rs = self._fetch_one(meta, start_year)
            if rs is not None:
                results.append(rs)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        meta = next(
            (m for m in self.series_config.values() if m["series_id"] == series_id),
            None,
        )
        if meta is None:
            return None
        start_year = (datetime.now(UTC).year - max(1, lookback_days // 365))
        return self._fetch_one(meta, start_year)

    def _fetch_one(
        self, meta: dict[str, Any], start_year: int
    ) -> RawSeries | None:
        sid = meta["series_id"]
        try:
            obs_list, payload, params = self.client.get_series_single_with_raw(
                sid, start_year=start_year,
            )
        except Exception:
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={"period": obs.period},
            )
            for obs in obs_list
        )
        content_hash = bls_content_hash(payload) if payload else None
        return RawSeries(
            source="bls",
            series_id=sid,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata=meta,
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
