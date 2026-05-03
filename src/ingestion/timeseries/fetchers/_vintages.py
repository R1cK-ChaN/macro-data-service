"""Vintage fetcher adapters — source clients to raw vintage batches."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.scrapers.fred import FredClient
from ingestion.series_config import MACRO_SERIES, VINTAGE_SERIES
from ingestion.timeseries.canonicalize import content_hash_for_source

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RawVintageObservation:
    date: str
    vintage_date: str
    value: float
    provider_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawVintageSeries:
    source: str
    storage_source: str
    series_id: str
    vintages: tuple[RawVintageObservation, ...]
    fetched_at: str
    series_metadata: dict[str, Any] = field(default_factory=dict)
    vintage_quality: str = "single_observation"
    raw_payload: dict[str, Any] | None = None
    content_hash: str | None = None
    request_params_json: str | None = None


class FredVintageFetcher:
    source_name = "fred_vintages"
    storage_source = "fred"

    def __init__(
        self,
        client: FredClient | None = None,
        *,
        series_ids: tuple[str, ...] | list[str] | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
        request_delay_seconds: float = 0.3,
        raise_on_error: bool = True,
    ) -> None:
        self.client = client or FredClient()
        self.series_ids = tuple(series_ids or VINTAGE_SERIES)
        self.series_config = series_config or MACRO_SERIES
        self.request_delay_seconds = request_delay_seconds
        self.raise_on_error = raise_on_error

    def fetch(self, *, lookback_days: int = 365) -> list[RawVintageSeries]:
        start_date = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        results: list[RawVintageSeries] = []
        for series_id in self.series_ids:
            rs = self._fetch_one(series_id, start_date)
            if rs is not None:
                results.append(rs)
            if self.request_delay_seconds > 0:
                time.sleep(self.request_delay_seconds)
        return results

    def _fetch_one(
        self, series_id: str, start_date: str,
    ) -> RawVintageSeries | None:
        try:
            obs_list, payload, params = self.client.get_vintages_with_raw(
                series_id, start_date=start_date,
            )
        except Exception:
            if self.raise_on_error:
                raise
            logger.warning("FRED vintage fetch failed for %s", series_id, exc_info=True)
            return None
        vintages = tuple(
            RawVintageObservation(
                date=obs.date,
                vintage_date=obs.vintage_date,
                value=obs.value,
            )
            for obs in obs_list
        )
        content_hash = (
            content_hash_for_source(self.source_name, payload)
            if payload else None
        )
        meta = self.series_config.get(series_id, {})
        return RawVintageSeries(
            source=self.source_name,
            storage_source=self.storage_source,
            series_id=series_id,
            vintages=vintages,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"name": meta.get("name", series_id)},
            vintage_quality="native_pit",
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
