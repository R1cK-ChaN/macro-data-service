"""Vintage fetcher adapters — source clients to raw vintage batches."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.scrapers.fred import FredClient
from ingestion.sdmx.providers.imf import IMFClient
from ingestion.series_config import (
    IMF_SERIES,
    IMF_VINTAGE_SERIES,
    MACRO_SERIES,
    VINTAGE_SERIES,
)
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


class IMFVintageFetcher:
    source_name = "imf_vintages"
    storage_source = "imf"

    def __init__(
        self,
        client: IMFClient | None = None,
        *,
        vintage_series: tuple[str, ...] | list[str] | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
        snapshot_count: int = 12,
        request_delay_seconds: float = 0.0,
        raise_on_error: bool = True,
    ) -> None:
        self.client = client or IMFClient()
        self.vintage_series = tuple(vintage_series or IMF_VINTAGE_SERIES)
        self.series_config = series_config or IMF_SERIES
        self.snapshot_count = snapshot_count
        self.request_delay_seconds = request_delay_seconds
        self.raise_on_error = raise_on_error

    def fetch(self, *, lookback_days: int = 365) -> list[RawVintageSeries]:
        now = datetime.now(UTC)
        as_of_dates = [
            (now - timedelta(days=30 * i)).strftime("%Y-%m-%d")
            for i in range(self.snapshot_count)
        ]
        results: list[RawVintageSeries] = []
        failures: list[str] = []
        vintage_count = 0
        for series_key in self.vintage_series:
            cfg = self.series_config[series_key]
            try:
                rs = self._fetch_one(cfg, as_of_dates)
            except Exception as exc:
                logger.warning("IMF vintage fetch failed for %s", series_key, exc_info=True)
                failures.append(f"{series_key}: {exc}")
            else:
                results.append(rs)
                vintage_count += len(rs.vintages)
            if self.request_delay_seconds > 0:
                time.sleep(self.request_delay_seconds)
        if failures and vintage_count == 0 and self.raise_on_error:
            raise RuntimeError(
                f"imf vintages failed for all {len(self.vintage_series)} series; "
                f"first error: {failures[0]}"
            )
        if failures:
            logger.warning(
                "imf vintage partial failures: %d/%d; first error: %s",
                len(failures),
                len(self.vintage_series),
                failures[0],
            )
        return results

    def _fetch_one(
        self, cfg: dict[str, Any], as_of_dates: list[str],
    ) -> RawVintageSeries:
        obs_list, payload, params = self.client.get_vintages_with_raw(
            cfg["dataflow"],
            cfg["key"],
            series_id=cfg["series_id"],
            version=cfg.get("version", ""),
            as_of_dates=as_of_dates,
            limit=30,
        )
        vintages = tuple(
            RawVintageObservation(
                date=obs.date,
                vintage_date=obs.vintage_date,
                value=obs.value,
                provider_metadata={"dataflow": obs.dataflow} if obs.dataflow else {},
            )
            for obs in obs_list
        )
        content_hash = (
            content_hash_for_source(self.source_name, payload)
            if payload else None
        )
        return RawVintageSeries(
            source=self.source_name,
            storage_source=self.storage_source,
            series_id=cfg["series_id"],
            vintages=vintages,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": cfg.get("category", ""),
                "dataflow": cfg.get("dataflow", ""),
            },
            vintage_quality="synthetic_snapshot",
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
