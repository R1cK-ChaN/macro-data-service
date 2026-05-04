"""EIA fetcher adapter — EIAClient → list[RawSeries]."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from ingestion.scrapers.fred import FredClient
from ingestion.scrapers.eia import EIAClient
from ingestion.series_config import EIA_SERIES
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EIACacheHit:
    source: str
    series_id: str
    cache_hit_count: int
    fetched_at: str
    series_metadata: dict[str, Any]
    raw_payload: dict[str, Any] | None = None
    content_hash: str | None = None
    request_params_json: str | None = None


class EIAFetcher:
    source_name = "eia"

    def __init__(
        self,
        client: EIAClient | None = None,
        series_config: dict[str, dict[str, Any]] | None = None,
        *,
        fred_client: FredClient | None = None,
        history_loader: Callable[[str, int], list[Any]] | None = None,
        live_limit: int = 100,
        request_delay_seconds: float = 0.3,
    ) -> None:
        self.client = client or EIAClient()
        self.series_config = series_config or EIA_SERIES
        self.fred_client = fred_client or FredClient()
        self.history_loader = history_loader
        self.live_limit = live_limit
        self.request_delay_seconds = request_delay_seconds

    _FRED_FALLBACK_SERIES: dict[str, str] = {
        "EIA_BRENT": "DCOILBRENTEU",
        "EIA_WTI": "DCOILWTICO",
        "EIA_CRUDE_STOCKS": "WCESTUS1",
        "EIA_NATGAS": "DHHNGSP",
    }

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries | EIACacheHit]:
        results: list[RawSeries | EIACacheHit] = []
        for _key, cfg in self.series_config.items():
            rs = self._fetch_one(cfg)
            if rs is not None:
                results.append(rs)
            if self.request_delay_seconds > 0:
                time.sleep(self.request_delay_seconds)
        return results

    def fetch_series(
        self, series_id: str, *, lookback_days: int = 365
    ) -> RawSeries | None:
        cfg = next(
            (c for c in self.series_config.values() if c["series_id"] == series_id),
            None,
        )
        if cfg is None:
            return None
        rs = self._fetch_one(cfg)
        return rs if isinstance(rs, RawSeries) else None

    def _fetch_one(self, cfg: dict[str, Any]) -> RawSeries | EIACacheHit | None:
        series_id = cfg["series_id"]
        frequency = self._frequency(cfg)
        try:
            obs_list, payload, params = self.client.get_series_with_raw(
                cfg["route"],
                params=dict(cfg["params"]),
                series_id=series_id,
                limit=self.live_limit,
            )
        except Exception as exc:
            logger.warning("EIA primary fetch failed for %s: %s", series_id, exc)
            obs_list, payload, params = [], {}, {}
        if obs_list:
            return self._raw_series(cfg, obs_list, payload, params)

        cached_count = self._recent_cache_count(series_id=series_id, frequency=frequency)
        if cached_count > 0:
            logger.info("EIA cache hit for %s (%d recent observations)", series_id, cached_count)
            return EIACacheHit(
                source=self.source_name,
                series_id=series_id,
                cache_hit_count=cached_count,
                fetched_at=datetime.now(UTC).isoformat(),
                series_metadata=self._series_metadata(cfg),
            )

        fallback_series = self._FRED_FALLBACK_SERIES.get(series_id)
        if fallback_series:
            fallback = self._fetch_fred_fallback(cfg, fallback_series)
            if fallback is not None and fallback.observations:
                logger.warning("EIA fallback used for %s via FRED %s", series_id, fallback_series)
                return fallback

        logger.warning("no live EIA data, cache, or FRED fallback available for %s", series_id)
        return None

    def _raw_series(
        self,
        cfg: dict[str, Any],
        obs_list: list[Any],
        payload: dict[str, Any],
        params: dict[str, str],
    ) -> RawSeries:
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={"unit": obs.unit} if obs.unit else {},
            )
            for obs in obs_list
        )
        content_hash = content_hash_for_source("eia", payload) if payload else None
        return RawSeries(
            source="eia",
            series_id=cfg["series_id"],
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata=self._series_metadata(cfg),
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )

    def _fetch_fred_fallback(
        self, cfg: dict[str, Any], fred_series_id: str,
    ) -> RawSeries | None:
        start_date = (datetime.now(UTC) - timedelta(days=14)).strftime("%Y-%m-%d")
        try:
            observations, payload, params = self.fred_client.get_series_with_raw(
                fred_series_id, start_date=start_date, limit=5,
            )
        except Exception:
            return None
        if not observations:
            return None
        fallback_payload = {
            "fallback_source": "fred",
            "fallback_series_id": fred_series_id,
            "response": payload,
        }
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={
                    "fallback_source": "fred",
                    "fallback_series_id": fred_series_id,
                },
            )
            for obs in observations
        )
        request_params = {
            "fallback_source": "fred",
            "fallback_series_id": fred_series_id,
            **{f"fred_{key}": str(value) for key, value in params.items()},
        }
        return RawSeries(
            source=self.source_name,
            series_id=cfg["series_id"],
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata=self._series_metadata(cfg),
            raw_payload=fallback_payload,
            content_hash=content_hash_for_source(self.source_name, fallback_payload),
            request_params_json=json.dumps(request_params, sort_keys=True),
        )

    def _recent_cache_count(self, *, series_id: str, frequency: str) -> int:
        if self.history_loader is None:
            return 0
        ttl_days = {"daily": 7, "weekly": 10, "monthly": 45}.get(frequency, 14)
        history = self.history_loader(series_id, 5)
        if not history:
            return 0
        latest = history[0]
        try:
            latest_dt = datetime.fromisoformat(latest.date)
        except ValueError:
            return 0
        age_days = (datetime.now(UTC) - latest_dt.replace(tzinfo=UTC)).days
        return len(history) if age_days <= ttl_days else 0

    @staticmethod
    def _frequency(cfg: dict[str, Any]) -> str:
        return str(cfg.get("params", {}).get("frequency", "daily")).lower()

    def _series_metadata(self, cfg: dict[str, Any]) -> dict[str, Any]:
        return {
            "category": cfg.get("category", "energy"),
            "freq": self._frequency(cfg),
        }
