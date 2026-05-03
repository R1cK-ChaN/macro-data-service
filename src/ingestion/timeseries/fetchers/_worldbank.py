"""World Bank fetcher adapter — WorldBankClient → list[RawSeries]."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from datetime import UTC, datetime

from ingestion.clients._worldbank_client import _WorldBankRateLimiter
from ingestion.scrapers.worldbank import WorldBankClient, WorldBankRateLimitError
from ingestion.series_config import WORLDBANK_SERIES, WorldBankSeriesConfig
from ingestion.timeseries.canonicalize import content_hash_for_source
from ingestion.types import RawObservation, RawSeries

logger = logging.getLogger(__name__)


class WorldBankFetcher:
    source_name = "worldbank"

    def __init__(
        self,
        client: WorldBankClient | None = None,
        series_config: dict[str, WorldBankSeriesConfig] | None = None,
    ) -> None:
        self.client = client or WorldBankClient()
        self.series_config = series_config or WORLDBANK_SERIES

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
            (c for c in self.series_config.values() if c.series_id == series_id),
            None,
        )
        if cfg is None:
            return None
        return self._fetch_one(cfg)

    def _fetch_one(self, cfg: WorldBankSeriesConfig) -> RawSeries | None:
        try:
            obs_list, payload, params = self.client.get_indicator_with_raw(
                cfg.indicator,
                cfg.country,
                series_id=cfg.series_id,
                start_year=cfg.start_year,
                limit=cfg.limit,
            )
        except Exception as exc:
            logger.error("WorldBank fetch failed [%s country=%s]: %s", cfg.indicator, cfg.country, exc)
            return None
        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={
                    k: v
                    for k, v in {
                        "indicator": obs.indicator,
                        "country_code": obs.country_code,
                        "country_name": obs.country_name,
                    }.items()
                    if v
                },
            )
            for obs in obs_list
        )
        content_hash = content_hash_for_source("worldbank", payload) if payload else None
        return RawSeries(
            source="worldbank",
            series_id=cfg.series_id,
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={"category": cfg.category, "indicator": cfg.indicator},
            raw_payload=payload or None,
            content_hash=content_hash,
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )


class WorldBankCatalogFetcher:
    source_name = "worldbank_catalog"
    storage_source = "worldbank"

    def __init__(
        self,
        client: WorldBankClient | None = None,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        latest_observations: int = 5,
        max_workers: int = 3,
        request_delay: float = 0.3,
    ) -> None:
        self.client = client or WorldBankClient()
        self.source_id = source_id
        self.topic_id = topic_id
        self.query = query
        self.indicator_limit = indicator_limit
        self.countries = countries
        self.latest_observations = latest_observations
        self.max_workers = max_workers
        self.request_delay = request_delay

    def fetch(self, *, lookback_days: int = 365) -> list[RawSeries]:
        indicators = self._list_indicators()
        country = ";".join(self.countries) if self.countries else "all"
        limiter = _WorldBankRateLimiter(min_interval=self.request_delay)
        results: list[RawSeries] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id: dict[concurrent.futures.Future[RawSeries | None], str] = {}
            for indicator in indicators:
                future = executor.submit(self._fetch_indicator, indicator, country, limiter)
                future_to_id[future] = indicator.id

            for future in concurrent.futures.as_completed(future_to_id):
                label = future_to_id[future]
                try:
                    rs = future.result()
                except Exception:
                    logger.warning(
                        "World Bank catalog fetch failed for %s", label, exc_info=True,
                    )
                    continue
                if rs is not None:
                    results.append(rs)
        return results

    def _list_indicators(self):
        if self.query:
            return self.client.search_indicators(
                self.query,
                source_id=self.source_id,
                topic_id=self.topic_id,
                limit=self.indicator_limit or 50,
            )
        indicators = self.client.list_indicators(
            source_id=self.source_id,
            topic_id=self.topic_id,
        )
        if self.indicator_limit:
            return indicators[:self.indicator_limit]
        return indicators

    def _fetch_indicator(self, indicator, country: str, limiter: _WorldBankRateLimiter) -> RawSeries:
        max_retries = 3
        for attempt in range(max_retries):
            limiter.wait()
            try:
                observations, payload, params = self.client.get_indicator_with_raw(
                    indicator.id,
                    country,
                    series_id=f"WB_{indicator.id}",
                    limit=(
                        self.latest_observations * 300
                        if country == "all" else self.latest_observations
                    ),
                    per_page=1000,
                    fetch_all_pages=country == "all",
                )
                break
            except WorldBankRateLimitError:
                if attempt < max_retries - 1:
                    backoff = max(5.0, self.request_delay) * (2 ** attempt)
                    logger.warning(
                        "World Bank 429 for %s, retry %d backing off %.1fs",
                        indicator.id, attempt + 1, backoff,
                    )
                    limiter.backoff(backoff)
                    continue
                raise

        raw_obs = tuple(
            RawObservation(
                date=obs.date,
                value=obs.value,
                provider_metadata={
                    "storage_series_id": (
                        f"WB_{indicator.id}_{obs.country_code}"
                        if obs.country_code else f"WB_{indicator.id}"
                    ),
                    "country_code": obs.country_code,
                    "country_name": obs.country_name,
                },
            )
            for obs in observations
        )
        return RawSeries(
            source=self.source_name,
            series_id=f"WB_{indicator.id}",
            observations=raw_obs,
            fetched_at=datetime.now(UTC).isoformat(),
            series_metadata={
                "category": "catalog",
                "indicator": indicator.id,
                "indicator_name": indicator.name,
                "source_name": indicator.source_name,
            },
            raw_payload=payload or None,
            content_hash=(
                content_hash_for_source(self.source_name, payload)
                if payload else None
            ),
            request_params_json=json.dumps(params, sort_keys=True) if params else None,
        )
