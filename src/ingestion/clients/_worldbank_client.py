"""World Bank ingestion client with rate limiting and catalog management."""

from __future__ import annotations

import concurrent.futures
import hashlib
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from ingestion.scrapers.worldbank import WorldBankClient, WorldBankRateLimitError
from ingestion.series_config import (
    WORLDBANK_SERIES,
    WorldBankSeriesConfig,
    _generated_wb_config_key,
    _generated_wb_series_id,
)
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class _WorldBankRateLimiter:
    """Thread-safe rate limiter for World Bank API requests.

    World Bank has no documented hard rate limit, but we enforce
    respectful defaults: 0.3s min interval, 500 requests/hour budget.
    """

    HOURLY_BUDGET = 500

    def __init__(self, min_interval: float = 0.3) -> None:
        self._lock = threading.Lock()
        self._last_request = 0.0
        self._min_interval = min_interval
        self._hour_start = time.monotonic()
        self._hour_count = 0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now - self._hour_start >= 3600.0:
                self._hour_start = now
                self._hour_count = 0
            if self._hour_count >= self.HOURLY_BUDGET:
                sleep_for = 3600.0 - (now - self._hour_start)
                if sleep_for > 0:
                    logger.warning(
                        "World Bank hourly budget (%d/%d) exhausted, sleeping %.0fs",
                        self._hour_count, self.HOURLY_BUDGET, sleep_for,
                    )
                    time.sleep(sleep_for)
                self._hour_start = time.monotonic()
                self._hour_count = 0
                now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()
            self._hour_count += 1

    def backoff(self, seconds: float) -> None:
        """Push the next allowed request time forward (called on 429)."""
        with self._lock:
            self._last_request = time.monotonic() + seconds - self._min_interval




class WorldBankIngestionClient:
    def __init__(
        self,
        client: WorldBankClient | None = None,
        *,
        series_configs: dict[str, WorldBankSeriesConfig] | None = None,
    ) -> None:
        self.client = client or WorldBankClient()
        self.series_configs = series_configs or WORLDBANK_SERIES

    # -- configured series refresh (backward compatible) ---------------------

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in self.series_configs.items():
            try:
                observations = self.client.get_indicator(
                    cfg.indicator,
                    cfg.country,
                    series_id=cfg.series_id,
                    limit=cfg.limit,
                )
                fam_id = family_lookup.get(("worldbank", cfg.series_id)) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="worldbank",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg.category, "indicator": obs.indicator},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("World Bank refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="worldbank", count=count)

    # -- parallel configured series refresh ----------------------------------

    def refresh_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
        max_workers: int = 4,
        request_delay: float = 0.3,
    ) -> RefreshStats:
        """Fetch all configured series using a thread pool."""
        limiter = _WorldBankRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_one(key: str, cfg: WorldBankSeriesConfig) -> list[IndicatorObservationRecord]:
            max_retries = 3
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    observations = self.client.get_indicator(
                        cfg.indicator,
                        cfg.country,
                        series_id=cfg.series_id,
                        start_year=cfg.start_year,
                        limit=cfg.limit,
                    )
                    break
                except WorldBankRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(5.0, request_delay) * (2 ** attempt)
                        logger.warning("World Bank 429 for %s, retry %d backing off %.1fs", key, attempt + 1, backoff)
                        limiter.backoff(backoff)
                        continue
                    raise
            fam_id = family_lookup.get(("worldbank", cfg.series_id)) if family_lookup else None
            return [
                IndicatorObservationRecord(
                    series_id=obs.series_id,
                    source="worldbank",
                    date=obs.date,
                    value=obs.value,
                    metadata={"category": cfg.category, "indicator": obs.indicator},
                    obs_family_id=fam_id,
                )
                for obs in observations
            ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for key, cfg in self.series_configs.items():
                future = executor.submit(_fetch_one, key, cfg)
                future_to_key[future] = key

            for future in concurrent.futures.as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("World Bank parallel refresh failed for %s", key, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="worldbank", count=count)

    # -- catalog discovery proxies -------------------------------------------

    def list_catalog_sources(self) -> list[Any]:
        return self.client.list_sources()

    def list_catalog_topics(self) -> list[Any]:
        return self.client.list_topics()

    def list_catalog_countries(self) -> list[Any]:
        return self.client.list_countries()

    def list_catalog_indicators(
        self,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if query:
            indicators = self.client.search_indicators(
                query, source_id=source_id, topic_id=topic_id, limit=limit or 50,
            )
        else:
            indicators = self.client.list_indicators(source_id=source_id, topic_id=topic_id)
            if limit:
                indicators = indicators[:limit]
        return indicators

    # -- config generation ---------------------------------------------------

    def generate_catalog_series_configs(
        self,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        category: str = "catalog",
    ) -> dict[str, WorldBankSeriesConfig]:
        """Auto-discover indicators and generate series configs."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        generated: dict[str, WorldBankSeriesConfig] = {}
        for ind in indicators:
            country = ";".join(countries) if countries else "all"
            config = WorldBankSeriesConfig(
                indicator=ind.id,
                series_id=_generated_wb_series_id(ind.id, country),
                category=category,
                country=country,
                source_id=ind.source_id,
            )
            generated[_generated_wb_config_key(ind.id, country)] = config
        return generated

    # -- catalog refresh (sequential) ----------------------------------------

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        latest_observations: int = 5,
        sleep_seconds: float = 0.3,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        """Discover indicators from catalog and fetch their latest data."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        country_str = ";".join(countries) if countries else "all"
        count = 0
        for ind in indicators:
            try:
                observations = self.client.get_indicator(
                    ind.id,
                    country_str,
                    series_id=f"WB_{ind.id}",
                    limit=latest_observations * 300 if country_str == "all" else latest_observations,
                    per_page=1000,
                    fetch_all_pages=country_str == "all",
                )
                for obs in observations:
                    sid = f"WB_{ind.id}_{obs.country_code}" if obs.country_code else f"WB_{ind.id}"
                    fam_id = family_lookup.get(("worldbank", sid)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=sid,
                            source="worldbank",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "indicator": ind.id,
                                "indicator_name": ind.name,
                                "source_name": ind.source_name,
                                "country_code": obs.country_code,
                                "country_name": obs.country_name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("World Bank catalog refresh failed for %s", ind.id, exc_info=True)
            time.sleep(max(sleep_seconds, 0.0))
        return RefreshStats(source="worldbank_catalog", count=count)

    # -- catalog refresh (parallel) ------------------------------------------

    def refresh_catalog_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        source_id: str | None = None,
        topic_id: str | None = None,
        query: str | None = None,
        indicator_limit: int | None = 10,
        countries: list[str] | None = None,
        max_workers: int = 3,
        request_delay: float = 0.3,
        latest_observations: int = 5,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        """Fetch catalog data from multiple indicators in parallel."""
        indicators = self.list_catalog_indicators(
            source_id=source_id, topic_id=topic_id, query=query, limit=indicator_limit,
        )
        country_str = ";".join(countries) if countries else "all"
        limiter = _WorldBankRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_indicator(ind: Any) -> list[IndicatorObservationRecord]:
            max_retries = 3
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    observations = self.client.get_indicator(
                        ind.id,
                        country_str,
                        series_id=f"WB_{ind.id}",
                        limit=latest_observations * 300 if country_str == "all" else latest_observations,
                        per_page=1000,
                        fetch_all_pages=country_str == "all",
                    )
                    break
                except WorldBankRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(5.0, request_delay) * (2 ** attempt)
                        logger.warning(
                            "World Bank 429 for %s, retry %d backing off %.1fs",
                            ind.id, attempt + 1, backoff,
                        )
                        limiter.backoff(backoff)
                        continue
                    raise
            records: list[IndicatorObservationRecord] = []
            for obs in observations:
                sid = f"WB_{ind.id}_{obs.country_code}" if obs.country_code else f"WB_{ind.id}"
                fam_id = family_lookup.get(("worldbank", sid)) if family_lookup else None
                records.append(
                    IndicatorObservationRecord(
                        series_id=sid,
                        source="worldbank",
                        date=obs.date,
                        value=obs.value,
                        metadata={
                            "category": "catalog",
                            "indicator": ind.id,
                            "indicator_name": ind.name,
                            "source_name": ind.source_name,
                            "country_code": obs.country_code,
                            "country_name": obs.country_name,
                        },
                        obs_family_id=fam_id,
                    )
                )
            return records

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for ind in indicators:
                future = executor.submit(_fetch_indicator, ind)
                future_to_id[future] = ind.id

            for future in concurrent.futures.as_completed(future_to_id):
                label = future_to_id[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("World Bank catalog parallel refresh failed for %s", label, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="worldbank_catalog", count=count)


