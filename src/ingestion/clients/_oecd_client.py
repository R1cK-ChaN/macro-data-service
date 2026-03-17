"""OECD ingestion client with rate limiting and catalog management."""

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

from ingestion.sdmx._errors import OECDRateLimitError
from ingestion.sdmx.providers.oecd import OECDClient
from ingestion.series_config import (
    OECD_SERIES,
    OECDSeriesConfig,
    _generated_oecd_config_key,
    _generated_oecd_series_id,
)
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class _OECDRateLimiter:
    """Thread-safe rate limiter for OECD API requests.

    OECD enforces a hard cap of 60 data downloads per hour (per IP).
    This limiter enforces both a minimum interval between requests and
    an hourly budget to stay under that cap.
    """

    HOURLY_BUDGET = 60

    def __init__(self, min_interval: float = 2.0) -> None:
        self._lock = threading.Lock()
        self._last_request = 0.0
        self._min_interval = min_interval
        self._hour_start = time.monotonic()
        self._hour_count = 0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            # Reset hourly window if >3600s elapsed
            if now - self._hour_start >= 3600.0:
                self._hour_start = now
                self._hour_count = 0
            # If we've hit the hourly budget, sleep until the window resets
            if self._hour_count >= self.HOURLY_BUDGET:
                sleep_for = 3600.0 - (now - self._hour_start)
                if sleep_for > 0:
                    logger.warning(
                        "OECD hourly budget (%d/%d) exhausted, sleeping %.0fs",
                        self._hour_count, self.HOURLY_BUDGET, sleep_for,
                    )
                    time.sleep(sleep_for)
                self._hour_start = time.monotonic()
                self._hour_count = 0
                now = time.monotonic()
            # Enforce minimum interval
            elapsed = now - self._last_request
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request = time.monotonic()
            self._hour_count += 1

    def backoff(self, seconds: float) -> None:
        """Push the next allowed request time forward (called on 429)."""
        with self._lock:
            self._last_request = time.monotonic() + seconds - self._min_interval


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


class OECDIngestionClient:
    def __init__(
        self,
        client: OECDClient | None = None,
        *,
        series_configs: dict[str, OECDSeriesConfig] | None = None,
    ) -> None:
        self.client = client or OECDClient()
        self.series_configs = series_configs or OECD_SERIES
        self._resolved_configs: dict[str, tuple[str, str]] = {}

    def _resolve_request(self, cfg: OECDSeriesConfig) -> tuple[str, str]:
        cached = self._resolved_configs.get(cfg.series_id)
        if cached is not None:
            return cached

        dataflow = self.client.get_dataflow(
            cfg.dataflow,
            agency_id=cfg.agency_id,
            version=cfg.version,
        )
        if cfg.key:
            resolved = (dataflow.version, cfg.key)
        else:
            resolved = (
                dataflow.version,
                self.client.build_key(
                    cfg.dataflow,
                    cfg.filters,
                    agency_id=cfg.agency_id,
                    version=dataflow.version,
                ),
            )
        self._resolved_configs[cfg.series_id] = resolved
        return resolved

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in self.series_configs.items():
            try:
                resolved_version, resolved_key = self._resolve_request(cfg)
                observations = self.client.fetch_data(
                    cfg.dataflow,
                    agency_id=cfg.agency_id,
                    version=resolved_version,
                    key=resolved_key,
                    series_id=cfg.series_id,
                    limit=30,
                )
                fam_id = family_lookup.get(("oecd", cfg.series_id)) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="oecd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": cfg.category,
                                "dataflow": obs.dataflow,
                                "agency_id": obs.agency_id,
                                "series_key": obs.series_key or resolved_key,
                                "dimensions": obs.dimensions,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("OECD refresh failed for %s", key, exc_info=True)
            time.sleep(2.0)
        return RefreshStats(source="oecd", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        agency_prefix: str = "OECD",
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows(agency_id="all")
        if agency_prefix:
            dataflows = [dataflow for dataflow in dataflows if dataflow.agency_id.startswith(agency_prefix)]
        if query:
            needle = query.lower().strip()
            dataflows = [
                dataflow for dataflow in dataflows
                if needle in dataflow.id.lower()
                or needle in dataflow.name.lower()
                or needle in dataflow.description.lower()
            ]
        dataflows.sort(key=lambda item: (item.agency_id, item.id, item.version))
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            if agency_id:
                return [
                    self.client.get_dataflow(dataflow_id, agency_id=agency_id, version="latest")
                    for dataflow_id in dataflow_ids
                ]
            allowed = set(dataflow_ids)
            matches = [
                dataflow for dataflow in self.list_catalog_dataflows(
                    agency_prefix=agency_prefix,
                    limit=None,
                )
                if dataflow.id in allowed
            ]
            ordered: list[Any] = []
            seen: set[tuple[str, str]] = set()
            for dataflow_id in dataflow_ids:
                for dataflow in matches:
                    marker = (dataflow.agency_id, dataflow.id)
                    if dataflow.id == dataflow_id and marker not in seen:
                        ordered.append(dataflow)
                        seen.add(marker)
            return ordered[:limit] if limit is not None else ordered
        return self.list_catalog_dataflows(query=query, agency_prefix=agency_prefix, limit=limit)

    def get_structure_summary(
        self,
        dataflow_id: str,
        *,
        agency_id: str = OECDClient.DEFAULT_AGENCY_ID,
        version: str = "latest",
    ) -> Any:
        return self.client.summarize_structure(dataflow_id, agency_id=agency_id, version=version)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, OECDSeriesConfig]:
        generated: dict[str, OECDSeriesConfig] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_id=agency_id,
            query=query,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        ):
            series_list = self.client.enumerate_series(
                dataflow.id,
                agency_id=dataflow.agency_id,
                version=dataflow.version,
                key="all",
                observation_limit=1,
                max_series=series_per_dataflow,
            )
            for series in series_list:
                filters = self.client.series_to_filters(
                    dataflow.id,
                    series,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                )
                if not filters:
                    continue
                series_key = self.client.build_key(
                    dataflow.id,
                    filters,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                )
                config = OECDSeriesConfig(
                    dataflow=dataflow.id,
                    series_id=_generated_oecd_series_id(dataflow.agency_id, dataflow.id, series_key),
                    category=category,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    filters=filters,
                )
                generated[_generated_oecd_config_key(dataflow.id, series_key)] = config
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        agency_id: str | None = None,
        query: str | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 3.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_id=agency_id,
            query=query,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        ):
            try:
                observations = self.client.fetch_data(
                    dataflow.id,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    key="all",
                    series_id=None,
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("oecd", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="oecd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                                "dataflow_description": dataflow.description,
                                "agency_id": obs.agency_id or dataflow.agency_id,
                                "series_key": obs.series_key,
                                "raw_series_key": obs.raw_series_key,
                                "dimensions": obs.dimensions,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("OECD catalog refresh failed for %s/%s", dataflow.agency_id, dataflow.id, exc_info=True)
            time.sleep(max(sleep_seconds, 0.0))
        return RefreshStats(source="oecd_catalog", count=count)

    def refresh_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
        max_workers: int = 3,
        request_delay: float = 2.0,
    ) -> RefreshStats:
        """Fetch all configured series using a thread pool.

        OECD enforces 60 data downloads/hour per IP.  The rate limiter
        enforces both ``request_delay`` spacing and the hourly budget.
        Defaults are conservative: 3 workers, 2 s between requests.
        """
        limiter = _OECDRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []
        errors: list[str] = []

        def _fetch_one(key: str, cfg: OECDSeriesConfig) -> list[IndicatorObservationRecord]:
            max_retries = 5
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    resolved_version, resolved_key = self._resolve_request(cfg)
                    observations = self.client.fetch_data(
                        cfg.dataflow,
                        agency_id=cfg.agency_id,
                        version=resolved_version,
                        key=resolved_key,
                        series_id=cfg.series_id,
                        limit=30,
                    )
                    break
                except OECDRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(10.0, request_delay) * (2 ** attempt)
                        logger.warning("OECD 429 for %s, retry %d backing off %.1fs", key, attempt + 1, backoff)
                        limiter.backoff(backoff)
                        continue
                    raise
            fam_id = family_lookup.get(("oecd", cfg.series_id)) if family_lookup else None
            return [
                IndicatorObservationRecord(
                    series_id=obs.series_id,
                    source="oecd",
                    date=obs.date,
                    value=obs.value,
                    metadata={
                        "category": cfg.category,
                        "dataflow": obs.dataflow,
                        "agency_id": obs.agency_id,
                        "series_key": obs.series_key or resolved_key,
                        "dimensions": obs.dimensions,
                    },
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
                    errors.append(key)
                    logger.warning("OECD parallel refresh failed for %s", key, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="oecd", count=count)

    def refresh_catalog_parallel(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        agency_prefix: str = "OECD",
        dataflow_limit: int | None = None,
        max_workers: int = 2,
        request_delay: float = 3.0,
        latest_observations: int = 1,
        updated_after: str | None = None,
        family_lookup: dict[tuple[str, str], str] | None = None,
        chunked: bool = True,
        obs_threshold: int = 1_000_000,
    ) -> RefreshStats:
        """Fetch catalog data from multiple dataflows in parallel.

        OECD enforces 60 data downloads/hour per IP.  Catalog fetches
        are heavier so defaults are more conservative: 2 workers, 3 s
        between requests.

        Pass ``updated_after`` (ISO-8601 datetime, e.g. '2026-03-01T00:00:00Z')
        to only fetch data that changed since your last sync.  Most OECD
        datasets update annually or monthly, so this dramatically reduces
        request volume for recurring ingestion.

        When ``chunked=True`` (default), datasets estimated to exceed
        ``obs_threshold`` observations are automatically split into
        decade-sized time-range queries to avoid timeouts and memory
        exhaustion on large cubes (e.g. SNA_TABLE1, ICIO).
        """
        dataflows = self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids,
            agency_prefix=agency_prefix,
            limit=dataflow_limit,
        )
        limiter = _OECDRateLimiter(min_interval=request_delay)
        results: list[list[IndicatorObservationRecord]] = []

        def _fetch_with_retry(
            fetch_fn: Callable[[], list[Any]],
            label: str,
        ) -> list[Any]:
            max_retries = 5
            for attempt in range(max_retries):
                limiter.wait()
                try:
                    return fetch_fn()
                except OECDRateLimitError:
                    if attempt < max_retries - 1:
                        backoff = max(10.0, request_delay) * (2 ** attempt)
                        logger.warning(
                            "OECD 429 for %s, retry %d backing off %.1fs",
                            label, attempt + 1, backoff,
                        )
                        limiter.backoff(backoff)
                        continue
                    raise
            return []  # unreachable, but satisfies type checker

        def _fetch_dataflow(dataflow: Any) -> list[IndicatorObservationRecord]:
            label = f"{dataflow.agency_id}/{dataflow.id}"
            if chunked:
                observations = self.client.fetch_dataset_chunked(
                    dataflow.id,
                    agency_id=dataflow.agency_id,
                    version=dataflow.version,
                    key="all",
                    series_id=None,
                    limit=latest_observations,
                    obs_threshold=obs_threshold,
                )
            else:
                observations = _fetch_with_retry(
                    lambda: self.client.fetch_data(
                        dataflow.id,
                        agency_id=dataflow.agency_id,
                        version=dataflow.version,
                        key="all",
                        series_id=None,
                        updated_after=updated_after,
                        limit=latest_observations,
                    ),
                    label,
                )
            records: list[IndicatorObservationRecord] = []
            for obs in observations:
                fam_id = family_lookup.get(("oecd", obs.series_id)) if family_lookup else None
                records.append(
                    IndicatorObservationRecord(
                        series_id=obs.series_id,
                        source="oecd",
                        date=obs.date,
                        value=obs.value,
                        metadata={
                            "category": "catalog",
                            "dataflow": obs.dataflow,
                            "dataflow_name": dataflow.name,
                            "dataflow_description": dataflow.description,
                            "agency_id": obs.agency_id or dataflow.agency_id,
                            "series_key": obs.series_key,
                            "raw_series_key": obs.raw_series_key,
                            "dimensions": obs.dimensions,
                        },
                        obs_family_id=fam_id,
                    )
                )
            return records

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id: dict[concurrent.futures.Future[list[IndicatorObservationRecord]], str] = {}
            for dataflow in dataflows:
                future = executor.submit(_fetch_dataflow, dataflow)
                future_to_id[future] = f"{dataflow.agency_id}/{dataflow.id}"

            for future in concurrent.futures.as_completed(future_to_id):
                label = future_to_id[future]
                try:
                    results.append(future.result())
                except Exception:
                    logger.warning("OECD catalog parallel refresh failed for %s", label, exc_info=True)

        count = 0
        for records in results:
            for record in records:
                store.upsert_indicator_observation(record)
                count += 1
        return RefreshStats(source="oecd_catalog", count=count)


