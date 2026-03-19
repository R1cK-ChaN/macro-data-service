"""EIA ingestion client."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.normalization import normalize_observation_date
from ingestion.scrapers.fred import FredClient
from ingestion.scrapers.eia import EIAClient
from ingestion.series_config import EIA_SERIES
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class EIAIngestionClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.client = EIAClient(api_key=api_key)
        self._fred = FredClient()

    _FRED_FALLBACK_SERIES: dict[str, str] = {
        "EIA_BRENT": "DCOILBRENTEU",
        "EIA_WTI": "DCOILWTICO",
        "EIA_CRUDE_STOCKS": "WCESTUS1",
        "EIA_NATGAS": "DHHNGSP",
    }

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        return self.refresh_subset(
            store,
            series_configs=EIA_SERIES,
            family_lookup=family_lookup,
        )

    def refresh_subset(
        self,
        store: SQLiteEngineStore,
        *,
        series_configs: dict[str, dict[str, Any]],
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        failures: list[str] = []
        for key, cfg in series_configs.items():
            try:
                count += self._refresh_one(
                    store,
                    cfg,
                    family_lookup=family_lookup,
                )
            except Exception as exc:
                logger.warning("EIA refresh failed for %s", key, exc_info=True)
                failures.append(f"{cfg['series_id']}: {exc}")
            time.sleep(0.5)
        if failures and count == 0:
            raise RuntimeError(f"EIA refresh failed for all configured series; first error: {failures[0]}")
        return RefreshStats(source="eia", count=count)

    def _refresh_one(
        self,
        store: SQLiteEngineStore,
        cfg: dict[str, Any],
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> int:
        series_id = cfg["series_id"]
        fam_id = family_lookup.get(("eia", series_id)) if family_lookup else None
        frequency = str(cfg.get("params", {}).get("frequency", "daily")).lower()
        try:
            observations = self.client.get_series(
                cfg["route"],
                params=dict(cfg["params"]),
                series_id=series_id,
                limit=30,
            )
        except Exception as exc:
            logger.warning("EIA primary fetch failed for %s: %s", series_id, exc)
            observations = []
        if observations:
            return self._store_eia_observations(
                store,
                observations,
                cfg,
                obs_family_id=fam_id,
            )

        cached_count = self._recent_cache_count(
            store,
            series_id=series_id,
            frequency=frequency,
        )
        if cached_count > 0:
            logger.info("EIA cache hit for %s (%d recent observations)", series_id, cached_count)
            return cached_count

        fallback_series = self._FRED_FALLBACK_SERIES.get(series_id)
        if fallback_series:
            fallback_count = self._store_fred_fallback(
                store,
                eia_series_id=series_id,
                fred_series_id=fallback_series,
                cfg=cfg,
                obs_family_id=fam_id,
            )
            if fallback_count > 0:
                logger.warning("EIA fallback used for %s via FRED %s", series_id, fallback_series)
                return fallback_count
        raise RuntimeError(f"no live EIA data, cache, or FRED fallback available for {series_id}")

    def _store_eia_observations(
        self,
        store: SQLiteEngineStore,
        observations: list[Any],
        cfg: dict[str, Any],
        *,
        obs_family_id: str | None = None,
    ) -> int:
        count = 0
        frequency = str(cfg.get("params", {}).get("frequency", "daily")).lower()
        for obs in observations:
            store.upsert_indicator_observation(
                IndicatorObservationRecord(
                    series_id=obs.series_id,
                    source="eia",
                    date=normalize_observation_date(obs.date, frequency),
                    value=obs.value,
                    metadata={"category": cfg["category"], "unit": obs.unit},
                    obs_family_id=obs_family_id,
                )
            )
            count += 1
        return count

    def _recent_cache_count(
        self,
        store: SQLiteEngineStore,
        *,
        series_id: str,
        frequency: str,
    ) -> int:
        ttl_days = {"daily": 7, "weekly": 10, "monthly": 45}.get(frequency, 14)
        history = store.get_indicator_history(series_id, limit=5)
        if not history:
            return 0
        latest = history[0]
        try:
            latest_dt = datetime.fromisoformat(latest.date)
        except ValueError:
            return 0
        age_days = (datetime.now(UTC) - latest_dt.replace(tzinfo=UTC)).days
        return len(history) if age_days <= ttl_days else 0

    def _store_fred_fallback(
        self,
        store: SQLiteEngineStore,
        *,
        eia_series_id: str,
        fred_series_id: str,
        cfg: dict[str, Any],
        obs_family_id: str | None = None,
    ) -> int:
        start_date = (datetime.now(UTC) - timedelta(days=14)).strftime("%Y-%m-%d")
        try:
            observations = self._fred.get_series(fred_series_id, start_date=start_date, limit=5)
        except Exception:
            return 0
        count = 0
        frequency = str(cfg.get("params", {}).get("frequency", "daily")).lower()
        for obs in observations:
            store.upsert_indicator_observation(
                IndicatorObservationRecord(
                    series_id=eia_series_id,
                    source="eia",
                    date=normalize_observation_date(obs.date, frequency),
                    value=obs.value,
                    metadata={
                        "category": cfg["category"],
                        "fallback_source": "fred",
                        "fallback_series_id": fred_series_id,
                    },
                    obs_family_id=obs_family_id,
                )
            )
            count += 1
        return count

