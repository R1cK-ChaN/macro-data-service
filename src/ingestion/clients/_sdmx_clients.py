"""SDMX-based ingestion clients: IMF, Eurostat, BIS, ECB."""

from __future__ import annotations

import concurrent.futures
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ingestion.sdmx.providers.bis import BISClient
from ingestion.sdmx.providers.ecb import ECBClient
from ingestion.sdmx.providers.eurostat import EurostatClient
from ingestion.sdmx.providers.imf import IMFClient
from ingestion.series_config import (
    BIS_SERIES,
    ECB_SERIES,
    EUROSTAT_SERIES,
    IMF_SERIES,
    IMF_VINTAGE_SERIES,
)
from storage import IndicatorObservationRecord, IndicatorVintageRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class IMFIngestionClient:
    def __init__(self) -> None:
        self.client = IMFClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in IMF_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    version=cfg["version"],
                    limit=30,
                )
                fam_id = family_lookup.get(("imf", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="imf",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("IMF refresh failed for %s", key, exc_info=True)
            time.sleep(1.0)
        return RefreshStats(source="imf", count=count)

    def refresh_vintages(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        failures: list[str] = []
        now = datetime.now(UTC)
        as_of_dates = [
            (now - timedelta(days=30 * i)).strftime("%Y-%m-%d")
            for i in range(12)
        ]
        for series_key in IMF_VINTAGE_SERIES:
            cfg = IMF_SERIES[series_key]
            try:
                vintages = self.client.get_vintages(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    version=cfg["version"],
                    as_of_dates=as_of_dates,
                    limit=30,
                )
                fam_id = family_lookup.get(("imf", cfg["series_id"])) if family_lookup else None
                for v in vintages:
                    store.upsert_indicator_vintage(
                        IndicatorVintageRecord(
                            series_id=v.series_id,
                            source="imf",
                            observation_date=v.date,
                            vintage_date=v.vintage_date,
                            value=v.value,
                            metadata={"category": cfg["category"], "dataflow": v.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception as exc:
                logger.warning("IMF vintage refresh failed for %s", series_key, exc_info=True)
                failures.append(f"{series_key}: {exc}")
        if failures and count == 0 and IMF_VINTAGE_SERIES:
            raise RuntimeError(
                f"imf vintages failed for all {len(IMF_VINTAGE_SERIES)} series; first error: {failures[0]}"
            )
        if failures:
            logger.warning(
                "imf vintage partial failures: %d/%d; first error: %s",
                len(failures),
                len(IMF_VINTAGE_SERIES),
                failures[0],
            )
        return RefreshStats(source="imf_vintages", count=count)


class EurostatIngestionClient:
    def __init__(self) -> None:
        self.client = EurostatClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in EUROSTAT_SERIES.items():
            try:
                observations = self.client.get_dataset(
                    cfg["dataset"],
                    params=dict(cfg["params"]),
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("eurostat", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eurostat",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataset": obs.dataset},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Eurostat refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="eurostat", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows()
        if query:
            needle = query.lower().strip()
            dataflows = [
                df for df in dataflows
                if needle in df.id.lower()
                or needle in df.name.lower()
                or needle in df.description.lower()
            ]
        dataflows.sort(key=lambda item: item.id)
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            allowed = set(dataflow_ids)
            return [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ][:limit] if limit is not None else [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ]
        return self.list_catalog_dataflows(query=query, limit=limit)

    def get_structure_summary(self, dataflow_id: str) -> Any:
        return self.client.summarize_structure(dataflow_id)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info("Skipping %s (estimated %d series — too large)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataset": dataflow.id,
                "params": {},
                "series_id": f"ESTAT_AUTO_{dataflow.id.upper()}",
                "category": category,
            }
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    ".",
                    series_id=f"ESTAT_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("eurostat", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="eurostat",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataset": obs.dataset,
                                "dataflow_name": dataflow.name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("Eurostat catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="eurostat", count=count)


class BISIngestionClient:
    def __init__(self) -> None:
        self.client = BISClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in BIS_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("bis", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="bis",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("BIS refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="bis", count=count)


class ECBIngestionClient:
    def __init__(self) -> None:
        self.client = ECBClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in ECB_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg["key"],
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("ecb", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ecb",
                            date=obs.date,
                            value=obs.value,
                            metadata={"category": cfg["category"], "dataflow": obs.dataflow},
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ECB refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="ecb", count=count)

    def list_catalog_dataflows(
        self,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        dataflows = self.client.list_dataflows()
        if query:
            needle = query.lower().strip()
            dataflows = [
                df for df in dataflows
                if needle in df.id.lower()
                or needle in df.name.lower()
                or needle in df.description.lower()
            ]
        dataflows.sort(key=lambda item: item.id)
        if limit is not None:
            return dataflows[:limit]
        return dataflows

    def resolve_catalog_dataflows(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        limit: int | None = None,
    ) -> list[Any]:
        if dataflow_ids:
            allowed = set(dataflow_ids)
            matches = [
                df for df in self.list_catalog_dataflows(limit=None)
                if df.id in allowed
            ]
            return matches[:limit] if limit is not None else matches
        return self.list_catalog_dataflows(query=query, limit=limit)

    def get_structure_summary(self, dataflow_id: str) -> Any:
        return self.client.summarize_structure(dataflow_id)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str = "catalog",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info("Skipping ECB %s (estimated %d series)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "key": ".",
                "series_id": f"ECB_AUTO_{dataflow.id.upper()}",
                "category": category,
            }
        return generated

    def refresh_catalog(
        self,
        store: SQLiteEngineStore,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    ".",
                    series_id=f"ECB_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("ecb", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ecb",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ECB catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="ecb", count=count)

