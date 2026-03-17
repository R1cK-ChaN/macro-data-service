"""ILO and UNSD SDMX ingestion clients."""

from __future__ import annotations

import logging
import time
from typing import Any

from ingestion.series_config import ILO_SERIES, UNSD_SERIES
from storage import IndicatorObservationRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class ILOIngestionClient:
    def __init__(self) -> None:
        from ingestion.sdmx.providers.ilo import ILOClient
        self.client = ILOClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in ILO_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg.get("key", "."),
                    series_id=cfg["series_id"],
                    limit=30,
                )
                fam_id = family_lookup.get(("ilo", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ilo",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": cfg.get("category", "labour"),
                                "dataflow": obs.dataflow,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ILO refresh failed for %s", key, exc_info=True)
            time.sleep(0.5)
        return RefreshStats(source="ilo", count=count)

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
        category: str = "labour",
    ) -> dict[str, dict[str, Any]]:
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info("Skipping ILO %s (estimated %d series)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "key": ".",
                "series_id": f"ILO_AUTO_{dataflow.id.upper()}",
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
                    series_id=f"ILO_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("ilo", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="ilo",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "labour",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("ILO catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="ilo", count=count)


class UNSDIngestionClient:
    def __init__(self) -> None:
        from ingestion.sdmx.providers.unsd import UNSDClient
        self.client = UNSDClient()

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for key, cfg in UNSD_SERIES.items():
            try:
                observations = self.client.get_data(
                    cfg["dataflow"],
                    cfg.get("key", "all"),
                    series_id=cfg["series_id"],
                    agency_id=cfg.get("agency_id", ""),
                    limit=30,
                )
                fam_id = family_lookup.get(("unsd", cfg["series_id"])) if family_lookup else None
                for obs in observations:
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="unsd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": cfg.get("category", ""),
                                "dataflow": obs.dataflow,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("UNSD refresh failed for %s", key, exc_info=True)
            time.sleep(1.0)
        return RefreshStats(source="unsd", count=count)

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
        dataflows.sort(key=lambda item: (item.agency_id, item.id))
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
                est = self.client.estimate_size(dataflow.id, agency_id=dataflow.agency_id)
                if est.total_series > 10_000_000:
                    logger.info("Skipping UNSD %s (estimated %d series)", dataflow.id, est.total_series)
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.agency_id}_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "agency_id": dataflow.agency_id,
                "key": "all",
                "series_id": f"UNSD_AUTO_{dataflow.id.upper()}",
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
        sleep_seconds: float = 1.5,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> RefreshStats:
        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    "all",
                    series_id=f"UNSD_{dataflow.id.upper()}",
                    agency_id=dataflow.agency_id,
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = family_lookup.get(("unsd", obs.series_id)) if family_lookup else None
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source="unsd",
                            date=obs.date,
                            value=obs.value,
                            metadata={
                                "category": "catalog",
                                "dataflow": obs.dataflow,
                                "dataflow_name": dataflow.name,
                                "agency_id": dataflow.agency_id,
                            },
                            obs_family_id=fam_id,
                        )
                    )
                    count += 1
            except Exception:
                logger.warning("UNSD catalog refresh failed for %s", dataflow.id, exc_info=True)
            time.sleep(sleep_seconds)
        return RefreshStats(source="unsd", count=count)


