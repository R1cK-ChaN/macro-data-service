"""Base SDMX ingestion client for sources.py catalog methods."""

from __future__ import annotations

import logging
import time
from typing import Any

from ._base_client import SDMXClient
from ._types import SDMXStructureSummary

logger = logging.getLogger(__name__)


class SDMXIngestionClient:
    """Base ingestion client providing shared catalog discovery methods.

    Subclasses set ``source_name``, ``series_id_prefix``, and ``default_category``,
    then inherit ``list_catalog_dataflows``, ``resolve_catalog_dataflows``,
    ``get_structure_summary``, ``generate_catalog_series_configs``, and
    ``refresh_catalog`` with no additional code.
    """

    source_name: str = ""
    series_id_prefix: str = ""
    default_category: str = "catalog"

    def __init__(self, client: SDMXClient) -> None:
        self.client = client

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

    def get_structure_summary(self, dataflow_id: str, **kwargs: Any) -> SDMXStructureSummary:
        return self.client.summarize_structure(dataflow_id, **kwargs)

    def generate_catalog_series_configs(
        self,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        series_per_dataflow: int = 3,
        category: str | None = None,
    ) -> dict[str, dict[str, Any]]:
        cat = category or self.default_category
        generated: dict[str, dict[str, Any]] = {}
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                est = self.client.estimate_size(dataflow.id, dataflow.version or "1.0")
                if est.total_series > 10_000_000:
                    logger.info(
                        "Skipping %s %s (estimated %d series — too large)",
                        self.source_name, dataflow.id, est.total_series,
                    )
                    continue
            except Exception:
                continue
            config_key = f"auto_{dataflow.id}"
            generated[config_key] = {
                "dataflow": dataflow.id,
                "key": self.client.config.default_all_key,
                "series_id": f"{self.series_id_prefix}_AUTO_{dataflow.id.upper()}",
                "category": cat,
            }
        return generated

    def refresh_catalog(
        self,
        store: Any,
        *,
        dataflow_ids: list[str] | None = None,
        query: str | None = None,
        dataflow_limit: int | None = 5,
        latest_observations: int = 1,
        sleep_seconds: float = 1.0,
        family_lookup: dict[tuple[str, str], str] | None = None,
    ) -> Any:
        from analyst.storage import IndicatorObservationRecord

        count = 0
        for dataflow in self.resolve_catalog_dataflows(
            dataflow_ids=dataflow_ids, query=query, limit=dataflow_limit,
        ):
            try:
                observations = self.client.get_data(
                    dataflow.id,
                    self.client.config.default_all_key,
                    series_id=f"{self.series_id_prefix}_{dataflow.id.upper()}",
                    limit=latest_observations,
                )
                for obs in observations:
                    fam_id = (
                        family_lookup.get((self.source_name, obs.series_id))
                        if family_lookup else None
                    )
                    store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=obs.series_id,
                            source=self.source_name,
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
                logger.warning(
                    "%s catalog refresh failed for %s",
                    self.source_name, dataflow.id, exc_info=True,
                )
            time.sleep(sleep_seconds)

        # Import RefreshStats lazily to avoid circular imports
        from analyst.ingestion.sources import RefreshStats
        return RefreshStats(source=self.source_name, count=count)
