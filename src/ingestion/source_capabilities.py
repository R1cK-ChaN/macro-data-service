from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import time
from typing import Any, Callable

from storage.sqlite import SQLiteEngineStore


@dataclass(frozen=True)
class CapabilitySyncResult:
    entities_total: int = 0
    entities_synced: int = 0
    observations_synced: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceCapabilityAdapter:
    source_id: str
    display_name: str
    source_type: str
    entity_type: str
    description: str
    notes: str = ""
    supports_discovery: bool = True
    supports_structure: bool = False
    supports_latest_sync: bool = True
    supports_backfill: bool = False
    is_default_scheduled: bool = False
    discover: Callable[[str | None, int | None], list[dict[str, Any]]] | None = None
    get_structure: Callable[[str], dict[str, Any] | None] | None = None
    sync_latest: Callable[[list[str] | None, int | None], CapabilitySyncResult] | None = None

    def capability_payload(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "display_name": self.display_name,
            "source_type": self.source_type,
            "entity_type": self.entity_type,
            "supports_discovery": self.supports_discovery,
            "supports_structure": self.supports_structure,
            "supports_latest_sync": self.supports_latest_sync,
            "supports_backfill": self.supports_backfill,
            "is_default_scheduled": self.is_default_scheduled,
            "description": self.description,
            "notes": self.notes,
        }


def _entity(
    source_id: str,
    entity_id: str,
    entity_type: str,
    display_name: str,
    *,
    description: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "entity_id": entity_id,
        "entity_type": entity_type,
        "display_name": display_name,
        "description": description,
        "metadata": metadata or {},
        "is_active": True,
    }


def _limit_items(items: list[dict[str, Any]], query: str | None, limit: int | None) -> list[dict[str, Any]]:
    if query:
        needle = query.lower().strip()
        items = [
            item for item in items
            if needle in item["entity_id"].lower()
            or needle in item["display_name"].lower()
            or needle in item.get("description", "").lower()
        ]
    items.sort(key=lambda item: (item["display_name"], item["entity_id"]))
    return items[:limit] if limit is not None else items


class SourceCapabilityManager:
    def __init__(
        self,
        store: SQLiteEngineStore,
        *,
        orchestrator: Any | None = None,
        adapters: dict[str, SourceCapabilityAdapter] | None = None,
    ) -> None:
        self._store = store
        self._orchestrator = orchestrator
        self._adapters = adapters or self._build_default_adapters(orchestrator)
        self.seed_registry()

    def seed_registry(self) -> None:
        for adapter in self._adapters.values():
            self._store.upsert_source_capability(adapter.capability_payload())

    def list_capabilities(self) -> list[dict[str, Any]]:
        self.seed_registry()
        return self._store.list_source_capabilities()

    def sync_discovery(
        self,
        source_id: str,
        *,
        query: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        adapter = self._require_adapter(source_id)
        if not adapter.supports_discovery or adapter.discover is None:
            return {"error": f"discovery unavailable for {source_id}"}
        started_at = datetime.now(UTC).isoformat()
        started = time.perf_counter()
        run_id = self._store.insert_catalog_sync_run({
            "source_id": source_id,
            "job_type": "discovery",
            "status": "running",
            "started_at": started_at,
        })
        try:
            entities = adapter.discover(query, limit)
            for entity in entities:
                self._store.upsert_catalog_entity(entity)
            finished_at = datetime.now(UTC).isoformat()
            duration_ms = int((time.perf_counter() - started) * 1000)
            self._store.upsert_catalog_sync_checkpoint({
                "source_id": source_id,
                "job_type": "discovery",
                "cursor": "",
                "entities_total": len(entities),
                "entities_synced": len(entities),
                "observations_synced": 0,
                "last_success_at": finished_at,
                "last_error": "",
                "metadata": {"query": query or "", "limit": limit},
            })
            self._store.update_catalog_sync_run(run_id, {
                "status": "success",
                "entities_total": len(entities),
                "entities_synced": len(entities),
                "observations_synced": 0,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "metadata": {"query": query or "", "limit": limit},
            })
            return {
                "source_id": source_id,
                "job_type": "discovery",
                "status": "success",
                "entities_total": len(entities),
                "entities_synced": len(entities),
                "duration_ms": duration_ms,
            }
        except Exception as exc:
            finished_at = datetime.now(UTC).isoformat()
            duration_ms = int((time.perf_counter() - started) * 1000)
            self._store.upsert_catalog_sync_checkpoint({
                "source_id": source_id,
                "job_type": "discovery",
                "cursor": "",
                "entities_total": 0,
                "entities_synced": 0,
                "observations_synced": 0,
                "last_success_at": "",
                "last_error": str(exc),
                "metadata": {"query": query or "", "limit": limit},
            })
            self._store.update_catalog_sync_run(run_id, {
                "status": "error",
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "error": str(exc),
                "metadata": {"query": query or "", "limit": limit},
            })
            return {
                "source_id": source_id,
                "job_type": "discovery",
                "status": "error",
                "error": str(exc),
                "duration_ms": duration_ms,
            }

    def list_entities(
        self,
        source_id: str,
        *,
        query: str | None = None,
        limit: int = 100,
        refresh: bool = False,
    ) -> dict[str, Any]:
        adapter = self._require_adapter(source_id)
        if refresh or (adapter.supports_discovery and self._store.count_catalog_entities(source_id) == 0):
            self.sync_discovery(source_id, query=query, limit=limit)
        items = self._store.list_catalog_entities(source_id, query=query, limit=limit)
        return {
            "source_id": source_id,
            "total": len(items),
            "entities": items,
        }

    def get_structure(self, source_id: str, entity_id: str) -> dict[str, Any]:
        adapter = self._require_adapter(source_id)
        if not adapter.supports_structure or adapter.get_structure is None:
            return {"error": f"structure unavailable for {source_id}", "structure": None}
        structure = adapter.get_structure(entity_id)
        if structure is None:
            return {"error": f"no structure for {entity_id}", "structure": None}
        return {"source_id": source_id, "entity_id": entity_id, "structure": structure}

    def sync_latest(
        self,
        source_id: str,
        *,
        entity_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        adapter = self._require_adapter(source_id)
        if not adapter.supports_latest_sync or adapter.sync_latest is None:
            return {"error": f"latest sync unavailable for {source_id}"}
        started_at = datetime.now(UTC).isoformat()
        started = time.perf_counter()
        run_id = self._store.insert_catalog_sync_run({
            "source_id": source_id,
            "job_type": "latest_sync",
            "status": "running",
            "started_at": started_at,
            "metadata": {"entity_ids": entity_ids or [], "limit": limit},
        })
        try:
            result = adapter.sync_latest(entity_ids, limit)
            finished_at = datetime.now(UTC).isoformat()
            duration_ms = int((time.perf_counter() - started) * 1000)
            self._store.upsert_catalog_sync_checkpoint({
                "source_id": source_id,
                "job_type": "latest_sync",
                "cursor": "",
                "entities_total": result.entities_total,
                "entities_synced": result.entities_synced,
                "observations_synced": result.observations_synced,
                "last_success_at": finished_at,
                "last_error": "",
                "metadata": result.metadata,
            })
            self._store.update_catalog_sync_run(run_id, {
                "status": "success",
                "entities_total": result.entities_total,
                "entities_synced": result.entities_synced,
                "observations_synced": result.observations_synced,
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "metadata": result.metadata,
            })
            return {
                "source_id": source_id,
                "job_type": "latest_sync",
                "status": "success",
                "entities_total": result.entities_total,
                "entities_synced": result.entities_synced,
                "observations_synced": result.observations_synced,
                "duration_ms": duration_ms,
                "metadata": result.metadata,
            }
        except Exception as exc:
            finished_at = datetime.now(UTC).isoformat()
            duration_ms = int((time.perf_counter() - started) * 1000)
            self._store.upsert_catalog_sync_checkpoint({
                "source_id": source_id,
                "job_type": "latest_sync",
                "cursor": "",
                "entities_total": 0,
                "entities_synced": 0,
                "observations_synced": 0,
                "last_success_at": "",
                "last_error": str(exc),
                "metadata": {"entity_ids": entity_ids or [], "limit": limit},
            })
            self._store.update_catalog_sync_run(run_id, {
                "status": "error",
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "error": str(exc),
                "metadata": {"entity_ids": entity_ids or [], "limit": limit},
            })
            return {
                "source_id": source_id,
                "job_type": "latest_sync",
                "status": "error",
                "error": str(exc),
                "duration_ms": duration_ms,
            }

    def get_status(self, source_id: str | None = None) -> dict[str, Any]:
        capabilities = (
            [self._store.get_source_capability(source_id)] if source_id else self._store.list_source_capabilities()
        )
        items: list[dict[str, Any]] = []
        for capability in capabilities:
            if capability is None:
                continue
            sid = capability["source_id"]
            items.append({
                "capability": capability,
                "entity_count": self._store.count_catalog_entities(sid),
                "checkpoints": self._store.list_catalog_sync_checkpoints(source_id=sid),
                "recent_runs": self._store.list_catalog_sync_runs(source_id=sid, limit=5),
            })
        return {"total": len(items), "sources": items}

    def _require_adapter(self, source_id: str) -> SourceCapabilityAdapter:
        adapter = self._adapters.get(source_id)
        if adapter is None:
            raise KeyError(f"unknown capability source: {source_id}")
        return adapter

    def _build_default_adapters(self, orchestrator: Any | None) -> dict[str, SourceCapabilityAdapter]:
        from ingestion.clients._ilo_unsd import ILOIngestionClient, UNSDIngestionClient
        from ingestion.fetchers._eia import EIAFetcher
        from ingestion.scrapers.bea import BEAClient
        from ingestion.scrapers.census import CensusClient
        from ingestion.scrapers.gov_report import _CN_SOURCES, _EU_SOURCES, _JP_SOURCES, _US_SOURCES
        from ingestion.series_config import (
            BEA_DATASETS,
            BLS_SERIES,
            CENSUS_DATASETS,
            EIA_SERIES,
            FED_FEEDS,
            ILO_SERIES,
            IMF_VINTAGE_SERIES,
            MACRO_SERIES,
            MACRO_WATCHLIST,
            TREASURY_DATASETS,
            UNSD_SERIES,
            VINTAGE_SERIES,
            ECB_SERIES,
            EUROSTAT_SERIES,
        )
        from ingestion.news_feeds import get_feeds
        from ingestion.scrapers.eia import EIAClient
        from ingestion.scrapers.treasury_fiscal import TreasuryFiscalClient
        from ingestion.clients._trends import _REDDIT_TREND_SOURCES

        adapters: dict[str, SourceCapabilityAdapter] = {}

        def _run_job(job_name: str) -> CapabilitySyncResult:
            if orchestrator is None:
                raise RuntimeError(f"orchestrator unavailable for {job_name}")
            report = orchestrator.run_source(job_name)
            if report.error:
                raise RuntimeError(report.error)
            return CapabilitySyncResult(
                entities_total=1,
                entities_synced=1,
                observations_synced=report.stored,
                metadata={"job_name": job_name},
            )

        def _configured_entities(source_id: str, entity_type: str, mapping: dict[str, Any]) -> Callable[[str | None, int | None], list[dict[str, Any]]]:
            def _inner(query: str | None, limit: int | None) -> list[dict[str, Any]]:
                entities: list[dict[str, Any]] = []
                for key, cfg in mapping.items():
                    series_id = cfg.get("series_id") or cfg.get("variable") or cfg.get("table") or cfg.get("route") or key
                    display_name = cfg.get("name") or cfg.get("dataset") or cfg.get("route") or series_id
                    description = cfg.get("category", "")
                    entities.append(_entity(
                        source_id,
                        str(series_id),
                        entity_type,
                        str(display_name),
                        description=str(description),
                        metadata=dict(cfg),
                    ))
                return _limit_items(entities, query, limit)
            return _inner

        def _news_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity(
                    "news",
                    feed.name,
                    "feed",
                    feed.name,
                    description=feed.url,
                    metadata={"url": feed.url, "category": feed.category},
                )
                for feed in get_feeds()
            ]
            return _limit_items(entities, query, limit)

        def _news_sync_latest(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for news sync")
            all_entries = orchestrator.news.fetch_entries(category=None)
            candidate_entries = all_entries
            if entity_ids:
                allowed = set(entity_ids)
                candidate_entries = [entry for entry in all_entries if entry.source_feed in allowed]
            if limit is not None and not entity_ids:
                candidate_entries = candidate_entries[: max(limit, 1)]

            def _store(entries: list[Any]) -> int:
                normalized = orchestrator.news.normalize_entries(entries)
                valid = orchestrator.news.validate_entries(normalized)
                deduplicated = orchestrator.news.deduplicate_entries(orchestrator.store, valid)
                return orchestrator.news.store_articles(orchestrator.store, deduplicated)

            stored = _store(candidate_entries)
            fallback_used = False
            if stored == 0 and entity_ids:
                stored = _store(all_entries)
                fallback_used = stored > 0
            return CapabilitySyncResult(
                entities_total=len(entity_ids) if entity_ids else len({entry.source_feed for entry in candidate_entries}),
                entities_synced=len(entity_ids) if entity_ids else len({entry.source_feed for entry in candidate_entries}),
                observations_synced=stored,
                metadata={"fallback_used": fallback_used},
            )

        def _market_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities: list[dict[str, Any]] = []
            for asset_class, symbols in MACRO_WATCHLIST.items():
                for symbol, name in symbols.items():
                    entities.append(_entity(
                        "market",
                        symbol,
                        "symbol",
                        name,
                        description=asset_class,
                        metadata={"asset_class": asset_class},
                    ))
            return _limit_items(entities, query, limit)

        def _fed_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity("fed", key, "feed", key.replace("_", " ").title(), description=cfg["url"], metadata=cfg)
                for key, cfg in FED_FEEDS.items()
            ]
            return _limit_items(entities, query, limit)

        def _calendar_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity("calendar", "investing", "provider", "Investing.com"),
                _entity("calendar", "forexfactory", "provider", "ForexFactory"),
                _entity("calendar", "tradingeconomics", "provider", "TradingEconomics"),
            ]
            return _limit_items(entities, query, limit)

        def _simple_entities(source_id: str, entity_type: str, items: list[tuple[str, str, dict[str, Any]]]) -> Callable[[str | None, int | None], list[dict[str, Any]]]:
            def _inner(query: str | None, limit: int | None) -> list[dict[str, Any]]:
                entities = [
                    _entity(source_id, entity_id, entity_type, display_name, metadata=metadata)
                    for entity_id, display_name, metadata in items
                ]
                return _limit_items(entities, query, limit)
            return _inner

        def _reddit_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity(
                    "reddit_trends",
                    cfg.subreddit,
                    "subreddit",
                    cfg.subreddit,
                    description=cfg.category,
                    metadata={"category": cfg.category, "region": cfg.region},
                )
                for cfg in _REDDIT_TREND_SOURCES
            ]
            return _limit_items(entities, query, limit)

        def _gov_report_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities: list[dict[str, Any]] = []
            for region, sources in {"us": _US_SOURCES, "cn": _CN_SOURCES, "jp": _JP_SOURCES, "eu": _EU_SOURCES}.items():
                for source_key, cfg in sources.items():
                    entities.append(_entity(
                        "gov_reports",
                        source_key,
                        "document_source",
                        cfg.get("institution", source_key),
                        description=cfg.get("data_category", region),
                        metadata={"region": region, **cfg},
                    ))
            return _limit_items(entities, query, limit)

        def _gov_report_sync_latest(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for gov report sync")
            items = orchestrator.gov_report.fetch_items()
            if entity_ids:
                allowed = set(entity_ids)
                items = [item for item in items if item.source_id in allowed]
            if limit is not None:
                items = items[: max(limit, 1)]
            valid = orchestrator._validate_gov_report_items(items)
            deduplicated = orchestrator._deduplicate_gov_report_items(valid)
            stored = orchestrator.gov_report.store_items(orchestrator.store, deduplicated)
            return CapabilitySyncResult(
                entities_total=len(entity_ids) if entity_ids else len({item.source_id for item in items}),
                entities_synced=len(entity_ids) if entity_ids else len({item.source_id for item in items}),
                observations_synced=stored,
            )

        def _sdmx_entities(source_id: str, client: Any, query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity(
                    source_id,
                    dataflow.id,
                    "dataflow",
                    dataflow.name or dataflow.id,
                    description=dataflow.description,
                    metadata={
                        "agency_id": dataflow.agency_id,
                        "version": dataflow.version,
                        "structure_id": dataflow.structure_id,
                        "structure_version": dataflow.structure_version,
                    },
                )
                for dataflow in client.list_dataflows()
            ]
            return _limit_items(entities, query, limit)

        def _sdmx_structure(client: Any, entity_id: str) -> dict[str, Any]:
            summary = client.summarize_structure(entity_id)
            if hasattr(summary, "__dict__"):
                return dict(summary.__dict__)
            return dict(summary)

        def _configured_structure(entity_id: str, mapping: dict[str, dict[str, Any]]) -> dict[str, Any]:
            for key, cfg in mapping.items():
                if cfg.get("series_id") == entity_id or cfg.get("dataset") == entity_id or cfg.get("dataflow") == entity_id:
                    return {"config_key": key, **cfg}
            raise KeyError(f"unknown configured entity: {entity_id}")

        def _sdmx_catalog_refresh(source_id: str, refresh_fn: Callable[..., Any]) -> Callable[[list[str] | None, int | None], CapabilitySyncResult]:
            def _inner(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
                kwargs = {}
                if entity_ids:
                    kwargs["dataflow_ids"] = entity_ids
                if limit is not None:
                    kwargs["dataflow_limit"] = limit
                stats = refresh_fn(self._store, **kwargs)
                entity_count = len(entity_ids) if entity_ids else (limit or self._store.count_catalog_entities(source_id))
                return CapabilitySyncResult(
                    entities_total=entity_count,
                    entities_synced=entity_count,
                    observations_synced=stats.count,
                )
            return _inner

        def _worldbank_discovery(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for worldbank discovery")
            indicators = orchestrator.worldbank.list_catalog_indicators(query=query, limit=limit)
            entities = [
                _entity(
                    "worldbank",
                    ind.id,
                    "indicator",
                    ind.name or ind.id,
                    description=ind.source_name,
                    metadata={
                        "source_id": ind.source_id,
                        "source_name": ind.source_name,
                        "topics": [topic.name for topic in ind.topics],
                    },
                )
                for ind in indicators
            ]
            return _limit_items(entities, None, limit)

        def _worldbank_sync_latest(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
            from storage import IndicatorObservationRecord

            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for worldbank sync")
            if entity_ids:
                count = 0
                entity_count = 0
                for indicator_id in entity_ids:
                    observations = orchestrator.worldbank.client.get_indicator(
                        indicator_id,
                        "all",
                        series_id=f"WB_{indicator_id}",
                        limit=300,
                        per_page=1000,
                        fetch_all_pages=True,
                    )
                    for obs in observations:
                        series_id = f"WB_{indicator_id}_{obs.country_code}" if obs.country_code else f"WB_{indicator_id}"
                        self._store.upsert_indicator_observation(
                            IndicatorObservationRecord(
                                series_id=series_id,
                                source="worldbank",
                                date=obs.date,
                                value=obs.value,
                                metadata={
                                    "category": "catalog",
                                    "indicator": indicator_id,
                                    "country_code": obs.country_code,
                                    "country_name": obs.country_name,
                                },
                            )
                        )
                        count += 1
                    entity_count += 1
                return CapabilitySyncResult(
                    entities_total=entity_count,
                    entities_synced=entity_count,
                    observations_synced=count,
                )
            else:
                stats = orchestrator.worldbank.refresh_catalog_parallel(
                    self._store,
                    indicator_limit=limit,
                )
                entity_count = limit or self._store.count_catalog_entities("worldbank")
            return CapabilitySyncResult(
                entities_total=entity_count,
                entities_synced=entity_count,
                observations_synced=stats.count,
            )

        def _oecd_structure(entity_id: str) -> dict[str, Any]:
            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for oecd structure")
            try:
                summary = orchestrator.oecd.get_structure_summary(entity_id)
                return dict(summary.__dict__) if hasattr(summary, "__dict__") else dict(summary)
            except Exception:
                return {"dataflow_id": entity_id, "structure_unavailable": True}

        def _bea_discovery(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            client = BEAClient()
            entities = [
                _entity("bea", dataset.dataset_name, "dataset", dataset.dataset_name, description=dataset.description)
                for dataset in client.list_datasets()
            ]
            return _limit_items(entities, query, limit)

        def _bea_structure(entity_id: str) -> dict[str, Any]:
            client = BEAClient()
            params = client.get_parameters(entity_id)
            return {
                "dataset": entity_id,
                "parameter_count": len(params),
                "parameters": [param.__dict__ for param in params[:100]],
            }

        def _census_discovery(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            client = CensusClient()
            entities = [
                _entity(
                    "census",
                    f"{dataset.dataset_path}:{dataset.vintage or 0}",
                    "dataset",
                    dataset.title or dataset.dataset_path,
                    description=dataset.description,
                    metadata={
                        "dataset_path": dataset.dataset_path,
                        "vintage": dataset.vintage,
                        "api_url": dataset.api_url,
                    },
                )
                for dataset in client.list_datasets()
            ]
            return _limit_items(entities, query, limit)

        def _census_structure(entity_id: str) -> dict[str, Any]:
            dataset_path, _, vintage_raw = entity_id.partition(":")
            vintage = int(vintage_raw or "0")
            client = CensusClient()
            variables = client.get_variables(dataset_path, vintage)
            geographies = client.get_geographies(dataset_path, vintage)
            return {
                "dataset_path": dataset_path,
                "vintage": vintage,
                "variable_count": len(variables),
                "geography_count": len(geographies),
                "sample_variables": [var.__dict__ for var in variables[:50]],
                "geographies": [geo.__dict__ for geo in geographies[:50]],
            }

        def _census_sync_latest(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
            from storage import IndicatorObservationRecord

            client = CensusClient()
            count = 0
            entities_synced = 0
            items = list(CENSUS_DATASETS.items())
            if entity_ids:
                allowed = set(entity_ids)
                items = [
                    (key, cfg)
                    for key, cfg in items
                    if cfg["variable"] in allowed
                    or key in allowed
                    or f"{cfg['dataset']}:{cfg['vintage']}" in allowed
                ]
            elif limit is not None:
                items = items[:limit]
            for key, cfg in items:
                observations = client.get_data(
                    cfg["dataset"],
                    cfg["vintage"],
                    [cfg["variable"]],
                    geo_for=cfg["geo_for"],
                )
                for obs in observations:
                    if obs.value is None:
                        continue
                    self._store.upsert_indicator_observation(
                        IndicatorObservationRecord(
                            series_id=f"CENSUS_{cfg['variable']}_{obs.geo_id}",
                            source="census",
                            date=f"{cfg['vintage']}-01-01",
                            value=float(obs.value),
                            metadata={
                                "dataset": cfg["dataset"],
                                "vintage": cfg["vintage"],
                                "variable": cfg["variable"],
                                "geo_name": obs.geo_name,
                                "geo_id": obs.geo_id,
                                "category": cfg["category"],
                            },
                        )
                    )
                    count += 1
                entities_synced += 1
            return CapabilitySyncResult(
                entities_total=len(items),
                entities_synced=entities_synced,
                observations_synced=count,
            )

        def _eia_route_discovery(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities: list[dict[str, Any]] = []
            for key, cfg in EIA_SERIES.items():
                entities.append(_entity(
                    "eia",
                    cfg["route"],
                    "route",
                    cfg["series_id"],
                    description=cfg.get("category", ""),
                    metadata=dict(cfg) | {"config_key": key},
                ))
            return _limit_items(entities, query, limit)

        def _eia_structure(entity_id: str) -> dict[str, Any]:
            client = EIAClient()
            matching = [
                {"config_key": key, **cfg}
                for key, cfg in EIA_SERIES.items()
                if cfg["route"] == entity_id
            ]
            try:
                metadata = client.get_metadata(entity_id)
            except Exception:
                metadata = {}
            try:
                facets = client.get_facets(entity_id)
            except Exception:
                facets = []
            return {
                "route": entity_id,
                "facet_count": len(facets),
                "facets": [facet.__dict__ for facet in facets],
                "metadata": metadata,
                "configured_series": matching,
            }

        def _eia_sync_latest(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
            if orchestrator is None:
                raise RuntimeError("orchestrator unavailable for eia sync")
            candidate_items: list[tuple[str, dict[str, Any]]] = []
            for key, cfg in EIA_SERIES.items():
                route = cfg["route"]
                series_id = cfg["series_id"]
                if entity_ids:
                    if route not in entity_ids and series_id not in entity_ids and not any(route.startswith(item) for item in entity_ids):
                        continue
                candidate_items.append((key, cfg))
            if not candidate_items:
                candidate_items = list(EIA_SERIES.items())
            if limit is not None and not entity_ids:
                candidate_items = candidate_items[: max(limit, 1)]

            stored = 0
            attempted: list[str] = []
            fallback_used = False
            for idx, (key, cfg) in enumerate(candidate_items):
                fetcher = EIAFetcher(client=orchestrator.eia.client, series_config={key: cfg})
                raw = fetcher.fetch()
                if not raw:
                    attempted.append(cfg["series_id"])
                    continue
                records = orchestrator._raw_series_to_records(raw)
                deduped = orchestrator._deduplicate_observations(records)
                stored = orchestrator._store_indicator_observations(deduped)
                attempted.append(cfg["series_id"])
                if stored > 0:
                    fallback_used = idx > 0
                    break
            if stored == 0:
                raise RuntimeError(f"EIA latest sync stored 0 observations after trying {attempted}")
            return CapabilitySyncResult(
                entities_total=1,
                entities_synced=1,
                observations_synced=stored,
                metadata={"attempted_series": attempted, "fallback_used": fallback_used},
            )

        def _treasury_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            entities = [
                _entity(
                    "treasury_fiscal",
                    cfg["series_id"],
                    "dataset",
                    key,
                    description=cfg["category"],
                    metadata=dict(cfg),
                )
                for key, cfg in TREASURY_DATASETS.items()
            ]
            return _limit_items(entities, query, limit)

        def _nyfed_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            items = [
                ("SOFR", "SOFR", {}),
                ("EFFR", "EFFR", {}),
                ("OBFR", "OBFR", {}),
            ]
            return _simple_entities("nyfed_rates", "rate", items)(query, limit)

        def _weibo_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            items = [("weibo_hot_topics", "Weibo Hot Topics", {})]
            return _simple_entities("weibo_trends", "provider", items)(query, limit)

        def _rate_probability_entities(query: str | None, limit: int | None) -> list[dict[str, Any]]:
            items = [("fed_funds_probabilities", "Fed Funds Probabilities", {})]
            return _simple_entities("rate_probability", "curve", items)(query, limit)

        def _sdmx_discovery_adapter(source_id: str, display_name: str, client: Any, refresh_job: str | None = None, refresh_fn: Callable[..., Any] | None = None, structure: bool = True, notes: str = "") -> SourceCapabilityAdapter:
            def _discover(query: str | None, limit: int | None) -> list[dict[str, Any]]:
                return _sdmx_entities(source_id, client, query, limit)

            def _sync(entity_ids: list[str] | None, limit: int | None) -> CapabilitySyncResult:
                if refresh_fn is not None:
                    return _sdmx_catalog_refresh(source_id, refresh_fn)(entity_ids, limit)
                if refresh_job is None:
                    raise RuntimeError(f"latest sync unavailable for {source_id}")
                return _run_job(refresh_job)

            return SourceCapabilityAdapter(
                source_id=source_id,
                display_name=display_name,
                source_type="catalog-crawlable" if refresh_fn is not None else "discovery-rich",
                entity_type="dataflow",
                description=f"{display_name} provider capabilities",
                notes=notes,
                supports_discovery=True,
                supports_structure=structure,
                supports_latest_sync=True,
                is_default_scheduled=refresh_job in {
                    "eurostat", "ecb", "imf", "bis",
                },
                discover=_discover,
                get_structure=(lambda entity_id: _sdmx_structure(client, entity_id)) if structure else None,
                sync_latest=_sync,
            )

        adapters["calendar"] = SourceCapabilityAdapter(
            source_id="calendar",
            display_name="Economic Calendar",
            source_type="fixed-scope-complete",
            entity_type="provider",
            description="Calendar providers with normalized event ingestion",
            is_default_scheduled=True,
            discover=_calendar_entities,
            sync_latest=lambda entity_ids, limit: _run_job("calendar"),
        )
        adapters["fed"] = SourceCapabilityAdapter(
            source_id="fed",
            display_name="Fed Communications",
            source_type="fixed-scope-complete",
            entity_type="feed",
            description="Federal Reserve communication feeds",
            is_default_scheduled=True,
            discover=_fed_entities,
            sync_latest=lambda entity_ids, limit: _run_job("fed"),
        )
        adapters["market"] = SourceCapabilityAdapter(
            source_id="market",
            display_name="Market Watchlist",
            source_type="fixed-scope-complete",
            entity_type="symbol",
            description="Configured macro market watchlist symbols",
            is_default_scheduled=True,
            discover=_market_entities,
            sync_latest=lambda entity_ids, limit: _run_job("market"),
        )
        adapters["news"] = SourceCapabilityAdapter(
            source_id="news",
            display_name="News Feeds",
            source_type="fixed-scope-complete",
            entity_type="feed",
            description="Configured RSS/news feed sources",
            is_default_scheduled=True,
            discover=_news_entities,
            sync_latest=_news_sync_latest,
        )
        adapters["reddit_trends"] = SourceCapabilityAdapter(
            source_id="reddit_trends",
            display_name="Reddit Trends",
            source_type="fixed-scope-complete",
            entity_type="subreddit",
            description="Configured Reddit trend source scopes",
            is_default_scheduled=True,
            discover=_reddit_entities,
            sync_latest=lambda entity_ids, limit: _run_job("reddit_trends"),
        )
        adapters["weibo_trends"] = SourceCapabilityAdapter(
            source_id="weibo_trends",
            display_name="Weibo Trends",
            source_type="fixed-scope-complete",
            entity_type="provider",
            description="Weibo trend provider scope",
            is_default_scheduled=True,
            discover=_weibo_entities,
            sync_latest=lambda entity_ids, limit: _run_job("weibo_trends"),
        )
        adapters["rate_probability"] = SourceCapabilityAdapter(
            source_id="rate_probability",
            display_name="Rate Probability",
            source_type="fixed-scope-complete",
            entity_type="curve",
            description="Fed funds probability curve source",
            is_default_scheduled=True,
            discover=_rate_probability_entities,
            sync_latest=lambda entity_ids, limit: _run_job("rate_probability"),
        )
        adapters["nyfed_rates"] = SourceCapabilityAdapter(
            source_id="nyfed_rates",
            display_name="NYFed Rates",
            source_type="fixed-scope-complete",
            entity_type="rate",
            description="NYFed reference rate set",
            is_default_scheduled=True,
            discover=_nyfed_entities,
            sync_latest=lambda entity_ids, limit: _run_job("nyfed_rates"),
        )
        adapters["gov_reports"] = SourceCapabilityAdapter(
            source_id="gov_reports",
            display_name="Government Reports",
            source_type="fixed-scope-complete",
            entity_type="document_source",
            description="Configured government report sources",
            is_default_scheduled=True,
            discover=_gov_report_entities,
            sync_latest=_gov_report_sync_latest,
        )
        adapters["fred"] = SourceCapabilityAdapter(
            source_id="fred",
            display_name="FRED",
            source_type="discovery-rich",
            entity_type="series",
            description="Configured FRED macro series",
            notes="Latest sync uses curated daily refresh, not full FRED catalog crawl.",
            supports_structure=False,
            is_default_scheduled=True,
            discover=_configured_entities("fred", "series", MACRO_SERIES),
            sync_latest=lambda entity_ids, limit: _run_job("fred_daily"),
        )
        adapters["fred_vintages"] = SourceCapabilityAdapter(
            source_id="fred_vintages",
            display_name="ALFRED Vintages",
            source_type="fixed-scope-complete",
            entity_type="series",
            description="Configured FRED revision-history series",
            notes="Latest sync uses configured vintage series only.",
            is_default_scheduled=True,
            discover=_simple_entities(
                "fred_vintages",
                "series",
                [(series_id, series_id, {}) for series_id in VINTAGE_SERIES],
            ),
            sync_latest=lambda entity_ids, limit: _run_job("fred_vintages"),
        )
        adapters["bls"] = SourceCapabilityAdapter(
            source_id="bls",
            display_name="BLS",
            source_type="discovery-rich",
            entity_type="series",
            description="Configured BLS series with survey-level catalog support upstream.",
            notes="Latest sync uses configured BLS series, not full BLS survey crawl.",
            is_default_scheduled=True,
            discover=_configured_entities("bls", "series", {key: {"series_id": cfg["series_id"], **cfg} for key, cfg in BLS_SERIES.items()}),
            sync_latest=lambda entity_ids, limit: _run_job("bls"),
        )
        adapters["eia"] = SourceCapabilityAdapter(
            source_id="eia",
            display_name="EIA",
            source_type="discovery-rich",
            entity_type="route",
            description="EIA route tree with configured route ingestion.",
            notes="Latest sync uses configured EIA routes.",
            supports_structure=True,
            is_default_scheduled=True,
            discover=_eia_route_discovery,
            get_structure=_eia_structure,
            sync_latest=_eia_sync_latest,
        )
        adapters["treasury_fiscal"] = SourceCapabilityAdapter(
            source_id="treasury_fiscal",
            display_name="Treasury Fiscal Data",
            source_type="fixed-scope-complete",
            entity_type="dataset",
            description="Treasury Fiscal fixed dataset set",
            is_default_scheduled=True,
            discover=_treasury_entities,
            sync_latest=lambda entity_ids, limit: _run_job("treasury_fiscal"),
        )

        if orchestrator is not None:
            adapters["oecd"] = SourceCapabilityAdapter(
                source_id="oecd",
                display_name="OECD",
                source_type="catalog-crawlable",
                entity_type="dataflow",
                description="OECD SDMX catalog discovery and latest sync",
                supports_structure=True,
                is_default_scheduled=True,
                discover=lambda query, limit: [
                    _entity(
                        "oecd",
                        df.id,
                        "dataflow",
                        df.name or df.id,
                        description=df.description,
                        metadata={"agency_id": df.agency_id, "version": df.version},
                    )
                    for df in orchestrator.oecd.list_catalog_dataflows(query=query, limit=limit)
                ],
                get_structure=_oecd_structure,
                sync_latest=_sdmx_catalog_refresh("oecd", orchestrator.oecd.refresh_catalog),
            )
            adapters["worldbank"] = SourceCapabilityAdapter(
                source_id="worldbank",
                display_name="World Bank",
                source_type="catalog-crawlable",
                entity_type="indicator",
                description="World Bank indicator catalog discovery and latest sync",
                notes="Latest sync uses parallel catalog refresh over discovered indicators.",
                supports_structure=False,
                is_default_scheduled=True,
                discover=_worldbank_discovery,
                sync_latest=_worldbank_sync_latest,
            )
            adapters["eurostat"] = SourceCapabilityAdapter(
                source_id="eurostat",
                display_name="Eurostat",
                source_type="discovery-rich",
                entity_type="dataset",
                description="Configured Eurostat datasets with structure introspection and curated latest sync.",
                notes="Provider-wide SDMX catalog discovery is not yet used here; discovery is based on the configured Eurostat datasets.",
                supports_structure=True,
                is_default_scheduled=True,
                discover=_configured_entities(
                    "eurostat",
                    "dataset",
                    {key: {"series_id": cfg["dataset"], "name": cfg["series_id"], **cfg} for key, cfg in EUROSTAT_SERIES.items()},
                ),
                get_structure=lambda entity_id: _configured_structure(entity_id, EUROSTAT_SERIES),
                sync_latest=lambda entity_ids, limit: _run_job("eurostat"),
            )
            adapters["ecb"] = SourceCapabilityAdapter(
                source_id="ecb",
                display_name="ECB",
                source_type="discovery-rich",
                entity_type="dataflow",
                description="Configured ECB dataflows with structure introspection and curated latest sync.",
                notes="Provider-wide ECB catalog discovery is not yet used here; discovery is based on configured ECB dataflows.",
                supports_structure=True,
                is_default_scheduled=True,
                discover=_configured_entities(
                    "ecb",
                    "dataflow",
                    {key: {"series_id": cfg["dataflow"], "name": cfg["series_id"], **cfg} for key, cfg in ECB_SERIES.items()},
                ),
                get_structure=lambda entity_id: _configured_structure(entity_id, ECB_SERIES),
                sync_latest=lambda entity_ids, limit: _run_job("ecb"),
            )
            adapters["imf"] = _sdmx_discovery_adapter(
                "imf",
                "IMF",
                orchestrator.imf.client,
                refresh_job="imf",
                notes="Latest sync currently uses curated IMF series refresh.",
            )
            adapters["imf_vintages"] = SourceCapabilityAdapter(
                source_id="imf_vintages",
                display_name="IMF Vintages",
                source_type="fixed-scope-complete",
                entity_type="series",
                description="Configured IMF vintage-enabled series",
                is_default_scheduled=True,
                discover=_simple_entities(
                    "imf_vintages",
                    "series",
                    [(series_key, series_key, {}) for series_key in IMF_VINTAGE_SERIES],
                ),
                sync_latest=lambda entity_ids, limit: _run_job("imf_vintages"),
            )
            adapters["bis"] = _sdmx_discovery_adapter(
                "bis",
                "BIS",
                orchestrator.bis.client,
                refresh_job="bis",
                notes="Latest sync currently uses curated BIS series refresh.",
            )

        ilo_client = ILOIngestionClient()
        adapters["ilo"] = SourceCapabilityAdapter(
            source_id="ilo",
            display_name="ILO",
            source_type="catalog-crawlable",
            entity_type="dataflow",
            description="ILO SDMX catalog discovery and latest sync",
            supports_structure=True,
            supports_latest_sync=bool(ILO_SERIES),
            discover=lambda query, limit: [
                _entity("ilo", df.id, "dataflow", df.name or df.id, description=df.description, metadata={"agency_id": df.agency_id, "version": df.version})
                for df in ilo_client.list_catalog_dataflows(query=query, limit=limit)
            ],
            get_structure=lambda entity_id: dict(ilo_client.get_structure_summary(entity_id).__dict__),
            sync_latest=_sdmx_catalog_refresh("ilo", ilo_client.refresh_catalog) if ILO_SERIES else None,
        )

        unsd_client = UNSDIngestionClient()
        adapters["unsd"] = SourceCapabilityAdapter(
            source_id="unsd",
            display_name="UNSD",
            source_type="catalog-crawlable",
            entity_type="dataflow",
            description="UNSD SDMX catalog discovery and latest sync",
            supports_structure=True,
            supports_latest_sync=bool(UNSD_SERIES),
            discover=lambda query, limit: [
                _entity("unsd", df.id, "dataflow", df.name or df.id, description=df.description, metadata={"agency_id": df.agency_id, "version": df.version})
                for df in unsd_client.list_catalog_dataflows(query=query, limit=limit)
            ],
            get_structure=lambda entity_id: dict(unsd_client.get_structure_summary(entity_id).__dict__),
            sync_latest=_sdmx_catalog_refresh("unsd", unsd_client.refresh_catalog) if UNSD_SERIES else None,
        )

        adapters["bea"] = SourceCapabilityAdapter(
            source_id="bea",
            display_name="BEA",
            source_type="discovery-rich",
            entity_type="dataset",
            description="BEA dataset discovery with dataset structure summaries",
            notes="Latest sync is not yet wired for arbitrary discovered BEA datasets; discovery and structure are implemented.",
            supports_structure=True,
            supports_latest_sync=False,
            discover=_bea_discovery,
            get_structure=_bea_structure,
            sync_latest=None,
        )
        adapters["census"] = SourceCapabilityAdapter(
            source_id="census",
            display_name="Census",
            source_type="discovery-rich",
            entity_type="dataset",
            description="Configured Census datasets with structure summaries and latest sync",
            notes="Discovery is scoped to configured Census dataset/vintage combinations so latest sync and structure share the same entity space.",
            supports_structure=True,
            supports_latest_sync=True,
            discover=_configured_entities(
                "census",
                "dataset",
                {
                    key: {
                        "series_id": f"{cfg['dataset']}:{cfg['vintage']}",
                        "name": f"{cfg['dataset']}:{cfg['vintage']}",
                        **cfg,
                    }
                    for key, cfg in CENSUS_DATASETS.items()
                },
            ),
            get_structure=_census_structure,
            sync_latest=_census_sync_latest,
        )

        return adapters
