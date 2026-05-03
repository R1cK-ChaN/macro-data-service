from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ingestion import IngestionOrchestrator
from storage import SQLiteEngineStore

from .service import LocalMacroDataService

logger = logging.getLogger(__name__)

_REQUIRED_READER_BOOTSTRAP_TABLES: tuple[str, ...] = (
    "concept_map",
    "release_schedule",
    "source_capability",
    "subjects",
    "subject_aliases",
)


class _ReadOnlyHealthBackend:
    def __init__(self, store: SQLiteEngineStore) -> None:
        self._store = store
        self._manager: Any | None = None

    def get_source_health_dashboard(self, *, include_internal: bool = False) -> dict[str, Any]:
        if self._manager is None:
            from ingestion.source_capabilities import SourceCapabilityManager
            self._manager = SourceCapabilityManager(
                self._store,
                adapters={},
                seed_registry=False,
            )
        return self._manager.get_customer_health(include_internal=include_internal)


def _validate_read_only_bootstrap(store: SQLiteEngineStore) -> None:
    empty_tables: list[str] = []
    with store._connection(commit=False) as connection:
        for table in _REQUIRED_READER_BOOTSTRAP_TABLES:
            row = connection.execute(f"SELECT COUNT(*) AS cnt FROM {table}").fetchone()
            count = int(row["cnt"]) if row is not None else 0
            if count == 0:
                empty_tables.append(table)
    if empty_tables:
        tables = ", ".join(empty_tables)
        raise RuntimeError(
            "read-only macro-data DB is missing reader bootstrap rows: "
            f"{tables}. Run the writable bootstrap before starting macro-data-api."
        )


def _build_clickhouse_market_store() -> Any | None:
    """Build a ``ClickHouseMarketStore`` from env vars.

    Returns ``None`` when ``clickhouse_connect`` cannot reach the server
    so the service still boots in environments without CH (CI without
    docker, local-only macro work). Market ops then short-circuit with
    a "market store not configured" error rather than crashing the
    whole process.

    Bilingual storage per issue #118: SQLite owns calendar / indicators
    / documents / news; ClickHouse owns market.
    """
    try:
        from storage.clickhouse import apply_clickhouse_schema
        from storage.clickhouse.store import (
            ClickHouseMarketStore,
            clickhouse_client_from_env,
        )
    except ImportError:
        logger.warning("clickhouse_connect not installed — market lane disabled")
        return None
    try:
        client = clickhouse_client_from_env()
        apply_clickhouse_schema(client)
    except Exception as exc:  # pragma: no cover - exercised by smoke test
        logger.warning("ClickHouse unavailable — market lane disabled: %s", exc)
        return None
    return ClickHouseMarketStore(client)


def build_local_macro_data_service(db_path: Path | None = None) -> LocalMacroDataService:
    store = SQLiteEngineStore(db_path=db_path)
    market_store = _build_clickhouse_market_store()
    return LocalMacroDataService(
        store=store,
        market_store=market_store,
        ingestion=IngestionOrchestrator(store),
    )


def build_read_only_macro_data_service(db_path: Path | None = None) -> LocalMacroDataService:
    store = SQLiteEngineStore(db_path=db_path, read_only=True)
    _validate_read_only_bootstrap(store)
    return LocalMacroDataService(
        store=store,
        health=_ReadOnlyHealthBackend(store),
    )
