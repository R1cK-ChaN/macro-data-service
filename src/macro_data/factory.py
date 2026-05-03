from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ingestion import IngestionOrchestrator
from storage import SQLiteEngineStore

from .service import LocalMacroDataService

logger = logging.getLogger(__name__)


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
