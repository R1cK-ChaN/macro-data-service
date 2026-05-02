from __future__ import annotations

from pathlib import Path

from ingestion import IngestionOrchestrator
from storage import SQLiteEngineStore

from .service import LocalMacroDataService


def build_local_macro_data_service(db_path: Path | None = None) -> LocalMacroDataService:
    store = SQLiteEngineStore(db_path=db_path)
    return LocalMacroDataService(
        store=store,
        ingestion=IngestionOrchestrator(store),
    )
