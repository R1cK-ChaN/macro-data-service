from __future__ import annotations

from pathlib import Path

from analyst.ingestion import IngestionOrchestrator
from analyst.storage import SQLiteEngineStore

from .service import LocalMacroDataService


def _build_optional_retriever():
    try:
        from analyst.rag import MacroRetriever

        return MacroRetriever.from_env()
    except Exception:
        return None


def build_local_macro_data_service(db_path: Path | None = None) -> LocalMacroDataService:
    store = SQLiteEngineStore(db_path=db_path)
    return LocalMacroDataService(
        store=store,
        ingestion=IngestionOrchestrator(store),
        retriever=_build_optional_retriever(),
    )
