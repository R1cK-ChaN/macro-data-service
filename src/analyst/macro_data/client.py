from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Protocol

import httpx

from analyst.env import get_env_value


class MacroDataClient(Protocol):
    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class MacroDataHttpConfig:
    base_url: str
    api_token: str = ""
    timeout_seconds: float = 20.0

    @classmethod
    def from_env(cls) -> MacroDataHttpConfig | None:
        base_url = get_env_value("ANALYST_MACRO_DATA_BASE_URL", default="").strip()
        if not base_url:
            return None
        timeout_raw = get_env_value("ANALYST_MACRO_DATA_TIMEOUT_SECONDS", default="20").strip()
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError:
            timeout_seconds = 20.0
        return cls(
            base_url=base_url.rstrip("/"),
            api_token=get_env_value("ANALYST_MACRO_DATA_API_TOKEN", default="").strip(),
            timeout_seconds=max(timeout_seconds, 1.0),
        )


class HttpMacroDataClient:
    def __init__(self, config: MacroDataHttpConfig) -> None:
        self._config = config

    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self._config.api_token:
            headers["Authorization"] = f"Bearer {self._config.api_token}"
        response = httpx.post(
            f"{self._config.base_url}/v1/ops/{operation}",
            headers=headers,
            json={"arguments": arguments or {}},
            timeout=self._config.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"macro-data response for {operation} must be an object")
        return payload


class LocalMacroDataClient:
    def __init__(self, service: Any) -> None:
        self._service = service

    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._service.invoke(operation, arguments or {})


def _default_local_client() -> MacroDataClient:
    from analyst.storage import SQLiteEngineStore

    from .service import LocalMacroDataService

    store = SQLiteEngineStore()
    return LocalMacroDataClient(LocalMacroDataService(store=store))


def coerce_macro_data_client(
    *,
    data_client: MacroDataClient | None = None,
    store: Any | None = None,
    ingestion: Any | None = None,
    retriever: Any | None = None,
) -> MacroDataClient:
    if data_client is not None:
        return data_client
    config = MacroDataHttpConfig.from_env()
    if config is not None:
        return HttpMacroDataClient(config)
    if store is not None or ingestion is not None or retriever is not None:
        from analyst.storage import SQLiteEngineStore

        from .service import LocalMacroDataService

        resolved_store = store if store is not None else SQLiteEngineStore()
        return LocalMacroDataClient(
            LocalMacroDataService(
                store=resolved_store,
                ingestion=ingestion,
                retriever=retriever,
            )
        )
    return _default_local_client()


def build_local_macro_data_client(
    *,
    db_path: Path | None = None,
    ingestion: Any | None = None,
    retriever: Any | None = None,
) -> MacroDataClient:
    from analyst.storage import SQLiteEngineStore

    from .service import LocalMacroDataService

    store = SQLiteEngineStore(db_path=db_path)
    return LocalMacroDataClient(
        LocalMacroDataService(store=store, ingestion=ingestion, retriever=retriever)
    )


def encode_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")
