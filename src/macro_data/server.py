from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, AsyncIterator, Callable

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from env import get_env_value

from .service import LocalMacroDataService

logger = logging.getLogger(__name__)
access_logger = logging.getLogger("macro_data.access")

DEFAULT_API_TOKENS_PATH = Path("/etc/macro-data/api_tokens.json")
DEFAULT_RATE_LIMIT_PER_MINUTE = 60
DEFAULT_RATE_LIMIT_DB_PATH = Path("/tmp/macro-data-api-rate-limits.sqlite3")


# Public-read allowlist for ``POST /v1/ops/<operation>`` (issue #104).
# Anything not in this set is rejected with HTTP 403 by the public server,
# even if the underlying ``LocalMacroDataService`` op exists. Admin / write
# ops (refresh_*, run_schedule, fundamentals_fetch, ...) stay reachable
# through the CLI / SSH / systemd jobs that share the same in-process
# service.
PUBLIC_READ_OPS: frozenset[str] = frozenset({
    "resolve_indicator",
    "resolve_indicator_history",
    "get_data_manifest",
    "list_items",
    "get_document",
    "get_release_schedule",
    "get_release_status",
})


@dataclass(frozen=True)
class ApiToken:
    consumer_id: str
    rate_limit_per_minute: int = DEFAULT_RATE_LIMIT_PER_MINUTE


class FixedWindowRateLimiter:
    def __init__(self, *, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._windows: dict[str, tuple[float, int]] = {}

    def check(self, key: str, limit: int) -> tuple[bool, int]:
        now = self._clock()
        window_seconds = 60.0
        with self._lock:
            start, count = self._windows.get(key, (now, 0))
            if now - start >= window_seconds:
                start, count = now, 0
            if count >= limit:
                retry_after = max(1, int(window_seconds - (now - start)))
                self._windows[key] = (start, count)
                return False, retry_after
            self._windows[key] = (start, count + 1)
            return True, 0


class SQLiteFixedWindowRateLimiter:
    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._path = path
        self._clock = clock or time.time
        self._init_lock = threading.Lock()
        self._initialized = False

    def _ensure_schema(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(self._path, timeout=5.0) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS rate_limit_windows (
                        token_hash TEXT PRIMARY KEY,
                        window_start REAL NOT NULL,
                        count INTEGER NOT NULL
                    )
                    """
                )
            self._initialized = True

    def check(self, key: str, limit: int) -> tuple[bool, int]:
        self._ensure_schema()
        now = self._clock()
        window_seconds = 60.0
        token_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        connection = sqlite3.connect(self._path, timeout=5.0, isolation_level=None)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT window_start, count
                FROM rate_limit_windows
                WHERE token_hash = ?
                """,
                (token_hash,),
            ).fetchone()
            if row is None or now - float(row[0]) >= window_seconds:
                connection.execute(
                    """
                    INSERT INTO rate_limit_windows (
                        token_hash, window_start, count
                    ) VALUES (?, ?, 1)
                    ON CONFLICT(token_hash) DO UPDATE SET
                        window_start = excluded.window_start,
                        count = excluded.count
                    """,
                    (token_hash, now),
                )
                connection.commit()
                return True, 0

            start = float(row[0])
            count = int(row[1])
            if count >= limit:
                connection.commit()
                retry_after = max(1, int(window_seconds - (now - start)))
                return False, retry_after

            connection.execute(
                """
                UPDATE rate_limit_windows
                SET count = count + 1
                WHERE token_hash = ?
                """,
                (token_hash,),
            )
            connection.commit()
            return True, 0
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _token_entry(
    token: str,
    raw: Any,
    *,
    default_rate_limit_per_minute: int,
) -> ApiToken | None:
    if isinstance(raw, str):
        consumer_id = raw.strip()
        if consumer_id:
            return ApiToken(
                consumer_id=consumer_id,
                rate_limit_per_minute=default_rate_limit_per_minute,
            )
        return None
    if isinstance(raw, dict):
        consumer_id = str(
            raw.get("consumer_id")
            or raw.get("consumer")
            or raw.get("id")
            or ""
        ).strip()
        if not consumer_id:
            return None
        rate_limit = _positive_int(
            raw.get("rate_limit_per_minute")
            or raw.get("limit_per_minute")
            or raw.get("rate_limit"),
            default=default_rate_limit_per_minute,
        )
        return ApiToken(
            consumer_id=consumer_id,
            rate_limit_per_minute=rate_limit,
        )
    if token and raw is True:
        return ApiToken(
            consumer_id=token,
            rate_limit_per_minute=default_rate_limit_per_minute,
        )
    return None


def _iter_token_entries(payload: Any) -> list[tuple[str, Any]]:
    if isinstance(payload, dict):
        entries = payload.get("tokens", payload)
        if isinstance(entries, dict):
            return [(str(token), raw) for token, raw in entries.items()]
        if isinstance(entries, list):
            return [
                (str(raw.get("token", "")), raw)
                for raw in entries
                if isinstance(raw, dict)
            ]
    if isinstance(payload, list):
        return [
            (str(raw.get("token", "")), raw)
            for raw in payload
            if isinstance(raw, dict)
        ]
    return []


def load_api_tokens(
    path: Path,
    *,
    inline_api_token: str = "",
    default_rate_limit_per_minute: int = DEFAULT_RATE_LIMIT_PER_MINUTE,
) -> dict[str, ApiToken]:
    tokens: dict[str, ApiToken] = {}
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        for token, raw in _iter_token_entries(payload):
            token = token.strip()
            if not token:
                continue
            entry = _token_entry(
                token,
                raw,
                default_rate_limit_per_minute=default_rate_limit_per_minute,
            )
            if entry is not None:
                tokens[token] = entry
    if inline_api_token and inline_api_token not in tokens:
        tokens[inline_api_token] = ApiToken(
            consumer_id="inline",
            rate_limit_per_minute=default_rate_limit_per_minute,
        )
    return tokens


def _api_tokens_path(configured_path: Path | None = None) -> Path:
    if configured_path is not None:
        return configured_path
    raw = get_env_value(
        "ANALYST_MACRO_DATA_API_TOKENS_PATH",
        "MACRO_DATA_API_TOKENS_PATH",
        default=str(DEFAULT_API_TOKENS_PATH),
    )
    return Path(raw)


def _inline_api_token() -> str:
    return get_env_value("ANALYST_MACRO_DATA_API_TOKEN", default="").strip()


def _default_rate_limit_per_minute() -> int:
    return _positive_int(
        get_env_value("ANALYST_MACRO_DATA_RATE_LIMIT_PER_MINUTE", default="60"),
        default=DEFAULT_RATE_LIMIT_PER_MINUTE,
    )


def _default_rate_limit_db_path() -> Path:
    raw = get_env_value(
        "ANALYST_MACRO_DATA_RATE_LIMIT_DB",
        default=str(DEFAULT_RATE_LIMIT_DB_PATH),
    )
    return Path(raw)


def _build_read_only_service_from_env() -> LocalMacroDataService:
    from macro_data.factory import build_read_only_macro_data_service

    raw_db_path = get_env_value("ANALYST_MACRO_DATA_DB_PATH", default="").strip()
    db_path = Path(raw_db_path) if raw_db_path else None
    return build_read_only_macro_data_service(db_path=db_path)


def _get_service(request: Request) -> LocalMacroDataService:
    service = request.app.state.service
    if service is None:
        service = request.app.state.service_factory()
        request.app.state.service = service
    return service


async def _invoke_service(
    request: Request,
    operation: str,
    arguments: dict[str, Any],
) -> Any:
    service = _get_service(request)
    if request.app.state.invoke_in_thread:
        return await asyncio.to_thread(service.invoke, operation, arguments)
    return service.invoke(operation, arguments)


def _json(payload: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> JSONResponse:
    return JSONResponse(content=payload, status_code=status_code, headers=headers)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    if app.state.service is None:
        app.state.service = app.state.service_factory()
    if app.state.token_config is not None:
        app.state.api_tokens = dict(app.state.token_config)
    else:
        path = _api_tokens_path(app.state.token_path)
        app.state.api_tokens = load_api_tokens(
            path,
            inline_api_token=_inline_api_token(),
            default_rate_limit_per_minute=_default_rate_limit_per_minute(),
        )
        if not app.state.api_tokens:
            logger.warning("macro-data API token config is empty: %s", path)
    yield


def create_app(
    *,
    service: LocalMacroDataService | None = None,
    service_factory: Callable[[], LocalMacroDataService] | None = None,
    token_config: dict[str, ApiToken] | None = None,
    token_path: Path | None = None,
    rate_limiter: FixedWindowRateLimiter | None = None,
    invoke_in_thread: bool | None = None,
) -> FastAPI:
    app = FastAPI(title="Analyst Macro Data API", lifespan=_lifespan)
    app.state.service = service
    app.state.service_factory = service_factory or _build_read_only_service_from_env
    app.state.token_config = token_config
    app.state.token_path = token_path
    app.state.api_tokens = dict(token_config) if token_config is not None else {}
    app.state.rate_limiter = (
        rate_limiter
        or (
            SQLiteFixedWindowRateLimiter(_default_rate_limit_db_path())
            if service is None
            else FixedWindowRateLimiter()
        )
    )
    app.state.invoke_in_thread = (
        service is None
        if invoke_in_thread is None
        else invoke_in_thread
    )

    @app.middleware("http")
    async def _auth_rate_limit_and_log(request: Request, call_next: Any) -> JSONResponse:
        start = time.perf_counter()
        consumer_id: str | None = None
        status_code = 500
        try:
            if request.url.path == "/healthz":
                response = await call_next(request)
                status_code = response.status_code
                return response

            token = request.headers.get("X-API-Key", "").strip()
            token_entry = app.state.api_tokens.get(token) if token else None
            if token_entry is None:
                response = _json({"error": "unauthorized"}, status_code=401)
                status_code = response.status_code
                return response

            consumer_id = token_entry.consumer_id
            request.state.consumer_id = consumer_id
            allowed, retry_after = app.state.rate_limiter.check(
                token,
                token_entry.rate_limit_per_minute,
            )
            if not allowed:
                response = _json(
                    {"error": "rate limit exceeded"},
                    status_code=429,
                    headers={"Retry-After": str(retry_after)},
                )
                status_code = response.status_code
                return response

            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            latency_ms = round((time.perf_counter() - start) * 1000, 3)
            access_logger.info(
                json.dumps(
                    {
                        "method": request.method,
                        "path": request.url.path,
                        "status": status_code,
                        "consumer_id": consumer_id,
                        "latency_ms": latency_ms,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return _json({"status": "ok"})

    @app.get("/health")
    async def health(
        request: Request,
        all_: str = Query(default="0", alias="all"),
    ) -> JSONResponse:
        try:
            payload = await _invoke_service(
                request,
                "get_source_health_dashboard",
                {"include_internal": _is_truthy(all_)},
            )
        except Exception as exc:
            logger.exception("macro-data health request failed")
            return _json({"status": "error", "error": str(exc)}, status_code=500)
        return _json(payload)

    @app.get("/v1/manifest")
    async def manifest(request: Request) -> JSONResponse:
        try:
            payload = await _invoke_service(request, "get_data_manifest", {})
        except Exception as exc:
            logger.exception("GET /v1/manifest failed")
            return _json({"error": str(exc)}, status_code=500)
        return _json(payload)

    @app.get("/v1/calendar")
    async def calendar(
        request: Request,
        domain: str = "",
        country: str = "",
        ticker: str = "",
        subtype: str = "",
        provider: str = "",
        as_of: str = "",
        page_offset: str = Query(default="", alias="page[offset]"),
        page_limit: str = Query(default="", alias="page[limit]"),
    ) -> JSONResponse:
        arguments: dict[str, Any] = {
            "domain": domain,
            "country": country,
            "ticker": ticker,
            "subtype": subtype,
            "provider": provider,
            "as_of": as_of,
            "page_offset": page_offset,
            "page_limit": page_limit,
        }
        try:
            result = await _invoke_service(request, "list_calendar_items", arguments)
        except Exception as exc:
            logger.exception("GET /v1/calendar failed")
            return _json({"error": str(exc)}, status_code=500)
        if isinstance(result, dict) and "error" in result:
            return _json(result, status_code=400)
        return _json(result)

    @app.get("/v1/calendar/revisions")
    async def calendar_revisions(
        request: Request,
        from_ts: str = "",
        to_ts: str = "",
        ticker: str = "",
        subtype: str = "",
        min_versions: str = "",
        event_id: str = "",
        include_versions: str = "",
        page_offset: str = Query(default="", alias="page[offset]"),
        page_limit: str = Query(default="", alias="page[limit]"),
    ) -> JSONResponse:
        arguments: dict[str, Any] = {
            "from_ts": from_ts,
            "to_ts": to_ts,
            "ticker": ticker,
            "subtype": subtype,
            "min_versions": min_versions,
            "event_id": event_id,
            "include_versions": _is_truthy(include_versions),
            "page_offset": page_offset,
            "page_limit": page_limit,
        }
        try:
            result = await _invoke_service(request, "list_corp_revisions", arguments)
        except Exception as exc:
            logger.exception("GET /v1/calendar/revisions failed")
            return _json({"error": str(exc)}, status_code=500)
        if isinstance(result, dict) and "error" in result:
            return _json(result, status_code=400)
        return _json(result)

    @app.get("/v1/fundamentals/{ticker:path}")
    async def fundamentals(
        request: Request,
        ticker: str,
        provider: str = "",
        as_of: str = "",
        statement: str = "",
        period: str = "",
        limit: str = "",
    ) -> JSONResponse:
        if not ticker:
            return _json({"error": "ticker is required"}, status_code=400)
        arguments: dict[str, Any] = {
            "ticker": ticker,
            "provider": provider,
            "as_of": as_of,
            "statement": statement,
            "period": period,
            "limit": limit,
        }
        try:
            result = await _invoke_service(request, "get_fundamentals", arguments)
        except Exception as exc:
            logger.exception("GET /v1/fundamentals/%s failed", ticker)
            return _json({"error": str(exc)}, status_code=500)
        if isinstance(result, dict) and "error" in result:
            return _json(result, status_code=400)
        return _json(result)

    @app.post("/v1/ops/{operation}")
    async def invoke_operation(request: Request, operation: str) -> JSONResponse:
        if operation not in PUBLIC_READ_OPS:
            return _json(
                {"error": "operation not permitted on public API"},
                status_code=403,
            )
        raw_body = await request.body()
        if not raw_body:
            raw_body = b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError:
            return _json({"error": "invalid json"}, status_code=400)
        arguments = payload.get("arguments") if isinstance(payload, dict) else {}
        if not isinstance(arguments, dict):
            return _json({"error": "arguments must be an object"}, status_code=400)
        try:
            result = await _invoke_service(request, operation, arguments)
        except KeyError as exc:
            return _json({"error": str(exc)}, status_code=404)
        except Exception as exc:
            logger.exception("macro-data operation failed: %s", operation)
            return _json({"error": str(exc)}, status_code=500)
        return _json(result)

    @app.api_route(
        "/{path_name:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def not_found(path_name: str) -> JSONResponse:
        return _json({"error": "not found"}, status_code=404)

    return app


app = create_app()


def serve(
    *,
    host: str,
    port: int,
    service: LocalMacroDataService | None = None,
    api_token: str = "",
    workers: int = 1,
) -> None:
    import uvicorn

    if api_token:
        os.environ["ANALYST_MACRO_DATA_API_TOKEN"] = api_token
    if service is not None:
        uvicorn.run(
            create_app(
                service=service,
                token_config=(
                    {api_token: ApiToken("inline")}
                    if api_token
                    else None
                ),
                rate_limiter=SQLiteFixedWindowRateLimiter(
                    _default_rate_limit_db_path()
                ),
                invoke_in_thread=True,
            ),
            host=host,
            port=port,
            workers=1,
        )
        return
    uvicorn.run(
        "macro_data.server:app",
        host=host,
        port=port,
        workers=max(workers, 1),
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve macro-data API")
    parser.add_argument("--host", default=get_env_value("ANALYST_MACRO_DATA_HOST", default="127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(get_env_value("ANALYST_MACRO_DATA_PORT", default="8765") or "8765"))
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--api-token", default=get_env_value("ANALYST_MACRO_DATA_API_TOKEN", default=""))
    parser.add_argument("--api-tokens-path", default=get_env_value("ANALYST_MACRO_DATA_API_TOKENS_PATH", default=""))
    parser.add_argument("--workers", type=int, default=int(get_env_value("ANALYST_MACRO_DATA_WORKERS", "UVICORN_WORKERS", default="2") or "2"))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.db_path:
        os.environ["ANALYST_MACRO_DATA_DB_PATH"] = args.db_path
    if args.api_token:
        os.environ["ANALYST_MACRO_DATA_API_TOKEN"] = args.api_token
    if args.api_tokens_path:
        os.environ["ANALYST_MACRO_DATA_API_TOKENS_PATH"] = args.api_tokens_path
    serve(
        host=args.host,
        port=args.port,
        api_token=args.api_token,
        workers=max(args.workers, 1),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
