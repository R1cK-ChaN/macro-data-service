"""Public-read allowlist tests for ``POST /v1/ops/<op>``."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import sqlite3
import time
from typing import Any

from fastapi import FastAPI
import httpx
import pytest

from macro_data.server import (
    ApiToken,
    FixedWindowRateLimiter,
    PUBLIC_READ_OPS,
    SQLiteFixedWindowRateLimiter,
    create_app,
    load_api_tokens,
)


class _RecordingService:
    def __init__(self) -> None:
        self.invoked: list[tuple[str, dict[str, Any]]] = []

    def invoke(self, operation: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.invoked.append((operation, arguments))
        return {"ok": True, "operation": operation}


def _app(
    service: _RecordingService,
    *,
    token: str = "valid-token",
    consumer_id: str = "consumer-a",
    rate_limit_per_minute: int = 1000,
) -> FastAPI:
    return create_app(
        service=service,  # type: ignore[arg-type]
        token_config={
            token: ApiToken(
                consumer_id=consumer_id,
                rate_limit_per_minute=rate_limit_per_minute,
            )
        },
        rate_limiter=FixedWindowRateLimiter(),
    )


async def _request(
    app: FastAPI,
    method: str,
    path: str,
    *,
    token: str | None = "valid-token",
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    headers = {"X-API-Key": token} if token is not None else {}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        return await client.request(
            method,
            path,
            json=json_body,
            headers=headers,
        )


def _run_request(
    app: FastAPI,
    method: str,
    path: str,
    *,
    token: str | None = "valid-token",
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    return asyncio.run(
        _request(
            app,
            method,
            path,
            token=token,
            json_body=json_body,
        )
    )


def _post(
    app: FastAPI,
    path: str,
    *,
    token: str | None = "valid-token",
) -> tuple[int, dict[str, Any]]:
    response = _run_request(
        app,
        "POST",
        path,
        token=token,
        json_body={"arguments": {}},
    )
    return response.status_code, response.json()


def test_public_read_ops_matches_issue_104_contract() -> None:
    assert PUBLIC_READ_OPS == frozenset({
        "resolve_indicator",
        "resolve_indicator_history",
        "get_data_manifest",
        "list_items",
        "get_document",
        "get_release_schedule",
        "get_release_status",
    })


@pytest.mark.parametrize("op", sorted(PUBLIC_READ_OPS))
def test_allowed_read_op_with_valid_token_reaches_service(op: str) -> None:
    service = _RecordingService()
    app = _app(service)
    status, payload = _post(app, f"/v1/ops/{op}")
    assert status == 200, f"{op} should be reachable, got {status} {payload}"
    assert payload == {"ok": True, "operation": op}
    assert service.invoked == [(op, {})]


@pytest.mark.parametrize("op", sorted(PUBLIC_READ_OPS))
def test_allowed_read_op_with_invalid_token_returns_401(op: str) -> None:
    service = _RecordingService()
    app = _app(service)
    status, payload = _post(app, f"/v1/ops/{op}", token="wrong")
    assert status == 401
    assert payload == {"error": "unauthorized"}
    assert service.invoked == []


@pytest.mark.parametrize("op", sorted(PUBLIC_READ_OPS))
def test_allowed_read_op_without_token_returns_401(op: str) -> None:
    service = _RecordingService()
    app = _app(service)
    status, payload = _post(app, f"/v1/ops/{op}", token=None)
    assert status == 401
    assert payload == {"error": "unauthorized"}
    assert service.invoked == []


@pytest.mark.parametrize("op", sorted(PUBLIC_READ_OPS))
def test_allowed_read_op_hits_per_token_rate_limit(op: str) -> None:
    service = _RecordingService()
    app = _app(service, rate_limit_per_minute=1)
    first_status, first_payload = _post(app, f"/v1/ops/{op}")
    second_status, second_payload = _post(app, f"/v1/ops/{op}")
    assert first_status == 200
    assert first_payload == {"ok": True, "operation": op}
    assert second_status == 429
    assert second_payload == {"error": "rate limit exceeded"}
    assert service.invoked == [(op, {})]


@pytest.mark.parametrize("op", [
    "refresh_source",
    "refresh_all_sources",
    "run_schedule",
    "refresh_news",
    "refresh_calendar",
    "fundamentals_fetch",
    "sync_catalog_latest",
    "sync_catalog_discovery",
    "validate_concept",
    "validate_all_concepts",
    "calendar_econ_backfill",
    "fetch_country_indicators",
    "fetch_reference_rates",
    "fetch_rate_expectations",
    "fetch_live_news",
    "fetch_live_markets",
    "fetch_article",
    "backfill_document_indexes",
])
def test_blocked_admin_op_returns_403(op: str) -> None:
    service = _RecordingService()
    app = _app(service)
    status, payload = _post(app, f"/v1/ops/{op}")
    assert status == 403, f"{op} must be 403, got {status} {payload}"
    assert payload == {"error": "operation not permitted on public API"}
    assert service.invoked == []


def test_unknown_op_returns_403() -> None:
    service = _RecordingService()
    app = _app(service)
    status, payload = _post(app, "/v1/ops/totally_made_up_op")
    assert status == 403
    assert payload == {"error": "operation not permitted on public API"}
    assert service.invoked == []


def test_healthz_exempts_token_auth() -> None:
    service = _RecordingService()
    app = _app(service)
    response = _run_request(app, "GET", "/healthz", token=None)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_requires_token_before_service_call() -> None:
    service = _RecordingService()
    app = _app(service)
    response = _run_request(app, "GET", "/health", token=None)
    assert response.status_code == 401
    assert response.json() == {"error": "unauthorized"}
    assert service.invoked == []


def test_token_file_loads_consumer_and_rate_overrides(tmp_path: Path) -> None:
    token_path = tmp_path / "api_tokens.json"
    token_path.write_text(
        json.dumps({
            "tokens": {
                "alpha": {
                    "consumer_id": "consumer-alpha",
                    "rate_limit_per_minute": 7,
                },
                "beta": "consumer-beta",
            }
        }),
        encoding="utf-8",
    )

    tokens = load_api_tokens(token_path)

    assert tokens["alpha"] == ApiToken(
        consumer_id="consumer-alpha",
        rate_limit_per_minute=7,
    )
    assert tokens["beta"] == ApiToken(
        consumer_id="consumer-beta",
        rate_limit_per_minute=60,
    )


def test_inline_token_preserves_file_metadata(tmp_path: Path) -> None:
    token_path = tmp_path / "api_tokens.json"
    token_path.write_text(
        json.dumps({
            "tokens": {
                "alpha": {
                    "consumer_id": "consumer-alpha",
                    "rate_limit_per_minute": 7,
                },
            }
        }),
        encoding="utf-8",
    )

    tokens = load_api_tokens(token_path, inline_api_token="alpha")

    assert tokens["alpha"] == ApiToken(
        consumer_id="consumer-alpha",
        rate_limit_per_minute=7,
    )


def test_sqlite_rate_limiter_shares_windows_across_instances(tmp_path: Path) -> None:
    db_path = tmp_path / "rate-limit.sqlite3"
    first = SQLiteFixedWindowRateLimiter(db_path)
    second = SQLiteFixedWindowRateLimiter(db_path)

    assert first.check("token-a", 1) == (True, 0)
    allowed, retry_after = second.check("token-a", 1)

    assert allowed is False
    assert retry_after > 0


def test_sqlite_rate_limiter_persists_epoch_window_start(tmp_path: Path) -> None:
    db_path = tmp_path / "rate-limit.sqlite3"
    limiter = SQLiteFixedWindowRateLimiter(db_path)

    before = time.time()
    assert limiter.check("token-a", 10) == (True, 0)
    after = time.time()

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            "SELECT window_start FROM rate_limit_windows"
        ).fetchone()

    assert row is not None
    assert before <= float(row[0]) <= after


def test_access_log_is_structured_json(caplog: pytest.LogCaptureFixture) -> None:
    service = _RecordingService()
    app = _app(service, consumer_id="consumer-log")
    caplog.set_level(logging.INFO, logger="macro_data.access")

    response = _run_request(
        app,
        "POST",
        "/v1/ops/list_items",
        token="valid-token",
        json_body={"arguments": {}},
    )

    assert response.status_code == 200
    payloads = [
        json.loads(record.message)
        for record in caplog.records
        if record.name == "macro_data.access"
    ]
    assert payloads[-1]["method"] == "POST"
    assert payloads[-1]["path"] == "/v1/ops/list_items"
    assert payloads[-1]["status"] == 200
    assert payloads[-1]["consumer_id"] == "consumer-log"
    assert isinstance(payloads[-1]["latency_ms"], float)
