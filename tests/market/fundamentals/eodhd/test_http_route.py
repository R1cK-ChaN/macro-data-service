"""HTTP route tests for ``GET /v1/fundamentals/{ticker}`` (issue #68 S2)."""

from __future__ import annotations

import json
import asyncio
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI
import httpx
import pytest

from ingestion.market.fundamentals.eodhd_fundamentals import (
    build_raw_record,
    parse_payload_records,
    project_fundamentals_company,
    project_fundamentals_financials,
    project_fundamentals_highlights,
    store_fundamentals_raw,
)
from macro_data.server import ApiToken, create_app
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


def _payload() -> dict:
    return {
        "General": {
            "Code":          "AAPL",
            "Name":          "Apple Inc",
            "Type":          "Common Stock",
            "Sector":        "Technology",
            "Industry":      "Consumer Electronics",
            "FiscalYearEnd": "September",
            "Exchange":      "NASDAQ",
            "CurrencyCode":  "USD",
            "CountryISO":    "US",
        },
        "Highlights": {"MarketCapitalization": 3.0e12, "PERatio": 28.5},
        "Financials": {
            "Income_Statement": {
                "currency_symbol": "USD",
                "yearly": {
                    "2024-09-30": {
                        "date":         "2024-09-30",
                        "totalRevenue": 391035000000.0,
                        "netIncome":    93736000000.0,
                    },
                },
                "quarterly": {},
            },
        },
    }


def _seed(store: SQLiteEngineStore) -> None:
    payload = _payload()
    text = json.dumps(payload, sort_keys=True)
    snap = 1_700_000_000_000
    conn = store.get_connection()
    try:
        raw = build_raw_record(
            ticker="AAPL.US", payload_text=text, snapshot_epoch_ms=snap,
        )
        store_fundamentals_raw(conn, [raw])
        company, highlights, financials = parse_payload_records(
            payload, ticker="AAPL.US", snapshot_epoch_ms=snap,
        )
        if company:
            project_fundamentals_company(conn, [company])
        if highlights:
            project_fundamentals_highlights(conn, [highlights])
        if financials:
            project_fundamentals_financials(conn, financials)
        conn.commit()
    finally:
        conn.close()


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    s = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    _seed(s)
    return s


@pytest.fixture()
def live_server(store: SQLiteEngineStore):
    svc = LocalMacroDataService(store=store)
    return create_app(
        service=svc,
        token_config={"valid-token": ApiToken("test-consumer")},
    )


async def _async_get(app: FastAPI, path: str) -> tuple[int, dict]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get(path, headers={"X-API-Key": "valid-token"})
    parsed = response.json() if response.content else {}
    return response.status_code, parsed


def _get(app: FastAPI, path: str) -> tuple[int, dict]:
    return asyncio.run(_async_get(app, path))


def test_get_fundamentals_returns_company_and_financials(live_server) -> None:
    status, body = _get(live_server, "/v1/fundamentals/AAPL.US")
    assert status == 200
    assert body["company"]["name"] == "Apple Inc"
    assert len(body["financials"]) == 1
    assert body["financials"][0]["revenue"] == 391035000000.0


def test_get_fundamentals_filter_by_statement_and_period(live_server) -> None:
    status, body = _get(
        live_server, "/v1/fundamentals/AAPL.US?statement=IS&period=A",
    )
    assert status == 200
    assert all(
        r["statement"] == "IS" and r["period_type"] == "A"
        for r in body["financials"]
    )


def test_get_fundamentals_invalid_statement_400(live_server) -> None:
    status, body = _get(live_server, "/v1/fundamentals/AAPL.US?statement=XX")
    assert status == 400
    assert "error" in body


def test_get_fundamentals_future_as_of_400(live_server) -> None:
    status, body = _get(
        live_server, "/v1/fundamentals/AAPL.US?as_of=9999-01-01T00:00:00Z",
    )
    assert status == 400
    assert "error" in body


def test_get_fundamentals_unknown_ticker_returns_empty_projections(
    live_server,
) -> None:
    status, body = _get(live_server, "/v1/fundamentals/UNKNOWN.X")
    assert status == 200
    assert body["company"] is None
    assert body["highlights"] is None
    assert body["financials"] == []


def test_get_fundamentals_url_decodes_ticker(live_server) -> None:
    encoded = quote("AAPL.US", safe="")
    status, body = _get(live_server, f"/v1/fundamentals/{encoded}")
    assert status == 200
    assert body["ticker"] == "AAPL.US"


def test_get_fundamentals_missing_ticker_400(live_server) -> None:
    status, body = _get(live_server, "/v1/fundamentals/")
    assert status == 400
    assert "error" in body
