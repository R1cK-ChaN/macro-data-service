"""Fetcher tests for the EODHD fundamentals package (issue #68 S1)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from ingestion.market.fundamentals.eodhd_fundamentals import (
    EODHDFundamentalsClient,
    FundamentalsFetcher,
)
from storage.sqlite import SQLiteEngineStore


def _payload(ticker: str = "AAPL") -> dict:
    return {
        "General": {
            "Code":          ticker,
            "Name":          f"{ticker} Inc",
            "Type":          "Common Stock",
            "Sector":        "Technology",
            "Industry":      "Consumer Electronics",
            "FiscalYearEnd": "September",
            "Exchange":      "NASDAQ",
            "CurrencyCode":  "USD",
            "CountryISO":    "US",
        },
        "Highlights":  {"MarketCapitalization": 1.0e12, "PERatio": 25.0},
        "Valuation":   {"PERatio": 25.0},
        "SharesStats": {"SharesOutstanding": 1.0e10},
        "Financials":  {
            "Income_Statement": {
                "currency_symbol": "USD",
                "yearly": {
                    "2024-09-30": {
                        "date":         "2024-09-30",
                        "totalRevenue": 100.0e9,
                        "netIncome":    25.0e9,
                    },
                },
                "quarterly": {},
            },
        },
    }


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


@pytest.fixture()
def connection(store: SQLiteEngineStore):
    conn = store.get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def test_fetch_dry_run_no_http(connection, monkeypatch) -> None:
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=10,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(
        tickers=["AAPL.US", "MSFT.US"], dry_run=True
    )
    assert summary.dry_run is True
    assert summary.tickers_planned == 2
    assert summary.tickers_fetched == 0
    assert summary.requests_spent == 0
    assert summary.stopped_reason == "dry_run"
    (count,) = connection.execute(
        "SELECT COUNT(*) FROM fundamentals_raw"
    ).fetchone()
    assert count == 0


@respx.mock
def test_fetch_writes_raw_company_highlights_financials(
    connection, monkeypatch
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=_payload("AAPL"))
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=5,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(tickers=["AAPL.US"], dry_run=False)
    assert summary.dry_run is False
    assert summary.tickers_fetched == 1
    assert summary.requests_spent == 1
    assert summary.raw_inserted == 1
    assert summary.company_upserted == 1
    assert summary.highlights_upserted == 1
    assert summary.financials_upserted == 1
    assert summary.stopped_reason == "completed"

    (raw_count,) = connection.execute(
        "SELECT COUNT(*) FROM fundamentals_raw"
    ).fetchone()
    assert raw_count == 1
    (company_count,) = connection.execute(
        "SELECT COUNT(*) FROM fundamentals_company"
    ).fetchone()
    assert company_count == 1


@respx.mock
def test_fetch_idempotent_when_payload_unchanged(
    connection, monkeypatch
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    body = json.dumps(_payload("AAPL"), sort_keys=True)
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, content=body, headers={"content-type": "application/json"})
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=5,
        clock_ms=lambda: 1_700_000_000_000,
    )
    fetcher.fetch(tickers=["AAPL.US"], dry_run=False)
    summary = fetcher.fetch(tickers=["AAPL.US"], dry_run=False)
    assert summary.raw_inserted == 0  # same content_hash → no new raw row
    (raw_count,) = connection.execute(
        "SELECT COUNT(*) FROM fundamentals_raw"
    ).fetchone()
    assert raw_count == 1


@respx.mock
def test_fetch_skips_404_continues_batch(connection, monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/UNKNOWN.X").mock(
        return_value=httpx.Response(404, text="")
    )
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=_payload("AAPL"))
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=5,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(
        tickers=["UNKNOWN.X", "AAPL.US"], dry_run=False
    )
    assert summary.tickers_fetched == 1
    assert summary.tickers_skipped_error == 1
    assert any(e["kind"] == "not_found" for e in summary.errors)
    assert summary.stopped_reason == "completed"


@respx.mock
def test_fetch_stops_on_throttle(connection, monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(429, text="")
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None, max_retries=0)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=5,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(
        tickers=["AAPL.US", "MSFT.US"], dry_run=False
    )
    assert summary.stopped_reason == "throttled"
    assert summary.tickers_fetched == 0
    assert summary.tickers_skipped_error == 1


@respx.mock
def test_fetch_max_requests_caps_run(connection, monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=_payload("AAPL"))
    )
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/MSFT.US").mock(
        return_value=httpx.Response(200, json=_payload("MSFT"))
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=1,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(
        tickers=["AAPL.US", "MSFT.US"], dry_run=False
    )
    assert summary.tickers_fetched == 1
    assert summary.requests_spent == 1
    assert summary.stopped_reason == "budget_exhausted"


@respx.mock
def test_fetch_budget_resets_per_call_when_client_reused(
    connection, monkeypatch
) -> None:
    """Reusing the client across batches must not let the prior run's
    request count consume the next call's ``max_requests`` budget."""
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=_payload("AAPL"))
    )
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/MSFT.US").mock(
        return_value=httpx.Response(200, json=_payload("MSFT"))
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=1,
        clock_ms=lambda: 1_700_000_000_000,
    )
    first = fetcher.fetch(tickers=["AAPL.US"], dry_run=False)
    assert first.requests_spent == 1
    assert first.tickers_fetched == 1
    assert first.stopped_reason == "completed"

    second = fetcher.fetch(tickers=["MSFT.US"], dry_run=False)
    # Without the per-call baseline this would report 2 spent and stop on
    # ``budget_exhausted`` after zero MSFT fetches.
    assert second.requests_spent == 1
    assert second.tickers_fetched == 1
    assert second.stopped_reason == "completed"
    assert client.requests_made == 2


@respx.mock
def test_fetch_budget_caps_per_call_retries_to_remaining_budget(
    connection, monkeypatch
) -> None:
    """A single ticker on a 429-then-200 sequence cannot exceed
    ``max_requests`` even after internal client retries — the fetcher
    must cap per-call retries to ``max_requests - spent``."""
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        side_effect=[
            httpx.Response(429, text=""),
            httpx.Response(200, json=_payload("AAPL")),
        ]
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection,
        client=client,
        max_requests=1,
        clock_ms=lambda: 1_700_000_000_000,
    )
    summary = fetcher.fetch(tickers=["AAPL.US"], dry_run=False)
    # With the cap the client cannot retry — the 429 surfaces as a throttle.
    assert summary.tickers_fetched == 0
    assert summary.requests_spent == 1
    assert summary.stopped_reason == "throttled"
    assert client.requests_made == 1


def test_fetcher_rejects_empty_ticker_list(connection, monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    fetcher = FundamentalsFetcher(
        connection=connection, client=client, max_requests=5
    )
    with pytest.raises(ValueError):
        fetcher.fetch(tickers=[], dry_run=False)


def test_fetcher_rejects_zero_max_requests(connection, monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    with pytest.raises(ValueError):
        FundamentalsFetcher(connection=connection, client=client, max_requests=0)
