"""Client tests for the EODHD fundamentals HTTP wrapper (issue #68 S1)."""

from __future__ import annotations

import httpx
import pytest
import respx

from ingestion.market.fundamentals.eodhd_fundamentals import (
    EODHDFundamentalsAuthMissing,
    EODHDFundamentalsClient,
    EODHDFundamentalsNotFound,
    EODHDFundamentalsThrottled,
)


def _payload() -> dict:
    return {
        "General": {
            "Code": "AAPL",
            "Name": "Apple Inc",
            "Type": "Common Stock",
            "Sector": "Technology",
            "Industry": "Consumer Electronics",
            "FiscalYearEnd": "September",
            "Exchange": "NASDAQ",
            "CurrencyCode": "USD",
            "CountryISO": "US",
            "ISIN": "US0378331005",
        },
        "Highlights": {"MarketCapitalization": 3000000000000.0, "PERatio": 28.5},
        "Financials": {"Income_Statement": {"quarterly": {}, "yearly": {}}},
    }


def test_client_auth_missing_raised_only_on_call(monkeypatch) -> None:
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    from env import clear_env_cache
    monkeypatch.setattr("env.DEFAULT_ENV_FILES", ())
    clear_env_cache()
    client = EODHDFundamentalsClient()
    with pytest.raises(EODHDFundamentalsAuthMissing):
        client.get_fundamentals("AAPL.US")


@respx.mock
def test_client_retries_on_429(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    route = respx.get(
        url__startswith="https://eodhd.com/api/fundamentals/AAPL.US"
    ).mock(
        side_effect=[
            httpx.Response(429, text=""),
            httpx.Response(200, json=_payload()),
        ]
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    result = client.get_fundamentals("AAPL.US")
    assert route.call_count == 2
    assert result.payload["General"]["Code"] == "AAPL"
    assert client.requests_made == 2


@respx.mock
def test_client_exhausts_429_and_raises(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(429, text="")
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None, max_retries=1)
    with pytest.raises(EODHDFundamentalsThrottled):
        client.get_fundamentals("AAPL.US")


@respx.mock
def test_client_404_raises_not_found(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/UNKNOWN.X").mock(
        return_value=httpx.Response(404, text="")
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    with pytest.raises(EODHDFundamentalsNotFound):
        client.get_fundamentals("UNKNOWN.X")


@respx.mock
def test_client_injects_fmt_json_token_and_filter(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "secret-42")
    captured = {}

    def _record(request):
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json=_payload())

    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        side_effect=_record
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    result = client.get_fundamentals(
        "AAPL.US", sections=["General", "Highlights"]
    )
    assert result.payload_text  # we kept the verbatim bytes
    assert captured["params"]["api_token"] == "secret-42"
    assert captured["params"]["fmt"] == "json"
    assert captured["params"]["filter"] == "General,Highlights"


def test_client_rejects_blank_ticker(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    with pytest.raises(ValueError):
        client.get_fundamentals("")


@respx.mock
def test_client_rejects_non_dict_payload(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/fundamentals/AAPL.US").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = EODHDFundamentalsClient(sleeper=lambda _s: None)
    with pytest.raises(RuntimeError):
        client.get_fundamentals("AAPL.US")
