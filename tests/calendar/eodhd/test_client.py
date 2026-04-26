"""EODHD scaffold tests: EODHDAPIClient auth + 429 retry + fmt-json injection.

Split out of the original tests/test_eodhd_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from ingestion.calendar.eodhd_api import (
    EODHDAPIClient,
    EODHDAuthMissing,
)
from ingestion.calendar.eodhd_api.client import EODHDThrottled


def _earnings_row(**overrides):
    base = {
        "code": "AAPL.US",
        "report_date": "2026-05-01",
        "date": "2026-04-30",
        "before_after_market": "AfterMarket",
        "currency": "USD",
        "actual": 1.53,
        "estimate": 1.50,
        "difference": 0.03,
        "percent": 2.0,
    }
    base.update(overrides)
    return base


def test_client_auth_missing_raised_only_on_call(monkeypatch) -> None:
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    from env import DEFAULT_ENV_FILES, clear_env_cache
    monkeypatch.setattr("env.DEFAULT_ENV_FILES", ())
    clear_env_cache()
    client = EODHDAPIClient()
    with pytest.raises(EODHDAuthMissing):
        client.get("/api/calendar/earnings")


@respx.mock
def test_client_retries_on_429(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    route = respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        side_effect=[
            httpx.Response(429, text=""),
            httpx.Response(200, json={"earnings": [_earnings_row()]}),
        ]
    )
    client = EODHDAPIClient(sleeper=lambda _s: None)
    result = client.get_rows(
        "/api/calendar/earnings", params={"from": "2026-05-01", "to": "2026-05-02"},
        rows_key="earnings",
    )
    assert route.call_count == 2
    assert len(result.rows) == 1


@respx.mock
def test_client_exhausts_429_and_raises(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        return_value=httpx.Response(429, text="")
    )
    client = EODHDAPIClient(sleeper=lambda _s: None, max_retries=1)
    with pytest.raises(EODHDThrottled):
        client.get("/api/calendar/earnings")


@respx.mock
def test_client_injects_fmt_json_and_api_token(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "secret-42")
    captured = {}

    def _record(request):
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"earnings": []})

    respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        side_effect=_record
    )
    client = EODHDAPIClient(sleeper=lambda _s: None)
    client.get("/api/calendar/earnings", params={"from": "2026-05-01"})
    assert captured["params"]["api_token"] == "secret-42"
    assert captured["params"]["fmt"] == "json"
    assert captured["params"]["from"] == "2026-05-01"
