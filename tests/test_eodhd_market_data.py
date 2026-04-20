"""Tests for the EODHD P1 global market-data layer (issue #1).

Covers:

* ``EODHDClient.get_daily_bars`` parses the real EODHD JSON list shape
* Handles EODHD's 200-OK ``"Ticker Not Found."`` string body
* ``EODHDMarketDataProvider`` seeds universe, auto-seeds on first refresh,
  preserves ``history_status``, flags missing corporate actions, and
  never downgrades a prior break alert on a partial-window refresh
* ``get_market_history`` returns the agent-native response shape
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market._eodhd_universe import (
    EODHD_GLOBAL_UNIVERSE,
    EODHD_UNIVERSE_BY_TICKER,
)
from ingestion.market.clients._eodhd import EODHDMarketDataProvider
from ingestion.market.scrapers._eodhd import (
    EODHDAuthError,
    EODHDClient,
    EODHDNotFoundError,
)
from storage import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _sample_rows() -> list[dict]:
    """A minimal EODHD EOD payload — matches the live response shape."""
    return [
        {
            "date": "2026-04-15",
            "open": 58265.18, "high": 58585.95, "low": 58028.75,
            "close": 58134.24, "adjusted_close": 58134.24, "volume": 161_000_000,
        },
        {
            "date": "2026-04-16",
            "open": 58479.83, "high": 59688.10, "low": 58428.19,
            "close": 59518.34, "adjusted_close": 59518.34, "volume": 150_300_000,
        },
        {
            "date": "2026-04-17",
            "open": 59255.09, "high": 59381.25, "low": 58475.90,
            "close": 58475.90, "adjusted_close": 58475.90, "volume": 140_000_000,
        },
    ]


def _mock_client(payload: object) -> EODHDClient:
    client = EODHDClient(api_key="test-key")
    response = Mock()
    response.content = b"not-empty"
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.get.return_value = response
    return client


# ── EODHDClient.get_daily_bars ─────────────────────────────────────────────


def test_client_parses_eod_list_shape() -> None:
    client = _mock_client(_sample_rows())
    bars = client.get_daily_bars("N225.INDX", start_date="2026-04-15", end_date="2026-04-17")
    assert [b.date for b in bars] == ["2026-04-15", "2026-04-16", "2026-04-17"]
    assert bars[0].ticker == "N225.INDX"
    assert bars[0].close == pytest.approx(58134.24)
    assert bars[0].adj_close == pytest.approx(58134.24)
    assert bars[0].div_cash == 0.0 and bars[0].split_factor == 1.0


def test_client_handles_ticker_not_found_string() -> None:
    # EODHD returns 200 OK with a plain string body when the ticker is bad.
    client = _mock_client("Ticker Not Found.")
    assert client.get_daily_bars("BOGUS.XX") == []


def test_client_raises_auth_error_on_401() -> None:
    import requests

    client = EODHDClient(api_key="bad-key")
    response = Mock()
    response.status_code = 401
    response.content = b""
    response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized", response=response)
    client.session = Mock()
    client.session.get.return_value = response
    with pytest.raises(EODHDAuthError):
        client.get_daily_bars("N225.INDX")


def test_client_without_api_key_returns_empty_list() -> None:
    client = EODHDClient(api_key="placeholder")
    client.api_key = ""  # simulate missing key regardless of .env
    assert client.get_daily_bars("N225.INDX") == []


# ── Seeding + refresh + read ───────────────────────────────────────────────


def test_seed_universe_upserts_global_instruments(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(client=_mock_client(_sample_rows()))
    count = provider.seed_universe(store)
    assert count == len(EODHD_GLOBAL_UNIVERSE) == 6

    nikkei = store.get_market_instrument("JP_NIKKEI225")
    assert nikkei is not None
    assert nikkei.primary_ticker == "N225"
    assert nikkei.provider_symbols_json == {"eodhd": "N225.INDX"}

    sap = store.get_market_instrument("DE_SAP")
    assert sap.isin == "DE0007164600"
    assert sap.currency == "EUR"


def test_refresh_market_history_persists_bars_and_flags(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    stats = provider.refresh_market_history(store, "N225.INDX")
    assert stats.count == 3

    bars = store.list_market_price_bars("JP_NIKKEI225")
    assert [b.date for b in bars] == ["2026-04-15", "2026-04-16", "2026-04-17"]
    # EODHD EOD endpoint has no div/split → every bar flagged missing CA.
    assert all(b.has_missing_corp_acts for b in bars)
    assert all(b.source_name == "EODHD" for b in bars)


def test_refresh_market_history_auto_seeds_known_ticker(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    # No seed_universe call first; auto-seed must fire.
    stats = provider.refresh_market_history(store, "N225.INDX")
    assert stats.count == 3
    assert store.get_market_instrument("JP_NIKKEI225") is not None


def test_refresh_accepts_bare_primary_ticker(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    stats = provider.refresh_market_history(store, "N225")
    assert stats.count == 3


def test_partial_refresh_does_not_clear_existing_break(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    store.update_instrument_history_status("JP_NIKKEI225", "break_detected")
    provider.refresh_market_history(
        store, "N225.INDX", start="2026-04-15", end="2026-04-17"
    )
    nikkei = store.get_market_instrument("JP_NIKKEI225")
    assert nikkei.history_status == "break_detected"


def test_seed_universe_preserves_existing_status(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(client=_mock_client(_sample_rows()))
    provider.seed_universe(store)
    store.update_instrument_history_status("DE_SAP", "manual_review")
    provider.seed_universe(store)
    assert store.get_market_instrument("DE_SAP").history_status == "manual_review"


def test_refresh_handles_ticker_not_found(store: SQLiteEngineStore) -> None:
    # Wire the scraper to return the EODHD "Ticker Not Found." string body.
    provider = EODHDMarketDataProvider(
        client=_mock_client("Ticker Not Found."),
        request_sleep=0,
    )
    provider.seed_universe(store)
    stats = provider.refresh_market_history(store, "N225.INDX")
    assert stats.count == 0


def test_refresh_swallows_http_not_found_error(store: SQLiteEngineStore) -> None:
    # Simulate an HTTP 404 from EODHD.
    import requests

    client = EODHDClient(api_key="test")
    client.session = Mock()
    response = Mock()
    response.status_code = 404
    response.content = b""
    response.raise_for_status.side_effect = requests.HTTPError(
        "404 Not Found", response=response
    )
    client.session.get.return_value = response

    provider = EODHDMarketDataProvider(client=client, request_sleep=0)
    provider.seed_universe(store)
    stats = provider.refresh_market_history(store, "N225.INDX")
    assert stats.count == 0


def test_get_market_history_agent_native_shape(store: SQLiteEngineStore) -> None:
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "N225.INDX")

    rows = provider.get_market_history(store, "N225.INDX")
    assert len(rows) == 3
    first = rows[0]
    assert first["instrument_id"] == "JP_NIKKEI225"
    assert first["ticker"] == "N225"
    assert first["source"] == "EODHD"
    assert "missing_corp_acts" in first["quality_flags"]
    assert "N225 closed at" in first["agent_summary"]


def test_universe_has_no_duplicate_tickers() -> None:
    tickers = [e.eodhd_ticker for e in EODHD_GLOBAL_UNIVERSE]
    assert len(tickers) == len(set(tickers))


def test_orchestrator_registers_eodhd_market_source(store: SQLiteEngineStore) -> None:
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    assert "eodhd_market" in orch.list_sources()


# ── Review-driven invariants ──────────────────────────────────────────────


def test_client_handles_real_ticker_not_found_text_body() -> None:
    """EODHD's real 'Ticker Not Found.' body is plain text, not JSON.

    response.json() raises a JSONDecodeError on that payload, so the parser
    must inspect raw text first and return []. This guards against an
    exception escaping get_daily_bars and aborting refresh_universe.
    """
    import json

    client = EODHDClient(api_key="test-key")
    response = Mock()
    response.content = b"Ticker Not Found."
    response.text = "Ticker Not Found."
    response.json.side_effect = json.JSONDecodeError("boom", "Ticker Not Found.", 0)
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.get.return_value = response

    assert client.get_daily_bars("BOGUS.XX") == []


def test_refresh_universe_ingests_custom_universe_entries(
    store: SQLiteEngineStore,
) -> None:
    """Custom universes must resolve via self.universe, not the module maps."""
    from ingestion.market._eodhd_universe import EODHDUniverseEntry

    custom = (
        EODHDUniverseEntry(
            instrument_id="UK_FOO",
            eodhd_ticker="FOO.LSE",
            primary_ticker="FOO",
            exchange_code="LSE",
            name="Foo plc",
            asset_class="equity",
            market="United Kingdom equity market (LSE)",
            currency="GBP",
        ),
    )
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        universe=custom,
        request_sleep=0,
    )
    stats = provider.refresh_universe(store, lookback_days=30)
    assert stats.count == 3
    bars = store.list_market_price_bars("UK_FOO")
    assert len(bars) == 3
