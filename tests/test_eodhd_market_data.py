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
    # 6 original (indices/ETF/equities) + 10 FX + 5 spot metals + 5 crypto
    # added by issue #67 slice 1.
    assert count == len(EODHD_GLOBAL_UNIVERSE) == 26

    nikkei = store.get_market_instrument("JP_NIKKEI225")
    assert nikkei is not None
    assert nikkei.primary_ticker == "N225"
    assert nikkei.provider_symbols_json == {"eodhd": "N225.INDX"}

    sap = store.get_market_instrument("DE_SAP")
    assert sap.isin == "DE0007164600"
    assert sap.currency == "EUR"

    eurusd = store.get_market_instrument("FX_EURUSD")
    assert eurusd is not None
    assert eurusd.asset_class == "fx"
    assert eurusd.exchange_code == "FOREX"

    btc = store.get_market_instrument("CRYPTO_BTC_USD")
    assert btc is not None
    assert btc.asset_class == "crypto"
    assert btc.exchange_code == "CC"

    gold = store.get_market_instrument("COMMOD_GOLD_SPOT")
    assert gold is not None
    assert gold.asset_class == "commodity"
    assert gold.provider_symbols_json == {"eodhd": "XAUUSD.FOREX"}


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


def test_universe_no_instrument_id_clash_with_macro_lane() -> None:
    """EODHD spot FX and the FRED/ECB macro projections both surface
    EUR/USD into ``market_price_bars`` — but they must keep distinct
    ``instrument_id`` values so downstream picks the right definition.
    The no-overlap-by-design rule (see module docstring) is structural;
    this test makes the contract explicit."""
    from ingestion.market._macro_map import MACRO_MARKET_BY_INSTRUMENT_ID

    eodhd_ids = {e.instrument_id for e in EODHD_GLOBAL_UNIVERSE}
    macro_ids = set(MACRO_MARKET_BY_INSTRUMENT_ID)
    assert eodhd_ids.isdisjoint(macro_ids)


def test_refresh_market_history_no_corp_acts_flag_for_fx_crypto_metal(
    store: SQLiteEngineStore,
) -> None:
    """FX, crypto, and spot-metal bars must not carry the
    ``has_missing_corp_acts`` or ``has_pre2018_delisted`` flags — these
    are equity-only signals and surfacing them on continuous-tape
    instruments would emit bogus warnings to downstream agents."""
    provider = EODHDMarketDataProvider(
        client=_mock_client(_sample_rows()),
        request_sleep=0,
    )
    provider.seed_universe(store)

    for instrument_id, ticker in (
        ("FX_EURUSD", "EURUSD.FOREX"),
        ("CRYPTO_BTC_USD", "BTC-USD.CC"),
        ("COMMOD_GOLD_SPOT", "XAUUSD.FOREX"),
    ):
        provider.refresh_market_history(store, ticker)
        bars = store.list_market_price_bars(instrument_id)
        assert len(bars) == 3, f"no bars persisted for {instrument_id}"
        assert all(not b.has_missing_corp_acts for b in bars), (
            f"{instrument_id} bars should not be flagged corp_acts_missing"
        )
        assert all(not b.has_pre2018_delisted for b in bars), (
            f"{instrument_id} bars should not be flagged pre2018_delisted"
        )
        assert all("corp_acts_missing" not in b.quality_flags_json for b in bars), (
            f"{instrument_id} bars must not carry the corp_acts_missing JSON flag"
        )

    # Sanity: equity rows still flagged as before so the gate doesn't
    # over-broadly suppress the warning.
    provider.refresh_market_history(store, "N225.INDX")
    nikkei_bars = store.list_market_price_bars("JP_NIKKEI225")
    assert all(b.has_missing_corp_acts for b in nikkei_bars), (
        "Index bars should still be flagged corp_acts_missing pending issue #67 slice 2"
    )


def test_refresh_market_history_break_detection_runs_for_continuous_tapes(
    store: SQLiteEngineStore,
) -> None:
    """FX / crypto / spot-metal series have ``adjusted_close == close``
    so the equity-style ``adjustment_applied`` gate would otherwise skip
    break detection entirely. Slice 1 fixes that — break detection still
    runs for these continuous-tape asset classes so a provider splice
    or scale jump on EURUSD/BTC/XAU surfaces as ``has_break_detected``.
    """
    # Construct a series with a 60% drop on day 3 — above the default
    # break threshold (DEFAULT_BREAK_THRESHOLD = 0.5, strict-greater check).
    payload = [
        {"date": "2026-04-15", "open": 1.10, "high": 1.11, "low": 1.10,
         "close": 1.10, "adjusted_close": 1.10, "volume": 1000},
        {"date": "2026-04-16", "open": 1.10, "high": 1.11, "low": 1.10,
         "close": 1.10, "adjusted_close": 1.10, "volume": 1000},
        {"date": "2026-04-17", "open": 0.40, "high": 0.40, "low": 0.40,
         "close": 0.40, "adjusted_close": 0.40, "volume": 1000},
    ]
    provider = EODHDMarketDataProvider(
        client=_mock_client(payload),
        request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "EURUSD.FOREX")
    bars = store.list_market_price_bars("FX_EURUSD")
    breaks = [b for b in bars if b.has_break_detected]
    assert len(breaks) >= 1, (
        "FX scale jump should surface as has_break_detected even when "
        "adjusted_close == close"
    )


def test_corp_action_bearing_classes_include_etf_variants() -> None:
    """bond_etf / commodity_etf are real asset_class values used by
    ``ingestion.market._tiingo_universe.TIINGO_MACRO_ETF_UNIVERSE`` —
    bond ETFs make distributions and commodity ETFs do split, so they
    must stay in the missing-corp-acts gate. Without this guard a
    custom EODHD universe of macro ETFs would silently bypass the
    quality flag."""
    from ingestion.market.clients._eodhd import (
        _CORP_ACTION_BEARING_ASSET_CLASSES,
    )

    assert "bond_etf" in _CORP_ACTION_BEARING_ASSET_CLASSES
    assert "commodity_etf" in _CORP_ACTION_BEARING_ASSET_CLASSES
    assert "equity_etf" in _CORP_ACTION_BEARING_ASSET_CLASSES
    # Negative invariants — non-corp-action-bearing classes must stay out.
    assert "fx" not in _CORP_ACTION_BEARING_ASSET_CLASSES
    assert "crypto" not in _CORP_ACTION_BEARING_ASSET_CLASSES
    assert "commodity" not in _CORP_ACTION_BEARING_ASSET_CLASSES


def test_universe_new_asset_classes_present() -> None:
    """Slice 1 adds 10 FX, 5 spot metals (asset_class=commodity), 5
    crypto. Guard against accidental drops on future edits."""
    classes = [e.asset_class for e in EODHD_GLOBAL_UNIVERSE]
    assert classes.count("fx") == 10
    assert classes.count("crypto") == 5
    # 5 spot metals; original universe had no commodity rows so the count
    # equals the slice-1 contribution.
    assert classes.count("commodity") == 5


def test_orchestrator_registers_eodhd_market_source(store: SQLiteEngineStore) -> None:
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    names = [s["name"] for s in orch.list_sources()]
    assert "eodhd_market" in names


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


# ── Issue #67 slice 2 — historical corp actions (raw + projection) ────────


def _mock_corp_action_client(
    *, dividends_payload: object, splits_payload: object,
) -> EODHDClient:
    """Build a mocked EODHDClient that returns one payload for /api/div
    and another for /api/splits, dispatched by URL."""
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"  # any JSON-ish string so the parser proceeds to .json()
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = dividends_payload
        elif "/splits/" in url:
            response.json.return_value = splits_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get
    return client


def _aapl_dividends() -> list[dict]:
    """Realistic /api/div/AAPL.US payload (probed live 2026-04-28)."""
    return [
        {"date": "2024-02-09", "declarationDate": "2024-02-01",
         "recordDate": "2024-02-12", "paymentDate": "2024-02-15",
         "period": "Quarterly", "value": 0.24, "unadjustedValue": 0.24,
         "currency": "USD"},
        {"date": "2024-05-10", "declarationDate": "2024-05-02",
         "recordDate": "2024-05-13", "paymentDate": "2024-05-16",
         "period": "Quarterly", "value": 0.25, "unadjustedValue": 0.25,
         "currency": "USD"},
        {"date": "2024-08-12", "declarationDate": "2024-08-01",
         "recordDate": "2024-08-12", "paymentDate": "2024-08-15",
         "period": "Quarterly", "value": 0.25, "unadjustedValue": 0.25,
         "currency": "USD"},
        {"date": "2024-11-08", "declarationDate": "2024-10-31",
         "recordDate": "2024-11-11", "paymentDate": "2024-11-14",
         "period": "Quarterly", "value": 0.25, "unadjustedValue": 0.25,
         "currency": "USD"},
    ]


def _aapl_splits() -> list[dict]:
    return [
        {"date": "2020-08-31", "split": "4.000000/1.000000"},
    ]


def _aapl_test_universe():
    """Single-entry custom universe for AAPL.US used by slice-2 tests."""
    from ingestion.market._eodhd_universe import EODHDUniverseEntry

    return (
        EODHDUniverseEntry(
            instrument_id="US_AAPL",
            eodhd_ticker="AAPL.US",
            primary_ticker="AAPL",
            exchange_code="US",
            name="Apple Inc.",
            asset_class="equity",
            market="United States equity market",
            currency="USD",
            description_for_agent="Apple — US mega-cap tech.",
        ),
    )


def test_get_historical_dividends_parses_full_history() -> None:
    client = _mock_corp_action_client(
        dividends_payload=_aapl_dividends(),
        splits_payload=[],
    )
    divs = client.get_historical_dividends("AAPL.US")
    assert [d.date for d in divs] == [
        "2024-02-09", "2024-05-10", "2024-08-12", "2024-11-08",
    ]
    assert all(d.value > 0 for d in divs)
    assert divs[0].declaration_date == "2024-02-01"
    assert divs[0].period == "Quarterly"
    assert divs[0].currency == "USD"


def test_get_historical_splits_parses_ratio() -> None:
    client = _mock_corp_action_client(
        dividends_payload=[], splits_payload=_aapl_splits(),
    )
    splits = client.get_historical_splits("AAPL.US")
    assert len(splits) == 1
    s = splits[0]
    assert s.date == "2020-08-31"
    assert s.new_shares == 4.0 and s.old_shares == 1.0
    assert s.raw_ratio == "4.000000/1.000000"


def test_refresh_corp_actions_populates_raw_and_projects_to_bars(
    store: SQLiteEngineStore,
) -> None:
    """Acceptance: ``market_corp_actions_raw`` carries every dividend
    + split, and ``market_price_bars`` rows for the matching dates land
    with non-default ``dividend_cash`` / ``split_factor``."""
    universe = _aapl_test_universe()
    bars_payload = [
        {"date": "2020-08-31", "open": 100.0, "high": 102.0, "low": 99.0,
         "close": 101.0, "adjusted_close": 25.25, "volume": 1_000_000},
        {"date": "2024-02-09", "open": 190.0, "high": 191.0, "low": 188.0,
         "close": 189.0, "adjusted_close": 189.0, "volume": 50_000_000},
        {"date": "2024-05-10", "open": 184.0, "high": 185.0, "low": 183.0,
         "close": 184.5, "adjusted_close": 184.5, "volume": 50_000_000},
        {"date": "2024-08-12", "open": 217.0, "high": 218.0, "low": 216.0,
         "close": 217.5, "adjusted_close": 217.5, "volume": 50_000_000},
        {"date": "2024-11-08", "open": 226.0, "high": 227.0, "low": 225.0,
         "close": 226.5, "adjusted_close": 226.5, "volume": 50_000_000},
    ]
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"  # any JSON-ish string so the parser proceeds to .json()
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = _aapl_dividends()
        elif "/splits/" in url:
            response.json.return_value = _aapl_splits()
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get

    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)

    # Step 1: populate market_price_bars via the existing path.
    provider.refresh_market_history(store, "AAPL.US")
    bars = store.list_market_price_bars("US_AAPL")
    assert {b.date for b in bars} == {
        "2020-08-31", "2024-02-09", "2024-05-10",
        "2024-08-12", "2024-11-08",
    }
    # Pre-projection: missing-CA flag is set, dividend/split fields are
    # the schema defaults.
    assert all(b.has_missing_corp_acts for b in bars)
    assert all(b.dividend_cash == 0.0 and b.split_factor == 1.0 for b in bars)

    # Step 2: refresh_corp_actions lands raw rows + projects.
    stats = provider.refresh_corp_actions(store, "AAPL.US")
    assert stats.count >= 5, "every dividend + the split should write its bar"

    raw_rows = store.latest_market_corp_actions_for_ticker(
        provider="eodhd", ticker="AAPL.US",
    )
    by_action = {(r.action_type, r.event_date): r for r in raw_rows}
    assert ("dividend", "2024-02-09") in by_action
    assert ("dividend", "2024-05-10") in by_action
    assert ("dividend", "2024-08-12") in by_action
    assert ("dividend", "2024-11-08") in by_action
    assert ("split", "2020-08-31") in by_action

    # Step 3: bars now carry the projected values + the missing flag is
    # cleared on rows that landed a corp-action snapshot.
    bars_after = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert bars_after["2024-02-09"].dividend_cash == 0.24
    assert bars_after["2024-05-10"].dividend_cash == 0.25
    assert bars_after["2024-08-12"].dividend_cash == 0.25
    assert bars_after["2024-11-08"].dividend_cash == 0.25
    assert bars_after["2020-08-31"].split_factor == 4.0
    for date in (
        "2020-08-31", "2024-02-09", "2024-05-10",
        "2024-08-12", "2024-11-08",
    ):
        assert not bars_after[date].has_missing_corp_acts, (
            f"projection should clear missing-CA flag on {date}"
        )


def test_refresh_corp_actions_idempotent_on_unchanged_data(
    store: SQLiteEngineStore,
) -> None:
    """Re-running the refresh on identical data inserts zero new rows
    in market_corp_actions_raw — content_hash + INSERT OR IGNORE."""
    universe = _aapl_test_universe()
    bars_payload = [
        {"date": "2024-02-09", "open": 190.0, "high": 191.0, "low": 188.0,
         "close": 189.0, "adjusted_close": 189.0, "volume": 50_000_000},
    ]
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"  # any JSON-ish string so the parser proceeds to .json()
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = _aapl_dividends()
        elif "/splits/" in url:
            response.json.return_value = _aapl_splits()
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get

    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_corp_actions(store, "AAPL.US")
    initial_rows = store.latest_market_corp_actions_for_ticker(
        provider="eodhd", ticker="AAPL.US",
    )
    provider.refresh_corp_actions(store, "AAPL.US")
    final_rows = store.latest_market_corp_actions_for_ticker(
        provider="eodhd", ticker="AAPL.US",
    )
    assert len(final_rows) == len(initial_rows), (
        "Re-running with identical data must not add new latest rows"
    )


def test_refresh_corp_actions_records_revision_on_changed_amount(
    store: SQLiteEngineStore,
) -> None:
    """A revised dividend amount produces a NEW raw row (different
    content_hash); old version is preserved; latest projects."""
    universe = _aapl_test_universe()
    bars_payload = [
        {"date": "2024-02-09", "open": 190.0, "high": 191.0, "low": 188.0,
         "close": 189.0, "adjusted_close": 189.0, "volume": 50_000_000},
    ]
    revised_dividends = [
        {"date": "2024-02-09", "declarationDate": "2024-02-01",
         "recordDate": "2024-02-12", "paymentDate": "2024-02-15",
         "period": "Quarterly", "value": 0.30,  # restated
         "unadjustedValue": 0.30, "currency": "USD"},
    ]
    original = [_aapl_dividends()[0]]  # value=0.24
    div_payload = original

    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"  # any JSON-ish string so the parser proceeds to .json()
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = div_payload
        elif "/splits/" in url:
            response.json.return_value = []
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get

    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "AAPL.US")
    provider.refresh_corp_actions(store, "AAPL.US")
    bars_v1 = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert bars_v1["2024-02-09"].dividend_cash == 0.24

    # EODHD restates the dividend.
    div_payload = revised_dividends
    provider.refresh_corp_actions(store, "AAPL.US")

    # Both revisions live in raw, the latest snapshot wins on projection.
    with store.get_connection() as conn:
        rows = conn.execute(
            "SELECT content_hash, snapshot_epoch_ms FROM market_corp_actions_raw "
            "WHERE provider='eodhd' AND ticker='AAPL.US' "
            "AND action_type='dividend' AND event_date='2024-02-09' "
            "ORDER BY snapshot_epoch_ms"
        ).fetchall()
    assert len(rows) == 2, "Restatement should add a second raw row"
    assert rows[0]["content_hash"] != rows[1]["content_hash"]

    bars_v2 = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert bars_v2["2024-02-09"].dividend_cash == 0.30, (
        "Latest snapshot must drive the projection"
    )


def test_refresh_corp_actions_clears_missing_flag_in_window(
    store: SQLiteEngineStore,
) -> None:
    """Codex slice-2 round-1 P2: a refresh window with bars on
    no-corp-action dates must clear ``has_missing_corp_acts`` on those
    bars too — the audit lane just confirmed there's nothing missing
    for those dates."""
    universe = _aapl_test_universe()
    bars_payload = [
        {"date": "2024-01-15", "open": 185.0, "high": 186.0, "low": 184.0,
         "close": 185.5, "adjusted_close": 185.5, "volume": 50_000_000},
        {"date": "2024-02-09", "open": 190.0, "high": 191.0, "low": 188.0,
         "close": 189.0, "adjusted_close": 189.0, "volume": 50_000_000},
        {"date": "2024-03-15", "open": 195.0, "high": 196.0, "low": 194.0,
         "close": 195.0, "adjusted_close": 195.0, "volume": 50_000_000},
    ]
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = [_aapl_dividends()[0]]  # 2024-02-09
        elif "/splits/" in url:
            response.json.return_value = []
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get

    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "AAPL.US")
    # Pre: every bar carries missing-CA.
    pre = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert all(b.has_missing_corp_acts for b in pre.values())

    provider.refresh_corp_actions(
        store, "AAPL.US", start="2024-01-01", end="2024-03-31",
    )

    post = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    # The dividend date carries div_cash and is no longer missing.
    assert post["2024-02-09"].dividend_cash == 0.24
    assert not post["2024-02-09"].has_missing_corp_acts
    # The two no-corp-action bars in window are also cleared — audit
    # lane has confirmed the nothing-to-project state.
    assert not post["2024-01-15"].has_missing_corp_acts
    assert not post["2024-03-15"].has_missing_corp_acts


def test_refresh_market_history_preserves_projected_corp_action_values(
    store: SQLiteEngineStore,
) -> None:
    """Codex slice-2 round-1 P2: after a corp-action projection lands
    ``dividend_cash`` / ``split_factor`` on a bar, a subsequent
    refresh_market_history call must NOT wipe those values back to the
    EOD-endpoint defaults (0.0 / 1.0). The fix re-projects from raw
    after every bar upsert in the refresh path.

    Round-2 follow-up: refresh_market_history does NOT clear missing-CA
    on no-event bars (audit-window awareness is reserved for
    refresh_corp_actions). Test only asserts on event-date bars because
    that's the contract refresh_market_history maintains."""
    universe = _aapl_test_universe()
    bars_payload = [
        {"date": "2024-02-09", "open": 190.0, "high": 191.0, "low": 188.0,
         "close": 189.0, "adjusted_close": 189.0, "volume": 50_000_000},
        {"date": "2020-08-31", "open": 100.0, "high": 102.0, "low": 99.0,
         "close": 101.0, "adjusted_close": 25.25, "volume": 1_000_000},
    ]
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = [_aapl_dividends()[0]]
        elif "/splits/" in url:
            response.json.return_value = _aapl_splits()
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get
    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "AAPL.US")
    provider.refresh_corp_actions(store, "AAPL.US")

    bars_v1 = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert bars_v1["2024-02-09"].dividend_cash == 0.24
    assert bars_v1["2020-08-31"].split_factor == 4.0

    # A scheduled price refresh (refresh_market_history called again with
    # the same bars) must NOT wipe the projected corp-action values.
    provider.refresh_market_history(store, "AAPL.US")
    bars_v2 = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    assert bars_v2["2024-02-09"].dividend_cash == 0.24, (
        "Price refresh should preserve projected dividend_cash"
    )
    assert bars_v2["2020-08-31"].split_factor == 4.0, (
        "Price refresh should preserve projected split_factor"
    )


def test_refresh_corp_actions_sums_same_day_dividends(
    store: SQLiteEngineStore,
) -> None:
    """Codex slice-2 round-1 P2: same ex-date can carry multiple
    dividend rows (regular + special). The projection must sum them
    instead of last-write-wins.
    """
    universe = _aapl_test_universe()
    same_day_dividends = [
        {"date": "2024-12-20", "declarationDate": "2024-12-01",
         "recordDate": "2024-12-21", "paymentDate": "2024-12-30",
         "period": "Quarterly", "value": 0.25, "unadjustedValue": 0.25,
         "currency": "USD"},
        {"date": "2024-12-20", "declarationDate": "2024-12-01",
         "recordDate": "2024-12-21", "paymentDate": "2024-12-30",
         "period": "Special", "value": 1.50, "unadjustedValue": 1.50,
         "currency": "USD"},
    ]
    bars_payload = [
        {"date": "2024-12-20", "open": 250.0, "high": 251.0, "low": 249.0,
         "close": 250.5, "adjusted_close": 250.5, "volume": 50_000_000},
    ]
    client = EODHDClient(api_key="test-key")
    client.session = Mock()

    def _route_get(url, params=None, timeout=None):
        response = Mock()
        response.content = b"not-empty"
        response.text = "[]"
        response.raise_for_status.return_value = None
        if "/div/" in url:
            response.json.return_value = same_day_dividends
        elif "/splits/" in url:
            response.json.return_value = []
        elif "/eod/" in url:
            response.json.return_value = bars_payload
        else:
            response.json.return_value = []
        return response

    client.session.get.side_effect = _route_get

    provider = EODHDMarketDataProvider(
        client=client, universe=universe, request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "AAPL.US")
    provider.refresh_corp_actions(store, "AAPL.US")

    bars = {b.date: b for b in store.list_market_price_bars("US_AAPL")}
    # Quarterly 0.25 + Special 1.50 = 1.75; last-write-wins would have
    # left the bar at 0.25 or 1.50 depending on iteration order.
    assert bars["2024-12-20"].dividend_cash == pytest.approx(1.75), (
        "Same-day dividends must accumulate, not overwrite"
    )


def test_split_ratio_parsed_correctly() -> None:
    """4-for-1 split lands as split_factor=4.0, matching the
    market_price_bars convention (split_factor = new / old)."""
    client = _mock_corp_action_client(
        dividends_payload=[],
        splits_payload=[{"date": "2020-08-31", "split": "4.000000/1.000000"}],
    )
    splits = client.get_historical_splits("AAPL.US")
    s = splits[0]
    assert s.new_shares / s.old_shares == 4.0


# ── Issue #67 slice 3 — bulk EOD backfill ──────────────────────────────────


def _bulk_payload_us_2024_06_14() -> list[dict]:
    """Realistic bulk-EOD shape — fields exactly match the live probe.
    Includes one universe-tracked ticker (AAPL) and four non-tracked
    rows so the universe filter has something to skip."""
    return [
        {"code": "AAPL", "exchange_short_name": "US", "date": "2024-06-14",
         "open": 213.85, "high": 215.17, "low": 213.4, "close": 212.5,
         "adjusted_close": 211.7, "volume": 70_000_000},
        {"code": "MSFT", "exchange_short_name": "US", "date": "2024-06-14",
         "open": 420.1, "high": 422.0, "low": 419.0, "close": 421.5,
         "adjusted_close": 421.5, "volume": 30_000_000},
        {"code": "GOOG", "exchange_short_name": "US", "date": "2024-06-14",
         "open": 175.0, "high": 176.5, "low": 174.5, "close": 176.0,
         "adjusted_close": 176.0, "volume": 25_000_000},
        {"code": "ZZZOF", "exchange_short_name": "US", "date": "2024-06-14",
         "open": 0.11, "high": 0.11, "low": 0.11, "close": 0.11,
         "adjusted_close": 0.11, "volume": 0},
        {"code": "^TNX", "exchange_short_name": "US", "date": "2024-06-14",
         "open": 4.31, "high": 4.35, "low": 4.31, "close": 4.336,
         "adjusted_close": 4.336, "volume": 0},
    ]


def test_get_bulk_last_day_parses_bulk_shape() -> None:
    """Bulk rows carry `code` + `exchange_short_name` instead of being
    URL-keyed. Parser must reconstruct the canonical CODE.EX form so
    downstream universe routing matches what `get_daily_bars` returns."""
    client = EODHDClient(api_key="test-key")
    response = Mock()
    response.content = b"not-empty"
    response.text = "[]"
    response.json.return_value = _bulk_payload_us_2024_06_14()
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.get.return_value = response

    bars = client.get_bulk_last_day("US", date="2024-06-14")
    assert len(bars) == 5
    assert bars[0].ticker == "AAPL.US"
    assert bars[0].date == "2024-06-14"
    assert bars[0].close == pytest.approx(212.5)


def test_get_bulk_last_day_handles_empty_body() -> None:
    """Holiday / weekend bulk requests can return empty body; parser
    must return [] without raising."""
    client = EODHDClient(api_key="test-key")
    response = Mock()
    response.content = b""
    response.text = ""
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.get.return_value = response
    assert client.get_bulk_last_day("US", date="2024-06-14") == []


def test_backfill_script_filters_universe_at_write(
    store: SQLiteEngineStore,
    tmp_path,
) -> None:
    """End-to-end test of the slice-3 backfill script: bulk returns 5
    rows but only AAPL is in the universe, so only one row lands in
    market_price_bars. Re-running on the same date is idempotent
    (existing PK)."""
    import sys as _sys
    from unittest.mock import patch
    from ingestion.market._eodhd_universe import EODHDUniverseEntry

    # Stub the EODHD client so the script doesn't hit the network.
    payload = _bulk_payload_us_2024_06_14()
    response = Mock()
    response.content = b"not-empty"
    response.text = "[]"
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    fake_session = Mock()
    fake_session.get.return_value = response

    # Single-AAPL universe so MSFT/GOOG/ZZZOF/^TNX are filtered out.
    custom_universe = (
        EODHDUniverseEntry(
            instrument_id="US_AAPL",
            eodhd_ticker="AAPL.US",
            primary_ticker="AAPL",
            exchange_code="US",
            name="Apple Inc.",
            asset_class="equity",
            market="United States equity market",
            currency="USD",
        ),
    )

    # Seed the universe in the store (script seeds via universe lookup,
    # not seed_universe — but the universe-filter helper reads the module
    # constant). Patch the module constant for the duration.
    REPO_ROOT = Path(__file__).resolve().parents[1]
    SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_market_bars_bulk.py"
    _sys.path.insert(0, str(SCRIPT_PATH.parent))
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backfill_market_bars_bulk", SCRIPT_PATH,
        )
        script_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_mod)

        with patch.object(script_mod, "EODHD_GLOBAL_UNIVERSE", custom_universe), \
             patch.object(
                 script_mod.EODHDClient, "session",
                 fake_session, create=True,
             ):
            # Patch out the network entry on a fresh client by replacing
            # the constructor return. Cleanest path: subclass.
            class StubbedClient(script_mod.EODHDClient):
                def __init__(self, *args, **kwargs):
                    self.api_key = "test-key"
                    self.session = fake_session

            with patch.object(script_mod, "EODHDClient", StubbedClient):
                # Pre-seed the AAPL instrument so the bar write doesn't
                # crash on FK / lookup expectations.
                EODHDMarketDataProvider(
                    universe=custom_universe, request_sleep=0,
                ).seed_universe(store)

                rc = script_mod.main([
                    "--exchange", "US",
                    "--date", "2024-06-14",
                    "--db-path", str(store.db_path),
                    "--request-sleep", "0",
                    "--quiet",
                    "--log-dir", str(tmp_path),
                ])
                assert rc == 0

        bars = store.list_market_price_bars("US_AAPL")
        assert len(bars) == 1, "Only AAPL is in the universe — others must be filtered"
        assert bars[0].date == "2024-06-14"
        assert bars[0].close == pytest.approx(212.5)
        assert bars[0].source_name == "EODHD"

        # Second run on the same date — idempotent via the PK
        # (instrument_id, date, bar_interval, source_name, source_symbol).
        with patch.object(script_mod, "EODHD_GLOBAL_UNIVERSE", custom_universe):
            class StubbedClient2(script_mod.EODHDClient):
                def __init__(self, *args, **kwargs):
                    self.api_key = "test-key"
                    self.session = fake_session

            with patch.object(script_mod, "EODHDClient", StubbedClient2):
                script_mod.main([
                    "--exchange", "US",
                    "--date", "2024-06-14",
                    "--db-path", str(store.db_path),
                    "--request-sleep", "0",
                    "--quiet",
                    "--log-dir", str(tmp_path),
                ])
        bars_again = store.list_market_price_bars("US_AAPL")
        assert len(bars_again) == 1, "Re-running on same date must be idempotent"

        # Issue #67 slice-3 codex-round-1 P2: backfill script must seed
        # the identity rows so the public read path resolves the bar.
        # get_market_history goes via market_instruments first; if seeding
        # was skipped the bar would land but the read would return [].
        history = EODHDMarketDataProvider(
            universe=custom_universe, request_sleep=0,
        ).get_market_history(store, "AAPL.US")
        assert len(history) == 1
        assert history[0]["ticker"] == "AAPL"
        assert history[0]["date"] == "2024-06-14"
    finally:
        _sys.path.remove(str(SCRIPT_PATH.parent))


def test_backfill_script_seeds_identity_rows_on_fresh_db(
    tmp_path,
) -> None:
    """Codex slice-3 round-1 P2: a fresh DB without any prior
    ``EODHDMarketDataProvider.seed_universe`` call must still result in
    ``market_instruments`` rows for the touched tickers — otherwise
    backfilled bars are orphans the read path can't resolve."""
    import importlib.util
    import sys as _sys
    from unittest.mock import patch
    from ingestion.market._eodhd_universe import EODHDUniverseEntry as _UE

    payload = _bulk_payload_us_2024_06_14()
    response = Mock()
    response.content = b"not-empty"
    response.text = "[]"
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    fake_session = Mock()
    fake_session.get.return_value = response

    custom_universe = (
        _UE(
            instrument_id="US_AAPL", eodhd_ticker="AAPL.US",
            primary_ticker="AAPL", exchange_code="US",
            name="Apple Inc.", asset_class="equity",
            market="United States equity market", currency="USD",
        ),
    )

    db_path = tmp_path / "fresh.db"
    fresh_store = SQLiteEngineStore(db_path=db_path)
    # Crucially: NO seed_universe call before the script runs. The
    # script's own seeding step is what we're exercising.
    assert fresh_store.get_market_instrument("US_AAPL") is None

    REPO_ROOT = Path(__file__).resolve().parents[1]
    SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_market_bars_bulk.py"
    _sys.path.insert(0, str(SCRIPT_PATH.parent))
    try:
        spec = importlib.util.spec_from_file_location(
            "backfill_market_bars_bulk", SCRIPT_PATH,
        )
        script_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_mod)

        class StubbedClient(script_mod.EODHDClient):
            def __init__(self, *args, **kwargs):
                self.api_key = "test-key"
                self.session = fake_session

        with patch.object(script_mod, "EODHD_GLOBAL_UNIVERSE", custom_universe), \
             patch.object(script_mod, "EODHDClient", StubbedClient):
            rc = script_mod.main([
                "--exchange", "US",
                "--date", "2024-06-14",
                "--db-path", str(db_path),
                "--request-sleep", "0",
                "--quiet",
                "--log-dir", str(tmp_path),
            ])
        assert rc == 0
        # Identity row landed.
        instrument = fresh_store.get_market_instrument("US_AAPL")
        assert instrument is not None
        assert instrument.primary_ticker == "AAPL"
        # Bar landed.
        bars = fresh_store.list_market_price_bars("US_AAPL")
        assert len(bars) == 1
        # Read path resolves both.
        history = EODHDMarketDataProvider(
            universe=custom_universe, request_sleep=0,
        ).get_market_history(fresh_store, "AAPL.US")
        assert len(history) == 1
    finally:
        _sys.path.remove(str(SCRIPT_PATH.parent))


def test_backfill_trading_days_includes_weekends_for_crypto() -> None:
    """Codex slice-3 round-2 P2: ``.CC`` exchange trades 24/7 so the
    ``--start ... --end ...`` walk must include Saturdays/Sundays.
    Equity exchanges keep the Mon-Fri filter."""
    import importlib.util
    REPO_ROOT = Path(__file__).resolve().parents[1]
    SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_market_bars_bulk.py"
    spec = importlib.util.spec_from_file_location(
        "backfill_market_bars_bulk", SCRIPT_PATH,
    )
    script_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(script_mod)

    import datetime as _dt
    # 2024-06-14 (Fri) → 2024-06-17 (Mon): Sat 15 + Sun 16 should be
    # included for CC, excluded for US.
    start = _dt.date(2024, 6, 14)
    end = _dt.date(2024, 6, 17)
    cc_days = script_mod._trading_days(start, end, exchange="CC")
    us_days = script_mod._trading_days(start, end, exchange="US")
    assert cc_days == ["2024-06-14", "2024-06-15", "2024-06-16", "2024-06-17"]
    assert us_days == ["2024-06-14", "2024-06-17"]


def test_backfill_aborts_on_missing_credentials(tmp_path) -> None:
    """Codex slice-3 round-2 P2: missing EODHD_API_KEY must produce a
    non-zero exit instead of silently completing with zero rows."""
    import importlib.util
    import sys as _sys
    from unittest.mock import patch
    REPO_ROOT = Path(__file__).resolve().parents[1]
    SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_market_bars_bulk.py"
    _sys.path.insert(0, str(SCRIPT_PATH.parent))
    try:
        spec = importlib.util.spec_from_file_location(
            "backfill_market_bars_bulk", SCRIPT_PATH,
        )
        script_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_mod)

        # Stub the EODHDClient so api_key resolves to "" regardless of
        # the operator's .env contents.
        class NoKeyClient(script_mod.EODHDClient):
            def __init__(self, *args, **kwargs):
                self.api_key = ""
                self.session = None

        # Empty universe filter would short-circuit before the
        # credential check, so use a real entry's exchange and patch
        # the universe to one row.
        from ingestion.market._eodhd_universe import EODHDUniverseEntry as _UE
        custom = (
            _UE(instrument_id="US_AAPL", eodhd_ticker="AAPL.US",
                primary_ticker="AAPL", exchange_code="US",
                name="Apple", asset_class="equity",
                market="US", currency="USD"),
        )
        with patch.object(script_mod, "EODHD_GLOBAL_UNIVERSE", custom), \
             patch.object(script_mod, "EODHDClient", NoKeyClient):
            db_path = tmp_path / "fresh.db"
            rc = script_mod.main([
                "--exchange", "US",
                "--date", "2024-06-14",
                "--db-path", str(db_path),
                "--request-sleep", "0",
                "--quiet",
                "--log-dir", str(tmp_path),
            ])
        assert rc == 2, "Missing API key must abort with non-zero exit"
    finally:
        _sys.path.remove(str(SCRIPT_PATH.parent))


def test_backfill_script_does_not_set_pre2018_delisted_flag(
    store: SQLiteEngineStore,
    tmp_path,
) -> None:
    """Codex slice-3 round-1 P2: bulk path can't run
    ``check_adjustment_applied`` (needs a per-ticker series) so it
    must NOT set ``has_pre2018_delisted`` based on date alone —
    otherwise adjusted history before 2018 surfaces as falsely
    flagged. Operators rely on per-ticker ``refresh_market_history``
    to set the flag correctly."""
    import importlib.util
    import sys as _sys
    from unittest.mock import patch
    from ingestion.market._eodhd_universe import EODHDUniverseEntry as _UE

    pre2018_payload = [
        {"code": "AAPL", "exchange_short_name": "US", "date": "2017-06-14",
         "open": 144.0, "high": 145.0, "low": 143.0, "close": 144.5,
         "adjusted_close": 144.5, "volume": 50_000_000},
    ]
    response = Mock()
    response.content = b"not-empty"
    response.text = "[]"
    response.json.return_value = pre2018_payload
    response.raise_for_status.return_value = None
    fake_session = Mock()
    fake_session.get.return_value = response

    custom_universe = (
        _UE(
            instrument_id="US_AAPL", eodhd_ticker="AAPL.US",
            primary_ticker="AAPL", exchange_code="US",
            name="Apple Inc.", asset_class="equity",
            market="United States equity market", currency="USD",
        ),
    )

    REPO_ROOT = Path(__file__).resolve().parents[1]
    SCRIPT_PATH = REPO_ROOT / "scripts" / "backfill_market_bars_bulk.py"
    _sys.path.insert(0, str(SCRIPT_PATH.parent))
    try:
        spec = importlib.util.spec_from_file_location(
            "backfill_market_bars_bulk", SCRIPT_PATH,
        )
        script_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(script_mod)

        class StubbedClient(script_mod.EODHDClient):
            def __init__(self, *args, **kwargs):
                self.api_key = "test-key"
                self.session = fake_session

        with patch.object(script_mod, "EODHD_GLOBAL_UNIVERSE", custom_universe), \
             patch.object(script_mod, "EODHDClient", StubbedClient):
            script_mod.main([
                "--exchange", "US",
                "--date", "2017-06-14",
                "--db-path", str(store.db_path),
                "--request-sleep", "0",
                "--quiet",
                "--log-dir", str(tmp_path),
            ])
        bars = store.list_market_price_bars("US_AAPL")
        assert len(bars) == 1
        assert bars[0].date == "2017-06-14"
        assert not bars[0].has_pre2018_delisted, (
            "Bulk path can't determine adjustment_applied — must not "
            "claim pre-2018 delisted on adjusted history"
        )
    finally:
        _sys.path.remove(str(SCRIPT_PATH.parent))


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
