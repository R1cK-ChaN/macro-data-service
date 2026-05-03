"""Tests for the Tiingo market-data layer (issue #1).

Skip-marked at module level after issue #118 P4 retired the SQLite
market lane. ``TiingoMarketDataProvider``'s writers reference
``SQLiteEngineStore`` query methods that no longer exist; the provider
is dormant pending the follow-up backfill issue rewires it against
``ClickHouseMarketStore``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.skip(
    reason=(
        "Tiingo market provider pending CH rewire — "
        "issue #118 P4 retired SQLite market lane"
    )
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market._tiingo_universe import TIINGO_MACRO_ETF_UNIVERSE
from ingestion.market.clients._tiingo import (
    TiingoMarketDataProvider,
    check_adjustment_applied,
    check_ohlc_sanity,
    detect_history_breaks,
)
from ingestion.market.scrapers._tiingo import (
    TiingoAuthError,
    TiingoClient,
    TiingoDailyBar,
)
from storage import SQLiteEngineStore


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _sample_bars() -> list[dict]:
    """A miniature Tiingo EOD payload — mirrors the real JSON shape."""
    return [
        {
            "date": "2026-04-15T00:00:00.000Z",
            "open": 508.20,
            "high": 513.10,
            "low": 506.80,
            "close": 512.34,
            "volume": 81234567,
            "adjOpen": 508.00,
            "adjHigh": 512.80,
            "adjLow": 506.60,
            "adjClose": 512.10,
            "adjVolume": 81234567,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2026-04-16T00:00:00.000Z",
            "open": 513.00,
            "high": 515.00,
            "low": 510.00,
            "close": 514.00,
            "volume": 70000000,
            "adjOpen": 512.70,
            "adjHigh": 514.70,
            "adjLow": 509.70,
            "adjClose": 513.70,
            "adjVolume": 70000000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
        {
            "date": "2026-04-17T00:00:00.000Z",
            "open": 514.00,
            "high": 516.20,
            "low": 513.10,
            "close": 515.80,
            "volume": 72000000,
            "adjOpen": 513.70,
            "adjHigh": 515.90,
            "adjLow": 512.80,
            "adjClose": 515.50,
            "adjVolume": 72000000,
            "divCash": 0.0,
            "splitFactor": 1.0,
        },
    ]


def _mock_tiingo_client(rows: list[dict]) -> TiingoClient:
    client = TiingoClient(api_key="test-key")
    response = Mock()
    response.json.return_value = rows
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.get.return_value = response
    return client


# ── TiingoClient.get_daily_bars ────────────────────────────────────────────


def test_tiingo_client_parses_eod_json_shape() -> None:
    client = _mock_tiingo_client(_sample_bars())
    bars = client.get_daily_bars("SPY", start_date="2026-04-15", end_date="2026-04-17")

    assert [bar.date for bar in bars] == ["2026-04-15", "2026-04-16", "2026-04-17"]
    assert bars[0].ticker == "SPY"
    assert bars[0].close == pytest.approx(512.34)
    assert bars[0].adj_close == pytest.approx(512.10)
    assert bars[0].div_cash == 0.0
    assert bars[0].split_factor == 1.0


def test_tiingo_client_without_api_key_returns_empty_list() -> None:
    client = TiingoClient(api_key="placeholder")
    client.api_key = ""  # simulate missing key regardless of .env
    assert client.get_daily_bars("SPY") == []


def test_tiingo_client_raises_auth_error_on_401() -> None:
    import requests

    client = TiingoClient(api_key="bad-key")
    response = Mock()
    response.status_code = 401
    response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized", response=response)
    client.session = Mock()
    client.session.get.return_value = response
    with pytest.raises(TiingoAuthError):
        client.get_daily_bars("SPY")


# ── Quality-check helpers ──────────────────────────────────────────────────


def test_check_adjustment_applied_detects_adjusted_series() -> None:
    bars = [
        TiingoDailyBar(
            ticker="SPY", date="2026-04-15",
            open=100.0, high=101.0, low=99.0, close=100.0, volume=1_000,
            adj_open=90.0, adj_high=91.0, adj_low=89.0, adj_close=90.0, adj_volume=1_000,
            div_cash=0.0, split_factor=1.0,
        ),
        TiingoDailyBar(
            ticker="SPY", date="2026-04-16",
            open=101.0, high=102.0, low=100.0, close=101.0, volume=1_000,
            adj_open=91.0, adj_high=92.0, adj_low=90.0, adj_close=91.0, adj_volume=1_000,
            div_cash=0.0, split_factor=1.0,
        ),
    ]
    assert check_adjustment_applied(bars) is True


def test_check_adjustment_applied_returns_false_when_adj_equals_close() -> None:
    bars = [
        TiingoDailyBar(
            ticker="SPY", date="2026-04-15",
            open=100.0, high=101.0, low=99.0, close=100.0, volume=1_000,
            adj_open=100.0, adj_high=101.0, adj_low=99.0, adj_close=100.0, adj_volume=1_000,
            div_cash=0.0, split_factor=1.0,
        ),
    ]
    assert check_adjustment_applied(bars) is False


def test_detect_history_breaks_flags_unexplained_jump() -> None:
    bars = [
        TiingoDailyBar(
            ticker="XYZ", date="2026-01-02",
            open=100.0, high=100.0, low=100.0, close=100.0, volume=1,
            adj_open=100.0, adj_high=100.0, adj_low=100.0, adj_close=100.0, adj_volume=1,
            div_cash=0.0, split_factor=1.0,
        ),
        TiingoDailyBar(
            ticker="XYZ", date="2026-01-03",
            open=101.0, high=101.0, low=101.0, close=101.0, volume=1,
            adj_open=101.0, adj_high=101.0, adj_low=101.0, adj_close=101.0, adj_volume=1,
            div_cash=0.0, split_factor=1.0,
        ),
        # 90% drop with no split/div reported → real break
        TiingoDailyBar(
            ticker="XYZ", date="2026-01-04",
            open=10.0, high=10.0, low=10.0, close=10.0, volume=1,
            adj_open=10.0, adj_high=10.0, adj_low=10.0, adj_close=10.0, adj_volume=1,
            div_cash=0.0, split_factor=1.0,
        ),
    ]
    assert detect_history_breaks(bars, threshold=0.5) == ["2026-01-04"]


def test_detect_history_breaks_skips_corporate_action_days() -> None:
    bars = [
        TiingoDailyBar(
            ticker="XYZ", date="2026-01-02",
            open=100.0, high=100.0, low=100.0, close=100.0, volume=1,
            adj_open=100.0, adj_high=100.0, adj_low=100.0, adj_close=100.0, adj_volume=1,
            div_cash=0.0, split_factor=1.0,
        ),
        # 2-for-1 split → 50%+ drop is expected, not a break
        TiingoDailyBar(
            ticker="XYZ", date="2026-01-03",
            open=50.0, high=50.0, low=50.0, close=50.0, volume=2,
            adj_open=50.0, adj_high=50.0, adj_low=50.0, adj_close=50.0, adj_volume=2,
            div_cash=0.0, split_factor=2.0,
        ),
    ]
    assert detect_history_breaks(bars, threshold=0.5) == []


def test_check_ohlc_sanity_rejects_invalid_rows() -> None:
    good = TiingoDailyBar(
        ticker="SPY", date="2026-04-15",
        open=100.0, high=101.0, low=99.0, close=100.5, volume=1,
        adj_open=None, adj_high=None, adj_low=None, adj_close=None, adj_volume=None,
        div_cash=0.0, split_factor=1.0,
    )
    bad = TiingoDailyBar(
        ticker="SPY", date="2026-04-16",
        open=100.0, high=99.0, low=101.0, close=100.5, volume=1,  # low > high
        adj_open=None, adj_high=None, adj_low=None, adj_close=None, adj_volume=None,
        div_cash=0.0, split_factor=1.0,
    )
    assert check_ohlc_sanity(good) is True
    assert check_ohlc_sanity(bad) is False


# ── Seeding + refresh + read API ───────────────────────────────────────────


def test_seed_universe_upserts_all_macro_etfs(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(client=_mock_tiingo_client(_sample_bars()))
    count = provider.seed_universe(store)

    assert count == len(TIINGO_MACRO_ETF_UNIVERSE) == 11
    spy = store.get_market_instrument("US_SPY")
    assert spy is not None
    assert spy.primary_ticker == "SPY"
    assert spy.isin == "US78462F1030"
    assert spy.provider_symbols_json == {"tiingo": "SPY"}
    segments = store.list_symbol_segments("US_SPY")
    assert any(seg.ticker == "SPY" for seg in segments)


def test_refresh_market_history_persists_bars_and_flags(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(_sample_bars()),
        request_sleep=0,
    )
    provider.seed_universe(store)

    stats = provider.refresh_market_history(store, "SPY")
    assert stats.count == 3

    bars = store.list_market_price_bars("US_SPY")
    assert [b.date for b in bars] == ["2026-04-15", "2026-04-16", "2026-04-17"]
    assert bars[0].close == pytest.approx(512.34)
    assert bars[0].adjusted_close == pytest.approx(512.10)
    assert all(not b.has_break_detected for b in bars)

    # Adjustment was applied → history_status stays continuous
    spy = store.get_market_instrument("US_SPY")
    assert spy.history_status == "provider_continuous"


def test_refresh_market_history_marks_break_detected(store: SQLiteEngineStore) -> None:
    # First two bars flat, third bar drops >50% with no split/div — a real break
    broken_rows = _sample_bars()
    broken_rows[2]["close"] = 50.0
    broken_rows[2]["adjClose"] = 50.0
    broken_rows[2]["open"] = 50.0
    broken_rows[2]["high"] = 50.0
    broken_rows[2]["low"] = 50.0

    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(broken_rows),
        request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "SPY")

    spy = store.get_market_instrument("US_SPY")
    assert spy.history_status == "break_detected"
    bars = store.list_market_price_bars("US_SPY")
    flagged = [b for b in bars if b.has_break_detected]
    assert [b.date for b in flagged] == ["2026-04-17"]


def test_refresh_market_history_ignores_unseeded_ticker(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(client=_mock_tiingo_client(_sample_bars()))
    # Universe not seeded and AAPL is not in the macro universe.
    stats = provider.refresh_market_history(store, "AAPL")
    assert stats.count == 0


def test_get_market_history_returns_agent_native_shape(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(_sample_bars()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "SPY")

    rows = provider.get_market_history(store, "SPY", adjusted=True)
    assert len(rows) == 3
    first = rows[0]
    assert first["instrument_id"] == "US_SPY"
    assert first["ticker"] == "SPY"
    assert first["isin"] == "US78462F1030"
    assert first["openfigi"] == "BBG000BDTBL9"
    assert first["history_status"] == "provider_continuous"
    assert first["source"] == "Tiingo"
    assert first["quality_flags"] == []
    assert "closed at 512.10" in first["agent_summary"]


def test_get_market_history_raw_vs_adjusted_diverge(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(_sample_bars()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    provider.refresh_market_history(store, "SPY")

    adjusted = provider.get_market_history(store, "SPY", adjusted=True)
    raw = provider.get_market_history(store, "SPY", adjusted=False)

    assert adjusted[0]["close"] == pytest.approx(512.10)   # adjusted_close
    assert raw[0]["close"] == pytest.approx(512.34)        # raw close


# ── Review-driven invariants ──────────────────────────────────────────────


def test_refresh_market_history_auto_seeds_known_ticker(store: SQLiteEngineStore) -> None:
    # Caller never ran seed_universe; refreshing a universe ticker must still
    # persist the instrument row so get_market_history can find it.
    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(_sample_bars()),
        request_sleep=0,
    )
    stats = provider.refresh_market_history(store, "SPY")
    assert stats.count == 3
    spy = store.get_market_instrument("US_SPY")
    assert spy is not None
    rows = provider.get_market_history(store, "SPY")
    assert len(rows) == 3


def test_seed_universe_preserves_existing_break_status(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(client=_mock_tiingo_client(_sample_bars()))
    provider.seed_universe(store)
    store.update_instrument_history_status("US_SPY", "break_detected")

    provider.seed_universe(store)  # re-seed

    spy = store.get_market_instrument("US_SPY")
    assert spy.history_status == "break_detected"


def test_partial_refresh_does_not_clear_existing_break(store: SQLiteEngineStore) -> None:
    provider = TiingoMarketDataProvider(
        client=_mock_tiingo_client(_sample_bars()),
        request_sleep=0,
    )
    provider.seed_universe(store)
    store.update_instrument_history_status("US_SPY", "break_detected")

    # A clean partial-window refresh must not downgrade the alert.
    provider.refresh_market_history(store, "SPY", start="2026-04-15", end="2026-04-17")

    spy = store.get_market_instrument("US_SPY")
    assert spy.history_status == "break_detected"


def test_tlt_has_no_shared_figi_with_qqq() -> None:
    from ingestion.market._tiingo_universe import TIINGO_UNIVERSE_BY_TICKER

    tlt = TIINGO_UNIVERSE_BY_TICKER["TLT"]
    qqq = TIINGO_UNIVERSE_BY_TICKER["QQQ"]
    assert tlt.composite_figi == ""
    assert qqq.composite_figi and qqq.composite_figi != tlt.composite_figi


# ── Orchestrator wiring ───────────────────────────────────────────────────


def test_orchestrator_registers_tiingo_market_source(store: SQLiteEngineStore) -> None:
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    names = [s["name"] for s in orch.list_sources()]
    assert "tiingo_market" in names
