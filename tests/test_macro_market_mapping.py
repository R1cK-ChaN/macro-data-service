"""Tests for the P1 macro → market projection (issue #1).

Covers:

* ``MacroMarketProvider.seed_universe`` upserts rates/FX/commodity rows
* ``refresh_market_history`` projects ``indicators`` rows into
  ``market_price_bars`` with open=high=low=close=value and no corp-act flag
* ``get_market_history`` returns the agent-native shape with unit/asset_class
* Orchestrator registers ``macro_market`` in the default refresh order
  after ``eia`` so the projection runs on fresh FRED/EIA data
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market._macro_map import (
    MACRO_MARKET_BY_TICKER,
    MACRO_MARKET_UNIVERSE,
)
from ingestion.market.clients._macro_market import MacroMarketProvider
from storage import IndicatorObservationRecord, SQLiteEngineStore


@pytest.fixture()
def store(tmp_path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_indicator(
    store: SQLiteEngineStore,
    *,
    source: str,
    series_id: str,
    date: str,
    value: float,
) -> None:
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id=series_id,
            source=source,
            date=date,
            value=value,
            metadata={},
        )
    )


# ── Universe + seeding ─────────────────────────────────────────────────────


def test_universe_covers_rates_fx_and_commodities() -> None:
    classes = {e.asset_class for e in MACRO_MARKET_UNIVERSE}
    assert {"rate", "fx", "commodity"}.issubset(classes)


def test_universe_includes_fred_eia_and_ecb_sources() -> None:
    """Issue #1 P1 explicitly names FRED / ECB / EIA — all three must project."""
    sources = {e.source for e in MACRO_MARKET_UNIVERSE}
    assert {"fred", "eia", "ecb"}.issubset(sources)


def test_ticker_lookup_for_us10y_returns_fred_dgs10() -> None:
    entry = MACRO_MARKET_BY_TICKER["US10Y"]
    assert entry.source == "fred" and entry.series_id == "DGS10"
    assert entry.unit == "percent"


def test_seed_universe_upserts_all_macro_market_instruments(
    store: SQLiteEngineStore,
) -> None:
    provider = MacroMarketProvider()
    count = provider.seed_universe(store)
    assert count == len(MACRO_MARKET_UNIVERSE)

    us10y = store.get_market_instrument("MACRO_RATES_US_10Y")
    assert us10y is not None
    assert us10y.asset_class == "rate"
    assert us10y.primary_provider == "fred"
    assert us10y.provider_symbols_json == {"fred": "DGS10"}


# ── Projection ─────────────────────────────────────────────────────────────


def test_refresh_projects_fred_series_into_market_bars(
    store: SQLiteEngineStore,
) -> None:
    _seed_indicator(store, source="fred", series_id="DGS10", date="2026-04-15", value=4.27)
    _seed_indicator(store, source="fred", series_id="DGS10", date="2026-04-16", value=4.31)
    _seed_indicator(store, source="fred", series_id="DGS10", date="2026-04-17", value=4.29)

    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(store, "US10Y")
    assert stats.count == 3

    bars = store.list_market_price_bars("MACRO_RATES_US_10Y")
    assert [b.date for b in bars] == ["2026-04-15", "2026-04-16", "2026-04-17"]
    assert bars[0].close == pytest.approx(4.27)
    assert bars[0].open == bars[0].high == bars[0].low == bars[0].close
    assert bars[0].split_factor == 1.0 and bars[0].dividend_cash == 0.0
    assert bars[0].has_missing_corp_acts is False
    assert bars[0].source_name == "FRED"
    assert bars[0].source_symbol == "DGS10"
    assert bars[0].quality_flags_json["asset_class"] == "rate"
    assert bars[0].quality_flags_json["unit"] == "percent"


def test_refresh_accepts_instrument_id_and_series_id(
    store: SQLiteEngineStore,
) -> None:
    _seed_indicator(store, source="eia", series_id="EIA_WTI", date="2026-04-17", value=71.5)

    provider = MacroMarketProvider()
    stats_by_id = provider.refresh_market_history(store, "MACRO_COMMOD_WTI")
    assert stats_by_id.count == 1

    # Clear the row to prove the second call re-projects against series_id key.
    stats_by_series = provider.refresh_market_history(store, "EIA_WTI")
    assert stats_by_series.count == 1


def test_refresh_auto_seeds_instrument_row_on_first_call(
    store: SQLiteEngineStore,
) -> None:
    _seed_indicator(store, source="fred", series_id="DGS2", date="2026-04-17", value=3.85)
    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(store, "US2Y")
    assert stats.count == 1
    assert store.get_market_instrument("MACRO_RATES_US_2Y") is not None


def test_refresh_unknown_symbol_returns_zero(store: SQLiteEngineStore) -> None:
    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(store, "NOT_A_TICKER")
    assert stats.count == 0


def test_refresh_respects_start_end_window(store: SQLiteEngineStore) -> None:
    for date, value in [
        ("2026-04-14", 4.20),
        ("2026-04-15", 4.27),
        ("2026-04-16", 4.31),
        ("2026-04-17", 4.29),
    ]:
        _seed_indicator(store, source="fred", series_id="DGS10", date=date, value=value)

    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(
        store, "US10Y", start="2026-04-15", end="2026-04-16"
    )
    assert stats.count == 2
    bars = store.list_market_price_bars("MACRO_RATES_US_10Y")
    assert [b.date for b in bars] == ["2026-04-15", "2026-04-16"]


# ── Read API ───────────────────────────────────────────────────────────────


def test_get_market_history_returns_agent_native_shape(store: SQLiteEngineStore) -> None:
    _seed_indicator(store, source="fred", series_id="DGS10", date="2026-04-17", value=4.29)
    provider = MacroMarketProvider()
    provider.refresh_market_history(store, "US10Y")

    rows = provider.get_market_history(store, "US10Y")
    assert len(rows) == 1
    first = rows[0]
    assert first["instrument_id"] == "MACRO_RATES_US_10Y"
    assert first["ticker"] == "US10Y"
    assert first["asset_class"] == "rate"
    assert first["unit"] == "percent"
    assert first["source"] == "FRED"
    assert "US10Y yield closed at 4.290%" in first["agent_summary"]


def test_refresh_projects_ecb_deposit_rate(store: SQLiteEngineStore) -> None:
    _seed_indicator(
        store, source="ecb", series_id="ECB_EA_DEPOSIT_RATE",
        date="2026-04-17", value=3.00,
    )
    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(store, "EADEPO")
    assert stats.count == 1

    rows = provider.get_market_history(store, "EADEPO")
    assert rows[0]["ticker"] == "EADEPO"
    assert rows[0]["asset_class"] == "rate"
    assert rows[0]["source"] == "ECB"
    assert "EADEPO yield closed at 3.000%" in rows[0]["agent_summary"]


def test_refresh_projects_ecb_eurusd(store: SQLiteEngineStore) -> None:
    # The market-layer EURUSD is backed by the daily ECB EXR series, not
    # the monthly one — a 1d bar must correspond to a daily observation.
    _seed_indicator(
        store, source="ecb", series_id="ECB_EURUSD_D",
        date="2026-04-17", value=1.0725,
    )
    provider = MacroMarketProvider()
    stats = provider.refresh_market_history(store, "EURUSD")
    assert stats.count == 1

    bar = store.list_market_price_bars("MACRO_FX_EURUSD")[0]
    assert bar.source_name == "ECB"
    assert bar.source_symbol == "ECB_EURUSD_D"
    assert bar.close == pytest.approx(1.0725)


def test_get_market_history_for_commodity_uses_usd_summary(
    store: SQLiteEngineStore,
) -> None:
    _seed_indicator(store, source="eia", series_id="EIA_WTI", date="2026-04-17", value=71.5)
    provider = MacroMarketProvider()
    provider.refresh_market_history(store, "WTI")
    rows = provider.get_market_history(store, "WTI")
    assert rows[0]["asset_class"] == "commodity"
    assert "WTI closed at 71.50 USD" in rows[0]["agent_summary"]


# ── Orchestrator wiring ────────────────────────────────────────────────────


def test_orchestrator_registers_macro_market_source(store: SQLiteEngineStore) -> None:
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    names = [s["name"] for s in orch.list_sources()]
    assert "macro_market" in names


# ── Review-driven invariants ──────────────────────────────────────────────


def test_source_registration_order_runs_after_upstream_sources(
    store: SQLiteEngineStore,
) -> None:
    """Scheduled cycles iterate `_sources` in registration order, so the
    macro projection must be registered after every upstream source whose
    rows it projects (FRED / EIA / ECB)."""
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    order = [s["name"] for s in orch.list_sources()]
    assert order.index("fred_daily") < order.index("macro_market")
    assert order.index("eia") < order.index("macro_market")
    assert order.index("ecb") < order.index("macro_market")


def test_refresh_universe_ingests_custom_universe_entries(
    store: SQLiteEngineStore,
) -> None:
    """Custom universes must resolve via self.universe, not module maps."""
    from ingestion.market._macro_map import MacroMarketEntry

    custom = (
        MacroMarketEntry(
            instrument_id="MACRO_CUSTOM_FOO",
            ticker="FOO",
            name="Custom Foo Series",
            asset_class="rate",
            market="custom",
            currency="USD",
            unit="percent",
            source="fred",
            series_id="CUSTOM_FOO",
        ),
    )
    _seed_indicator(store, source="fred", series_id="CUSTOM_FOO", date="2026-04-17", value=1.23)

    provider = MacroMarketProvider(universe=custom)
    stats = provider.refresh_universe(store)
    assert stats.count == 1
    bars = store.list_market_price_bars("MACRO_CUSTOM_FOO")
    assert len(bars) == 1 and bars[0].close == pytest.approx(1.23)
