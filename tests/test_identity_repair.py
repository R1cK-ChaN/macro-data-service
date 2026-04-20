"""Tests for the P2 identity + history repair flow (issue #1).

Covers:

* EODHDIdentityClient: search, delisted list, symbol-change-history,
  exchanges-list parsing
* OpenFIGIClient: /v3/mapping batch + by-ISIN / by-ticker convenience
* IdentityRepairService: lazy repair flow on break_detected instruments
  - ISIN-first EODHD search → auto_isin segments
  - OpenFIGI fallback → auto_figi segments (includes FIGI enrichment)
  - delisted scan → name_match segments
  - symbol-change-history → ticker_rename segments for old + new
  - history_status promotions: break_detected → stitched or manual_review
  - idempotency: running twice doesn't duplicate segments
* Orchestrator registers `identity_repair` source
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market.clients._identity_repair import (
    IdentityRepairService,
    _fuzzy_name_matches,
    _normalize_tokens,
)
from ingestion.market.scrapers._eodhd_identity import (
    EODHDExchange,
    EODHDIdentityClient,
    EODHDSearchHit,
    EODHDSymbolChangeEvent,
    EODHDSymbolListEntry,
)
from ingestion.market.scrapers._openfigi import OpenFIGIClient, OpenFIGIHit
from storage import MarketInstrumentRecord, SQLiteEngineStore


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_broken_instrument(
    store: SQLiteEngineStore,
    *,
    instrument_id: str = "US_ACME",
    primary_ticker: str = "ACME",
    name: str = "Acme Corporation",
    isin: str = "US00000A0001",
    exchange_code: str = "US",
) -> MarketInstrumentRecord:
    record = MarketInstrumentRecord(
        instrument_id=instrument_id,
        primary_ticker=primary_ticker,
        name=name,
        asset_class="equity",
        market="United States equity market",
        exchange_code=exchange_code,
        currency="USD",
        isin=isin,
        primary_provider="tiingo",
        provider_symbols_json={"tiingo": primary_ticker},
        history_status="break_detected",
    )
    store.upsert_market_instrument(record)
    return record


# ── EODHDIdentityClient ────────────────────────────────────────────────────


def _mock_identity_response(payload: object) -> Mock:
    response = Mock()
    response.content = b"not-empty"
    response.text = "[]"  # default to non-empty-JSON-looking
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_eodhd_search_parses_isin_match() -> None:
    client = EODHDIdentityClient(api_key="test-key")
    response = _mock_identity_response([
        {
            "Code": "SPY", "Exchange": "US", "Name": "SPDR S&P 500 ETF Trust",
            "Type": "ETF", "Country": "USA", "Currency": "USD",
            "ISIN": "US78462F1030",
        },
    ])
    response.text = "[{...}]"
    client.session = Mock()
    client.session.get.return_value = response

    hits = client.search("US78462F1030")
    assert len(hits) == 1 and hits[0].code == "SPY" and hits[0].isin == "US78462F1030"


def test_eodhd_exchange_symbols_reads_delisted_flag() -> None:
    client = EODHDIdentityClient(api_key="test-key")
    response = _mock_identity_response([
        {"Code": "AAAB", "Name": "Admiralty Bancorp Inc", "Country": "USA",
         "Exchange": "NASDAQ", "Currency": "USD", "Type": "Common Stock", "Isin": None},
    ])
    response.text = "[{...}]"
    client.session = Mock()
    client.session.get.return_value = response

    entries = client.list_exchange_symbols("US", delisted=True)
    assert len(entries) == 1 and entries[0].code == "AAAB"
    # Verify the delisted flag was actually sent to EODHD.
    _, kwargs = client.session.get.call_args
    assert kwargs["params"]["delisted"] == "1"


def test_eodhd_symbol_change_history_parses_rename_events() -> None:
    client = EODHDIdentityClient(api_key="test-key")
    response = _mock_identity_response([
        {"exchange": "US", "old_symbol": "ACME",
         "new_symbol": "ACMX", "company_name": "Acme Corp",
         "effective": "2024-03-15"},
    ])
    response.text = "[{...}]"
    client.session = Mock()
    client.session.get.return_value = response

    events = client.symbol_change_history(from_date="2024-01-01", to_date="2024-06-30")
    assert len(events) == 1
    assert events[0].old_symbol == "ACME"
    assert events[0].new_symbol == "ACMX"
    assert events[0].effective == "2024-03-15"


def test_eodhd_list_exchanges_parses_mic_codes() -> None:
    client = EODHDIdentityClient(api_key="test-key")
    response = _mock_identity_response([
        {"Name": "USA Stocks", "Code": "US", "OperatingMIC": "XNAS, XNYS, OTCM",
         "Country": "USA", "Currency": "USD"},
    ])
    response.text = "[{...}]"
    client.session = Mock()
    client.session.get.return_value = response

    exchanges = client.list_exchanges()
    assert exchanges == [
        EODHDExchange(code="US", name="USA Stocks", country="USA",
                      currency="USD", operating_mic="XNAS, XNYS, OTCM"),
    ]


def test_eodhd_identity_without_api_key_returns_empty_list() -> None:
    client = EODHDIdentityClient(api_key="placeholder")
    client.api_key = ""
    assert client.search("ANYTHING") == []
    assert client.list_exchange_symbols("US") == []
    assert client.symbol_change_history() == []
    assert client.list_exchanges() == []


# ── OpenFIGIClient ─────────────────────────────────────────────────────────


def test_openfigi_maps_isin_to_figi() -> None:
    client = OpenFIGIClient(api_key="test-key")
    response = Mock()
    response.json.return_value = [
        {"data": [{
            "figi": "BBG000BDTBL9",
            "name": "SS SPDR S&P 500 ETF TRUST-US",
            "ticker": "SPY", "exchCode": "US",
            "compositeFIGI": "BBG000BDTBL9",
            "shareClassFIGI": "BBG001S72SM3",
            "securityType": "ETP", "marketSector": "Equity",
            "securityDescription": "SPY",
        }]}
    ]
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.post.return_value = response

    hits = client.map_by_isin("US78462F1030")
    assert len(hits) == 1
    assert hits[0].figi == "BBG000BDTBL9"
    assert hits[0].composite_figi == "BBG000BDTBL9"
    assert hits[0].share_class_figi == "BBG001S72SM3"


def test_openfigi_map_aligns_batch_results_with_jobs() -> None:
    client = OpenFIGIClient(api_key="test-key")
    response = Mock()
    response.json.return_value = [
        {"data": [{"figi": "BBG000BDTBL9", "ticker": "SPY", "exchCode": "US",
                   "compositeFIGI": "BBG000BDTBL9", "shareClassFIGI": "",
                   "name": "SPY", "securityType": "ETP", "marketSector": "Equity",
                   "securityDescription": "SPY"}]},
        {"error": "No identifier found."},
    ]
    response.raise_for_status.return_value = None
    client.session = Mock()
    client.session.post.return_value = response

    results = client.map([
        {"idType": "ID_ISIN", "idValue": "US78462F1030"},
        {"idType": "ID_ISIN", "idValue": "BAD-ISIN"},
    ])
    assert len(results) == 2
    assert len(results[0]) == 1 and results[0][0].ticker == "SPY"
    assert results[1] == []


# ── Token / fuzzy helpers ──────────────────────────────────────────────────


def test_fuzzy_name_matches_handles_corporate_suffixes() -> None:
    class Row:
        def __init__(self, name: str) -> None:
            self.name = name
    rows = [Row("ACME INC"), Row("Zephyr Corp"), Row("Acme Corporation")]
    matches = _fuzzy_name_matches("Acme Inc", rows)
    matched_names = {r.name for r in matches}
    assert matched_names == {"ACME INC", "Acme Corporation"}


def test_normalize_tokens_strips_noise() -> None:
    assert _normalize_tokens("Apple Inc.") == {"apple"}
    assert _normalize_tokens("Tencent Holdings Ltd") == {"tencent"}


# ── IdentityRepairService ──────────────────────────────────────────────────


def test_repair_skipped_when_status_is_not_break(store: SQLiteEngineStore) -> None:
    # Seed an instrument that is NOT break_detected.
    store.upsert_market_instrument(
        MarketInstrumentRecord(
            instrument_id="US_OK",
            primary_ticker="OK",
            name="Okay Corp",
            asset_class="equity",
            market="US",
            history_status="provider_continuous",
        )
    )
    service = IdentityRepairService(eodhd=Mock(), openfigi=Mock())
    report = service.repair(store, "US_OK")
    assert report.final_history_status == "provider_continuous"
    assert "skipped_no_break" in report.notes


def test_repair_writes_isin_search_segments_and_promotes_status(
    store: SQLiteEngineStore,
) -> None:
    _seed_broken_instrument(store)

    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="ACME", exchange="US", name="Acme Corporation",
            type="Common Stock", country="USA", currency="USD",
            isin="US00000A0001",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []

    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    # Refetch callback required to promote to stitched — without it the
    # instrument stays break_detected (metadata-only repair).
    callback = Mock()
    service = IdentityRepairService(
        eodhd=eodhd, openfigi=openfigi, refetch_callback=callback,
    )
    report = service.repair(store, "US_ACME")

    assert report.final_history_status == "stitched"
    assert report.segments_written >= 1
    assert report.confidence_breakdown["auto_isin"] >= 1
    segments = store.list_symbol_segments("US_ACME")
    assert any(
        s.mapping_confidence == "auto_isin" and s.ticker == "ACME.US"
        for s in segments
    )


def test_repair_falls_back_to_openfigi_when_search_empty(
    store: SQLiteEngineStore,
) -> None:
    _seed_broken_instrument(store)
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = [
        OpenFIGIHit(
            figi="BBG000TEST1", name="Acme Corporation", ticker="ACME",
            exch_code="US", composite_figi="BBG000TEST1",
            share_class_figi="BBG001TEST1", security_type="Common Stock",
            market_sector="Equity", security_description="ACME",
        ),
    ]
    openfigi.map_by_ticker.return_value = []

    callback = Mock()
    service = IdentityRepairService(
        eodhd=eodhd, openfigi=openfigi, refetch_callback=callback,
    )
    report = service.repair(store, "US_ACME")

    assert report.final_history_status == "stitched"
    assert report.confidence_breakdown.get("auto_figi") == 1
    segments = store.list_symbol_segments("US_ACME")
    assert any(s.figi == "BBG000TEST1" for s in segments)


def test_repair_uses_delisted_list_name_match(store: SQLiteEngineStore) -> None:
    _seed_broken_instrument(store, isin="", name="Acme Corporation")
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = [
        EODHDSymbolListEntry(
            code="ACMX", name="ACME CORPORATION", country="USA",
            exchange="NASDAQ", currency="USD", type="Common Stock", isin="",
        ),
        EODHDSymbolListEntry(
            code="ZZZZ", name="Unrelated Holdings", country="USA",
            exchange="NASDAQ", currency="USD", type="Common Stock", isin="",
        ),
    ]
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    callback = Mock()
    service = IdentityRepairService(
        eodhd=eodhd, openfigi=openfigi, refetch_callback=callback,
    )
    report = service.repair(store, "US_ACME")

    assert report.final_history_status == "stitched"
    assert report.confidence_breakdown.get("name_match") == 1
    # Only the matching row was accepted — unrelated company ignored.
    segments = store.list_symbol_segments("US_ACME")
    assert not any(s.ticker.startswith("ZZZZ") for s in segments)


def test_repair_parses_rename_event_into_two_segments(
    store: SQLiteEngineStore,
) -> None:
    _seed_broken_instrument(
        store, isin="", primary_ticker="ACMX", exchange_code="US",
    )
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = [
        EODHDSymbolChangeEvent(
            exchange="US", old_symbol="ACME", new_symbol="ACMX",
            company_name="Acme Corp", effective="2024-03-15",
        ),
    ]
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    service.repair(store, "US_ACME")

    segments = store.list_symbol_segments("US_ACME")
    tickers = sorted(s.ticker for s in segments)
    assert "ACME.US" in tickers and "ACMX.US" in tickers
    # Old segment has a valid_to boundary, new segment has a valid_from.
    old_seg = next(s for s in segments if s.ticker == "ACME.US")
    new_seg = next(s for s in segments if s.ticker == "ACMX.US")
    assert old_seg.valid_to == "2024-03-15"
    assert new_seg.valid_from == "2024-03-15"


def test_repair_is_idempotent(store: SQLiteEngineStore) -> None:
    _seed_broken_instrument(store)
    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="ACME", exchange="US", name="Acme Corporation",
            type="Common Stock", country="USA", currency="USD",
            isin="US00000A0001",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    callback = Mock()
    service = IdentityRepairService(
        eodhd=eodhd, openfigi=openfigi, refetch_callback=callback,
    )
    service.repair(store, "US_ACME")
    # After first pass, status is "stitched" so the second pass is a no-op.
    first_segments = store.list_symbol_segments("US_ACME")
    store.update_instrument_history_status("US_ACME", "break_detected")
    service.repair(store, "US_ACME")
    second_segments = store.list_symbol_segments("US_ACME")
    assert len(first_segments) == len(second_segments)


def test_repair_without_candidates_moves_status_to_manual_review(
    store: SQLiteEngineStore,
) -> None:
    _seed_broken_instrument(store, isin="")
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    report = service.repair(store, "US_ACME")
    assert report.final_history_status == "manual_review"
    assert "no_candidates_found" in report.notes


def test_repair_all_breaks_only_targets_flagged_rows(store: SQLiteEngineStore) -> None:
    _seed_broken_instrument(store, instrument_id="US_A", primary_ticker="A", isin="")
    store.upsert_market_instrument(
        MarketInstrumentRecord(
            instrument_id="US_B", primary_ticker="B", name="Bravo",
            asset_class="equity", market="US",
            history_status="provider_continuous",
        )
    )
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    reports = service.repair_all_breaks(store)
    assert [r.instrument_id for r in reports] == ["US_A"]


def test_repair_triggers_refetch_callback(store: SQLiteEngineStore) -> None:
    _seed_broken_instrument(store)
    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="ACME", exchange="US", name="Acme Corporation",
            type="Common Stock", country="USA", currency="USD",
            isin="US00000A0001",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []
    openfigi.map_by_ticker.return_value = []

    callback = Mock()
    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi, refetch_callback=callback)
    service.repair(store, "US_ACME")
    callback.assert_called_once()
    instrument_arg, segments_arg = callback.call_args.args
    assert instrument_arg.instrument_id == "US_ACME"
    assert len(segments_arg) >= 1


# ── Orchestrator wiring ────────────────────────────────────────────────────


def test_orchestrator_registers_identity_repair_source(store: SQLiteEngineStore) -> None:
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    names = [s["name"] for s in orch.list_sources()]
    assert "identity_repair" in names


def test_source_registration_runs_after_market_providers(store: SQLiteEngineStore) -> None:
    """Repair must follow tiingo/eodhd so their break-detection has already
    flagged the affected instruments."""
    from ingestion.sources import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    order = [s["name"] for s in orch.list_sources()]
    assert order.index("tiingo_market") < order.index("identity_repair")
    assert order.index("eodhd_market") < order.index("identity_repair")


# ── Review-driven invariants ──────────────────────────────────────────────


def test_repair_keeps_break_status_when_no_refetch_callback(
    store: SQLiteEngineStore,
) -> None:
    """Writing segment metadata alone does not repair has_break_detected
    rows in market_price_bars, so without a refetch the instrument must
    stay in break_detected (not be promoted to stitched)."""
    _seed_broken_instrument(store)
    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="ACME", exchange="US", name="Acme Corporation",
            type="Common Stock", country="USA", currency="USD",
            isin="US00000A0001",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)  # no refetch callback
    report = service.repair(store, "US_ACME")

    assert report.segments_written >= 1
    assert report.final_history_status == "break_detected"
    assert "segments_discovered_pending_refetch" in report.notes
    assert store.get_market_instrument("US_ACME").history_status == "break_detected"


def test_repair_only_stitches_after_successful_refetch(
    store: SQLiteEngineStore,
) -> None:
    _seed_broken_instrument(store)
    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="ACME", exchange="US", name="Acme Corporation",
            type="Common Stock", country="USA", currency="USD",
            isin="US00000A0001",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_isin.return_value = []

    callback = Mock()
    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi, refetch_callback=callback)
    report = service.repair(store, "US_ACME")
    assert report.final_history_status == "stitched"
    callback.assert_called_once()


def test_repair_maps_nysearca_to_us_for_delisted_scan(
    store: SQLiteEngineStore,
) -> None:
    """Tiingo seeds NYSE Arca ETFs with exchange_code='NYSEARCA', but EODHD
    expects 'US'. The repair flow must translate before calling
    list_exchange_symbols."""
    _seed_broken_instrument(
        store,
        instrument_id="US_SPY",
        primary_ticker="SPY",
        name="SPDR S&P 500 ETF",
        isin="",
        exchange_code="NYSEARCA",
    )
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    service.repair(store, "US_SPY")

    # Verify the translated code ("US") was used, not "NYSEARCA".
    eodhd.list_exchange_symbols.assert_called_once()
    args, kwargs = eodhd.list_exchange_symbols.call_args
    assert args[0] == "US"
    assert kwargs.get("delisted") is True


def test_repair_filters_rename_events_by_exchange(store: SQLiteEngineStore) -> None:
    """A DE_SAP repair must not ingest US 'SAP' rename events."""
    _seed_broken_instrument(
        store,
        instrument_id="DE_SAP",
        primary_ticker="SAP",
        name="SAP SE",
        isin="",
        exchange_code="XETRA",
    )
    eodhd = Mock()
    eodhd.search.return_value = []
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = [
        EODHDSymbolChangeEvent(
            exchange="US", old_symbol="SAP", new_symbol="SAPX",
            company_name="Bogus US Co", effective="2024-03-15",
        ),
    ]
    openfigi = Mock()
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    service.repair(store, "DE_SAP")

    segments = store.list_symbol_segments("DE_SAP")
    # No US rename should have leaked into the German instrument.
    assert not any(s.ticker.endswith(".US") for s in segments)


def test_repair_falls_back_to_ticker_when_isin_missing(
    store: SQLiteEngineStore,
) -> None:
    """Indices carry no ISIN; the flow must search/map by ticker instead."""
    _seed_broken_instrument(
        store,
        instrument_id="JP_NIKKEI225",
        primary_ticker="N225",
        name="Nikkei 225 Index",
        isin="",
        exchange_code="INDX",
    )
    eodhd = Mock()
    eodhd.search.return_value = [
        EODHDSearchHit(
            code="N225", exchange="INDX", name="Nikkei 225 Index",
            type="INDEX", country="Japan", currency="JPY", isin="",
        ),
    ]
    eodhd.list_exchange_symbols.return_value = []
    eodhd.symbol_change_history.return_value = []
    openfigi = Mock()
    openfigi.map_by_ticker.return_value = []

    service = IdentityRepairService(eodhd=eodhd, openfigi=openfigi)
    report = service.repair(store, "JP_NIKKEI225")

    assert report.segments_written >= 1
    # Search was called with the ticker, since ISIN is empty.
    called_with = [c.args[0] for c in eodhd.search.call_args_list]
    assert "N225" in called_with
    # OpenFIGI fell back to map_by_ticker, not map_by_isin.
    openfigi.map_by_ticker.assert_called_once()
    assert not hasattr(openfigi.map_by_isin, "called") or not openfigi.map_by_isin.called
