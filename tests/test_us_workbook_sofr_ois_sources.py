from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.market._bloomberg_rates import BLOOMBERG_RATE_UNIVERSE
from ingestion.market.clients._bloomberg_rates import BloombergRatesFileProvider
from ingestion.market.scrapers._bloomberg_rates import (
    BloombergRateObservation,
    parse_bloomberg_rate_csv,
)
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


def test_bloomberg_rate_universe_maps_workbook_ussoc_security() -> None:
    entry = BLOOMBERG_RATE_UNIVERSE[0]

    assert entry.instrument_id == "RATES_USD_OIS_3M_BLOOMBERG"
    assert entry.ticker == "USSOC"
    assert entry.provider_symbol == "USSOC BGN Curncy"
    assert entry.curve == "USD OIS"
    assert entry.tenor == "3M"
    assert entry.unit == "percent"


def test_bloomberg_rate_csv_parser_accepts_px_last_and_bid_ask_shapes() -> None:
    entry = BLOOMBERG_RATE_UNIVERSE[0]
    px_last_rows, raw_rows = parse_bloomberg_rate_csv(
        "Dates,PX_LAST\n2026-04-30,3.6558\n2026-05-01,3.6473%\n",
        entry=entry,
    )

    assert [row.date for row in px_last_rows] == ["2026-04-30", "2026-05-01"]
    assert [row.value for row in px_last_rows] == [3.6558, 3.6473]
    assert raw_rows[0]["PX_LAST"] == "3.6558"

    bid_ask_rows, _ = parse_bloomberg_rate_csv(
        "Date,3 Month Bid,3 Month Ask\n1st May 2026,3.6473,3.6673\n",
        entry=entry,
    )
    assert bid_ask_rows == [
        BloombergRateObservation(
            date="2026-05-01",
            value=3.6573,
            metadata={
                "curve": "USD OIS",
                "tenor": "3M",
                "provider": "bloomberg",
                "provider_symbol": "USSOC BGN Curncy",
                "value_column": "3monthbid/3monthask",
                "bid": 3.6473,
                "ask": 3.6673,
            },
        )
    ]

    timestamp_rows, _ = parse_bloomberg_rate_csv(
        "Timestamp,PX_LAST\n"
        "2026-05-01T13:30:00Z,3.6473\n"
        "2026-05-02T13:30:00Z,NaN\n",
        entry=entry,
    )
    assert timestamp_rows == [
        BloombergRateObservation(
            date="2026-05-01",
            value=3.6473,
            metadata={
                "curve": "USD OIS",
                "tenor": "3M",
                "provider": "bloomberg",
                "provider_symbol": "USSOC BGN Curncy",
                "value_column": "pxlast",
            },
        )
    ]


def test_bloomberg_rates_provider_stores_market_bars_raw_and_subject_links(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    csv_path = tmp_path / "ussoc.csv"
    csv_path.write_text(
        "Date,PX_LAST\n2026-04-30,3.6558\n2026-05-01,3.6473\n",
        encoding="utf-8",
    )
    provider = BloombergRatesFileProvider(
        file_map={"USSOC BGN Curncy": csv_path}
    )

    stats = provider.refresh_market_history(store, "USSOC")

    assert stats.count == 2
    instrument = store.get_market_instrument("RATES_USD_OIS_3M_BLOOMBERG")
    assert instrument is not None
    assert instrument.primary_provider == "bloomberg"
    assert instrument.provider_symbols_json == {"bloomberg": "USSOC BGN Curncy"}

    bars = store.list_market_price_bars("RATES_USD_OIS_3M_BLOOMBERG")
    assert [bar.date for bar in bars] == ["2026-04-30", "2026-05-01"]
    assert [bar.close for bar in bars] == [3.6558, 3.6473]
    assert all(bar.source_name == "Bloomberg BGN" for bar in bars)
    assert all(bar.has_missing_corp_acts is False for bar in bars)
    assert bars[-1].quality_flags_json["provider_symbol"] == "USSOC BGN Curncy"

    raw = store.latest_market_price_bars_raw("bloomberg", "USSOC BGN Curncy")
    assert raw is not None
    request_params = json.loads(raw.request_params_json)
    assert request_params["path"] == str(csv_path)
    assert request_params["instrument_id"] == "RATES_USD_OIS_3M_BLOOMBERG"

    sync_from_yaml(store)
    subject_rows = store.list_subject_market_bars("rate.us.sofr", limit=10)
    assert subject_rows
    assert subject_rows[0]["instrument_id"] == "RATES_USD_OIS_3M_BLOOMBERG"


def test_bloomberg_rates_orchestrator_and_capability_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    csv_path = tmp_path / "ussoc.csv"
    csv_path.write_text("Date,PX_LAST\n2026-05-01,3.6473\n", encoding="utf-8")
    provider = BloombergRatesFileProvider(file_map={"USSOC": csv_path})
    orchestrator = IngestionOrchestrator(store, bloomberg_rates=provider)

    names = {source["name"] for source in orchestrator.list_sources()}
    assert "bloomberg_rates" in names

    report = orchestrator.run_source("bloomberg_rates")
    assert report.stored == 1
    bars = store.list_market_price_bars("RATES_USD_OIS_3M_BLOOMBERG")
    assert len(bars) == 1

    manager = SourceCapabilityManager(store, orchestrator=orchestrator)
    entities = manager.list_entities(
        "bloomberg_rates", query="USSOC", limit=5,
    )["entities"]
    assert len(entities) == 1
    assert entities[0]["entity_id"] == "USSOC BGN Curncy"
    assert entities[0]["metadata"]["instrument_id"] == (
        "RATES_USD_OIS_3M_BLOOMBERG"
    )

    health_source = next(
        source for source in manager.get_customer_health()["sources"]
        if source["source_id"] == "bloomberg_rates"
    )
    assert health_source["status"] == "healthy"
    assert health_source["storage_table"] == "market_price_bars"
    assert health_source["record_count"] == 1


def test_bloomberg_rates_health_stays_empty_without_saved_bars(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    orchestrator = IngestionOrchestrator(store)
    manager = SourceCapabilityManager(store, orchestrator=orchestrator)

    entities = manager.list_entities("bloomberg_rates")["entities"]
    result = manager.sync_latest("bloomberg_rates")

    assert len(entities) == 1
    assert result["status"] == "success"
    assert result["observations_synced"] == 0

    health_source = next(
        source for source in manager.get_customer_health()["sources"]
        if source["source_id"] == "bloomberg_rates"
    )
    assert health_source["status"] == "empty"
    assert health_source["storage_table"] == "market_price_bars"
    assert health_source["record_count"] == 0
