"""ECB calendar scaffold tests: calendar_econ_fetch_ecb / calendar_econ_schedule_ecb wiring.

Split out of the original tests/test_ecb_calendar_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from pathlib import Path
import pytest
from ingestion.calendar.ecb_api import (
    INDICATOR_REGISTRY,
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    fetch_ecb_calendar,
    parse_observation,
    project_events,
    store_raw,
)
from ingestion.calendar.ecb_api.parser import PROVIDER, _content_hash
from ingestion.timeseries.sdmx._types import SDMXObservation
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_ecb", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert set(result["series_planned"]) == set(INDICATOR_REGISTRY.keys())


def test_service_op_honors_explicit_series_ids(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True, "series_ids": ["FM.B.U2.EUR.4F.KR.DFR.LEV"]},
    )
    assert result["series_planned"] == ["FM.B.U2.EUR.4F.KR.DFR.LEV"]


def test_service_op_dry_run_surfaces_unknown_series(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True,
         "series_ids": ["FM.B.U2.EUR.4F.KR.DFR.LEV", "BOGUS"]},
    )
    assert result["series_planned"] == ["FM.B.U2.EUR.4F.KR.DFR.LEV"]
    assert result["series_unknown"] == ["BOGUS"]


def test_service_op_passes_window_through(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_ecb",
        {"dry_run": True,
         "start_period": "2023-01-01", "end_period": "2024-01-01"},
    )
    assert result["start_period"] == "2023-01-01"
    assert result["end_period"] == "2024-01-01"
