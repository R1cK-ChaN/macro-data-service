from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.sources import IngestionOrchestrator
from ingestion.timeseries._config import MACRO_SERIES
from ingestion.timeseries.scrapers.fred import FredObservation


def _bare_orchestrator() -> IngestionOrchestrator:
    orchestrator = IngestionOrchestrator.__new__(IngestionOrchestrator)
    orchestrator._validation = None
    orchestrator._last_run_reports = {}
    orchestrator._ensure_obs_seed = lambda: None
    orchestrator._fetch_with_obs_raw = (
        lambda fetcher, *, lookback_days: fetcher.fetch(lookback_days=lookback_days)
    )
    orchestrator._raw_series_to_records = lambda rows: []
    orchestrator._deduplicate_observations = lambda rows: rows
    orchestrator._store_indicator_observations = lambda rows: len(rows)
    return orchestrator


def test_issue_131_fred_daily_uses_fetcher_pipeline(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._fred.time.sleep", lambda _seconds: None)
    calls: list[tuple[str, str, int]] = []

    class FakeFredClient:
        def get_series_with_raw(self, series_id: str, *, start_date: str, limit: int):
            calls.append((series_id, start_date, limit))
            payload = {"observations": [{"date": "2026-01-01", "value": "1.0"}]}
            params = {"series_id": series_id, "observation_start": start_date}
            return [FredObservation(series_id, "2026-01-01", 1.0)], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.fred = SimpleNamespace(client=FakeFredClient())

    definition = orchestrator._build_fred_daily_source()
    rows = definition.fetch()

    daily = {sid for sid, meta in MACRO_SERIES.items() if meta["freq"] == "daily"}
    assert definition.execute is None
    assert {series_id for series_id, _start, _limit in calls} == daily
    assert {limit for _series_id, _start, limit in calls} == {5}
    assert {row.series_id for row in rows} == daily


def test_issue_131_fred_nondaily_uses_fetcher_pipeline(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._fred.time.sleep", lambda _seconds: None)
    calls: list[tuple[str, str, int]] = []

    class FakeFredClient:
        def get_series_with_raw(self, series_id: str, *, start_date: str, limit: int):
            calls.append((series_id, start_date, limit))
            payload = {"observations": [{"date": "2026-01-01", "value": "1.0"}]}
            params = {"series_id": series_id, "observation_start": start_date}
            return [FredObservation(series_id, "2026-01-01", 1.0)], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.fred = SimpleNamespace(client=FakeFredClient())

    definition = orchestrator._build_fred_nondaily_source()
    rows = definition.fetch()

    nondaily = {sid for sid, meta in MACRO_SERIES.items() if meta["freq"] != "daily"}
    assert definition.execute is None
    assert {series_id for series_id, _start, _limit in calls} == nondaily
    assert {limit for _series_id, _start, limit in calls} == {10}
    assert {row.series_id for row in rows} == nondaily


def test_issue_131_fred_daily_surfaces_fetch_errors() -> None:
    class BrokenFredClient:
        def get_series_with_raw(self, series_id: str, *, start_date: str, limit: int):
            raise RuntimeError("fred outage")

    orchestrator = _bare_orchestrator()
    orchestrator.fred = SimpleNamespace(client=BrokenFredClient())

    report = orchestrator._run_definition(orchestrator._build_fred_daily_source())

    assert report.source == "fred_daily"
    assert report.stored == 0
    assert report.error == "fred outage"
