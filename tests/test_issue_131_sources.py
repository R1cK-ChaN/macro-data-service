from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.sources import IngestionOrchestrator
from ingestion.timeseries._config import (
    IMF_SERIES,
    IMF_VINTAGE_SERIES,
    MACRO_SERIES,
    VINTAGE_SERIES,
)
from ingestion.timeseries.sdmx.providers.imf import IMFVintageObservation
from ingestion.timeseries.scrapers.fred import FredObservation, FredVintageObservation


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


def test_issue_131_fred_vintages_uses_fetcher_pipeline(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._vintages.time.sleep", lambda _seconds: None)
    calls: list[tuple[str, str]] = []

    class FakeFredClient:
        def get_vintages_with_raw(self, series_id: str, *, start_date: str):
            calls.append((series_id, start_date))
            payload = {
                "observations": [
                    {
                        "date": "2026-01-01",
                        "realtime_start": "2026-02-01",
                        "value": "1.0",
                    }
                ],
            }
            params = {"series_id": series_id, "observation_start": start_date}
            return [
                FredVintageObservation(series_id, "2026-01-01", "2026-02-01", 1.0)
            ], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.fred = SimpleNamespace(client=FakeFredClient())

    definition = orchestrator._build_fred_vintages_source()
    rows = definition.fetch()

    assert definition.execute is None
    assert {row.source for row in rows} == {"fred_vintages"}
    assert {row.storage_source for row in rows} == {"fred"}
    assert {series_id for series_id, _start in calls} == set(VINTAGE_SERIES)


def test_issue_131_fred_vintages_preserves_partial_failures(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._vintages.time.sleep", lambda _seconds: None)
    failed_series = VINTAGE_SERIES[1]
    calls: list[str] = []

    class FakeFredClient:
        def get_vintages_with_raw(self, series_id: str, *, start_date: str):
            calls.append(series_id)
            if series_id == failed_series:
                raise RuntimeError("alfred outage")
            payload = {
                "observations": [
                    {
                        "date": "2026-01-01",
                        "realtime_start": "2026-02-01",
                        "value": "1.0",
                    }
                ],
            }
            params = {"series_id": series_id, "observation_start": start_date}
            return [
                FredVintageObservation(series_id, "2026-01-01", "2026-02-01", 1.0)
            ], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.fred = SimpleNamespace(client=FakeFredClient())

    definition = orchestrator._build_fred_vintages_source()
    rows = definition.fetch()

    assert calls == list(VINTAGE_SERIES)
    assert {row.series_id for row in rows} == set(VINTAGE_SERIES) - {failed_series}


def test_issue_131_imf_vintages_uses_fetcher_pipeline() -> None:
    calls: list[tuple[str, str, str, tuple[str, ...], int]] = []

    class FakeIMFClient:
        def get_vintages_with_raw(
            self,
            dataflow_id: str,
            key: str,
            *,
            series_id: str,
            version: str,
            as_of_dates,
            limit: int,
        ):
            calls.append((dataflow_id, key, series_id, tuple(as_of_dates), limit))
            payload = {
                "dataflow_id": dataflow_id,
                "key": key,
                "series_id": series_id,
                "version": version,
                "responses": [],
            }
            params = {"series_id": series_id, "asOfDates": list(as_of_dates)}
            return [
                IMFVintageObservation(series_id, "2026-01-01", as_of_dates[0], 1.0, dataflow_id)
            ], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.imf = SimpleNamespace(client=FakeIMFClient())

    definition = orchestrator._build_imf_vintages_source()
    rows = definition.fetch()

    expected_series_ids = {
        IMF_SERIES[series_key]["series_id"]
        for series_key in IMF_VINTAGE_SERIES
    }
    assert definition.execute is None
    assert {row.source for row in rows} == {"imf_vintages"}
    assert {row.storage_source for row in rows} == {"imf"}
    assert {row.vintage_quality for row in rows} == {"synthetic_snapshot"}
    assert {row.series_id for row in rows} == expected_series_ids
    assert {limit for *_prefix, limit in calls} == {30}
    assert {len(as_of_dates) for *_prefix, as_of_dates, _limit in calls} == {12}


def test_issue_131_imf_vintages_preserves_partial_failures() -> None:
    failed_key = IMF_VINTAGE_SERIES[1]
    calls: list[str] = []

    class FakeIMFClient:
        def get_vintages_with_raw(
            self,
            dataflow_id: str,
            key: str,
            *,
            series_id: str,
            version: str,
            as_of_dates,
            limit: int,
        ):
            calls.append(series_id)
            if series_id == IMF_SERIES[failed_key]["series_id"]:
                raise RuntimeError("imf outage")
            payload = {
                "dataflow_id": dataflow_id,
                "key": key,
                "series_id": series_id,
                "version": version,
                "responses": [],
            }
            params = {"series_id": series_id, "asOfDates": list(as_of_dates)}
            return [
                IMFVintageObservation(series_id, "2026-01-01", as_of_dates[0], 1.0, dataflow_id)
            ], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.imf = SimpleNamespace(client=FakeIMFClient())

    definition = orchestrator._build_imf_vintages_source()
    rows = definition.fetch()

    expected_series_ids = [
        IMF_SERIES[series_key]["series_id"]
        for series_key in IMF_VINTAGE_SERIES
    ]
    assert calls == expected_series_ids
    assert {row.series_id for row in rows} == set(expected_series_ids) - {IMF_SERIES[failed_key]["series_id"]}
