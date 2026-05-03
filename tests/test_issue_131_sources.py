from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from storage import IndicatorObservationRecord
from ingestion.sources import IngestionOrchestrator
from ingestion.timeseries._config import (
    EIA_SERIES,
    IMF_SERIES,
    IMF_VINTAGE_SERIES,
    MACRO_SERIES,
    VINTAGE_SERIES,
)
from ingestion.timeseries.scrapers.eia import EIAObservation
from ingestion.timeseries.sdmx.providers.imf import IMFVintageObservation
from ingestion.timeseries.scrapers.fred import FredObservation, FredVintageObservation
from ingestion.timeseries.scrapers.worldbank import (
    WorldBankIndicatorInfo,
    WorldBankObservation,
)


def _bare_orchestrator() -> IngestionOrchestrator:
    orchestrator = IngestionOrchestrator.__new__(IngestionOrchestrator)
    orchestrator._validation = None
    orchestrator._last_run_reports = {}
    orchestrator._family_lookup = {}
    orchestrator._ensure_obs_seed = lambda: None
    orchestrator._fetch_with_obs_raw = (
        lambda fetcher, *, lookback_days: fetcher.fetch(lookback_days=lookback_days)
    )
    orchestrator._raw_series_to_records = lambda rows: []
    orchestrator._deduplicate_observations = lambda rows: rows
    orchestrator._store_indicator_observations = lambda rows: len(rows)
    return orchestrator


def _raw_series_to_indicator_records(raw_series_list):
    return [
        IndicatorObservationRecord(
            series_id=rs.series_id,
            source=rs.source,
            date=obs.date,
            value=obs.value,
            metadata={**obs.provider_metadata, **rs.series_metadata},
        )
        for rs in raw_series_list
        for obs in rs.observations
    ]


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


def test_issue_131_worldbank_catalog_uses_fetcher_pipeline() -> None:
    calls: list[tuple[str, str, int, int, bool]] = []

    class FakeWorldBankClient:
        def list_indicators(self, *, source_id=None, topic_id=None):
            return [
                WorldBankIndicatorInfo(
                    id="SP.POP.TOTL",
                    name="Population, total",
                    source_name="World Development Indicators",
                )
            ]

        def get_indicator_with_raw(
            self,
            indicator_code: str,
            country: str,
            *,
            series_id: str,
            limit: int,
            per_page: int,
            fetch_all_pages: bool,
        ):
            calls.append((indicator_code, country, limit, per_page, fetch_all_pages))
            payload = {
                "response": [
                    {"total": 2, "page": 1, "pages": 1, "per_page": per_page},
                    [
                        {"countryiso3code": "USA", "date": "2025", "value": 1.0},
                        {"countryiso3code": "JPN", "date": "2025", "value": 2.0},
                    ],
                ]
            }
            params = {"indicator": indicator_code, "country": country}
            return [
                WorldBankObservation(series_id, "2025-01-01", 1.0, indicator_code, "US", "United States"),
                WorldBankObservation(series_id, "2025-01-01", 2.0, indicator_code, "JP", "Japan"),
            ], payload, params

    orchestrator = _bare_orchestrator()
    orchestrator.worldbank = SimpleNamespace(client=FakeWorldBankClient())

    definition = orchestrator._build_worldbank_catalog_source()
    rows = definition.fetch()
    records = definition.normalize(rows)

    assert definition.execute is None
    assert {row.source for row in rows} == {"worldbank_catalog"}
    assert {row.series_id for row in rows} == {"WB_SP.POP.TOTL"}
    assert {record.source for record in records} == {"worldbank"}
    assert {record.series_id for record in records} == {
        "WB_SP.POP.TOTL_US",
        "WB_SP.POP.TOTL_JP",
    }
    assert {record.metadata["category"] for record in records} == {"catalog"}
    assert calls == [("SP.POP.TOTL", "all", 1500, 1000, True)]


def test_issue_131_eia_uses_fetcher_pipeline(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._eia.time.sleep", lambda _seconds: None)
    calls: list[tuple[str, int]] = []
    stored: list[IndicatorObservationRecord] = []

    class FakeEIAClient:
        def get_series_with_raw(self, route, *, params, series_id, limit):
            calls.append((series_id, limit))
            payload = {
                "response": {
                    "data": [
                        {"period": "2026-01-01", "value": 82.5, "units": "usd"},
                    ],
                }
            }
            return [
                EIAObservation(series_id, "2026-01-01", 82.5, "usd")
            ], payload, {"route": route}

    class FakeFredClient:
        def get_series_with_raw(self, *args, **kwargs):
            raise AssertionError("fred fallback should stay idle")

    orchestrator = _bare_orchestrator()
    orchestrator.eia = SimpleNamespace(client=FakeEIAClient(), _fred=FakeFredClient())
    orchestrator.store = SimpleNamespace(get_indicator_history=lambda _series_id, limit: [])
    orchestrator._raw_series_to_records = _raw_series_to_indicator_records
    orchestrator._store_indicator_observations = (
        lambda rows: stored.extend(rows) or len(rows)
    )

    report = orchestrator._run_definition(orchestrator._build_eia_source())

    assert report.source == "eia"
    assert report.stored == len(EIA_SERIES)
    assert calls[0] == ("EIA_BRENT", 30)
    assert {record.source for record in stored} == {"eia"}
    assert stored[0].metadata == {"category": "energy", "unit": "usd"}


def test_issue_131_eia_route_counts_recent_cache(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._eia.time.sleep", lambda _seconds: None)
    stored: list[IndicatorObservationRecord] = []

    class FakeEIAClient:
        def get_series_with_raw(self, route, *, params, series_id, limit):
            raise RuntimeError("upstream down")

    class FakeFredClient:
        def get_series_with_raw(self, *args, **kwargs):
            raise AssertionError("cache hit should short-circuit fallback")

    orchestrator = _bare_orchestrator()
    orchestrator.eia = SimpleNamespace(client=FakeEIAClient(), _fred=FakeFredClient())
    orchestrator.store = SimpleNamespace(
        get_indicator_history=lambda _series_id, limit: [
            SimpleNamespace(date="2099-01-01")
        ],
    )
    orchestrator._raw_series_to_records = _raw_series_to_indicator_records
    orchestrator._store_indicator_observations = (
        lambda rows: stored.extend(rows) or len(rows)
    )

    report = orchestrator._run_definition(orchestrator._build_eia_source())

    assert report.source == "eia"
    assert report.stored == len(EIA_SERIES)
    assert stored == []


def test_issue_131_eia_route_uses_fred_fallback(monkeypatch) -> None:
    monkeypatch.setattr("ingestion.timeseries.fetchers._eia.time.sleep", lambda _seconds: None)
    stored: list[IndicatorObservationRecord] = []

    class FakeEIAClient:
        def get_series_with_raw(self, route, *, params, series_id, limit):
            return [], {}, {"route": route}

    class FakeFredClient:
        def get_series_with_raw(self, series_id, *, start_date, limit):
            payload = {
                "observations": [
                    {"date": "2026-03-19", "value": "68.5"},
                ],
            }
            return [
                FredObservation(series_id, "2026-03-19", 68.5)
            ], payload, {"series_id": series_id, "observation_start": start_date}

    orchestrator = _bare_orchestrator()
    orchestrator.eia = SimpleNamespace(client=FakeEIAClient(), _fred=FakeFredClient())
    orchestrator.store = SimpleNamespace(get_indicator_history=lambda _series_id, limit: [])
    orchestrator._raw_series_to_records = _raw_series_to_indicator_records
    orchestrator._store_indicator_observations = (
        lambda rows: stored.extend(rows) or len(rows)
    )

    definition = orchestrator._build_eia_source()
    rows = definition.fetch()
    fallback_rows = [row for row in rows if row.series_id == "EIA_BRENT"]
    records = definition.normalize(fallback_rows)
    stored_count = definition.store(records)

    assert definition.execute is None
    assert fallback_rows[0].raw_payload["fallback_source"] == "fred"
    assert fallback_rows[0].content_hash is not None
    assert stored_count == 1
    assert stored[0].series_id == "EIA_BRENT"
    assert stored[0].metadata["fallback_source"] == "fred"
