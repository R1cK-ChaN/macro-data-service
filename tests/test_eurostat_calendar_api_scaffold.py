"""Mocked tests for the Eurostat calendar connector (issue #15 P1)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.eurostat_api import (
    INDICATOR_REGISTRY,
    EurostatCalendarEventRecord,
    EurostatCalendarRawRecord,
    fetch_eurostat_calendar,
    fetch_release_calendar_json,
    parse_observation,
    parse_release_calendar_json,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    schedule_eurostat_calendar,
    store_raw,
)
from ingestion.calendar.eurostat_api.parser import PROVIDER
from ingestion.timeseries.sdmx._types import SDMXObservation
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(name: str) -> str:
    return (Path(__file__).parent / "fixtures" / "eurostat_calendar" / name).read_text()


def _hicp_obs(value: float = 2.5, date_str: str = "2026-03-01") -> SDMXObservation:
    return SDMXObservation(
        series_id="EUROSTAT_HICP_FLASH_YOY",
        date=date_str,
        value=value,
        dataset="prc_hicp_fpd",
    )


def _gdp_obs(value: float = 0.3, date_str: str = "2026-01-01") -> SDMXObservation:
    return SDMXObservation(
        series_id="EUROSTAT_GDP_FLASH_QOQ",
        date=date_str,
        value=value,
        dataset="namq_10_gdp",
    )


class _FakeEurostatClient:
    def __init__(self, by_series_id: dict[str, list[SDMXObservation]]):
        self._data = by_series_id
        self.calls: list[dict] = []

    def get_dataset(
        self,
        dataset_code,
        *,
        params=None,
        series_id,
        limit=100,
    ) -> list[SDMXObservation]:
        self.calls.append({
            "dataset_code": dataset_code,
            "params": dict(params or {}),
            "series_id": series_id,
            "limit": limit,
        })
        rows = list(self._data.get(series_id, []))
        if limit and limit > 0:
            rows = rows[:limit]
        return rows


def test_registry_contains_issue_15_p1_anchors() -> None:
    hicp = INDICATOR_REGISTRY["EUROSTAT_HICP_FLASH_YOY"]
    assert hicp.dataset == "prc_hicp_fpd"
    assert dict(hicp.params) == {
        "unit": "RCH_A",
        "coicop18": "TOTAL",
        "release": "FLS",
        "geo": "EA20",
    }
    assert hicp.country_code == "EU"
    assert hicp.importance == "high"

    gdp = INDICATOR_REGISTRY["EUROSTAT_GDP_FLASH_QOQ"]
    assert gdp.dataset == "namq_10_gdp"
    assert gdp.reference_cadence == "quarterly"

    unemployment = INDICATOR_REGISTRY["EUROSTAT_UNEMPLOYMENT_RATE"]
    assert unemployment.dataset == "une_rt_m"
    assert unemployment.indicator == "Unemployment Rate"


def test_parser_projects_monthly_reference_to_period_end() -> None:
    _, event = parse_observation(
        _hicp_obs(value=2.5, date_str="2026-03-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "eurostat"
    assert event.event_time_utc == "2026-03-31T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.reference_date == "2026-03-01"
    assert event.title == "Euro Area CPI Flash YoY"
    assert event.actual == "2.5"
    assert event.source == "Eurostat"


def test_parser_projects_quarterly_reference_to_quarter_end() -> None:
    _, event = parse_observation(
        _gdp_obs(value=0.3, date_str="2026-01-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.event_time_utc == "2026-03-31T00:00:00+00:00"
    assert event.reference_date == "2026-03-31"
    assert event.title == "Euro Area GDP Flash QoQ"


def test_parser_synthesises_revision_stable_event_id() -> None:
    a = parse_observation(
        _hicp_obs(value=2.5, date_str="2026-03-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )[1]
    b = parse_observation(
        _hicp_obs(value=2.6, date_str="2026-03-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )[1]
    c = parse_observation(
        _hicp_obs(value=2.5, date_str="2026-02-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )[1]
    assert a.provider_event_id == b.provider_event_id
    assert a.provider_event_id != c.provider_event_id


def test_schedule_parser_extracts_whitelisted_releases() -> None:
    entries = parse_release_calendar_json(_fixture_text("events_2026.json"))
    assert [e.series_id for e in entries] == [
        "EUROSTAT_HICP_FLASH_YOY",
        "EUROSTAT_GDP_FLASH_QOQ",
        "EUROSTAT_UNEMPLOYMENT_RATE",
    ]
    assert entries[0].reference_date == "2026-02-01"
    assert entries[0].event_time_utc == "2026-03-03T11:00:00+00:00"
    assert entries[1].reference_date == "2026-03-31"
    assert entries[2].reference_date == "2026-03-01"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_calendar_json(
        _fixture_text("events_2026.json"),
        series_ids={"EUROSTAT_GDP_FLASH_QOQ"},
    )
    assert [e.series_id for e in entries] == ["EUROSTAT_GDP_FLASH_QOQ"]


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_release_calendar_json(_fixture_text("events_2026.json"))[0]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    _, value_event = parse_observation(
        _hicp_obs(value=1.9, date_str="2026-02-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_and_value_share_gdp_id_on_quarter_end_reference() -> None:
    entry = parse_release_calendar_json(_fixture_text("events_2026.json"))[1]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    _, value_event = parse_observation(
        _gdp_obs(value=0.3, date_str="2026-01-01"),
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.reference_date == "2026-03-31"
    assert value_event.reference_date == "2026-03-31"
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_then_value_merge_preserves_datetime_and_adds_actual(
    store: SQLiteEngineStore,
) -> None:
    entry = parse_release_calendar_json(_fixture_text("events_2026.json"))[0]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    raw_value, event_value = parse_observation(
        _hicp_obs(value=1.9, date_str="2026-02-01"),
        snapshot_epoch_ms=1_800_000_001_000,
    )
    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        store_raw(conn, [raw_value])
        project_events(conn, [event_value])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider = 'eurostat'"
        ).fetchone()
    assert row[0] == "2026-03-03T11:00:00+00:00"
    assert row[1] == "datetime"
    assert row[2] == "1.9"


def test_fetcher_filters_window_and_surfaces_unknown(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeEurostatClient({
        "EUROSTAT_HICP_FLASH_YOY": [
            _hicp_obs(value=1.7, date_str="2026-01-01"),
            _hicp_obs(value=1.9, date_str="2026-02-01"),
        ],
    })
    with store.get_connection() as conn:
        summary = fetch_eurostat_calendar(
            conn,
            client,  # type: ignore[arg-type]
            start_period="2026-02",
            end_period="2026-02",
            series_ids=["EUROSTAT_HICP_FLASH_YOY", "UNKNOWN"],
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        row = conn.execute(
            "SELECT actual, reference_date FROM cal_econ_event "
            "WHERE provider = 'eurostat'"
        ).fetchone()
    assert summary.series_ok == ["EUROSTAT_HICP_FLASH_YOY"]
    assert summary.series_unknown == ["UNKNOWN"]
    assert summary.observations_seen == 1
    assert tuple(row) == ("1.9", "2026-02-01")
    assert client.calls[0]["dataset_code"] == "prc_hicp_fpd"
    assert client.calls[0]["params"]["coicop18"] == "TOTAL"


def test_fetcher_accepts_quarterly_bounds_for_gdp(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeEurostatClient({
        "EUROSTAT_GDP_FLASH_QOQ": [
            _gdp_obs(value=0.3, date_str="2026-01-01"),
            _gdp_obs(value=0.4, date_str="2026-04-01"),
        ],
    })
    with store.get_connection() as conn:
        summary = fetch_eurostat_calendar(
            conn,
            client,  # type: ignore[arg-type]
            start_period="2026Q1",
            end_period="2026-Q1",
            series_ids=["EUROSTAT_GDP_FLASH_QOQ"],
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        row = conn.execute(
            "SELECT actual, reference_date FROM cal_econ_event "
            "WHERE provider = 'eurostat'"
        ).fetchone()
    assert summary.series_ok == ["EUROSTAT_GDP_FLASH_QOQ"]
    assert summary.observations_seen == 1
    assert tuple(row) == ("0.3", "2026-03-31")


def test_release_calendar_fetch_uses_inclusive_end_date() -> None:
    class _Response:
        text = "[]"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.params = None

        def get(self, url, *, params, headers, timeout):  # noqa: ANN001
            self.params = params
            return _Response()

    session = _Session()
    assert fetch_release_calendar_json(
        date(2026, 3, 3),
        date(2026, 3, 3),
        session=session,  # type: ignore[arg-type]
    ) == "[]"
    assert session.params["start"].startswith("2026-03-03T00:00:00")
    assert session.params["end"].startswith("2026-03-03T23:59:59")


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    def _fetch(start_date, end_date, *, session=None):
        assert start_date.isoformat() == "2026-03-01"
        assert end_date.isoformat() == "2026-05-31"
        return _fixture_text("events_2026.json")

    with store.get_connection() as conn:
        summary = schedule_eurostat_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-05-31",
            dry_run=False,
            json_fetcher=_fetch,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'eurostat'"
        ).fetchone()[0]
    assert summary.entries_parsed == 3
    assert summary.series_ok == [
        "EUROSTAT_HICP_FLASH_YOY",
        "EUROSTAT_GDP_FLASH_QOQ",
        "EUROSTAT_UNEMPLOYMENT_RATE",
    ]
    assert count == 3


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_eurostat", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_eurostat",
        {"dry_run": True, "series_ids": ["EUROSTAT_GDP_FLASH_QOQ"]},
    )
    assert schedule_result["series_planned"] == ["EUROSTAT_GDP_FLASH_QOQ"]
    assert schedule_result["series_unknown"] == []


def test_canonical_aliases_cover_eurostat_titles() -> None:
    assert canonicalize_indicator("Flash estimate inflation euro area") == "CPI"
    assert canonicalize_indicator("Euro Area CPI Flash YoY") == "CPI"
    assert (
        canonicalize_indicator("Preliminary flash estimate GDP - EU and euro area")
        == "GDP"
    )
    assert canonicalize_indicator("Euro Area GDP Flash QoQ") == "GDP"
    assert canonicalize_indicator("Euro area unemployment rate") == "UNEMPLOYMENT_RATE"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert EurostatCalendarRawRecord.__name__ == "EurostatCalendarRawRecord"
    assert EurostatCalendarEventRecord.__name__ == "EurostatCalendarEventRecord"
