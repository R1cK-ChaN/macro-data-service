"""Mocked tests for the Census calendar connector (issue #13 P1)."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.census_api import (
    INDICATOR_REGISTRY,
    CensusEITSObservation,
    CensusScheduleEntry,
    fetch_census_calendar,
    parse_observation,
    parse_schedule_html,
    project_events,
    project_schedule_events,
    schedule_census_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.calendar.census_api.parser import PROVIDER, _content_hash
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "census_schedule"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html() -> str:
    return (FIXTURE_DIR / "calendar-listview.html").read_text(encoding="utf-8")


def _retail_obs(value: str = "0.7", time: str = "2024-04") -> CensusEITSObservation:
    raw = {
        "data_type_code": "MPCSM",
        "seasonally_adj": "yes",
        "category_code": "44X72",
        "cell_value": value,
        "error_data": "no",
        "time_slot_id": "0",
        "time_slot_name": "Apr2024",
        "time": time,
        "us": "1",
    }
    return CensusEITSObservation(
        series_id="CENSUS_EITS_MARTS_RETAIL_SALES_MOM",
        dataset="marts",
        time=time,
        data_type_code="MPCSM",
        category_code="44X72",
        seasonally_adj="yes",
        time_slot_id="0",
        time_slot_name="Apr2024",
        cell_value=value,
        error_data="no",
        raw=raw,
    )


class _FakeCensusClient:
    def __init__(self, data: dict[tuple[str, int], list[dict[str, str]]]):
        self._data = data
        self.requests_made = 0
        self.calls: list[tuple[str, int]] = []

    def get_dataset_year(self, dataset: str, year: int) -> list[dict[str, str]]:
        self.requests_made += 1
        self.calls.append((dataset, year))
        return self._data.get((dataset, year), [])


def test_registry_contains_issue13_census_anchors() -> None:
    specs = INDICATOR_REGISTRY
    assert specs["CENSUS_EITS_MARTS_RETAIL_SALES_MOM"].dataset == "marts"
    assert specs["CENSUS_EITS_ADVM3_DURABLE_GOODS_ORDERS_MOM"].dataset == "advm3"
    assert specs["CENSUS_EITS_RESCONST_HOUSING_STARTS"].category_code == "ASTARTS"
    assert specs["CENSUS_EITS_RESCONST_BUILDING_PERMITS"].category_code == "APERMITS"
    assert all(spec.country_code == "US" for spec in specs.values())


def test_parser_projects_monthly_reference_with_approximate_time() -> None:
    raw, event = parse_observation(
        _retail_obs(),
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert raw.provider == PROVIDER == "census"
    assert event.event_time_utc == "2024-04-30T00:00:00+00:00"
    assert event.event_time_precision == "approximate"
    assert event.reference_date == "2024-04-01"
    assert event.reference_label == "2024-04"
    assert event.actual == "0.7"
    assert event.title == "Retail Sales MoM"
    assert event.source_url.endswith("/marts")


def test_parser_synthesizes_stable_event_id_for_revisions() -> None:
    first = parse_observation(
        _retail_obs(value="0.7"),
        snapshot_epoch_ms=1_700_000_000_000,
    )[1]
    revised = parse_observation(
        _retail_obs(value="0.9"),
        snapshot_epoch_ms=1_700_000_100_000,
    )[1]
    assert first.provider_event_id == revised.provider_event_id
    assert first.content_hash != revised.content_hash


def test_parser_hashes_value_and_error_metadata() -> None:
    base = _content_hash({
        "cell_value": "0.7",
        "error_data": "no",
        "time_slot_id": "0",
        "time_slot_name": "Apr2024",
    })
    same = _content_hash({
        "cell_value": "0.7",
        "error_data": "no",
        "time_slot_id": "0",
        "time_slot_name": "Apr2024",
    })
    revised = _content_hash({
        "cell_value": "0.8",
        "error_data": "no",
        "time_slot_id": "0",
        "time_slot_name": "Apr2024",
    })
    assert base == same
    assert base != revised


def test_fetch_census_calendar_filters_dataset_year_rows(
    store: SQLiteEngineStore,
) -> None:
    client = _FakeCensusClient({
        ("marts", 2024): [
            {
                "data_type_code": "MPCSM",
                "seasonally_adj": "yes",
                "category_code": "44X72",
                "cell_value": "0.7",
                "error_data": "no",
                "time_slot_id": "0",
                "time_slot_name": "Apr2024",
                "time": "2024-04",
                "us": "1",
            },
            {
                "data_type_code": "SM",
                "seasonally_adj": "yes",
                "category_code": "44X72",
                "cell_value": "558388",
                "error_data": "no",
                "time_slot_id": "0",
                "time_slot_name": "Apr2024",
                "time": "2024-04",
                "us": "1",
            },
        ],
    })
    with store._connection(commit=True) as conn:
        summary = fetch_census_calendar(
            conn,
            client,
            start_year=2024,
            end_year=2024,
            series_ids=["CENSUS_EITS_MARTS_RETAIL_SALES_MOM"],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
        )

    assert summary.observations_seen == 1
    assert summary.rows_raw_inserted == 1
    assert summary.events_upserted == 1
    assert summary.series_ok == ["CENSUS_EITS_MARTS_RETAIL_SALES_MOM"]
    assert summary.requests_made == 1
    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT provider, title, actual FROM cal_econ_event"
        ).fetchone()
    assert tuple(row) == ("census", "Retail Sales MoM", "0.7")


def test_fetch_dry_run_surfaces_unknown_series(store: SQLiteEngineStore) -> None:
    client = _FakeCensusClient({})
    with store._connection(commit=False) as conn:
        summary = fetch_census_calendar(
            conn,
            client,
            start_year=2024,
            end_year=2024,
            series_ids=["UNKNOWN"],
            dry_run=True,
        )
    assert summary.series_planned == []
    assert summary.series_unknown == ["UNKNOWN"]
    assert client.calls == []


def test_parse_schedule_html_extracts_whitelisted_entries() -> None:
    entries = parse_schedule_html(_fixture_html())
    by_series: dict[str, int] = {}
    for entry in entries:
        by_series[entry.series_id] = by_series.get(entry.series_id, 0) + 1
    assert by_series == {
        "CENSUS_EITS_RESCONST_HOUSING_STARTS": 1,
        "CENSUS_EITS_RESCONST_BUILDING_PERMITS": 1,
        "CENSUS_EITS_MARTS_RETAIL_SALES_MOM": 1,
        "CENSUS_EITS_ADVM3_DURABLE_GOODS_ORDERS_MOM": 1,
    }


def test_parse_schedule_uses_dst_aware_eastern_time() -> None:
    entries = parse_schedule_html(_fixture_html())
    winter = next(
        e for e in entries
        if e.series_id == "CENSUS_EITS_MARTS_RETAIL_SALES_MOM"
    )
    summer = next(
        e for e in entries
        if e.series_id == "CENSUS_EITS_ADVM3_DURABLE_GOODS_ORDERS_MOM"
    )
    assert "T13:30" in winter.event_time_utc
    assert "T12:30" in summer.event_time_utc


def test_schedule_entry_id_matches_value_side_id() -> None:
    entry = CensusScheduleEntry(
        series_id="CENSUS_EITS_MARTS_RETAIL_SALES_MOM",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_title="Advance Monthly Sales for Retail and Food Services",
        release_date="2024-05-15",
        release_time_local="8:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
        source_url="https://www.census.gov/retail",
    )
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    _, value_event = parse_observation(
        _retail_obs(time="2024-04"),
        snapshot_epoch_ms=1_700_000_100_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_merge_schedule_then_value_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    entry = CensusScheduleEntry(
        series_id="CENSUS_EITS_MARTS_RETAIL_SALES_MOM",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_title="Advance Monthly Sales for Retail and Food Services",
        release_date="2024-05-15",
        release_time_local="8:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
        source_url="https://www.census.gov/retail",
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    raw_value, evt_value = parse_observation(
        _retail_obs(value="0.7"),
        snapshot_epoch_ms=1_700_000_100_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_value])
        project_events(conn, [evt_value])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider='census'"
        ).fetchone()
    assert tuple(row) == (
        "2024-05-15T12:30:00+00:00",
        "datetime",
        "0.7",
    )


def test_schedule_census_calendar_projects_fixture(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_census_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda session=None: _fixture_html(),
        )
    assert summary.entries_parsed == 4
    assert summary.events_upserted == 4
    assert summary.series_empty == []


def test_schedule_census_calendar_flags_zero_entry_parse(
    store: SQLiteEngineStore,
) -> None:
    html = (
        '<table id="calendar">'
        "<tr><th>Indicator</th><th>Release Date</th><th>Time</th>"
        "<th>Period Covered</th></tr>"
        "<tr><td>Untracked Census Release</td><td>January 8, 2026</td>"
        "<td>8:30 AM</td><td>October 2025</td></tr>"
        "</table>"
    )
    with store._connection(commit=True) as conn:
        summary = schedule_census_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda session=None: html,
        )
    assert summary.entries_parsed == 0
    assert summary.events_upserted == 0
    assert summary.fetch_error == "no Census schedule entries parsed"
    assert set(summary.series_empty) == set(INDICATOR_REGISTRY)


def test_service_ops_dry_run(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_census", {"dry_run": True})
    schedule_result = svc.invoke("calendar_econ_schedule_census", {"dry_run": True})
    assert fetch_result["dry_run"] is True
    assert schedule_result["dry_run"] is True
    assert "CENSUS_EITS_MARTS_RETAIL_SALES_MOM" in fetch_result["series_planned"]
