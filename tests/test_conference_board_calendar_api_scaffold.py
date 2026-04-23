"""Mocked tests for the Conference Board calendar connector (issue #13 P4)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.calendar.conference_board_api import (
    CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    INDICATOR_REGISTRY,
    ConferenceBoardScheduleEntry,
    current_value_to_records,
    fetch_conference_board_calendar,
    parse_calendar_events_json,
    parse_consumer_confidence_html,
    parse_leading_index_html,
    project_events,
    project_schedule_events,
    schedule_conference_board_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.calendar.conference_board_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


CALENDAR_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "conference_board_calendar"
CURRENT_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "conference_board_current"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _calendar_json() -> str:
    return (CALENDAR_FIXTURE_DIR / "events_2026.json").read_text(encoding="utf-8")


def _consumer_confidence_html() -> str:
    return (
        CURRENT_FIXTURE_DIR / "consumer_confidence_march.html"
    ).read_text(encoding="utf-8")


def _leading_index_html() -> str:
    return (
        CURRENT_FIXTURE_DIR / "us_leading_indicators_january.html"
    ).read_text(encoding="utf-8")


def _load_validator_module():
    module_name = "validate_calendar_acquisition"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(
        module_name,
        REPO_ROOT / "scripts" / "validate_calendar_acquisition.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_registry_contains_issue13_conference_board_anchors() -> None:
    confidence = INDICATOR_REGISTRY["TCB_CONSUMER_CONFIDENCE"]
    leading = INDICATOR_REGISTRY["TCB_LEADING_INDEX"]
    assert confidence.indicator == "CB Consumer Confidence"
    assert confidence.country_code == "US"
    assert confidence.unit == "index"
    assert confidence.reference_month_lag == 0
    assert leading.indicator == "CB Leading Index"
    assert leading.unit == "%"
    assert leading.reference_month_lag == 2


def test_parse_calendar_json_extracts_us_rows_and_reference_months() -> None:
    entries = parse_calendar_events_json(_calendar_json())
    assert len(entries) == 4
    assert {entry.series_id for entry in entries} == {
        "TCB_CONSUMER_CONFIDENCE",
        "TCB_LEADING_INDEX",
    }
    march_lei = next(entry for entry in entries if entry.calendar_event_id == "25648")
    assert march_lei.reference_date == "2026-01-01"
    assert march_lei.reference_label == "January 2026"
    assert march_lei.release_date == "2026-03-19"
    march_confidence = next(
        entry for entry in entries if entry.calendar_event_id == "25662"
    )
    assert march_confidence.reference_date == "2026-03-01"
    assert march_confidence.release_time_local == "10:00 AM"


def test_parse_calendar_json_uses_dst_aware_epoch_times() -> None:
    entries = parse_calendar_events_json(_calendar_json())
    march_lei = next(entry for entry in entries if entry.release_date == "2026-03-19")
    april_confidence = next(
        entry
        for entry in entries
        if entry.release_date == "2026-04-28"
        and entry.series_id == "TCB_CONSUMER_CONFIDENCE"
    )
    assert "T14:00" in march_lei.event_time_utc
    assert "T14:00" in april_confidence.event_time_utc


def test_parse_current_consumer_confidence_html_extracts_value() -> None:
    value = parse_consumer_confidence_html(
        _consumer_confidence_html(),
        source_url=CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    )
    assert value.series_id == "TCB_CONSUMER_CONFIDENCE"
    assert value.reference_date == "2026-03-01"
    assert value.reference_label == "March 2026"
    assert value.actual == "91.8"
    assert value.previous == "91.0"


def test_parse_current_leading_index_html_extracts_monthly_change() -> None:
    value = parse_leading_index_html(
        _leading_index_html(),
        source_url=CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    )
    assert value.series_id == "TCB_LEADING_INDEX"
    assert value.reference_date == "2026-01-01"
    assert value.reference_label == "January 2026"
    assert value.actual == "-0.1"
    assert value.previous == "-0.2"
    assert value.index_level == "97.5"


def test_schedule_entry_id_matches_consumer_confidence_value_side_id() -> None:
    schedule_entry = next(
        entry
        for entry in parse_calendar_events_json(_calendar_json())
        if entry.calendar_event_id == "25662"
    )
    _, schedule_event = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_consumer_confidence_html(
        _consumer_confidence_html(),
        source_url=CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    )
    _, value_event = current_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_100_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_entry_id_matches_leading_index_value_side_id() -> None:
    schedule_entry = next(
        entry
        for entry in parse_calendar_events_json(_calendar_json())
        if entry.calendar_event_id == "25648"
    )
    _, schedule_event = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_leading_index_html(
        _leading_index_html(),
        source_url=CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    )
    _, value_event = current_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_100_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_merge_schedule_then_value_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    schedule_entry = next(
        entry
        for entry in parse_calendar_events_json(_calendar_json())
        if entry.calendar_event_id == "25662"
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_consumer_confidence_html(
        _consumer_confidence_html(),
        source_url=CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    )
    raw_value, evt_value = current_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_100_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_value])
        project_events(conn, [evt_value])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            """
            SELECT provider, event_time_utc, event_time_precision,
                   reference_date, actual, previous, title
            FROM cal_econ_event
            WHERE provider='conference-board'
            """
        ).fetchone()
    assert tuple(row) == (
        "conference-board",
        "2026-03-31T14:00:00+00:00",
        "datetime",
        "2026-03-01",
        "91.8",
        "91.0",
        "CB Consumer Confidence",
    )


def test_schedule_conference_board_calendar_writes_fixture_rows(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_conference_board_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda from_epoch_ms, to_epoch_ms, session=None: (
                _calendar_json()
            ),
        )

    assert summary.entries_parsed == 4
    assert summary.series_ok == ["TCB_CONSUMER_CONFIDENCE", "TCB_LEADING_INDEX"]
    assert summary.rows_raw_inserted == 4
    assert summary.events_upserted == 4


def test_schedule_zero_entries_sets_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_conference_board_calendar(
            conn,
            dry_run=False,
            json_fetcher=lambda from_epoch_ms, to_epoch_ms, session=None: (
                '{"success":"1","result":[]}'
            ),
        )
    assert summary.series_empty == ["TCB_CONSUMER_CONFIDENCE", "TCB_LEADING_INDEX"]
    assert summary.fetch_error == "no Conference Board schedule entries parsed"


def test_fetch_conference_board_calendar_writes_current_values(
    store: SQLiteEngineStore,
) -> None:
    html_by_url = {
        CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL: _consumer_confidence_html(),
        CONFERENCE_BOARD_LEADING_INDICATORS_URL: _leading_index_html(),
    }

    def _fetcher(url: str, session=None) -> str:
        return html_by_url[url]

    with store._connection(commit=True) as conn:
        summary = fetch_conference_board_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            current_html_fetcher=_fetcher,
        )

    assert summary.series_ok == ["TCB_CONSUMER_CONFIDENCE", "TCB_LEADING_INDEX"]
    assert summary.observations_seen == 2
    assert summary.events_upserted == 2
    with store._connection(commit=False) as conn:
        rows = [
            tuple(row)
            for row in conn.execute(
                """
                SELECT provider, title, actual, previous
                FROM cal_econ_event
                ORDER BY title
                """
            ).fetchall()
        ]
    assert rows == [
        (PROVIDER, "CB Consumer Confidence", "91.8", "91.0"),
        (PROVIDER, "CB Leading Index", "-0.1", "-0.2"),
    ]


def test_service_dry_run_exposes_conference_board_ops(
    store: SQLiteEngineStore,
) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke(
        "calendar_econ_fetch_conference_board",
        {"dry_run": True},
    )
    schedule_result = svc.invoke(
        "calendar_econ_schedule_conference_board",
        {"dry_run": True},
    )
    assert fetch_result["series_planned"] == [
        "TCB_CONSUMER_CONFIDENCE",
        "TCB_LEADING_INDEX",
    ]
    assert schedule_result["series_planned"] == [
        "TCB_CONSUMER_CONFIDENCE",
        "TCB_LEADING_INDEX",
    ]


def test_live_validator_dry_run_accepts_conference_board_provider(capsys) -> None:
    validator = _load_validator_module()

    rc = validator.main(["--provider", "conference-board"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DRY RUN (conference-board)" in captured.out
    assert "conference_board_release_calendar" in captured.out


def test_live_validator_conference_board_probe_with_fixtures() -> None:
    validator = _load_validator_module()
    schedule_probe, confidence_probe, leading_probe = (
        validator.plan_conference_board_probes()
    )
    html_by_url = {
        CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL: _consumer_confidence_html(),
        CONFERENCE_BOARD_LEADING_INDICATORS_URL: _leading_index_html(),
    }

    schedule_result = validator.run_conference_board_probe(
        schedule_probe,
        schedule_fetcher=lambda from_epoch_ms, to_epoch_ms: _calendar_json(),
    )
    confidence_result = validator.run_conference_board_probe(
        confidence_probe,
        current_fetcher=lambda url: html_by_url[url],
    )
    leading_result = validator.run_conference_board_probe(
        leading_probe,
        current_fetcher=lambda url: html_by_url[url],
    )
    assert schedule_result.status == "ok"
    assert schedule_result.row_count == 4
    assert confidence_result.status == "ok"
    assert confidence_result.sample_row is not None
    assert confidence_result.sample_row["actual"] == "91.8"
    assert leading_result.status == "ok"
    assert leading_result.sample_row is not None
    assert leading_result.sample_row["actual"] == "-0.1"
