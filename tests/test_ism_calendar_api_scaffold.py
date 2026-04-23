"""Mocked tests for the ISM calendar connector (issue #13 P2)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.calendar.ism_api import (
    INDICATOR_REGISTRY,
    ISMScheduleEntry,
    discover_current_report_url,
    fetch_ism_calendar,
    parse_report_html,
    parse_schedule_html,
    project_events,
    project_schedule_events,
    report_value_to_records,
    schedule_entry_to_records,
    schedule_ism_calendar,
    store_raw,
)
from ingestion.calendar.ism_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


SCHEDULE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ism_schedule"
REPORT_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ism_report"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _schedule_html() -> str:
    return (
        SCHEDULE_FIXTURE_DIR / "rob-report-calendar.html"
    ).read_text(encoding="utf-8")


def _landing_html() -> str:
    return (REPORT_FIXTURE_DIR / "landing.html").read_text(encoding="utf-8")


def _report_html() -> str:
    return (
        REPORT_FIXTURE_DIR / "manufacturing_march.html"
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


def test_registry_contains_issue13_ism_anchor() -> None:
    spec = INDICATOR_REGISTRY["ISM_MANUFACTURING_PMI"]
    assert spec.indicator == "ISM Manufacturing PMI"
    assert spec.country_code == "US"
    assert spec.unit == "index"
    assert spec.value_fetch is True


def test_parse_schedule_html_extracts_manufacturing_rows() -> None:
    entries = parse_schedule_html(_schedule_html())
    assert len(entries) == 12
    assert {entry.series_id for entry in entries} == {"ISM_MANUFACTURING_PMI"}
    jan = entries[0]
    assert jan.release_date == "2026-01-05"
    assert jan.reference_date == "2025-12-01"
    assert jan.reference_label == "December 2025"


def test_parse_schedule_uses_dst_aware_eastern_time() -> None:
    entries = parse_schedule_html(_schedule_html())
    winter = next(e for e in entries if e.release_date == "2026-01-05")
    summer = next(e for e in entries if e.release_date == "2026-05-01")
    assert "T15:00" in winter.event_time_utc
    assert "T14:00" in summer.event_time_utc


def test_discover_current_manufacturing_report_url() -> None:
    url = discover_current_report_url(_landing_html())
    assert url == (
        "https://www.ismworld.org/supply-management-news-and-reports/"
        "reports/ism-pmi-reports/pmi/march/"
    )


def test_parse_report_html_extracts_current_value() -> None:
    value = parse_report_html(
        _report_html(),
        source_url="https://www.ismworld.org/current",
    )
    assert value.series_id == "ISM_MANUFACTURING_PMI"
    assert value.reference_date == "2026-03-01"
    assert value.reference_label == "March 2026"
    assert value.actual == "52.7"
    assert value.previous == "52.4"


def test_schedule_entry_id_matches_value_side_id() -> None:
    schedule_entry = ISMScheduleEntry(
        series_id="ISM_MANUFACTURING_PMI",
        reference_date="2026-03-01",
        reference_label="March 2026",
        release_month_label="April 2026",
        release_date="2026-04-01",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-01T14:00:00+00:00",
        source_url="https://www.ismworld.org/calendar",
    )
    _, schedule_event = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_report_html(
        _report_html(),
        source_url="https://www.ismworld.org/current",
    )
    _, value_event = report_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_100_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_merge_schedule_then_value_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    schedule_entry = ISMScheduleEntry(
        series_id="ISM_MANUFACTURING_PMI",
        reference_date="2026-03-01",
        reference_label="March 2026",
        release_month_label="April 2026",
        release_date="2026-04-01",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-01T14:00:00+00:00",
        source_url="https://www.ismworld.org/calendar",
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_report_html(
        _report_html(),
        source_url="https://www.ismworld.org/current",
    )
    raw_value, evt_value = report_value_to_records(
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
                   reference_date, actual, previous
            FROM cal_econ_event
            WHERE provider='ism'
            """
        ).fetchone()
    assert tuple(row) == (
        "ism",
        "2026-04-01T14:00:00+00:00",
        "datetime",
        "2026-03-01",
        "52.7",
        "52.4",
    )


def test_schedule_ism_calendar_writes_fixture_rows(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_ism_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda session=None: _schedule_html(),
        )

    assert summary.entries_parsed == 12
    assert summary.series_ok == ["ISM_MANUFACTURING_PMI"]
    assert summary.rows_raw_inserted == 12
    assert summary.events_upserted == 12


def test_schedule_zero_entries_sets_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_ism_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda session=None: "<html><table></table></html>",
        )
    assert summary.series_empty == ["ISM_MANUFACTURING_PMI"]
    assert summary.fetch_error == "no ISM schedule entries parsed"


def test_fetch_ism_calendar_discovers_and_writes_current_report(
    store: SQLiteEngineStore,
) -> None:
    requested_urls: list[str] = []

    def _report_fetcher(url: str, session=None) -> str:
        requested_urls.append(url)
        return _report_html()

    with store._connection(commit=True) as conn:
        summary = fetch_ism_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            landing_html_fetcher=lambda session=None: _landing_html(),
            report_html_fetcher=_report_fetcher,
        )

    assert requested_urls == [
        "https://www.ismworld.org/supply-management-news-and-reports/"
        "reports/ism-pmi-reports/pmi/march/"
    ]
    assert summary.series_ok == ["ISM_MANUFACTURING_PMI"]
    assert summary.observations_seen == 1
    assert summary.events_upserted == 1
    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT provider, title, actual, previous FROM cal_econ_event"
        ).fetchone()
    assert tuple(row) == (
        PROVIDER,
        "ISM Manufacturing PMI",
        "52.7",
        "52.4",
    )


def test_service_dry_run_exposes_ism_ops(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_ism", {"dry_run": True})
    schedule_result = svc.invoke("calendar_econ_schedule_ism", {"dry_run": True})
    assert fetch_result["series_planned"] == ["ISM_MANUFACTURING_PMI"]
    assert schedule_result["series_planned"] == ["ISM_MANUFACTURING_PMI"]


def test_live_validator_dry_run_accepts_ism_provider(capsys) -> None:
    validator = _load_validator_module()

    rc = validator.main(["--provider", "ism"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DRY RUN (ism)" in captured.out
    assert "ism_release_calendar" in captured.out


def test_live_validator_ism_probe_with_fixtures() -> None:
    validator = _load_validator_module()

    schedule_probe, report_probe = validator.plan_ism_probes()
    schedule_result = validator.run_ism_probe(
        schedule_probe,
        schedule_fetcher=lambda: _schedule_html(),
    )
    report_result = validator.run_ism_probe(
        report_probe,
        landing_fetcher=lambda: _landing_html(),
        report_fetcher=lambda url: _report_html(),
    )
    assert schedule_result.status == "ok"
    assert schedule_result.row_count == 12
    assert report_result.status == "ok"
    assert report_result.sample_row is not None
    assert report_result.sample_row["actual"] == "52.7"
