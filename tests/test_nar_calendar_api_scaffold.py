"""Mocked tests for the NAR calendar connector (issue #13 P5)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.calendar.nar_api import (
    INDICATOR_REGISTRY,
    NAR_EXISTING_HOME_SALES_URL,
    NAR_PENDING_HOME_SALES_URL,
    current_value_to_records,
    fetch_nar_calendar,
    parse_existing_home_sales_html,
    parse_pending_home_sales_html,
    parse_schedule_html,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    schedule_nar_calendar,
    store_raw,
)
from ingestion.calendar.nar_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


SCHEDULE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nar_schedule"
CURRENT_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nar_current"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _schedule_html() -> str:
    return (SCHEDULE_FIXTURE_DIR / "2026_schedule.html").read_text(
        encoding="utf-8"
    )


def _existing_html() -> str:
    return (
        CURRENT_FIXTURE_DIR / "existing_home_sales_march.html"
    ).read_text(encoding="utf-8")


def _pending_html() -> str:
    return (
        CURRENT_FIXTURE_DIR / "pending_home_sales_march.html"
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


def test_registry_contains_issue13_nar_anchors() -> None:
    existing = INDICATOR_REGISTRY["NAR_EXISTING_HOME_SALES"]
    pending = INDICATOR_REGISTRY["NAR_PENDING_HOME_SALES_MOM"]
    assert existing.indicator == "Existing Home Sales"
    assert existing.country_code == "US"
    assert existing.unit == "million"
    assert pending.indicator == "Pending Home Sales MoM"
    assert pending.unit == "%"


def test_parse_schedule_extracts_housing_rows_and_reference_months() -> None:
    entries = parse_schedule_html(_schedule_html())
    assert len(entries) == 8
    assert {entry.series_id for entry in entries} == {
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    }
    december_existing = next(
        entry
        for entry in entries
        if entry.raw_title == "December Existing-Home Sales"
    )
    assert december_existing.reference_date == "2025-12-01"
    assert december_existing.release_date == "2026-01-14"
    march_pending = next(
        entry
        for entry in entries
        if entry.raw_title == "March Pending Home Sales Index"
    )
    assert march_pending.reference_date == "2026-03-01"
    assert march_pending.release_time_local == "10:00 AM"


def test_parse_schedule_uses_dst_aware_eastern_times() -> None:
    entries = parse_schedule_html(_schedule_html())
    jan_existing = next(
        entry for entry in entries if entry.raw_title == "December Existing-Home Sales"
    )
    april_existing = next(
        entry for entry in entries if entry.raw_title == "March Existing-Home Sales"
    )
    assert "T15:00" in jan_existing.event_time_utc
    assert "T14:00" in april_existing.event_time_utc


def test_parse_existing_home_sales_html_extracts_value() -> None:
    value = parse_existing_home_sales_html(
        _existing_html(),
        source_url=NAR_EXISTING_HOME_SALES_URL,
    )
    assert value.series_id == "NAR_EXISTING_HOME_SALES"
    assert value.reference_date == "2026-03-01"
    assert value.reference_label == "March 2026"
    assert value.actual == "3.98"
    assert value.raw_change == "-3.6"


def test_parse_pending_home_sales_html_extracts_mom_value() -> None:
    value = parse_pending_home_sales_html(
        _pending_html(),
        source_url=NAR_PENDING_HOME_SALES_URL,
    )
    assert value.series_id == "NAR_PENDING_HOME_SALES_MOM"
    assert value.reference_date == "2026-03-01"
    assert value.reference_label == "March 2026"
    assert value.actual == "1.5"


def test_schedule_entry_id_matches_existing_home_sales_value_side_id() -> None:
    schedule_entry = next(
        entry
        for entry in parse_schedule_html(_schedule_html())
        if entry.raw_title == "March Existing-Home Sales"
    )
    _, schedule_event = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_existing_home_sales_html(
        _existing_html(),
        source_url=NAR_EXISTING_HOME_SALES_URL,
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
        for entry in parse_schedule_html(_schedule_html())
        if entry.raw_title == "March Pending Home Sales Index"
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_pending_home_sales_html(
        _pending_html(),
        source_url=NAR_PENDING_HOME_SALES_URL,
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
                   reference_date, actual, title
            FROM cal_econ_event
            WHERE provider='nar'
            """
        ).fetchone()
    assert tuple(row) == (
        "nar",
        "2026-04-21T14:00:00+00:00",
        "datetime",
        "2026-03-01",
        "1.5",
        "Pending Home Sales MoM",
    )


def test_schedule_nar_calendar_writes_fixture_rows(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_nar_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda session=None: _schedule_html(),
        )

    assert summary.entries_parsed == 8
    assert summary.series_ok == [
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    ]
    assert summary.rows_raw_inserted == 8
    assert summary.events_upserted == 8


def test_schedule_zero_entries_sets_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    html = """
    <html><body>
      <h1>NAR Statistical News Release Schedule</h1>
      <h2>2026 Statistical News Release Schedule</h2>
      <p>Tue., May 5 First Quarter Metro Home Prices</p>
    </body></html>
    """
    with store._connection(commit=True) as conn:
        summary = schedule_nar_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda session=None: html,
        )
    assert summary.series_empty == [
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    ]
    assert summary.fetch_error == "no NAR schedule entries parsed"


def test_fetch_nar_calendar_writes_current_values(
    store: SQLiteEngineStore,
) -> None:
    html_by_url = {
        NAR_EXISTING_HOME_SALES_URL: _existing_html(),
        NAR_PENDING_HOME_SALES_URL: _pending_html(),
    }

    def _fetcher(url: str, session=None) -> str:
        return html_by_url[url]

    with store._connection(commit=True) as conn:
        summary = fetch_nar_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            current_html_fetcher=_fetcher,
        )

    assert summary.series_ok == [
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    ]
    assert summary.observations_seen == 2
    assert summary.events_upserted == 2
    with store._connection(commit=False) as conn:
        rows = [
            tuple(row)
            for row in conn.execute(
                """
                SELECT provider, title, actual
                FROM cal_econ_event
                ORDER BY title
                """
            ).fetchall()
        ]
    assert rows == [
        (PROVIDER, "Existing Home Sales", "3.98"),
        (PROVIDER, "Pending Home Sales MoM", "1.5"),
    ]


def test_service_dry_run_exposes_nar_ops(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_nar", {"dry_run": True})
    schedule_result = svc.invoke("calendar_econ_schedule_nar", {"dry_run": True})
    assert fetch_result["series_planned"] == [
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    ]
    assert schedule_result["series_planned"] == [
        "NAR_EXISTING_HOME_SALES",
        "NAR_PENDING_HOME_SALES_MOM",
    ]


def test_live_validator_dry_run_accepts_nar_provider(capsys) -> None:
    validator = _load_validator_module()

    rc = validator.main(["--provider", "nar"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DRY RUN (nar)" in captured.out
    assert "nar_statistical_release_schedule" in captured.out


def test_live_validator_nar_probe_with_fixtures() -> None:
    validator = _load_validator_module()
    schedule_probe, existing_probe, pending_probe = validator.plan_nar_probes()
    html_by_url = {
        NAR_EXISTING_HOME_SALES_URL: _existing_html(),
        NAR_PENDING_HOME_SALES_URL: _pending_html(),
    }

    schedule_result = validator.run_nar_probe(
        schedule_probe,
        schedule_fetcher=lambda: _schedule_html(),
    )
    existing_result = validator.run_nar_probe(
        existing_probe,
        current_fetcher=lambda url: html_by_url[url],
    )
    pending_result = validator.run_nar_probe(
        pending_probe,
        current_fetcher=lambda url: html_by_url[url],
    )
    assert schedule_result.status == "ok"
    assert schedule_result.row_count == 8
    assert existing_result.status == "ok"
    assert existing_result.sample_row is not None
    assert existing_result.sample_row["actual"] == "3.98"
    assert pending_result.status == "ok"
    assert pending_result.sample_row is not None
    assert pending_result.sample_row["actual"] == "1.5"
