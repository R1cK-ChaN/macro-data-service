"""Mocked tests for the U Michigan calendar connector (issue #13 P3)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.calendar.umich_api import (
    INDICATOR_REGISTRY,
    UMICH_SURVEY_INFO_URL,
    UMichScheduleDocument,
    UMichScheduleEntry,
    current_value_to_records,
    fetch_umich_calendar,
    parse_current_results_html,
    parse_release_dates_text,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    schedule_umich_calendar,
    store_raw,
)
from ingestion.calendar.umich_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


SCHEDULE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "umich_schedule"
RESULTS_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "umich_results"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _schedule_text() -> str:
    return (
        SCHEDULE_FIXTURE_DIR / "release_dates_2026.txt"
    ).read_text(encoding="utf-8")


def _results_html() -> str:
    return (
        RESULTS_FIXTURE_DIR / "preliminary_april.html"
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


def test_registry_contains_issue13_umich_anchor() -> None:
    spec = INDICATOR_REGISTRY["UMICH_CONSUMER_SENTIMENT"]
    assert spec.indicator == "Michigan Consumer Sentiment"
    assert spec.country_code == "US"
    assert spec.unit == "index"
    assert spec.value_fetch is True


def test_parse_release_dates_text_extracts_prelim_and_final_rows() -> None:
    entries = parse_release_dates_text(
        _schedule_text(),
        source_url="https://data.sca.isr.umich.edu/fetchdoc.php?docid=79628",
    )
    assert len(entries) == 24
    assert {entry.series_id for entry in entries} == {"UMICH_CONSUMER_SENTIMENT"}
    jan_prelim = entries[0]
    assert jan_prelim.release_date == "2026-01-09"
    assert jan_prelim.reference_date == "2026-01-01"
    assert jan_prelim.release_stage == "preliminary"
    assert jan_prelim.reference_label == "January 2026 Prelim"
    jan_final = entries[1]
    assert jan_final.release_date == "2026-01-23"
    assert jan_final.release_stage == "final"


def test_parse_release_dates_uses_dst_aware_eastern_time() -> None:
    entries = parse_release_dates_text(_schedule_text())
    winter = next(e for e in entries if e.release_date == "2026-01-09")
    summer = next(e for e in entries if e.release_date == "2026-04-10")
    assert "T15:00" in winter.event_time_utc
    assert "T14:00" in summer.event_time_utc


def test_parse_current_results_html_extracts_current_value() -> None:
    value = parse_current_results_html(
        _results_html(),
        source_url="https://www.sca.isr.umich.edu/",
    )
    assert value.series_id == "UMICH_CONSUMER_SENTIMENT"
    assert value.reference_date == "2026-04-01"
    assert value.reference_label == "April 2026 Prelim"
    assert value.release_stage == "preliminary"
    assert value.actual == "47.6"
    assert value.previous == "53.3"


def test_prelim_and_final_have_distinct_ids() -> None:
    prelim = UMichScheduleEntry(
        series_id="UMICH_CONSUMER_SENTIMENT",
        reference_date="2026-04-01",
        reference_label="April 2026 Prelim",
        release_stage="preliminary",
        release_date="2026-04-10",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-10T14:00:00+00:00",
        source_url="https://example.test/umich",
    )
    final = UMichScheduleEntry(
        series_id="UMICH_CONSUMER_SENTIMENT",
        reference_date="2026-04-01",
        reference_label="April 2026 Final",
        release_stage="final",
        release_date="2026-04-24",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-24T14:00:00+00:00",
        source_url="https://example.test/umich",
    )
    _, prelim_event = schedule_entry_to_records(
        prelim,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    _, final_event = schedule_entry_to_records(
        final,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert prelim_event.provider_event_id != final_event.provider_event_id


def test_schedule_entry_id_matches_value_side_id() -> None:
    schedule_entry = UMichScheduleEntry(
        series_id="UMICH_CONSUMER_SENTIMENT",
        reference_date="2026-04-01",
        reference_label="April 2026 Prelim",
        release_stage="preliminary",
        release_date="2026-04-10",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-10T14:00:00+00:00",
        source_url="https://example.test/umich",
    )
    _, schedule_event = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_current_results_html(
        _results_html(),
        source_url="https://www.sca.isr.umich.edu/",
    )
    _, value_event = current_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_100_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_merge_schedule_then_value_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    schedule_entry = UMichScheduleEntry(
        series_id="UMICH_CONSUMER_SENTIMENT",
        reference_date="2026-04-01",
        reference_label="April 2026 Prelim",
        release_stage="preliminary",
        release_date="2026-04-10",
        release_time_local="10:00 AM",
        event_time_utc="2026-04-10T14:00:00+00:00",
        source_url="https://example.test/umich",
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        schedule_entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = parse_current_results_html(
        _results_html(),
        source_url="https://www.sca.isr.umich.edu/",
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
            WHERE provider='umich'
            """
        ).fetchone()
    assert tuple(row) == (
        "umich",
        "2026-04-10T14:00:00+00:00",
        "datetime",
        "2026-04-01",
        "47.6",
        "53.3",
        "Michigan Consumer Sentiment Prelim",
    )


def test_schedule_umich_calendar_writes_fixture_rows(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_umich_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            document_fetcher=lambda year=None, session=None: UMichScheduleDocument(
                text=_schedule_text(),
                source_url="https://data.sca.isr.umich.edu/fetchdoc.php?docid=79628",
            ),
        )

    assert summary.entries_parsed == 24
    assert summary.series_ok == ["UMICH_CONSUMER_SENTIMENT"]
    assert summary.rows_raw_inserted == 24
    assert summary.events_upserted == 24


def test_schedule_zero_entries_sets_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = schedule_umich_calendar(
            conn,
            dry_run=False,
            document_fetcher=lambda year=None, session=None: (
                "RELEASE DATES FOR 2026:"
            ),
        )
    assert summary.series_empty == ["UMICH_CONSUMER_SENTIMENT"]
    assert summary.fetch_error == "no U Michigan schedule entries parsed"


def test_fetch_umich_calendar_writes_current_result(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = fetch_umich_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            current_html_fetcher=lambda session=None: _results_html(),
        )

    assert summary.series_ok == ["UMICH_CONSUMER_SENTIMENT"]
    assert summary.release_stage == "preliminary"
    assert summary.observations_seen == 1
    assert summary.events_upserted == 1
    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT provider, title, actual, previous FROM cal_econ_event"
        ).fetchone()
    assert tuple(row) == (
        PROVIDER,
        "Michigan Consumer Sentiment Prelim",
        "47.6",
        "53.3",
    )


def test_service_dry_run_exposes_umich_ops(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_umich", {"dry_run": True})
    schedule_result = svc.invoke("calendar_econ_schedule_umich", {"dry_run": True})
    assert fetch_result["series_planned"] == ["UMICH_CONSUMER_SENTIMENT"]
    assert schedule_result["series_planned"] == ["UMICH_CONSUMER_SENTIMENT"]


def test_live_validator_dry_run_accepts_umich_provider(capsys) -> None:
    validator = _load_validator_module()

    rc = validator.main(["--provider", "umich"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "DRY RUN (umich)" in captured.out
    assert "umich_release_dates" in captured.out


def test_live_validator_umich_probe_with_fixtures() -> None:
    validator = _load_validator_module()

    schedule_probe, current_probe = validator.plan_umich_probes()
    schedule_result = validator.run_umich_probe(
        schedule_probe,
        document_fetcher=lambda year=None: UMichScheduleDocument(
            text=_schedule_text(),
            source_url=UMICH_SURVEY_INFO_URL,
        ),
    )
    current_result = validator.run_umich_probe(
        current_probe,
        current_fetcher=lambda: _results_html(),
    )
    assert schedule_result.status == "ok"
    assert schedule_result.row_count == 24
    assert current_result.status == "ok"
    assert current_result.sample_row is not None
    assert current_result.sample_row["actual"] == "47.6"
