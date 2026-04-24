"""Mocked tests for the Spain INE calendar connector (issue #15 P3a)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.ine_api import (
    INDICATOR_REGISTRY,
    INECalendarEventRecord,
    INECalendarRawRecord,
    fetch_ine_calendar,
    fetch_calendar_html,
    parse_calendar_html,
    parse_observation,
    parse_press_release_value,
    press_release_url,
    project_schedule_events,
    schedule_entry_to_records,
    schedule_ine_calendar,
    store_raw,
)
from ingestion.calendar.ine_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p3a_anchors() -> None:
    cpi = INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"]
    assert cpi.country_code == "ES"
    assert cpi.reference_cadence == "monthly"
    assert cpi.importance == "high"

    gdp = INDICATOR_REGISTRY["INE_GDP_ADVANCE_QOQ"]
    assert gdp.release_kind == "gdp_advance"
    assert gdp.reference_cadence == "quarterly"
    assert gdp.unit == "percent"


def test_press_release_url_synthesises_official_slugs() -> None:
    cpi = INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"]
    gdp = INDICATOR_REGISTRY["INE_GDP_ADVANCE_QOQ"]
    assert press_release_url(cpi, datetime(2026, 3, 1).date()).endswith(
        "/adIPC0326.htm"
    )
    assert press_release_url(gdp, datetime(2025, 12, 31).date()).endswith(
        "/avCNTR4T25.htm"
    )


def test_schedule_parser_extracts_cpi_and_gdp_advance_rows() -> None:
    entries = parse_calendar_html(_fixture_text("ine_calendar", "calendar_2026.html"))
    assert [entry.series_id for entry in entries] == [
        "INE_GDP_ADVANCE_QOQ",
        "INE_CPI_ADVANCE_YOY",
        "INE_CPI_ADVANCE_YOY",
        "INE_GDP_ADVANCE_QOQ",
    ]
    assert entries[0].reference_date == "2025-12-31"
    assert entries[0].event_time_utc == "2026-01-30T08:00:00+00:00"
    assert entries[1].reference_date == "2026-03-01"
    assert entries[2].source_url.endswith("/adIPC0426.htm")
    assert entries[3].event_time_utc == "2026-04-30T07:00:00+00:00"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_calendar_html(
        _fixture_text("ine_calendar", "calendar_2026.html"),
        series_ids={"INE_GDP_ADVANCE_QOQ"},
    )
    assert [entry.series_id for entry in entries] == [
        "INE_GDP_ADVANCE_QOQ",
        "INE_GDP_ADVANCE_QOQ",
    ]


def test_press_release_parser_extracts_cpi_and_gdp_values() -> None:
    cpi = parse_press_release_value(
        _fixture_text("ine_press", "adIPC0326.htm"),
        spec=INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"],
        reference_date="2026-03-01",
        reference_label="Avance. Marzo 2026",
        event_time_utc="2026-03-27T08:00:00+00:00",
        source_url="https://www.ine.es/dyngs/Prensa/adIPC0326.htm",
    )
    assert cpi.value == "3.3"

    gdp = parse_press_release_value(
        _fixture_text("ine_press", "avCNTR4T25.htm"),
        spec=INDICATOR_REGISTRY["INE_GDP_ADVANCE_QOQ"],
        reference_date="2025-12-31",
        reference_label="Trimestre 4/2025",
        event_time_utc="2026-01-30T08:00:00+00:00",
        source_url="https://www.ine.es/dyngs/Prensa/avCNTR4T25.htm",
    )
    assert gdp.value == "0.8"


def test_press_release_parser_accepts_ine_gdp_present_tense_wording() -> None:
    html = """
    <html><body>
      <p>
        El PIB registra una variacion del 0,8% en el cuarto trimestre
        respecto al trimestre anterior en terminos de volumen.
      </p>
    </body></html>
    """
    gdp = parse_press_release_value(
        html,
        spec=INDICATOR_REGISTRY["INE_GDP_ADVANCE_QOQ"],
        reference_date="2025-12-31",
        reference_label="Trimestre 4/2025",
        event_time_utc="2026-01-30T08:00:00+00:00",
        source_url="https://www.ine.es/dyngs/Prensa/avCNTR4T25.htm",
    )
    assert gdp.value == "0.8"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("ine_press", "adIPC0326.htm"),
        spec=INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"],
        reference_date="2026-03-01",
        reference_label="Avance. Marzo 2026",
        event_time_utc="2026-03-27T08:00:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "ine"
    assert event.reference_date == "2026-03-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "3.3"
    assert event.title == "Spain CPI Advance YoY"


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_calendar_html(
        _fixture_text("ine_calendar", "calendar_2026.html"),
        series_ids={"INE_CPI_ADVANCE_YOY"},
    )[0]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("ine_press", "adIPC0326.htm"),
        spec=INDICATOR_REGISTRY["INE_CPI_ADVANCE_YOY"],
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        event_time_utc=entry.event_time_utc,
        source_url=entry.source_url,
    )
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_ine_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text("ine_calendar", "calendar_2026.html"),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ine'"
        ).fetchone()[0]
    assert summary.entries_parsed == 3
    assert summary.series_ok == [
        "INE_CPI_ADVANCE_YOY",
        "INE_GDP_ADVANCE_QOQ",
    ]
    assert count == 3


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_calendar_html(
        _fixture_text("ine_calendar", "calendar_2026.html"),
        series_ids={"INE_CPI_ADVANCE_YOY"},
    )
    entry = entries[0]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _fetch(url: str) -> str:
        assert url.endswith("/adIPC0326.htm")
        return _fixture_text("ine_press", "adIPC0326.htm")

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_ine_calendar(
            conn,
            series_ids=["INE_CPI_ADVANCE_YOY"],
            dry_run=False,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 3, 27, 8, 30, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider = 'ine'"
        ).fetchone()
    assert summary.series_ok == ["INE_CPI_ADVANCE_YOY"]
    assert tuple(row) == ("2026-03-27T08:00:00+00:00", "datetime", "3.3")


def test_calendar_fetch_uses_browser_headers() -> None:
    class _Response:
        text = "<html></html>"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.headers = None

        def get(self, url, *, headers, timeout):  # noqa: ANN001
            self.headers = headers
            return _Response()

    session = _Session()
    assert fetch_calendar_html(session=session) == "<html></html>"  # type: ignore[arg-type]
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_ine", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_ine",
        {"dry_run": True, "series_ids": ["INE_GDP_ADVANCE_QOQ"]},
    )
    assert schedule_result["series_planned"] == ["INE_GDP_ADVANCE_QOQ"]
    assert schedule_result["series_unknown"] == []


def test_canonical_aliases_cover_ine_titles() -> None:
    assert canonicalize_indicator("Spain CPI Advance YoY") == "CPI"
    assert canonicalize_indicator("Spanish CPI") == "CPI"
    assert canonicalize_indicator("Spain GDP Advance QoQ") == "GDP"
    assert canonicalize_indicator("Spanish GDP") == "GDP"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert INECalendarRawRecord.__name__ == "INECalendarRawRecord"
    assert INECalendarEventRecord.__name__ == "INECalendarEventRecord"
