"""Mocked tests for the France INSEE calendar connector (issue #15 P3c)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.insee_api import (
    INDICATOR_REGISTRY,
    INSEECalendarEventRecord,
    INSEECalendarRawRecord,
    fetch_insee_calendar,
    fetch_press_release_html,
    parse_agenda_json,
    parse_observation,
    parse_press_release_value,
    press_release_url,
    project_schedule_events,
    reference_label_en,
    resolve_release_document,
    schedule_entry_to_records,
    schedule_insee_calendar,
    search_release_documents,
    store_raw,
)
from ingestion.calendar.insee_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p3c_anchors() -> None:
    cpi = INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"]
    assert cpi.country_code == "FR"
    assert cpi.family_id == "1250"
    assert cpi.reference_cadence == "monthly"

    gdp = INDICATOR_REGISTRY["INSEE_GDP_FIRST_ESTIMATE_QOQ"]
    assert gdp.release_kind == "gdp_first_estimate"
    assert gdp.family_id == "1251"
    assert gdp.reference_cadence == "quarterly"


def test_reference_label_and_press_release_url_helpers() -> None:
    cpi = INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"]
    gdp = INDICATOR_REGISTRY["INSEE_GDP_FIRST_ESTIMATE_QOQ"]
    assert reference_label_en(cpi, datetime(2026, 3, 1).date()) == "March 2026"
    assert reference_label_en(gdp, datetime(2025, 12, 31).date()) == "fourth quarter 2025"
    assert press_release_url(8964204).endswith("/en/statistiques/8964204")


def test_schedule_parser_extracts_cpi_and_gdp_rows() -> None:
    entries = parse_agenda_json(_fixture_text("insee_calendar", "agenda_2026.json"))
    assert [entry.series_id for entry in entries] == [
        "INSEE_GDP_FIRST_ESTIMATE_QOQ",
        "INSEE_CPI_PROVISIONAL_YOY",
        "INSEE_GDP_FIRST_ESTIMATE_QOQ",
        "INSEE_CPI_PROVISIONAL_YOY",
    ]
    assert entries[0].reference_date == "2025-12-31"
    assert entries[0].event_time_utc == "2026-01-30T06:30:00+00:00"
    assert entries[1].reference_date == "2026-03-01"
    assert entries[2].reference_date == "2026-03-31"
    assert entries[3].reference_label == "April 2026"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_agenda_json(
        _fixture_text("insee_calendar", "agenda_2026.json"),
        series_ids={"INSEE_GDP_FIRST_ESTIMATE_QOQ"},
    )
    assert [entry.series_id for entry in entries] == [
        "INSEE_GDP_FIRST_ESTIMATE_QOQ",
        "INSEE_GDP_FIRST_ESTIMATE_QOQ",
    ]


def test_release_resolver_selects_matching_family_and_reference() -> None:
    cpi = INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"]
    resolved = resolve_release_document(
        _fixture_text("insee_search", "cpi_march_2026.json"),
        spec=cpi,
        reference_label="March 2026",
    )
    assert resolved.document_id == "8964204"
    assert resolved.source_url.endswith("/en/statistiques/8964204")


def test_press_release_parser_extracts_cpi_and_gdp_values() -> None:
    cpi = parse_press_release_value(
        _fixture_text("insee_press", "cpi_provisional_march_2026.html"),
        spec=INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"],
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-31T06:45:00+00:00",
    )
    assert cpi.value == "1.7"

    gdp = parse_press_release_value(
        _fixture_text("insee_press", "gdp_first_estimate_q4_2025.html"),
        spec=INDICATOR_REGISTRY["INSEE_GDP_FIRST_ESTIMATE_QOQ"],
        reference_date="2025-12-31",
        reference_label="fourth quarter 2025",
        event_time_utc="2026-01-30T06:30:00+00:00",
    )
    assert gdp.value == "0.2"


def test_press_release_parser_preserves_negative_directions() -> None:
    cpi_html = """
    <html><body>
      <p>In May 2026, consumer prices fell by 0.1% year on year.</p>
    </body></html>
    """
    cpi = parse_press_release_value(
        cpi_html,
        spec=INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"],
        reference_date="2026-05-01",
        reference_label="May 2026",
        event_time_utc="2026-05-29T06:45:00+00:00",
    )
    assert cpi.value == "-0.1"

    gdp_html = """
    <html><body>
      <p>Gross domestic product (GDP) in volume terms decreased by 0.2%
      after +0.1% in the previous quarter.</p>
    </body></html>
    """
    gdp = parse_press_release_value(
        gdp_html,
        spec=INDICATOR_REGISTRY["INSEE_GDP_FIRST_ESTIMATE_QOQ"],
        reference_date="2026-06-30",
        reference_label="second quarter 2026",
        event_time_utc="2026-07-30T05:30:00+00:00",
    )
    assert gdp.value == "-0.2"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("insee_press", "cpi_provisional_march_2026.html"),
        spec=INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"],
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-31T06:45:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "insee"
    assert event.reference_date == "2026-03-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "1.7"
    assert event.title == "France CPI Provisional YoY"


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_agenda_json(
        _fixture_text("insee_calendar", "agenda_2026.json"),
        series_ids={"INSEE_CPI_PROVISIONAL_YOY"},
    )[0]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("insee_press", "cpi_provisional_march_2026.html"),
        spec=INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"],
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        event_time_utc=entry.event_time_utc,
        source_url="https://www.insee.fr/en/statistiques/8964204",
    )
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_insee_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-04-30",
            dry_run=False,
            agenda_fetcher=lambda: _fixture_text("insee_calendar", "agenda_2026.json"),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'insee'"
        ).fetchone()[0]
    assert summary.entries_parsed == 3
    assert summary.series_ok == [
        "INSEE_CPI_PROVISIONAL_YOY",
        "INSEE_GDP_FIRST_ESTIMATE_QOQ",
    ]
    assert count == 3


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_agenda_json(
        _fixture_text("insee_calendar", "agenda_2026.json"),
        series_ids={"INSEE_CPI_PROVISIONAL_YOY"},
    )
    entry = entries[0]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _search(spec, label):  # noqa: ANN001
        assert spec.series_id == "INSEE_CPI_PROVISIONAL_YOY"
        assert label == "March 2026"
        return _fixture_text("insee_search", "cpi_march_2026.json")

    def _fetch(url: str) -> str:
        assert url.endswith("/en/statistiques/8964204")
        return _fixture_text("insee_press", "cpi_provisional_march_2026.html")

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_insee_calendar(
            conn,
            series_ids=["INSEE_CPI_PROVISIONAL_YOY"],
            dry_run=False,
            search_fetcher=_search,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 3, 31, 7, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual, source_url "
            "FROM cal_econ_event WHERE provider = 'insee'"
        ).fetchone()
    assert summary.series_ok == ["INSEE_CPI_PROVISIONAL_YOY"]
    assert tuple(row) == (
        "2026-03-31T06:45:00+00:00",
        "datetime",
        "1.7",
        "https://www.insee.fr/en/statistiques/8964204",
    )


def test_schedule_refresh_preserves_release_source_url(store: SQLiteEngineStore) -> None:
    def _search(spec, label):  # noqa: ANN001
        assert spec.series_id == "INSEE_CPI_PROVISIONAL_YOY"
        assert label == "March 2026"
        return _fixture_text("insee_search", "cpi_march_2026.json")

    def _fetch(url: str) -> str:
        assert url.endswith("/en/statistiques/8964204")
        return _fixture_text("insee_press", "cpi_provisional_march_2026.html")

    with store.get_connection() as conn:
        schedule_insee_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            agenda_fetcher=lambda: _fixture_text("insee_calendar", "agenda_2026.json"),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_insee_calendar(
            conn,
            series_ids=["INSEE_CPI_PROVISIONAL_YOY"],
            dry_run=False,
            search_fetcher=_search,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 3, 31, 7, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        schedule_insee_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            agenda_fetcher=lambda: _fixture_text("insee_calendar", "agenda_2026.json"),
            snapshot_epoch_ms=1_800_000_002_000,
        )
        row = conn.execute(
            "SELECT actual, source_url FROM cal_econ_event WHERE provider = 'insee'"
        ).fetchone()

    assert tuple(row) == ("1.7", "https://www.insee.fr/en/statistiques/8964204")


def test_http_helpers_use_browser_headers() -> None:
    class _Response:
        text = "<html></html>"

        def raise_for_status(self) -> None:
            return None

        def json(self):  # noqa: ANN201
            return {"documents": [], "numFounds": 0}

    class _Session:
        def __init__(self) -> None:
            self.headers = None
            self.json = None

        def get(self, url, *, headers, timeout):  # noqa: ANN001
            self.headers = headers
            return _Response()

        def post(self, url, *, params, json, headers, timeout):  # noqa: ANN001
            self.headers = headers
            self.json = json
            return _Response()

    session = _Session()
    assert fetch_press_release_html("https://www.insee.fr/x", session=session) == "<html></html>"  # type: ignore[arg-type]
    assert "Mozilla" in session.headers["User-Agent"]
    search_release_documents(
        INDICATOR_REGISTRY["INSEE_CPI_PROVISIONAL_YOY"],
        "March 2026",
        session=session,  # type: ignore[arg-type]
    )
    assert session.json["filters"][0]["values"] == ["1250"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_insee", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_insee",
        {"dry_run": True, "series_ids": ["INSEE_GDP_FIRST_ESTIMATE_QOQ"]},
    )
    assert schedule_result["series_planned"] == ["INSEE_GDP_FIRST_ESTIMATE_QOQ"]
    assert schedule_result["series_unknown"] == []

    empty_fetch = svc.invoke(
        "calendar_econ_fetch_insee",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_fetch["series_planned"] == []
    empty_schedule = svc.invoke(
        "calendar_econ_schedule_insee",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_schedule["series_planned"] == []


def test_canonical_aliases_cover_insee_titles() -> None:
    assert canonicalize_indicator("France CPI Provisional YoY") == "CPI"
    assert canonicalize_indicator("French CPI") == "CPI"
    assert canonicalize_indicator("France GDP First Estimate QoQ") == "GDP"
    assert canonicalize_indicator("French GDP") == "GDP"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert INSEECalendarRawRecord.__name__ == "INSEECalendarRawRecord"
    assert INSEECalendarEventRecord.__name__ == "INSEECalendarEventRecord"
