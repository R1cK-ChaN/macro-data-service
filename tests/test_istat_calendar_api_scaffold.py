"""Mocked tests for the Italy ISTAT calendar connector (issue #15 P3b)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.istat_api import (
    INDICATOR_REGISTRY,
    ISTATCalendarEventRecord,
    ISTATCalendarRawRecord,
    discover_calendar_pdf_url,
    fetch_istat_calendar,
    fetch_press_release_html,
    parse_calendar_text,
    parse_observation,
    parse_press_release_value,
    press_release_url,
    project_schedule_events,
    schedule_entry_to_records,
    schedule_istat_calendar,
    store_raw,
)
from ingestion.calendar.istat_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p3b_anchors() -> None:
    cpi = INDICATOR_REGISTRY["ISTAT_CPI_PROVISIONAL_YOY"]
    assert cpi.country_code == "IT"
    assert cpi.reference_cadence == "monthly"
    assert cpi.importance == "high"

    gdp = INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"]
    assert gdp.release_kind == "gdp_preliminary"
    assert gdp.reference_cadence == "quarterly"
    assert gdp.unit == "percent"


def test_press_release_url_synthesises_official_slugs() -> None:
    cpi = INDICATOR_REGISTRY["ISTAT_CPI_PROVISIONAL_YOY"]
    gdp = INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"]
    assert press_release_url(cpi, datetime(2026, 1, 1).date()).endswith(
        "/consumer-prices-provisional-data-january-2026/"
    )
    assert press_release_url(gdp, datetime(2025, 12, 31).date()).endswith(
        "/preliminary-estimate-of-gdp-q4-2025/"
    )


def test_schedule_parser_extracts_cpi_and_gdp_rows() -> None:
    entries = parse_calendar_text(
        _fixture_text("istat_calendar", "calendar_2026.txt")
    )
    assert [entry.series_id for entry in entries] == [
        "ISTAT_CPI_PROVISIONAL_YOY",
        "ISTAT_GDP_PRELIMINARY_QOQ",
        "ISTAT_CPI_PROVISIONAL_YOY",
        "ISTAT_GDP_PRELIMINARY_QOQ",
    ]
    assert entries[0].reference_date == "2025-12-01"
    assert entries[0].event_time_utc == "2026-01-07T10:00:00+00:00"
    assert entries[1].reference_date == "2025-12-31"
    assert entries[1].source_url.endswith("/preliminary-estimate-of-gdp-q4-2025/")
    assert entries[3].event_time_utc == "2026-04-30T08:00:00+00:00"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_calendar_text(
        _fixture_text("istat_calendar", "calendar_2026.txt"),
        series_ids={"ISTAT_GDP_PRELIMINARY_QOQ"},
    )
    assert [entry.series_id for entry in entries] == [
        "ISTAT_GDP_PRELIMINARY_QOQ",
        "ISTAT_GDP_PRELIMINARY_QOQ",
    ]


def test_schedule_filter_accepts_empty_series_set() -> None:
    entries = parse_calendar_text(
        _fixture_text("istat_calendar", "calendar_2026.txt"),
        series_ids=set(),
    )
    assert entries == []


def test_schedule_parser_re_raises_direct_row_errors() -> None:
    text = """
    Press release calendar 2026

    JANUARY 2026
    Consumer prices P December 2025 Wednesday 32 11 a.m.
    """
    with pytest.raises(ValueError, match="day is out of range"):
        parse_calendar_text(text)

    row_issues: list[str] = []
    assert parse_calendar_text(text, row_issues=row_issues) == []
    assert "Consumer prices P December 2025" in row_issues[0]


def test_press_release_parser_extracts_cpi_and_gdp_values() -> None:
    cpi = parse_press_release_value(
        _fixture_text(
            "istat_press", "consumer-prices-provisional-data-january-2026.html"
        ),
        spec=INDICATOR_REGISTRY["ISTAT_CPI_PROVISIONAL_YOY"],
        reference_date="2026-01-01",
        reference_label="January 2026 provisional",
        event_time_utc="2026-02-03T10:00:00+00:00",
        source_url="https://www.istat.it/en/press-release/consumer-prices-provisional-data-january-2026/",
    )
    assert cpi.value == "1.4"

    gdp = parse_press_release_value(
        _fixture_text("istat_press", "preliminary-estimate-of-gdp-q4-2025.html"),
        spec=INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"],
        reference_date="2025-12-31",
        reference_label="Q4 2025 preliminary",
        event_time_utc="2026-01-30T09:00:00+00:00",
        source_url="https://www.istat.it/en/press-release/preliminary-estimate-of-gdp-q4-2025/",
    )
    assert gdp.value == "0.2"


def test_press_release_parser_preserves_gdp_contraction_sign() -> None:
    html = """
    <html><body>
      <p>
        Gross domestic product (GDP) decreased by 0.2 per cent with respect
        to the previous quarter.
      </p>
    </body></html>
    """
    gdp = parse_press_release_value(
        html,
        spec=INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"],
        reference_date="2026-03-31",
        reference_label="Q1 2026 preliminary",
        event_time_utc="2026-04-30T08:00:00+00:00",
    )
    assert gdp.value == "-0.2"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text(
            "istat_press", "consumer-prices-provisional-data-january-2026.html"
        ),
        spec=INDICATOR_REGISTRY["ISTAT_CPI_PROVISIONAL_YOY"],
        reference_date="2026-01-01",
        reference_label="January 2026 provisional",
        event_time_utc="2026-02-03T10:00:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "istat"
    assert event.reference_date == "2026-01-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "1.4"
    assert event.title == "Italy CPI Provisional YoY"


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_calendar_text(
        _fixture_text("istat_calendar", "calendar_2026.txt"),
        series_ids={"ISTAT_GDP_PRELIMINARY_QOQ"},
    )[0]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("istat_press", "preliminary-estimate-of-gdp-q4-2025.html"),
        spec=INDICATOR_REGISTRY["ISTAT_GDP_PRELIMINARY_QOQ"],
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
        summary = schedule_istat_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-02-28",
            dry_run=False,
            calendar_text_fetcher=lambda: _fixture_text(
                "istat_calendar", "calendar_2026.txt"
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'istat'"
        ).fetchone()[0]
    assert summary.entries_parsed == 3
    assert summary.series_ok == [
        "ISTAT_CPI_PROVISIONAL_YOY",
        "ISTAT_GDP_PRELIMINARY_QOQ",
    ]
    assert count == 3


def test_schedule_fetcher_uses_window_end_year_for_pdf(
    store: SQLiteEngineStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, int | None] = {}

    def _fetch_pdf(*, session=None, year=None):  # noqa: ANN001
        del session
        seen["year"] = year
        return b"fake-pdf"

    monkeypatch.setattr(
        "ingestion.calendar.istat_api.fetcher.fetch_calendar_pdf",
        _fetch_pdf,
    )
    monkeypatch.setattr(
        "ingestion.calendar.istat_api.fetcher.extract_calendar_pdf_text",
        lambda payload: _fixture_text("istat_calendar", "calendar_2026.txt"),
    )
    with store.get_connection() as conn:
        summary = schedule_istat_calendar(
            conn,
            start_date="2025-12-25",
            end_date="2026-12-31",
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert seen["year"] == 2026
    assert summary.entries_parsed == 4


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_calendar_text(
        _fixture_text("istat_calendar", "calendar_2026.txt"),
        series_ids={"ISTAT_CPI_PROVISIONAL_YOY"},
    )
    entry = entries[1]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _fetch(url: str) -> str:
        assert url.endswith("/consumer-prices-provisional-data-january-2026/")
        return _fixture_text(
            "istat_press", "consumer-prices-provisional-data-january-2026.html"
        )

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_istat_calendar(
            conn,
            series_ids=["ISTAT_CPI_PROVISIONAL_YOY"],
            dry_run=False,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 2, 3, 10, 30, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider = 'istat'"
        ).fetchone()
    assert summary.series_ok == ["ISTAT_CPI_PROVISIONAL_YOY"]
    assert tuple(row) == ("2026-02-03T10:00:00+00:00", "datetime", "1.4")


def test_calendar_pdf_link_discovery_and_fetch_headers() -> None:
    page = """
    <html><body>
      <a href="/wp-content/uploads/2026/01/Calendario-2026_-EN.pdf">
        2026 press release calendar
      </a>
    </body></html>
    """
    assert discover_calendar_pdf_url(page, year=2026).endswith(
        "/wp-content/uploads/2026/01/Calendario-2026_-EN.pdf"
    )

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
    assert fetch_press_release_html("https://www.istat.it/x", session=session) == "<html></html>"  # type: ignore[arg-type]
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_istat", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_istat",
        {"dry_run": True, "series_ids": ["ISTAT_GDP_PRELIMINARY_QOQ"]},
    )
    assert schedule_result["series_planned"] == ["ISTAT_GDP_PRELIMINARY_QOQ"]
    assert schedule_result["series_unknown"] == []


def test_canonical_aliases_cover_istat_titles() -> None:
    assert canonicalize_indicator("Italy CPI Provisional YoY") == "CPI"
    assert canonicalize_indicator("Italian CPI") == "CPI"
    assert canonicalize_indicator("Italy GDP Preliminary QoQ") == "GDP"
    assert canonicalize_indicator("Italian GDP") == "GDP"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert ISTATCalendarRawRecord.__name__ == "ISTATCalendarRawRecord"
    assert ISTATCalendarEventRecord.__name__ == "ISTATCalendarEventRecord"
