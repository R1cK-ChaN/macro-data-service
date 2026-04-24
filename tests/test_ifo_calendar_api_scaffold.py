"""Mocked tests for the Germany Ifo calendar connector (issue #15 P4b)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.ifo_api import (
    IFO_SURVEY_URL,
    INDICATOR_REGISTRY,
    IfoCalendarEventRecord,
    IfoCalendarRawRecord,
    IfoScheduleParseError,
    fetch_ifo_calendar,
    fetch_press_release_html,
    parse_observation,
    parse_press_release_value,
    parse_release_dates_html,
    project_schedule_events,
    reference_label_en,
    resolve_press_release_link,
    schedule_entry_to_records,
    schedule_ifo_calendar,
    store_raw,
)
from ingestion.calendar.ifo_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p4b_anchor() -> None:
    spec = INDICATOR_REGISTRY["IFO_BUSINESS_CLIMATE"]
    assert spec.country_code == "DE"
    assert spec.indicator == "Ifo Business Climate"
    assert spec.category == "Business Confidence"
    assert spec.unit == "index"
    assert spec.title == "Germany Ifo Business Climate Index"
    assert IFO_SURVEY_URL.endswith("/en/survey/ifo-business-climate-index-germany")


def test_reference_label_helper() -> None:
    assert reference_label_en(datetime(2026, 4, 1).date()) == "April 2026"


def test_schedule_parser_extracts_release_dates_and_dst_times() -> None:
    entries = parse_release_dates_html(
        _fixture_text("ifo_schedule", "survey_page_2026.html"),
        source_url=IFO_SURVEY_URL,
    )
    assert [entry.reference_label for entry in entries[:4]] == [
        "January 2026",
        "February 2026",
        "March 2026",
        "April 2026",
    ]
    # CET (winter, UTC+1) → 10:30 Berlin = 09:30 UTC
    assert entries[0].event_time_utc == "2026-01-22T09:30:00+00:00"
    # CEST (summer, UTC+2) → 10:30 Berlin = 08:30 UTC
    assert entries[3].event_time_utc == "2026-04-24T08:30:00+00:00"
    assert all(entry.event_time_precision == "datetime" for entry in entries)
    assert entries[0].source_url == IFO_SURVEY_URL


def test_schedule_parser_raises_on_empty_page() -> None:
    with pytest.raises(IfoScheduleParseError, match="no IFO_BUSINESS_CLIMATE"):
        parse_release_dates_html(
            "<html><body>Press releases</body></html>",
            source_url=IFO_SURVEY_URL,
        )


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_dates_html(
        _fixture_text("ifo_schedule", "survey_page_2026.html"),
        series_ids=set(),
    )
    assert entries == []


def test_schedule_parser_defaults_release_time_when_absent() -> None:
    html = """
    <html><body>
      <p>ifo Business Climate release dates</p>
      <ul><li>22 May 2026</li></ul>
    </body></html>
    """
    entries = parse_release_dates_html(html, source_url=IFO_SURVEY_URL)
    # Default 10:30 Europe/Berlin (CEST in May) → 08:30 UTC.
    assert entries[0].event_time_utc == "2026-05-22T08:30:00+00:00"


def test_listing_resolver_selects_matching_release_by_date_slug() -> None:
    resolved = resolve_press_release_link(
        _fixture_text("ifo_listing", "latest_april_2026.html"),
        release_date=datetime(2026, 4, 24).date(),
    )
    assert resolved.source_url.endswith(
        "/en/press-release/2026-04-24/ifo-business-climate-index-down-april-2026"
    )
    assert resolved.release_date == "2026-04-24"


def test_listing_resolver_rejects_non_business_climate_release_on_same_day() -> None:
    """The Employment Barometer shares the release date but is a different indicator."""
    resolved = resolve_press_release_link(
        _fixture_text("ifo_listing", "latest_april_2026.html"),
        release_date=datetime(2026, 4, 24).date(),
    )
    assert "employment" not in resolved.source_url.lower()


def test_listing_resolver_raises_when_date_not_found() -> None:
    with pytest.raises(IfoScheduleParseError, match="2026-12-31"):
        resolve_press_release_link(
            _fixture_text("ifo_listing", "latest_april_2026.html"),
            release_date=datetime(2026, 12, 31).date(),
        )


def test_press_release_parser_extracts_fell_and_rose_values() -> None:
    spec = INDICATOR_REGISTRY["IFO_BUSINESS_CLIMATE"]
    april = parse_press_release_value(
        _fixture_text("ifo_press", "april_2026.html"),
        spec=spec,
        reference_date="2026-04-01",
        reference_label="April 2026",
        event_time_utc="2026-04-24T08:30:00+00:00",
    )
    assert april.value == "84.4"

    march = parse_press_release_value(
        _fixture_text("ifo_press", "march_2026.html"),
        spec=spec,
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-25T09:30:00+00:00",
    )
    assert march.value == "86.3"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("ifo_press", "april_2026.html"),
        spec=INDICATOR_REGISTRY["IFO_BUSINESS_CLIMATE"],
        reference_date="2026-04-01",
        reference_label="April 2026",
        event_time_utc="2026-04-24T08:30:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "ifo"
    assert event.reference_date == "2026-04-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "84.4"
    assert event.title == "Germany Ifo Business Climate Index"
    assert event.country_code == "DE"


def test_schedule_and_value_share_provider_event_id() -> None:
    entries = parse_release_dates_html(
        _fixture_text("ifo_schedule", "survey_page_2026.html"),
    )
    april_entry = next(e for e in entries if e.reference_date == "2026-04-01")
    _, schedule_event = schedule_entry_to_records(
        april_entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("ifo_press", "april_2026.html"),
        spec=INDICATOR_REGISTRY["IFO_BUSINESS_CLIMATE"],
        reference_date=april_entry.reference_date,
        reference_label=april_entry.reference_label,
        event_time_utc=april_entry.event_time_utc,
        source_url="https://www.ifo.de/en/press-release/2026-04-24/ifo-business-climate-index-down-april-2026",
    )
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_ifo_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text(
                "ifo_schedule", "survey_page_2026.html"
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ifo'"
        ).fetchone()[0]
    assert summary.entries_parsed == 4
    assert summary.series_ok == ["IFO_BUSINESS_CLIMATE"]
    assert count == 4


def test_schedule_fetcher_flags_empty_upstream_page(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_ifo_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-12-31",
            dry_run=False,
            html_fetcher=lambda: "<html><body>No dates here</body></html>",
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ifo'"
        ).fetchone()[0]
    assert summary.fetch_error is not None
    assert "no IFO_BUSINESS_CLIMATE" in summary.fetch_error
    assert summary.series_ok == []
    assert count == 0


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_release_dates_html(
        _fixture_text("ifo_schedule", "survey_page_2026.html"),
    )
    april_entry = next(e for e in entries if e.reference_date == "2026-04-01")
    raw_schedule, event_schedule = schedule_entry_to_records(
        april_entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-04-24"
        return _fixture_text("ifo_listing", "latest_april_2026.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/ifo-business-climate-index-down-april-2026")
        return _fixture_text("ifo_press", "april_2026.html")

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_ifo_calendar(
            conn,
            series_ids=["IFO_BUSINESS_CLIMATE"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 4, 24, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual, source_url "
            "FROM cal_econ_event WHERE provider = 'ifo'"
        ).fetchone()
    assert summary.series_ok == ["IFO_BUSINESS_CLIMATE"]
    assert tuple(row) == (
        "2026-04-24T08:30:00+00:00",
        "datetime",
        "84.4",
        "https://www.ifo.de/en/press-release/2026-04-24/ifo-business-climate-index-down-april-2026",
    )


def test_schedule_refresh_preserves_release_source_url(store: SQLiteEngineStore) -> None:
    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-04-24"
        return _fixture_text("ifo_listing", "latest_april_2026.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/ifo-business-climate-index-down-april-2026")
        return _fixture_text("ifo_press", "april_2026.html")

    def _schedule_html() -> str:
        return _fixture_text("ifo_schedule", "survey_page_2026.html")

    with store.get_connection() as conn:
        schedule_ifo_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=_schedule_html,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_ifo_calendar(
            conn,
            series_ids=["IFO_BUSINESS_CLIMATE"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 4, 24, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        schedule_ifo_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=_schedule_html,
            snapshot_epoch_ms=1_800_000_002_000,
        )
        row = conn.execute(
            "SELECT actual, source_url FROM cal_econ_event WHERE provider = 'ifo'"
        ).fetchone()

    assert tuple(row) == (
        "84.4",
        "https://www.ifo.de/en/press-release/2026-04-24/ifo-business-climate-index-down-april-2026",
    )


def test_http_helpers_use_browser_headers() -> None:
    class _Response:
        text = "<html></html>"

        def raise_for_status(self) -> None:
            return None

    class _Session:
        def __init__(self) -> None:
            self.headers: dict[str, str] | None = None

        def get(self, url, *, headers, timeout):  # noqa: ANN001
            self.headers = headers
            return _Response()

    session = _Session()
    assert fetch_press_release_html(  # type: ignore[arg-type]
        "https://www.ifo.de/x", session=session
    ) == "<html></html>"
    assert session.headers is not None
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_ifo", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_ifo",
        {"dry_run": True, "series_ids": ["IFO_BUSINESS_CLIMATE"]},
    )
    assert schedule_result["series_planned"] == ["IFO_BUSINESS_CLIMATE"]
    assert schedule_result["series_unknown"] == []

    empty_fetch = svc.invoke(
        "calendar_econ_fetch_ifo",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_fetch["series_planned"] == []
    empty_schedule = svc.invoke(
        "calendar_econ_schedule_ifo",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_schedule["series_planned"] == []


def test_canonical_aliases_cover_ifo_title() -> None:
    assert canonicalize_indicator("Ifo Business Climate Index") == "IFO_BUSINESS_CLIMATE"
    assert canonicalize_indicator("Germany Ifo Business Climate Index") == "IFO_BUSINESS_CLIMATE"
    assert canonicalize_indicator("Ifo Business Climate") == "IFO_BUSINESS_CLIMATE"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert IfoCalendarRawRecord.__name__ == "IfoCalendarRawRecord"
    assert IfoCalendarEventRecord.__name__ == "IfoCalendarEventRecord"
