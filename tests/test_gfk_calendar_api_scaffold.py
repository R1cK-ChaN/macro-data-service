"""Mocked tests for the Germany GfK / NIM Consumer Climate connector (issue #15 P4c)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.gfk_api import (
    GFK_CONSUMER_CLIMATE_URL,
    INDICATOR_REGISTRY,
    GfKCalendarEventRecord,
    GfKCalendarRawRecord,
    GfKScheduleParseError,
    fetch_gfk_calendar,
    fetch_press_release_html,
    parse_observation,
    parse_press_release_value,
    parse_release_dates_html,
    project_schedule_events,
    reference_label_en,
    resolve_press_release_link,
    schedule_entry_to_records,
    schedule_gfk_calendar,
    store_raw,
)
from ingestion.calendar.gfk_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p4c_anchor() -> None:
    spec = INDICATOR_REGISTRY["GFK_CONSUMER_CLIMATE"]
    assert spec.country_code == "DE"
    assert spec.indicator == "GfK Consumer Climate"
    assert spec.category == "Consumer Confidence"
    assert spec.unit == "index"
    assert spec.title == "Germany GfK Consumer Climate"
    assert GFK_CONSUMER_CLIMATE_URL.endswith("/en/consumer-climate")


def test_reference_label_helper() -> None:
    assert reference_label_en(datetime(2026, 4, 1).date()) == "April 2026"


def test_schedule_parser_extracts_planned_dates_with_dst_times() -> None:
    entries = parse_release_dates_html(
        _fixture_text("gfk_schedule", "consumer_climate_page.html"),
        source_url=GFK_CONSUMER_CLIMATE_URL,
    )
    assert [entry.reference_label for entry in entries[:4]] == [
        "January 2026",
        "February 2026",
        "March 2026",
        "April 2026",
    ]
    # CET (winter, UTC+1) → 08:00 Berlin = 07:00 UTC
    assert entries[0].event_time_utc == "2026-01-29T07:00:00+00:00"
    # CEST (summer, UTC+2) → 08:00 Berlin = 06:00 UTC
    assert entries[3].event_time_utc == "2026-04-27T06:00:00+00:00"
    assert all(entry.event_time_precision == "datetime" for entry in entries)
    assert entries[0].source_url == GFK_CONSUMER_CLIMATE_URL


def test_schedule_parser_raises_on_empty_page() -> None:
    with pytest.raises(GfKScheduleParseError, match="no GFK_CONSUMER_CLIMATE"):
        parse_release_dates_html(
            "<html><body>No planned dates listed.</body></html>",
            source_url=GFK_CONSUMER_CLIMATE_URL,
        )


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_dates_html(
        _fixture_text("gfk_schedule", "consumer_climate_page.html"),
        series_ids=set(),
    )
    assert entries == []


def test_schedule_parser_defaults_release_time_when_absent() -> None:
    html = """
    <html><body>
      <p>Planned publication dates:</p>
      <ul><li>Friday, May 22, 2026</li></ul>
    </body></html>
    """
    entries = parse_release_dates_html(html, source_url=GFK_CONSUMER_CLIMATE_URL)
    # Default 08:00 Europe/Berlin (CEST in May) → 06:00 UTC.
    assert entries[0].event_time_utc == "2026-05-22T06:00:00+00:00"


def test_listing_resolver_selects_matching_release_by_date() -> None:
    resolved = resolve_press_release_link(
        _fixture_text("gfk_listing", "all_releases.html"),
        release_date=datetime(2026, 3, 26).date(),
    )
    assert resolved.source_url.endswith(
        "/en/consumer-climate/detail-consumer-climate/konsumklima-iran-krieg-drueckt-verbraucherstimmung"
    )
    assert resolved.release_date == "2026-03-26"


def test_listing_resolver_rejects_non_consumer_climate_entry_on_same_day() -> None:
    """A newsletter entry sharing the release date must not be picked."""
    resolved = resolve_press_release_link(
        _fixture_text("gfk_listing", "all_releases.html"),
        release_date=datetime(2026, 3, 26).date(),
    )
    assert "/detail-consumer-climate/" in resolved.source_url


def test_listing_resolver_raises_when_date_not_found() -> None:
    with pytest.raises(GfKScheduleParseError, match="2026-12-31"):
        resolve_press_release_link(
            _fixture_text("gfk_listing", "all_releases.html"),
            release_date=datetime(2026, 12, 31).date(),
        )


def test_press_release_parser_extracts_negative_and_positive_levels() -> None:
    spec = INDICATOR_REGISTRY["GFK_CONSUMER_CLIMATE"]
    march = parse_press_release_value(
        _fixture_text("gfk_press", "march_2026.html"),
        spec=spec,
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-26T07:00:00+00:00",
    )
    assert march.value == "-28.0"

    january = parse_press_release_value(
        _fixture_text("gfk_press", "january_2026.html"),
        spec=spec,
        reference_date="2026-01-01",
        reference_label="January 2026",
        event_time_utc="2026-01-29T07:00:00+00:00",
    )
    assert january.value == "-18.4"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("gfk_press", "march_2026.html"),
        spec=INDICATOR_REGISTRY["GFK_CONSUMER_CLIMATE"],
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-26T07:00:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "gfk"
    assert event.reference_date == "2026-03-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "-28.0"
    assert event.title == "Germany GfK Consumer Climate"
    assert event.country_code == "DE"


def test_schedule_and_value_share_provider_event_id() -> None:
    entries = parse_release_dates_html(
        _fixture_text("gfk_schedule", "consumer_climate_page.html"),
    )
    march_entry = next(e for e in entries if e.reference_date == "2026-03-01")
    _, schedule_event = schedule_entry_to_records(
        march_entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("gfk_press", "march_2026.html"),
        spec=INDICATOR_REGISTRY["GFK_CONSUMER_CLIMATE"],
        reference_date=march_entry.reference_date,
        reference_label=march_entry.reference_label,
        event_time_utc=march_entry.event_time_utc,
        source_url="https://www.nim.org/en/consumer-climate/detail-consumer-climate/konsumklima-iran-krieg-drueckt-verbraucherstimmung",
    )
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_gfk_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text(
                "gfk_schedule", "consumer_climate_page.html"
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'gfk'"
        ).fetchone()[0]
    assert summary.entries_parsed == 4
    assert summary.series_ok == ["GFK_CONSUMER_CLIMATE"]
    assert count == 4


def test_schedule_fetcher_flags_empty_upstream_page(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_gfk_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-12-31",
            dry_run=False,
            html_fetcher=lambda: "<html><body>No planned dates listed.</body></html>",
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'gfk'"
        ).fetchone()[0]
    assert summary.fetch_error is not None
    assert "no GFK_CONSUMER_CLIMATE" in summary.fetch_error
    assert summary.series_ok == []
    assert count == 0


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_release_dates_html(
        _fixture_text("gfk_schedule", "consumer_climate_page.html"),
    )
    march_entry = next(e for e in entries if e.reference_date == "2026-03-01")
    raw_schedule, event_schedule = schedule_entry_to_records(
        march_entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-03-26"
        return _fixture_text("gfk_listing", "all_releases.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/konsumklima-iran-krieg-drueckt-verbraucherstimmung")
        return _fixture_text("gfk_press", "march_2026.html")

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_gfk_calendar(
            conn,
            series_ids=["GFK_CONSUMER_CLIMATE"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 3, 26, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual, source_url "
            "FROM cal_econ_event WHERE provider = 'gfk'"
        ).fetchone()
    assert summary.series_ok == ["GFK_CONSUMER_CLIMATE"]
    assert tuple(row) == (
        "2026-03-26T07:00:00+00:00",
        "datetime",
        "-28.0",
        "https://www.nim.org/en/consumer-climate/detail-consumer-climate/konsumklima-iran-krieg-drueckt-verbraucherstimmung",
    )


def test_schedule_refresh_preserves_release_source_url(store: SQLiteEngineStore) -> None:
    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-03-26"
        return _fixture_text("gfk_listing", "all_releases.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/konsumklima-iran-krieg-drueckt-verbraucherstimmung")
        return _fixture_text("gfk_press", "march_2026.html")

    def _schedule_html() -> str:
        return _fixture_text("gfk_schedule", "consumer_climate_page.html")

    with store.get_connection() as conn:
        schedule_gfk_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            html_fetcher=_schedule_html,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_gfk_calendar(
            conn,
            series_ids=["GFK_CONSUMER_CLIMATE"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 3, 26, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        schedule_gfk_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            html_fetcher=_schedule_html,
            snapshot_epoch_ms=1_800_000_002_000,
        )
        row = conn.execute(
            "SELECT actual, source_url FROM cal_econ_event WHERE provider = 'gfk'"
        ).fetchone()

    assert tuple(row) == (
        "-28.0",
        "https://www.nim.org/en/consumer-climate/detail-consumer-climate/konsumklima-iran-krieg-drueckt-verbraucherstimmung",
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
        "https://www.nim.org/x", session=session
    ) == "<html></html>"
    assert session.headers is not None
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_gfk", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_gfk",
        {"dry_run": True, "series_ids": ["GFK_CONSUMER_CLIMATE"]},
    )
    assert schedule_result["series_planned"] == ["GFK_CONSUMER_CLIMATE"]
    assert schedule_result["series_unknown"] == []

    empty_fetch = svc.invoke(
        "calendar_econ_fetch_gfk",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_fetch["series_planned"] == []
    empty_schedule = svc.invoke(
        "calendar_econ_schedule_gfk",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_schedule["series_planned"] == []


def test_canonical_aliases_cover_gfk_title() -> None:
    assert canonicalize_indicator("GfK Consumer Climate") == "GFK_CONSUMER_CLIMATE"
    assert canonicalize_indicator("GfK Consumer Confidence") == "GFK_CONSUMER_CLIMATE"
    assert canonicalize_indicator("Germany GfK Consumer Climate") == "GFK_CONSUMER_CLIMATE"
    assert canonicalize_indicator("NIM Consumer Climate") == "GFK_CONSUMER_CLIMATE"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert GfKCalendarRawRecord.__name__ == "GfKCalendarRawRecord"
    assert GfKCalendarEventRecord.__name__ == "GfKCalendarEventRecord"
