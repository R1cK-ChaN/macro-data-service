"""Mocked tests for the Germany ZEW calendar connector (issue #15 P4a)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.zew_api import (
    INDICATOR_REGISTRY,
    ZEWCalendarEventRecord,
    ZEWCalendarRawRecord,
    ZEWScheduleParseError,
    fetch_press_release_html,
    fetch_zew_calendar,
    parse_observation,
    parse_press_release_value,
    parse_release_dates_html,
    project_schedule_events,
    reference_label_en,
    release_dates_url,
    resolve_press_release_link,
    schedule_entry_to_records,
    schedule_zew_calendar,
    store_raw,
)
from ingestion.calendar.zew_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_contains_issue_15_p4a_anchor() -> None:
    spec = INDICATOR_REGISTRY["ZEW_ECONOMIC_SENTIMENT"]
    assert spec.country_code == "DE"
    assert spec.indicator == "ZEW Economic Sentiment"
    assert spec.category == "Business Confidence"
    assert spec.unit == "index"
    assert release_dates_url(2026).endswith(
        "/2026-release-dates-for-zew-indicator-of-economic-sentiment-fixed"
    )


def test_reference_label_helper() -> None:
    assert reference_label_en(datetime(2026, 4, 1).date()) == "April 2026"


def test_schedule_parser_extracts_release_dates_and_dst_times() -> None:
    entries = parse_release_dates_html(
        _fixture_text("zew_schedule", "release_dates_2026.html"),
        source_url=release_dates_url(2026),
    )
    assert [entry.reference_label for entry in entries[:4]] == [
        "January 2026",
        "February 2026",
        "March 2026",
        "April 2026",
    ]
    assert entries[0].event_time_utc == "2026-01-20T10:05:00+00:00"
    assert entries[3].event_time_utc == "2026-04-21T09:05:00+00:00"
    assert entries[-1].reference_date == "2027-01-01"


def test_schedule_parser_raises_on_empty_planned_page() -> None:
    with pytest.raises(ZEWScheduleParseError, match="no ZEW_ECONOMIC_SENTIMENT"):
        parse_release_dates_html(
            "<html><body>Press releases</body></html>",
            source_url=release_dates_url(2026),
        )


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_dates_html(
        _fixture_text("zew_schedule", "release_dates_2026.html"),
        series_ids=set(),
    )
    assert entries == []


def test_listing_resolver_selects_matching_date_and_release() -> None:
    resolved = resolve_press_release_link(
        _fixture_text("zew_listing", "latest_april_2026.html"),
        release_date=datetime(2026, 4, 21).date(),
    )
    assert resolved.source_url.endswith(
        "/en/press/latest-press-releases/zew-index-continues-to-deteriorate"
    )


def test_press_release_parser_extracts_plus_and_minus_values() -> None:
    spec = INDICATOR_REGISTRY["ZEW_ECONOMIC_SENTIMENT"]
    april = parse_press_release_value(
        _fixture_text("zew_press", "april_2026.html"),
        spec=spec,
        reference_date="2026-04-01",
        reference_label="April 2026",
        event_time_utc="2026-04-21T09:05:00+00:00",
    )
    assert april.value == "-17.2"

    february = parse_press_release_value(
        _fixture_text("zew_press", "february_2026.html"),
        spec=spec,
        reference_date="2026-02-01",
        reference_label="February 2026",
        event_time_utc="2026-02-17T10:05:00+00:00",
    )
    assert february.value == "58.3"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("zew_press", "april_2026.html"),
        spec=INDICATOR_REGISTRY["ZEW_ECONOMIC_SENTIMENT"],
        reference_date="2026-04-01",
        reference_label="April 2026",
        event_time_utc="2026-04-21T09:05:00+00:00",
    )
    _, event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.provider == PROVIDER == "zew"
    assert event.reference_date == "2026-04-01"
    assert event.event_time_precision == "datetime"
    assert event.actual == "-17.2"
    assert event.title == "Germany ZEW Economic Sentiment Index"


def test_schedule_and_value_share_provider_event_id() -> None:
    entry = parse_release_dates_html(
        _fixture_text("zew_schedule", "release_dates_2026.html"),
    )[3]
    _, schedule_event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    obs = parse_press_release_value(
        _fixture_text("zew_press", "april_2026.html"),
        spec=INDICATOR_REGISTRY["ZEW_ECONOMIC_SENTIMENT"],
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        event_time_utc=entry.event_time_utc,
        source_url="https://www.zew.de/en/press/latest-press-releases/zew-index-continues-to-deteriorate",
    )
    _, value_event = parse_observation(
        obs,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_zew_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda year: _fixture_text(
                "zew_schedule", f"release_dates_{year}.html"
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'zew'"
        ).fetchone()[0]
    assert summary.entries_parsed == 4
    assert summary.series_ok == ["ZEW_ECONOMIC_SENTIMENT"]
    assert count == 4


def test_schedule_fetcher_flags_empty_upstream_page(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_zew_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-12-31",
            dry_run=False,
            html_fetcher=lambda year: "<html><body>Press releases</body></html>",
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'zew'"
        ).fetchone()[0]
    assert summary.fetch_error is not None
    assert "no ZEW_ECONOMIC_SENTIMENT" in summary.fetch_error
    assert summary.series_ok == []
    assert summary.entries_parsed == 0
    assert count == 0


def test_fetcher_fills_due_pending_release(store: SQLiteEngineStore) -> None:
    entry = parse_release_dates_html(
        _fixture_text("zew_schedule", "release_dates_2026.html"),
    )[3]
    raw_schedule, event_schedule = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_800_000_000_000,
    )

    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-04-21"
        return _fixture_text("zew_listing", "latest_april_2026.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/zew-index-continues-to-deteriorate")
        return _fixture_text("zew_press", "april_2026.html")

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_zew_calendar(
            conn,
            series_ids=["ZEW_ECONOMIC_SENTIMENT"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual, source_url "
            "FROM cal_econ_event WHERE provider = 'zew'"
        ).fetchone()
    assert summary.series_ok == ["ZEW_ECONOMIC_SENTIMENT"]
    assert tuple(row) == (
        "2026-04-21T09:05:00+00:00",
        "datetime",
        "-17.2",
        "https://www.zew.de/en/press/latest-press-releases/zew-index-continues-to-deteriorate",
    )


def test_schedule_refresh_preserves_release_source_url(store: SQLiteEngineStore) -> None:
    def _listing(release_date):  # noqa: ANN001
        assert release_date.isoformat() == "2026-04-21"
        return _fixture_text("zew_listing", "latest_april_2026.html")

    def _fetch(url: str) -> str:
        assert url.endswith("/zew-index-continues-to-deteriorate")
        return _fixture_text("zew_press", "april_2026.html")

    with store.get_connection() as conn:
        schedule_zew_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda year: _fixture_text(
                "zew_schedule", f"release_dates_{year}.html"
            ),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_zew_calendar(
            conn,
            series_ids=["ZEW_ECONOMIC_SENTIMENT"],
            dry_run=False,
            listing_fetcher=_listing,
            html_fetcher=_fetch,
            now_utc=datetime(2026, 4, 21, 10, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        schedule_zew_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda year: _fixture_text(
                "zew_schedule", f"release_dates_{year}.html"
            ),
            snapshot_epoch_ms=1_800_000_002_000,
        )
        row = conn.execute(
            "SELECT actual, source_url FROM cal_econ_event WHERE provider = 'zew'"
        ).fetchone()

    assert tuple(row) == (
        "-17.2",
        "https://www.zew.de/en/press/latest-press-releases/zew-index-continues-to-deteriorate",
    )


def test_http_helpers_use_browser_headers() -> None:
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
    assert fetch_press_release_html("https://www.zew.de/x", session=session) == "<html></html>"  # type: ignore[arg-type]
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_zew", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_zew",
        {"dry_run": True, "series_ids": ["ZEW_ECONOMIC_SENTIMENT"]},
    )
    assert schedule_result["series_planned"] == ["ZEW_ECONOMIC_SENTIMENT"]
    assert schedule_result["series_unknown"] == []

    empty_fetch = svc.invoke(
        "calendar_econ_fetch_zew",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_fetch["series_planned"] == []
    empty_schedule = svc.invoke(
        "calendar_econ_schedule_zew",
        {"dry_run": True, "series_ids": []},
    )
    assert empty_schedule["series_planned"] == []


def test_canonical_aliases_cover_zew_title() -> None:
    assert canonicalize_indicator("ZEW Economic Sentiment Index") == "ZEW_SENTIMENT"
    assert canonicalize_indicator("Germany ZEW Economic Sentiment Index") == "ZEW_SENTIMENT"
    assert canonicalize_indicator("ZEW Economic Sentiment") == "ZEW_SENTIMENT"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert ZEWCalendarRawRecord.__name__ == "ZEWCalendarRawRecord"
    assert ZEWCalendarEventRecord.__name__ == "ZEWCalendarEventRecord"
