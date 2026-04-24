"""Mocked tests for the Germany HCOB / S&P Global PMI connector (issue #15 P5, schedule-only)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.hcob_api import (
    HCOB_RELEASE_DATES_URL,
    HCOBCalendarEventRecord,
    HCOBCalendarRawRecord,
    HCOBScheduleParseError,
    INDICATOR_REGISTRY,
    fetch_release_dates_html,
    parse_release_dates_html,
    schedule_entry_to_records,
    schedule_hcob_calendar,
    spec_for_calendar_title,
)
from ingestion.calendar.hcob_api.parser import PROVIDER
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_ships_issue_15_p5_anchors() -> None:
    expected = {"HCOB_FLASH_PMI", "HCOB_MANUFACTURING_PMI", "HCOB_SERVICES_PMI"}
    assert set(INDICATOR_REGISTRY) == expected
    for spec in INDICATOR_REGISTRY.values():
        assert spec.country_code == "DE"
        assert spec.category == "Business Confidence"
        assert spec.unit == "index"
    assert HCOB_RELEASE_DATES_URL.endswith(
        "/Public/Release/ReleaseDates?language=en"
    )


def test_spec_for_calendar_title_maps_upstream_labels() -> None:
    assert spec_for_calendar_title("s&p global flash germany pmi").series_id == "HCOB_FLASH_PMI"
    assert spec_for_calendar_title(
        "s&p global germany manufacturing pmi"
    ).series_id == "HCOB_MANUFACTURING_PMI"
    assert spec_for_calendar_title(
        "s&p global germany services pmi"
    ).series_id == "HCOB_SERVICES_PMI"
    # Construction PMI is out of scope for P5; must not map.
    assert spec_for_calendar_title("s&p global germany construction pmi") is None


def test_schedule_parser_picks_only_whitelisted_german_rows() -> None:
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        source_url=HCOB_RELEASE_DATES_URL,
        today=date(2026, 4, 24),
    )
    # France rows, Construction, and French Flash must be filtered out.
    assert {e.series_id for e in entries} == {
        "HCOB_FLASH_PMI",
        "HCOB_MANUFACTURING_PMI",
        "HCOB_SERVICES_PMI",
    }
    assert len(entries) == 5  # 2x Mfg (May + Jun + rollover-Jan) — see rollover test
    # Title is rebranded from "S&P Global" to "HCOB" in storage.
    assert all(e.release_title.startswith("Germany HCOB") for e in entries)


def test_schedule_parser_resolves_year_forward_from_today() -> None:
    # Today = 2026-04-24. "January 05" is in the past for 2026 → rolls to 2027.
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        source_url=HCOB_RELEASE_DATES_URL,
        today=date(2026, 4, 24),
    )
    dates = sorted({(e.series_id, e.release_date.isoformat()) for e in entries})
    # May 04 and June 02 → 2026 Manufacturing; January 05 → 2027 Manufacturing.
    assert ("HCOB_MANUFACTURING_PMI", "2026-05-04") in dates
    assert ("HCOB_MANUFACTURING_PMI", "2026-06-02") in dates
    assert ("HCOB_MANUFACTURING_PMI", "2027-01-05") in dates


def test_schedule_parser_sets_event_time_at_calendar_clock() -> None:
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        source_url=HCOB_RELEASE_DATES_URL,
        today=date(2026, 4, 24),
    )
    flash = next(e for e in entries if e.series_id == "HCOB_FLASH_PMI")
    manufacturing_may = next(
        e for e in entries
        if e.series_id == "HCOB_MANUFACTURING_PMI" and e.release_date.month == 5
    )
    assert flash.event_time_utc == "2026-05-21T07:30:00+00:00"
    assert manufacturing_may.event_time_utc == "2026-05-04T07:55:00+00:00"
    assert all(e.event_time_precision == "datetime" for e in entries)


def test_flash_reference_is_release_month_final_reference_is_prior_month() -> None:
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        source_url=HCOB_RELEASE_DATES_URL,
        today=date(2026, 4, 24),
    )
    flash = next(e for e in entries if e.series_id == "HCOB_FLASH_PMI")
    manufacturing_may = next(
        e for e in entries
        if e.series_id == "HCOB_MANUFACTURING_PMI" and e.release_date.month == 5
    )
    manufacturing_jan_rollover = next(
        e for e in entries
        if e.series_id == "HCOB_MANUFACTURING_PMI" and e.release_date.year == 2027
    )
    # Flash on May 21, 2026 reports the May 2026 flash number.
    assert flash.reference_date == "2026-05-01"
    assert flash.reference_label == "May 2026"
    # Final Manufacturing on May 4, 2026 reports April 2026 data.
    assert manufacturing_may.reference_date == "2026-04-01"
    assert manufacturing_may.reference_label == "April 2026"
    # Final Manufacturing on Jan 5, 2027 reports December 2026 data
    # (year rollover on the reporting side, not just the release side).
    assert manufacturing_jan_rollover.reference_date == "2026-12-01"
    assert manufacturing_jan_rollover.reference_label == "December 2026"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        series_ids={"HCOB_FLASH_PMI"},
        today=date(2026, 4, 24),
    )
    assert {e.series_id for e in entries} == {"HCOB_FLASH_PMI"}


def test_schedule_parser_raises_when_no_germany_rows_match() -> None:
    with pytest.raises(HCOBScheduleParseError, match="no matching HCOB Germany"):
        parse_release_dates_html(
            "<html><body><div class='listSubHeading'><span>May 04</span></div>"
            "<div class='listItem'>S&P Global UK Manufacturing PMI</div>"
            "</body></html>",
            source_url=HCOB_RELEASE_DATES_URL,
            today=date(2026, 4, 24),
        )


def test_schedule_and_value_records_share_provider_event_id() -> None:
    entries = parse_release_dates_html(
        _fixture_text("hcob_calendar", "release_dates.html"),
        today=date(2026, 4, 24),
    )
    flash = next(e for e in entries if e.series_id == "HCOB_FLASH_PMI")
    _, event_record = schedule_entry_to_records(
        flash,
        snapshot_epoch_ms=1_800_000_000_000,
    )
    # Regression: the provider_event_id hashes (provider, country,
    # canonical indicator, reference_date). A future P5-values slice
    # upserting on the same tuple must converge on this exact id.
    assert event_record.provider == PROVIDER == "hcob"
    assert event_record.title == "Germany HCOB Flash PMI"
    assert event_record.reference_date == "2026-05-01"
    assert event_record.actual is None


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_hcob_calendar(
            conn,
            start_date="2026-05-01",
            end_date="2026-06-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text(
                "hcob_calendar", "release_dates.html"
            ),
            today=date(2026, 4, 24),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'hcob'"
        ).fetchone()[0]
    # Inside the window: May 04 Mfg, May 06 Svc, May 21 Flash, June 02 Mfg.
    assert summary.entries_parsed == 4
    assert set(summary.series_ok) == {
        "HCOB_FLASH_PMI", "HCOB_MANUFACTURING_PMI", "HCOB_SERVICES_PMI"
    }
    assert count == 4


def test_schedule_fetcher_flags_empty_upstream_page(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        summary = schedule_hcob_calendar(
            conn,
            start_date="2026-05-01",
            end_date="2026-12-31",
            dry_run=False,
            html_fetcher=lambda: "<html><body>Upcoming</body></html>",
            today=date(2026, 4, 24),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'hcob'"
        ).fetchone()[0]
    assert summary.fetch_error is not None
    assert "no matching HCOB Germany" in summary.fetch_error
    assert summary.series_ok == []
    assert count == 0


def test_schedule_refresh_is_idempotent(store: SQLiteEngineStore) -> None:
    def _html() -> str:
        return _fixture_text("hcob_calendar", "release_dates.html")

    with store.get_connection() as conn:
        schedule_hcob_calendar(
            conn,
            start_date="2026-05-01",
            end_date="2026-06-30",
            dry_run=False,
            html_fetcher=_html,
            today=date(2026, 4, 24),
            snapshot_epoch_ms=1_800_000_000_000,
        )
        schedule_hcob_calendar(
            conn,
            start_date="2026-05-01",
            end_date="2026-06-30",
            dry_run=False,
            html_fetcher=_html,
            today=date(2026, 4, 24),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'hcob'"
        ).fetchone()[0]
    assert count == 4


def test_http_helper_uses_browser_headers() -> None:
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
    assert fetch_release_dates_html(session=session) == "<html></html>"  # type: ignore[arg-type]
    assert session.headers is not None
    assert "Mozilla" in session.headers["User-Agent"]


def test_service_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    schedule_result = svc.invoke(
        "calendar_econ_schedule_hcob",
        {"dry_run": True},
    )
    assert set(schedule_result["series_planned"]) == set(INDICATOR_REGISTRY)
    assert schedule_result["stopped_reason"] == "dry_run"

    empty = svc.invoke(
        "calendar_econ_schedule_hcob",
        {"dry_run": True, "series_ids": []},
    )
    assert empty["series_planned"] == []


def test_canonical_aliases_cover_te_hcob_titles() -> None:
    # Final / Flash mapping for the two-slot, day-distinct flavours:
    assert canonicalize_indicator("HCOB Manufacturing PMI") == "HCOB_MANUFACTURING_PMI"
    assert canonicalize_indicator("HCOB Services PMI") == "HCOB_SERVICES_PMI"
    assert canonicalize_indicator("Germany HCOB Flash PMI") == "HCOB_FLASH_PMI"
    assert canonicalize_indicator("S&P Global Flash Germany PMI") == "HCOB_FLASH_PMI"
    assert canonicalize_indicator(
        "S&P Global Germany Manufacturing PMI"
    ) == "HCOB_MANUFACTURING_PMI"
    # Documented collision: the shared _MODIFIER_SUFFIXES table strips
    # a trailing " flash" before alias lookup, so TE's per-component
    # flash labels ("HCOB Manufacturing PMI Flash") collapse onto the
    # same canonical as the *final* monthly release. P5 ships one
    # combined `HCOB Flash PMI` row on flash day; a future P5-values
    # slice can split out per-component flash rows for parity.
    assert canonicalize_indicator(
        "HCOB Manufacturing PMI Flash"
    ) == "HCOB_MANUFACTURING_PMI"


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert HCOBCalendarRawRecord.__name__ == "HCOBCalendarRawRecord"
    assert HCOBCalendarEventRecord.__name__ == "HCOBCalendarEventRecord"
