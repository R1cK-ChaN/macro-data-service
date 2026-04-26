"""Mocked tests for the Cabinet Office / ESRI GDP connector.

Fixtures are small slices of the official archive/menu/CSV shapes:
``toukei_2025.html`` lists staged GDP releases, ``gdemenuea`` exposes
the real seasonally adjusted quarter-to-quarter CSV link, and the CSV
keeps the headline GDP column as the first data series.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.cao_gdp_api import (
    ALL_INDICATORS,
    FIRST_PRELIMINARY,
    SECOND_PRELIMINARY,
    INDICATOR_REGISTRY,
    PROVIDER,
    CaoGdpArchiveParseError,
    CaoGdpReportParseError,
    CaoGdpValue,
    archive_year_url,
    build_report_url,
    fetch_cao_gdp_calendar,
    fetch_cao_gdp_values,
    gdp_value_to_records,
    parse_gdp_archive_html,
    parse_gdp_archive_index_html,
    parse_gdp_growth_csv,
    parse_gdp_report_menu_html,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    select_archive_years,
    store_raw,
)
from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ALL_VALUE_SIDE_CONNECTORS,
)
from storage.sqlite import SQLiteEngineStore


ARCHIVE_FIXTURES = Path(__file__).parent / "fixtures" / "cao_gdp_archive"
REPORT_FIXTURES = Path(__file__).parent / "fixtures" / "cao_gdp_reports"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _archive_fixture(name: str) -> str:
    return (ARCHIVE_FIXTURES / name).read_text(encoding="utf-8")


def _report_fixture(name: str) -> str:
    return (REPORT_FIXTURES / name).read_text(encoding="utf-8")


def test_registry_holds_two_gdp_stages() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {
        "GDP_QOQ_FIRST_PRELIMINARY",
        "GDP_QOQ_SECOND_PRELIMINARY",
    }
    first = INDICATOR_REGISTRY["GDP_QOQ_FIRST_PRELIMINARY"]
    second = INDICATOR_REGISTRY["GDP_QOQ_SECOND_PRELIMINARY"]
    assert first.release_stage == FIRST_PRELIMINARY
    assert second.release_stage == SECOND_PRELIMINARY
    assert first.title == "GDP Growth Rate QoQ Prel"
    assert second.title == "GDP Growth Rate QoQ Final"
    assert first.country_code == "JP"
    assert second.unit == "percent"
    assert PROVIDER == "cao"
    assert ALL_INDICATORS == sorted(INDICATOR_REGISTRY.keys())


def test_gdp_titles_canonicalize_to_gdp() -> None:
    assert canonicalize_indicator("GDP Growth Rate QoQ Prel") == "GDP"
    assert canonicalize_indicator("GDP Growth Rate QoQ Final") == "GDP"
    assert canonicalize_indicator("Japan GDP Growth Rate QoQ") == "GDP"


def test_archive_index_selects_latest_years() -> None:
    years = parse_gdp_archive_index_html(_archive_fixture("toukei_top.html"))
    assert years == [2025, 2024]
    assert select_archive_years([2023, 2025, 2024], limit=2) == [2025, 2024]


def test_archive_parser_extracts_staged_releases() -> None:
    entries = parse_gdp_archive_html(
        _archive_fixture("toukei_2025.html"),
        base_url=archive_year_url(2025),
    )
    assert [(e.reference_date, e.release_stage) for e in entries] == [
        (date(2025, 12, 31), SECOND_PRELIMINARY),
        (date(2025, 12, 31), FIRST_PRELIMINARY),
        (date(2025, 9, 30), SECOND_PRELIMINARY),
        (date(2025, 9, 30), FIRST_PRELIMINARY),
    ]
    assert [e.release_date for e in entries] == [
        date(2026, 3, 10),
        date(2026, 2, 16),
        date(2025, 12, 8),
        date(2025, 11, 17),
    ]
    assert entries[2].reference_label == "Q3 2025"
    assert entries[2].report_url.endswith("/2025/qe253_2/gdemenuea.html")


def test_archive_parser_dedups_republished_release() -> None:
    html = """<html><body><table><tbody>
    <tr><td>Jun 10, 2024</td><td><a href="/2024/qe241_2/gdemenuea.html">Quarterly Estimates of GDP for Jan.-Mar.2024 (The Second preliminary Estimates)</a></td></tr>
    <tr><td>Dec  8, 2024</td><td><a href="/2024/qe241_2/gdemenuea.html">Quarterly Estimates of GDP for Jan.-Mar.2024 (The Second preliminary Estimates)</a></td></tr>
    </tbody></table></body></html>"""
    entries = parse_gdp_archive_html(html, base_url=archive_year_url(2024))
    assert len(entries) == 1
    assert entries[0].reference_date == date(2024, 3, 31)
    assert entries[0].release_stage == SECOND_PRELIMINARY
    assert entries[0].release_date == date(2024, 12, 8)


def test_build_report_url_matches_archive_shape() -> None:
    assert build_report_url(date(2025, 9, 30), FIRST_PRELIMINARY).endswith(
        "/2025/qe253/gdemenuea.html"
    )
    assert build_report_url(date(2025, 9, 30), SECOND_PRELIMINARY).endswith(
        "/2025/qe253_2/gdemenuea.html"
    )


def test_schedule_records_use_stage_qualified_ids() -> None:
    entries = parse_gdp_archive_html(
        _archive_fixture("toukei_2025.html"),
        base_url=archive_year_url(2025),
    )
    second = entries[2]
    first = entries[3]
    raw_second, event_second = schedule_entry_to_records(
        second,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    raw_first, event_first = schedule_entry_to_records(
        first,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    assert raw_second.provider_event_id != raw_first.provider_event_id
    assert event_second.title == "GDP Growth Rate QoQ Final"
    assert event_first.title == "GDP Growth Rate QoQ Prel"
    assert event_second.event_time_utc == "2025-12-07T23:50:00+00:00"
    assert event_second.reference_date == "2025-09-30"
    assert event_second.source_url == second.report_url


def test_report_menu_finds_real_qoq_csv() -> None:
    csv_url = parse_gdp_report_menu_html(
        _report_fixture("gdemenuea_qe253_2.html"),
        report_url=build_report_url(date(2025, 9, 30), SECOND_PRELIMINARY),
    )
    assert csv_url.endswith("/2025/qe253_2/tables/ritu-jk2532.csv")


def test_report_menu_requires_single_exact_csv_link() -> None:
    html = """<html><body>
    <a href="/tables/nritu-jk2532.csv">Real, Seasonally Adjusted Series (Quarter-to-Quarter, Annualized)</a>
    </body></html>"""
    with pytest.raises(CaoGdpReportParseError):
        parse_gdp_report_menu_html(
            html,
            report_url=build_report_url(date(2025, 9, 30), SECOND_PRELIMINARY),
        )


def test_csv_parser_extracts_headline_reference_value() -> None:
    value = parse_gdp_growth_csv(
        _report_fixture("ritu-jk2532.csv"),
        reference_date=date(2025, 9, 30),
        csv_url="https://example.test/ritu-jk2532.csv",
    )
    assert value.reference_label == "Q3 2025"
    assert value.actual == "-0.6"


def test_csv_parser_carries_year_forward() -> None:
    value = parse_gdp_growth_csv(
        _report_fixture("ritu-jk2532.csv"),
        reference_date=date(2025, 12, 31),
        csv_url="https://example.test/ritu-jk2532.csv",
    )
    assert value.reference_label == "Q4 2025"
    assert value.actual == "0.3"


def test_csv_parser_raises_on_missing_reference() -> None:
    with pytest.raises(CaoGdpReportParseError):
        parse_gdp_growth_csv(
            _report_fixture("ritu-jk2532.csv"),
            reference_date=date(2026, 3, 31),
            csv_url="https://example.test/ritu-jk2532.csv",
        )


def test_value_records_match_schedule_id_and_source_url() -> None:
    entry = parse_gdp_archive_html(
        _archive_fixture("toukei_2025.html"),
        base_url=archive_year_url(2025),
    )[2]
    raw_sched, event_sched = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = CaoGdpValue(
        reference_date=date(2025, 9, 30),
        reference_label="Q3 2025",
        actual="-0.6",
        csv_url="https://example.test/ritu-jk2532.csv",
    )
    raw_value, event_value = gdp_value_to_records(
        value,
        release_stage=SECOND_PRELIMINARY,
        snapshot_epoch_ms=1_700_000_000_100,
        report_url=entry.report_url,
        event_time_utc=event_sched.event_time_utc,
    )
    assert raw_value.provider_event_id == raw_sched.provider_event_id
    assert event_value.actual == "-0.6"
    assert event_value.source_url == entry.report_url


def test_projector_schedule_then_value_fills_actual(
    store: SQLiteEngineStore,
) -> None:
    entry = parse_gdp_archive_html(
        _archive_fixture("toukei_2025.html"),
        base_url=archive_year_url(2025),
    )[2]
    raw_sched, event_sched = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=1_700_000_000_000,
    )
    value = CaoGdpValue(
        reference_date=date(2025, 9, 30),
        reference_label="Q3 2025",
        actual="-0.6",
        csv_url="https://example.test/ritu-jk2532.csv",
    )
    raw_value, event_value = gdp_value_to_records(
        value,
        release_stage=SECOND_PRELIMINARY,
        snapshot_epoch_ms=1_700_000_000_100,
        report_url=entry.report_url,
        event_time_utc=event_sched.event_time_utc,
    )
    with store._connection(commit=True) as c:
        store_raw(c, [raw_sched])
        project_schedule_events(c, [event_sched])
        store_raw(c, [raw_value])
        project_events(c, [event_value])
        row = c.execute(
            "SELECT title, actual, event_time_utc, source_url "
            "FROM cal_econ_event WHERE provider='cao' "
            "AND reference_date='2025-09-30'"
        ).fetchone()
    assert row is not None
    assert row[0] == "GDP Growth Rate QoQ Final"
    assert row[1] == "-0.6"
    assert row[2] == event_sched.event_time_utc
    assert row[3] == entry.report_url


def test_fetch_calendar_with_fixture_fetchers(
    store: SQLiteEngineStore,
) -> None:
    def _archive_html(year: int) -> str:
        assert year == 2025
        return _archive_fixture("toukei_2025.html")

    with store._connection(commit=True) as c:
        summary = fetch_cao_gdp_calendar(
            c,
            dry_run=False,
            archive_years=[2025],
            archive_html_fetcher=_archive_html,
        )
        count = c.execute(
            "SELECT COUNT(*) FROM cal_econ_event "
            "WHERE provider='cao' AND title LIKE 'GDP Growth Rate QoQ%'"
        ).fetchone()[0]
    assert summary.archive_pages_fetched == 1
    assert summary.releases_parsed == 4
    assert summary.events_upserted == 4
    assert count == 4


def test_fetch_calendar_preserves_explicit_multi_year_backfills(
    store: SQLiteEngineStore,
) -> None:
    called_years: list[int] = []

    def _archive_html(year: int) -> str:
        called_years.append(year)
        yy = year % 100
        return f"""<!doctype html><html><body><table><tbody>
        <tr><td>Dec 8, {year}</td><td>
        <a href="/en/sna/data/sokuhou/files/{year}/qe{yy}3_2/gdemenuea.html">
        Quarterly Estimates of GDP for Jul.-Sep.{year}
        (The Second preliminary Estimates)
        </a></td></tr>
        </tbody></table></body></html>"""

    with store._connection(commit=True) as c:
        summary = fetch_cao_gdp_calendar(
            c,
            dry_run=False,
            archive_years=[2025, 2024, 2023],
            archive_html_fetcher=_archive_html,
        )
    assert called_years == [2025, 2024, 2023]
    assert summary.archive_years_planned == [2025, 2024, 2023]
    assert summary.archive_pages_fetched == 3
    assert summary.releases_parsed == 3


def test_fetch_calendar_raises_on_empty_selected_archive_year(
    store: SQLiteEngineStore,
) -> None:
    def _archive_html(year: int) -> str:
        if year == 2025:
            return "<html><body><table><tbody></tbody></table></body></html>"
        return _archive_fixture("toukei_2025.html")

    with store._connection(commit=True) as c:
        with pytest.raises(
            CaoGdpArchiveParseError,
            match="archive year 2025 returned zero releases",
        ):
            fetch_cao_gdp_calendar(
                c,
                dry_run=False,
                archive_years=[2025, 2024],
                archive_html_fetcher=_archive_html,
            )


def test_fetch_values_auto_discovers_pending_rows(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as c:
        fetch_cao_gdp_calendar(
            c,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            archive_years=[2025],
            archive_html_fetcher=lambda year: _archive_fixture("toukei_2025.html"),
        )
        summary = fetch_cao_gdp_values(
            c,
            dry_run=False,
            snapshot_epoch_ms=1_767_000_000_000,
            menu_html_fetcher=lambda url: _report_fixture("gdemenuea_qe253_2.html"),
            csv_fetcher=lambda url: _report_fixture("ritu-jk2532.csv"),
        )
        rows = c.execute(
            "SELECT title, actual FROM cal_econ_event "
            "WHERE provider='cao' AND reference_date='2025-09-30' "
            "ORDER BY title"
        ).fetchall()
    assert summary.releases_planned == 2
    assert summary.releases_fetched == 2
    assert {tuple(r) for r in rows} == {
        ("GDP Growth Rate QoQ Final", "-0.6"),
        ("GDP Growth Rate QoQ Prel", "-0.6"),
    }


def test_fetch_values_collects_fetch_and_parse_failures(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as c:
        fetch_cao_gdp_calendar(
            c,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            archive_years=[2025],
            archive_html_fetcher=lambda year: _archive_fixture("toukei_2025.html"),
        )
        summary_fetch = fetch_cao_gdp_values(
            c,
            dry_run=False,
            snapshot_epoch_ms=1_767_000_000_000,
            menu_html_fetcher=lambda url: (_ for _ in ()).throw(
                RuntimeError("simulated 503")
            ),
        )
        summary_parse = fetch_cao_gdp_values(
            c,
            dry_run=False,
            snapshot_epoch_ms=1_767_000_000_000,
            menu_html_fetcher=lambda url: "<html><body>missing csv</body></html>",
        )
    assert len(summary_fetch.fetch_failures) == 2
    assert summary_fetch.parse_failures == []
    assert summary_parse.fetch_failures == []
    assert len(summary_parse.parse_failures) == 2


def test_cao_gdp_registered_in_scheduler_rosters() -> None:
    assert "cao-gdp" in ALL_CONNECTORS
    assert "cao-gdp-values" in ALL_VALUE_SIDE_CONNECTORS


def test_service_cao_gdp_dry_runs(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    schedule = svc.invoke("calendar_econ_fetch_cao_gdp", {"dry_run": True})
    values = svc.invoke("calendar_econ_fetch_cao_gdp_values", {"dry_run": True})
    assert schedule["dry_run"] is True
    assert schedule["indicators_planned"] == list(ALL_INDICATORS)
    assert values["dry_run"] is True
    assert values["releases_planned"] == 0
