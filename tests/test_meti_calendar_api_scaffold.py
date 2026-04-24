"""Mocked tests for the METI calendar connector (issue #14 P5)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.meti_api import (
    fetch_meti_calendar,
    fetch_meti_values,
    parse_iip_release_calendar_xml,
    parse_iip_report_html,
    parse_retail_current_page_html,
    parse_retail_outline_text,
    parse_retail_schedule_html,
    project_schedule_events,
    schedule_entry_to_records,
    store_raw,
)
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(path: str) -> str:
    return (FIXTURES / path).read_text(encoding="utf-8")


def _epoch_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def test_iip_release_calendar_xml_filters_preliminary_rows() -> None:
    entries = parse_iip_release_calendar_xml(
        _fixture("meti_iip/release_calendar.xml")
    )

    assert [(e.reference_date, e.release_date) for e in entries] == [
        (date(2026, 2, 1), date(2026, 3, 31)),
        (date(2026, 3, 1), date(2026, 4, 30)),
    ]
    assert {e.indicator for e in entries} == {"INDUSTRIAL_PRODUCTION"}
    assert entries[0].release_time_local == "08:50"


def test_iip_release_calendar_xml_ignores_parent_aggregate_text() -> None:
    xml = """
    <releaseCalendar>
      <release>
        <statistics>Indices of Industrial Production</statistics>
        <reportType>Revised Report</reportType>
        <referenceMonth>January 2026</referenceMonth>
        <releaseDate>March 14, 2026</releaseDate>
      </release>
      <release>
        <statistics>Indices of Industrial Production</statistics>
        <reportType>Preliminary Report</reportType>
        <referenceMonth>February 2026</referenceMonth>
        <releaseDate>March 31, 2026</releaseDate>
      </release>
    </releaseCalendar>
    """

    entries = parse_iip_release_calendar_xml(xml)

    assert [(e.reference_date, e.release_date) for e in entries] == [
        (date(2026, 2, 1), date(2026, 3, 31)),
    ]


def test_retail_schedule_html_parses_next_release() -> None:
    entry = parse_retail_schedule_html(_fixture("meti_retail/index.html"))

    assert entry.indicator == "RETAIL_SALES"
    assert entry.reference_date == date(2026, 3, 1)
    assert entry.release_date == date(2026, 4, 30)
    assert entry.release_time_local == "08:50"
    assert entry.report_url.endswith("/statistics/tyo/syoudou/index.html")


def test_schedule_entry_projects_jst_release_time() -> None:
    entry = parse_iip_release_calendar_xml(
        _fixture("meti_iip/release_calendar.xml")
    )[0]
    raw, event = schedule_entry_to_records(entry, snapshot_epoch_ms=1)

    assert raw.provider == "meti"
    assert event.title == "Industrial Production MoM Prel"
    assert event.event_time_utc == "2026-03-30T23:50:00+00:00"
    assert event.reference_date == "2026-02-01"
    assert event.actual is None


def test_iip_report_parser_extracts_production_mom() -> None:
    value = parse_iip_report_html(
        _fixture("meti_iip/b2020_202602se.html"),
        source_url="https://example.test/iip.html",
    )

    assert value.reference_date == date(2026, 2, 1)
    assert value.release_date == date(2026, 3, 31)
    assert value.production_index_sa == "102.3"
    assert value.production_mom_percent == "-2.1"
    assert value.production_yoy_percent == "0.3"


def test_retail_report_parser_extracts_retail_yoy() -> None:
    page = parse_retail_current_page_html(_fixture("meti_retail/index.html"))
    value = parse_retail_outline_text(
        _fixture("meti_retail/202602S.txt"),
        page=page,
    )

    assert value.reference_date == date(2026, 2, 1)
    assert value.release_date == date(2026, 3, 31)
    assert value.retail_sales_billion_yen == "12155"
    assert value.retail_sales_yoy_percent == "-0.2"
    assert value.retail_sales_mom_sa_percent == "-2.0"


def test_fetch_meti_calendar_projects_iip_and_retail_rows(
    store: SQLiteEngineStore,
) -> None:
    conn = store.get_connection()
    try:
        summary = fetch_meti_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-01T00:00:00+00:00"),
            iip_xml_fetcher=lambda: _fixture("meti_iip/release_calendar.xml"),
            retail_html_fetcher=lambda: _fixture("meti_retail/index.html"),
        )
        conn.commit()
    finally:
        conn.close()

    assert summary.releases_parsed == 3
    assert summary.events_upserted == 3
    with store.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, reference_date, event_time_utc, actual
            FROM cal_econ_event
            WHERE provider = 'meti'
            ORDER BY title, reference_date
            """
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        (
            "Industrial Production MoM Prel",
            "2026-02-01",
            "2026-03-30T23:50:00+00:00",
            None,
        ),
        (
            "Industrial Production MoM Prel",
            "2026-03-01",
            "2026-04-29T23:50:00+00:00",
            None,
        ),
        (
            "Retail Sales YoY",
            "2026-03-01",
            "2026-04-29T23:50:00+00:00",
            None,
        ),
    ]


def test_fetch_meti_values_fills_iip_pending_and_current_retail(
    store: SQLiteEngineStore,
) -> None:
    conn = store.get_connection()
    try:
        fetch_meti_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-01T00:00:00+00:00"),
            iip_xml_fetcher=lambda: _fixture("meti_iip/release_calendar.xml"),
            retail_html_fetcher=lambda: _fixture("meti_retail/index.html"),
        )
        conn.commit()
    finally:
        conn.close()

    conn = store.get_connection()
    try:
        summary = fetch_meti_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-01T01:00:00+00:00"),
            iip_html_fetcher=lambda _ref: _fixture("meti_iip/b2020_202602se.html"),
            retail_page_fetcher=lambda: _fixture("meti_retail/index.html"),
            retail_pdf_text_fetcher=lambda _url: _fixture("meti_retail/202602S.txt"),
        )
        conn.commit()
    finally:
        conn.close()

    assert summary.releases_fetched == 2
    with store.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT title, reference_date, event_time_utc, actual, source_url
            FROM cal_econ_event
            WHERE provider = 'meti'
            ORDER BY title, reference_date
            """
        ).fetchall()
    by_key = {(r["title"], r["reference_date"]): r for r in rows}
    iip = by_key[("Industrial Production MoM Prel", "2026-02-01")]
    assert iip["actual"] == "-2.1"
    assert iip["event_time_utc"] == "2026-03-30T23:50:00+00:00"
    retail = by_key[("Retail Sales YoY", "2026-02-01")]
    assert retail["actual"] == "-0.2"
    assert retail["event_time_utc"] == "2026-03-30T23:50:00+00:00"
    assert retail["source_url"].endswith("/statistics/tyo/syoudou/result/pdf/202602S.pdf")

    pdf_requested = False

    def _unexpected_pdf_fetch(_url: str) -> str:
        nonlocal pdf_requested
        pdf_requested = True
        return _fixture("meti_retail/202602S.txt")

    conn = store.get_connection()
    try:
        second = fetch_meti_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-04-01T02:00:00+00:00"),
            iip_html_fetcher=lambda _ref: _fixture("meti_iip/b2020_202602se.html"),
            retail_page_fetcher=lambda: _fixture("meti_retail/index.html"),
            retail_pdf_text_fetcher=_unexpected_pdf_fetch,
        )
        conn.commit()
    finally:
        conn.close()

    assert second.releases_planned == 0
    assert second.releases_fetched == 0
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == 0
    assert pdf_requested is False


def test_fetch_meti_values_reports_stale_retail_reference_without_failure(
    store: SQLiteEngineStore,
) -> None:
    entry = parse_retail_schedule_html(_fixture("meti_retail/index.html"))
    raw, event = schedule_entry_to_records(
        entry,
        snapshot_epoch_ms=_epoch_ms("2026-04-01T00:00:00+00:00"),
    )
    with store.get_connection() as conn:
        store_raw(conn, [raw])
        project_schedule_events(conn, [event])
        conn.commit()

    with store.get_connection() as conn:
        summary = fetch_meti_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=_epoch_ms("2026-05-01T00:00:00+00:00"),
            retail_page_fetcher=lambda: _fixture("meti_retail/index.html"),
            retail_pdf_text_fetcher=lambda _url: _fixture("meti_retail/202602S.txt"),
        )
        conn.commit()

    assert summary.stale_references == [("2026-03-01", "2026-02-01")]
    assert summary.fetch_failures == []
    assert summary.parse_failures == []
    assert summary.releases_fetched == 1


def test_meti_canonicalize_aliases() -> None:
    assert canonicalize_indicator("Industrial Production MoM Prel") == (
        "INDUSTRIAL_PRODUCTION"
    )
    assert canonicalize_indicator("Retail Sales YoY") == "RETAIL_SALES"
    assert canonicalize_indicator("Japan Retail Sales") == "RETAIL_SALES"


def test_service_dry_runs_expose_meti_ops(store: SQLiteEngineStore) -> None:
    service = LocalMacroDataService(store=store)

    assert service.invoke("calendar_econ_fetch_meti", {"dry_run": True}) == {
        "dry_run": True,
        "indicators_planned": ["INDUSTRIAL_PRODUCTION", "RETAIL_SALES"],
        "stopped_reason": "dry_run",
    }
    values = service.invoke("calendar_econ_fetch_meti_values", {"dry_run": True})
    assert values["dry_run"] is True
    assert values["indicators_planned"] == [
        "INDUSTRIAL_PRODUCTION",
        "RETAIL_SALES",
    ]
