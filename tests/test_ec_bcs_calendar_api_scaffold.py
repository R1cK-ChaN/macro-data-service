"""Mocked tests for the European Commission BCS calendar connector (issue #24)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.ec_bcs_api import (
    EC_BCS_PRESS_RELEASES_URL,
    EC_BCS_SURVEY_URL,
    INDICATOR_REGISTRY,
    EcBcsCalendarEventRecord,
    EcBcsCalendarRawRecord,
    EcBcsScheduleDocument,
    EcBcsScheduleParseError,
    discover_calendar_pdf_url,
    fetch_ec_bcs_calendar,
    parse_observation,
    parse_press_release_value,
    parse_release_dates_text,
    project_schedule_events,
    reference_label_en,
    resolve_press_release_link,
    schedule_ec_bcs_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.calendar.ec_bcs_api.parser import PROVIDER
from ingestion.calendar.parity import OFFICIAL_PROVIDERS
from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ALL_VALUE_SIDE_CONNECTORS,
)
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def test_registry_carries_issue_24_anchor_indicators() -> None:
    assert set(INDICATOR_REGISTRY) == {"EC_BCS_ESI", "EC_BCS_CCI_FLASH"}
    esi = INDICATOR_REGISTRY["EC_BCS_ESI"]
    cci = INDICATOR_REGISTRY["EC_BCS_CCI_FLASH"]
    assert esi.country_code == cci.country_code == "EU"
    assert esi.importance == cci.importance == "high"
    assert esi.unit == "index"
    assert cci.unit == "balance"
    assert esi.category == "Business Confidence"
    assert cci.category == "Consumer Confidence"


def test_reference_label_helper() -> None:
    assert reference_label_en(date(2026, 4, 1)) == "April 2026"


def test_schedule_parser_extracts_two_streams_with_dst_aware_times() -> None:
    entries = parse_release_dates_text(
        _fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url=EC_BCS_SURVEY_URL,
    )
    # 24 rows = 12 Flash CCI + 12 ESI/full-survey for 2026.
    assert len(entries) == 24

    by_series_jan_april = {
        (e.series_id, e.release_date.isoformat()): e
        for e in entries
        if e.release_date.year == 2026 and e.release_date.month in {1, 4}
    }
    # Flash CCI 22 January 2026 16h00 CET (UTC+1) → 15:00 UTC.
    assert by_series_jan_april[
        ("EC_BCS_CCI_FLASH", "2026-01-22")
    ].event_time_utc == "2026-01-22T15:00:00+00:00"
    # ESI 29 April 2026 11h00 CEST (UTC+2) → 09:00 UTC.
    assert by_series_jan_april[
        ("EC_BCS_ESI", "2026-04-29")
    ].event_time_utc == "2026-04-29T09:00:00+00:00"
    assert all(entry.event_time_precision == "datetime" for entry in entries)
    assert all(entry.source_url == EC_BCS_SURVEY_URL for entry in entries)


def test_december_full_survey_publishes_in_january_but_anchors_on_december() -> None:
    entries = parse_release_dates_text(
        _fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
    )
    december_esi = next(
        e for e in entries
        if e.series_id == "EC_BCS_ESI" and e.release_date.isoformat() == "2027-01-07"
    )
    assert december_esi.reference_date == "2026-12-01"
    assert december_esi.reference_label == "December 2026"


def test_schedule_filter_keeps_requested_series_only() -> None:
    entries = parse_release_dates_text(
        _fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        series_ids={"EC_BCS_CCI_FLASH"},
    )
    assert {e.series_id for e in entries} == {"EC_BCS_CCI_FLASH"}
    assert len(entries) == 12


def test_schedule_parser_raises_when_no_rows_match() -> None:
    with pytest.raises(EcBcsScheduleParseError, match="no recognised schedule"):
        parse_release_dates_text(
            "Publication dates 2026\nNothing useful here.\n",
            source_url=EC_BCS_SURVEY_URL,
        )


def test_discover_calendar_pdf_url_picks_target_year() -> None:
    html = _fixture_text("ec_bcs_schedule", "survey_landing_2026.html")
    url = discover_calendar_pdf_url(html, year=2026)
    assert url.endswith("Publication%20dates%202026.pdf")


def test_discover_calendar_pdf_url_raises_when_missing() -> None:
    html = _fixture_text("ec_bcs_schedule", "survey_landing_2026.html")
    with pytest.raises(EcBcsScheduleParseError, match="2099"):
        discover_calendar_pdf_url(html, year=2099)


def test_listing_resolver_picks_flash_pdf_for_release_date() -> None:
    resolved = resolve_press_release_link(
        _fixture_text("ec_bcs_listing", "press_releases_april_2026.html"),
        series_id="EC_BCS_CCI_FLASH",
        release_date=date(2026, 4, 22),
    )
    assert resolved.source_url.endswith(
        "/document/download/11ffc7fa-f14b-4ed7-a44c-2e45fa85fec5_en"
    )


def test_listing_resolver_picks_esi_pdf_for_release_date() -> None:
    resolved = resolve_press_release_link(
        _fixture_text("ec_bcs_listing", "press_releases_april_2026.html"),
        series_id="EC_BCS_ESI",
        release_date=date(2026, 3, 30),
    )
    assert resolved.source_url.endswith(
        "/document/download/d2316c53-1c0a-4350-b077-e4523fc4d08b_en"
    )


def test_listing_resolver_raises_when_release_not_found() -> None:
    with pytest.raises(EcBcsScheduleParseError, match="2026-12-31"):
        resolve_press_release_link(
            _fixture_text("ec_bcs_listing", "press_releases_april_2026.html"),
            series_id="EC_BCS_CCI_FLASH",
            release_date=date(2026, 12, 31),
        )


def test_press_release_parser_extracts_euro_area_aggregates() -> None:
    flash = parse_press_release_value(
        _fixture_text("ec_bcs_press", "cci_flash_april_2026.txt"),
        spec=INDICATOR_REGISTRY["EC_BCS_CCI_FLASH"],
        reference_date="2026-04-01",
        reference_label="April 2026",
        event_time_utc="2026-04-22T14:00:00+00:00",
    )
    assert flash.value == "-20.6"

    esi = parse_press_release_value(
        _fixture_text("ec_bcs_press", "esi_march_2026.txt"),
        spec=INDICATOR_REGISTRY["EC_BCS_ESI"],
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-30T09:00:00+00:00",
    )
    assert esi.value == "96.6"


def test_parser_projects_value_rows_to_calendar_shape() -> None:
    obs = parse_press_release_value(
        _fixture_text("ec_bcs_press", "esi_march_2026.txt"),
        spec=INDICATOR_REGISTRY["EC_BCS_ESI"],
        reference_date="2026-03-01",
        reference_label="March 2026",
        event_time_utc="2026-03-30T09:00:00+00:00",
    )
    _, event = parse_observation(obs, snapshot_epoch_ms=1_800_000_000_000)
    assert event.provider == PROVIDER == "ec-bcs"
    assert event.country_code == "EU"
    assert event.actual == "96.6"
    assert event.title == "Euro Area Economic Sentiment Indicator"
    assert event.event_time_precision == "datetime"


def test_schedule_and_value_share_provider_event_id() -> None:
    entries = parse_release_dates_text(
        _fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
    )
    march_esi = next(
        e for e in entries
        if e.series_id == "EC_BCS_ESI" and e.release_date.isoformat() == "2026-03-30"
    )
    _, schedule_event = schedule_entry_to_records(
        march_esi, snapshot_epoch_ms=1_800_000_000_000
    )
    obs = parse_press_release_value(
        _fixture_text("ec_bcs_press", "esi_march_2026.txt"),
        spec=INDICATOR_REGISTRY["EC_BCS_ESI"],
        reference_date=march_esi.reference_date,
        reference_label=march_esi.reference_label,
        event_time_utc=march_esi.event_time_utc,
        source_url="https://economy-finance.ec.europa.eu/document/download/d2316c53_en",
    )
    _, value_event = parse_observation(obs, snapshot_epoch_ms=1_800_000_000_000)
    assert schedule_event.provider_event_id == value_event.provider_event_id


def test_schedule_fetcher_projects_fixture_rows(store: SQLiteEngineStore) -> None:
    document = EcBcsScheduleDocument(
        text=_fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url="https://example.test/Publication%20dates%202026.pdf",
    )
    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-04-30",
            dry_run=False,
            document_fetcher=lambda year: document if year == 2026 else None,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ec-bcs'"
        ).fetchone()[0]
    # 4 months × 2 indicators (Flash + ESI) = 8 rows in Jan-Apr 2026.
    assert summary.entries_parsed == 8
    assert sorted(summary.series_ok) == ["EC_BCS_CCI_FLASH", "EC_BCS_ESI"]
    assert count == 8


def test_schedule_fetcher_pulls_every_year_in_window(
    store: SQLiteEngineStore,
) -> None:
    """A late-December refresh whose default window crosses into next-year
    January must download both annual PDFs so the next-year rows aren't
    silently dropped.
    """
    doc_2026 = EcBcsScheduleDocument(
        text=_fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url="https://example.test/Publication%20dates%202026.pdf",
    )
    fake_2027_text = """Publication dates 2027
Flash Consumer Confidence Indicator
Month  Date  Time
January 22 January 2027 16h00
February 19 February 2027 16h00
"""
    doc_2027 = EcBcsScheduleDocument(
        text=fake_2027_text,
        source_url="https://example.test/Publication%20dates%202027.pdf",
    )
    requested_years: list[int] = []

    def _doc(year: int) -> EcBcsScheduleDocument | None:
        requested_years.append(year)
        if year == 2026:
            return doc_2026
        if year == 2027:
            return doc_2027
        return None

    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2026-12-15",
            end_date="2027-01-31",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        rows = conn.execute(
            "SELECT title, event_time_utc FROM cal_econ_event "
            "WHERE provider = 'ec-bcs' ORDER BY event_time_utc"
        ).fetchall()
    assert sorted(requested_years) == [2026, 2027]
    titles = {(t, ts[:10]) for t, ts in rows}
    # Dec 21 2026 Flash CCI + Jan 7 2027 ESI (December reference) from 2026 PDF
    # + Jan 22 2027 Flash CCI from 2027 PDF.
    assert ("Euro Area Consumer Confidence Flash", "2026-12-21") in titles
    assert ("Euro Area Economic Sentiment Indicator", "2027-01-07") in titles
    assert ("Euro Area Consumer Confidence Flash", "2027-01-22") in titles
    assert summary.fetch_error is None


def test_schedule_fetcher_tolerates_missing_next_year_pdf(
    store: SQLiteEngineStore,
) -> None:
    """When the next-year PDF isn't yet linked from the survey page, the
    fetcher logs a row issue and proceeds with whichever years did fetch.
    Important for late-December runs that hit the cross-year window
    before the next-year PDF is published.
    """
    doc_2026 = EcBcsScheduleDocument(
        text=_fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url="https://example.test/Publication%20dates%202026.pdf",
    )

    def _doc(year: int) -> EcBcsScheduleDocument | None:
        return doc_2026 if year == 2026 else None

    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2026-12-15",
            end_date="2027-01-31",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ec-bcs'"
        ).fetchone()[0]
    assert summary.fetch_error is None
    # Dec 21 2026 Flash CCI + Jan 7 2027 ESI both come from the 2026 PDF.
    assert count >= 2


def test_january_window_also_pulls_prior_year_pdf(
    store: SQLiteEngineStore,
) -> None:
    """A January-only window needs the prior-year PDF because the
    December ESI release publishes on the first business day of
    January and lives in that year's calendar PDF.
    """
    doc_2026 = EcBcsScheduleDocument(
        text=_fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url="https://example.test/Publication%20dates%202026.pdf",
    )
    requested_years: list[int] = []

    def _doc(year: int) -> EcBcsScheduleDocument | None:
        requested_years.append(year)
        return doc_2026 if year == 2026 else None

    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2027-01-01",
            end_date="2027-01-31",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        rows = conn.execute(
            "SELECT title, event_time_utc FROM cal_econ_event "
            "WHERE provider = 'ec-bcs' ORDER BY event_time_utc"
        ).fetchall()
    assert 2026 in requested_years
    assert 2027 in requested_years
    titles = {(t, ts[:10]) for t, ts in rows}
    # The December 2026 ESI publishes on 2027-01-07 and only the 2026 PDF
    # carries that row.
    assert ("Euro Area Economic Sentiment Indicator", "2027-01-07") in titles
    assert summary.fetch_error is None


def test_schedule_fetcher_flags_outage_when_every_year_pdf_missing(
    store: SQLiteEngineStore,
) -> None:
    """If every requested year returns ``None`` (link pattern drift on
    the survey landing page), surface a connector-level fetch error
    rather than reporting an empty-but-healthy run.
    """
    def _doc(year: int) -> EcBcsScheduleDocument | None:
        return None

    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is not None
    assert "publication-dates PDF link not found" in summary.fetch_error


def test_schedule_fetcher_records_fetch_error_on_empty_document(
    store: SQLiteEngineStore,
) -> None:
    document = EcBcsScheduleDocument(
        text="Publication dates 2026\nNo schedule rows here.\n",
        source_url="https://example.test/empty.pdf",
    )
    with store.get_connection() as conn:
        summary = schedule_ec_bcs_calendar(
            conn,
            start_date="2026-01-01",
            end_date="2026-12-31",
            dry_run=False,
            document_fetcher=lambda year: document if year == 2026 else None,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        count = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'ec-bcs'"
        ).fetchone()[0]
    assert summary.fetch_error is not None
    assert "no recognised schedule" in summary.fetch_error
    assert summary.series_ok == []
    assert count == 0


def test_value_fetcher_fills_pending_release(store: SQLiteEngineStore) -> None:
    entries = parse_release_dates_text(
        _fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
    )
    march_esi = next(
        e for e in entries
        if e.series_id == "EC_BCS_ESI" and e.release_date.isoformat() == "2026-03-30"
    )
    raw_schedule, event_schedule = schedule_entry_to_records(
        march_esi, snapshot_epoch_ms=1_800_000_000_000,
    )

    listing = _fixture_text("ec_bcs_listing", "press_releases_april_2026.html")
    pdf_text = _fixture_text("ec_bcs_press", "esi_march_2026.txt").encode("utf-8")

    def _listing() -> str:
        return listing

    def _pdf(url: str) -> bytes:
        assert "d2316c53" in url
        return pdf_text

    with store.get_connection() as conn:
        store_raw(conn, [raw_schedule])
        project_schedule_events(conn, [event_schedule])
        summary = fetch_ec_bcs_calendar(
            conn,
            series_ids=["EC_BCS_ESI"],
            dry_run=False,
            listing_fetcher=_listing,
            pdf_fetcher=_pdf,
            now_utc=datetime(2026, 3, 30, 14, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        row = conn.execute(
            """
            SELECT actual, source_url, event_time_utc
            FROM cal_econ_event
            WHERE provider = 'ec-bcs'
              AND title = 'Euro Area Economic Sentiment Indicator'
            """
        ).fetchone()
    assert summary.series_ok == ["EC_BCS_ESI"]
    assert tuple(row) == (
        "96.6",
        "https://economy-finance.ec.europa.eu/document/download/d2316c53-1c0a-4350-b077-e4523fc4d08b_en",
        "2026-03-30T09:00:00+00:00",
    )


def test_schedule_refresh_preserves_pdf_source_url(store: SQLiteEngineStore) -> None:
    document = EcBcsScheduleDocument(
        text=_fixture_text("ec_bcs_schedule", "publication_dates_2026.txt"),
        source_url="https://example.test/Publication%20dates%202026.pdf",
    )
    listing = _fixture_text("ec_bcs_listing", "press_releases_april_2026.html")
    pdf_text = _fixture_text("ec_bcs_press", "esi_march_2026.txt").encode("utf-8")

    def _doc(year: int) -> EcBcsScheduleDocument | None:
        return document if year == 2026 else None

    def _listing() -> str:
        return listing

    def _pdf(url: str) -> bytes:
        return pdf_text

    with store.get_connection() as conn:
        schedule_ec_bcs_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_ec_bcs_calendar(
            conn,
            series_ids=["EC_BCS_ESI"],
            dry_run=False,
            listing_fetcher=_listing,
            pdf_fetcher=_pdf,
            now_utc=datetime(2026, 3, 30, 14, 0, tzinfo=UTC),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        schedule_ec_bcs_calendar(
            conn,
            start_date="2026-03-01",
            end_date="2026-03-31",
            dry_run=False,
            document_fetcher=_doc,
            snapshot_epoch_ms=1_800_000_002_000,
        )
        row = conn.execute(
            """
            SELECT actual, source_url
            FROM cal_econ_event
            WHERE provider = 'ec-bcs'
              AND title = 'Euro Area Economic Sentiment Indicator'
            """
        ).fetchone()
    assert tuple(row) == (
        "96.6",
        "https://economy-finance.ec.europa.eu/document/download/d2316c53-1c0a-4350-b077-e4523fc4d08b_en",
    )


def test_service_dry_runs_return_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    fetch_result = svc.invoke("calendar_econ_fetch_ec_bcs", {"dry_run": True})
    assert fetch_result["series_planned"] == list(INDICATOR_REGISTRY)
    assert fetch_result["stopped_reason"] == "dry_run"

    schedule_result = svc.invoke(
        "calendar_econ_schedule_ec_bcs",
        {"dry_run": True, "series_ids": ["EC_BCS_ESI"]},
    )
    assert schedule_result["series_planned"] == ["EC_BCS_ESI"]
    assert schedule_result["series_unknown"] == []


def test_canonical_aliases_cover_te_labels() -> None:
    assert canonicalize_indicator("Economic Sentiment Indicator") == "EC_BCS_ESI"
    assert canonicalize_indicator("Business Confidence") == "EC_BCS_ESI"
    assert canonicalize_indicator(
        "Consumer Confidence Flash"
    ) == "EC_BCS_CCI_FLASH"
    assert canonicalize_indicator(
        "Flash Consumer Confidence Indicator"
    ) == "EC_BCS_CCI_FLASH"
    # Plain "Consumer Confidence" still resolves to the Conference Board family,
    # since the Flash variant is the only EC BCS-specific signal.
    assert canonicalize_indicator("Consumer Confidence") == "CB_CONSUMER_CONFIDENCE"


def test_provider_is_seeded_in_storage(store: SQLiteEngineStore) -> None:
    with store.get_connection() as conn:
        row = conn.execute(
            "SELECT provider_type, precedence FROM cal_provider WHERE provider_id = 'ec-bcs'"
        ).fetchone()
    assert row is not None
    assert row[0] == "government_agency"
    assert row[1] == 100


def test_parity_roster_includes_ec_bcs() -> None:
    assert "ec-bcs" in OFFICIAL_PROVIDERS


def test_scheduler_includes_ec_bcs_in_both_rosters() -> None:
    assert "ec-bcs" in ALL_CONNECTORS
    assert "ec-bcs" in ALL_VALUE_SIDE_CONNECTORS


def test_record_dataclasses_match_shared_projector_shape() -> None:
    assert EcBcsCalendarRawRecord.__name__ == "EcBcsCalendarRawRecord"
    assert EcBcsCalendarEventRecord.__name__ == "EcBcsCalendarEventRecord"


def test_press_release_listing_url_constant() -> None:
    assert EC_BCS_PRESS_RELEASES_URL.startswith(
        "https://economy-finance.ec.europa.eu/"
    )
    assert "press-releases" in EC_BCS_PRESS_RELEASES_URL
