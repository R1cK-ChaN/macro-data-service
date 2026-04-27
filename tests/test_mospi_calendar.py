"""Mocked tests for the MoSPI calendar connector (issue #54 P1).

The captured fixture
``tests/fixtures/mospi_release_calendar/year_2026.json`` was
recorded live on 2026-04-27 from
``POST mospi.gov.in/api/release-calender/fetch-all-release-calender-Web``
with ``{"lang": "en", "year": 2026}``. It includes the four CPI
releases (Dec 2025 → Mar 2026 reference periods), three IIP releases
(Dec 2025 / Jan 2026 / Feb 2026), two GDP releases (First Advance
Estimate of FY 2025-26 and Press Note on the new GDP series), and
~20 unrelated MoSPI publications (PLFS bulletins, infrastructure
review reports, etc.) the title-substring matcher must skip.

No real HTTP in CI — every test injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.mospi_api import (
    INDICATOR_REGISTRY,
    MoSPICalendarParseError,
    announcement_matches_spec,
    announcement_to_records,
    fetch_mospi_calendar,
    parse_release_calendar,
)
from ingestion.calendar.mospi_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "mospi_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _year_2026_json() -> str:
    return (FIXTURE_DIR / "year_2026.json").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_calendar_extracts_28_rows_from_real_capture() -> None:
    """The 2026 capture has 28 schedule rows across CPI / IIP / GDP /
    PLFS / Infrastructure / Energy / etc. Every row with a parseable
    ``year`` / ``month`` / ``day`` and a non-empty title must land
    in the parsed list — the indicator matcher decides which rows
    project, not the parser."""
    announcements = parse_release_calendar(_year_2026_json())
    assert len(announcements) == 28
    # Sanity: every row carries a date triple within calendar 2026.
    assert all(a.year == 2026 for a in announcements)
    assert all(1 <= a.month <= 12 for a in announcements)
    assert all(1 <= a.day <= 31 for a in announcements)


def test_announcement_matches_spec_routes_cpi_iip_gdp() -> None:
    announcements = parse_release_calendar(_year_2026_json())
    cpi_spec = INDICATOR_REGISTRY["CPI"]
    iip_spec = INDICATOR_REGISTRY["INDUSTRIAL_PRODUCTION"]
    gdp_spec = INDICATOR_REGISTRY["GDP"]
    cpi_matches = [a for a in announcements if announcement_matches_spec(a, cpi_spec)]
    iip_matches = [a for a in announcements if announcement_matches_spec(a, iip_spec)]
    gdp_matches = [a for a in announcements if announcement_matches_spec(a, gdp_spec)]
    # Capture window covers four CPI prints (Dec 2025 / Jan / Feb / Mar 2026 reference).
    assert len(cpi_matches) == 4
    # Three IIP prints (Dec 2025 / Jan 2026 / Feb 2026 reference).
    assert len(iip_matches) == 3
    # Two GDP releases — First Advance Estimate FY 2025-26 + new-series Press Note.
    assert len(gdp_matches) == 2


def test_parse_release_calendar_drops_undated_rows() -> None:
    """Rows without ``day`` (the API's monthly placeholder shape) and
    rows with non-integer date fields must be skipped silently —
    the connector cannot anchor a calendar event on a missing date."""
    payload = """{
        "success": true, "code": 200,
        "data": [
            {"id": 1, "title": "All India CPI", "year": 2026, "month": 4, "day": 13, "level": "day"},
            {"id": 2, "title": "Some Quarterly Bulletin", "year": 2026, "month": 4, "day": null, "level": "month"},
            {"id": 3, "title": "Bad Row", "year": 2026, "month": 13, "day": 1, "level": "day"}
        ]
    }"""
    announcements = parse_release_calendar(payload)
    assert len(announcements) == 1
    assert announcements[0].title == "All India CPI"


def test_parse_release_calendar_raises_on_non_success_envelope() -> None:
    payload = '{"success": false, "message": "Year is required."}'
    with pytest.raises(MoSPICalendarParseError, match="non-success envelope"):
        parse_release_calendar(payload)


def test_parse_release_calendar_raises_on_zero_rows() -> None:
    payload = '{"success": true, "data": []}'
    with pytest.raises(MoSPICalendarParseError, match="zero schedule rows"):
        parse_release_calendar(payload)


def test_parse_release_calendar_raises_on_invalid_json() -> None:
    with pytest.raises(MoSPICalendarParseError, match="not JSON"):
        parse_release_calendar("not json at all")


# ── projection ───────────────────────────────────────────────────


def test_announcement_to_records_anchors_cpi_on_prior_month_at_ist() -> None:
    """CPI release on 2026-04-13 (April release of March data) must
    anchor on March 2026 reference_date and 17:30 IST event time —
    SDDS standard for Indian official statistics."""
    announcements = parse_release_calendar(_year_2026_json())
    spec = INDICATOR_REGISTRY["CPI"]
    march_release = [
        a for a in announcements
        if announcement_matches_spec(a, spec)
        and a.month == 4 and a.day == 13
    ][0]
    raw_rec, event_rec = announcement_to_records(
        march_release, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "IN"
    assert event_rec.title == "India Consumer Price Index"
    assert event_rec.currency == "INR"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "datetime"
    # 17:30 IST = 12:00 UTC (IST is UTC+05:30 year-round).
    assert event_rec.event_time_utc.startswith("2026-04-13T12:00:00")
    # CPI lag = 1; March 2026 release reports February 2026 data.
    assert event_rec.reference_date == "2026-03-01"
    assert event_rec.reference_label == "March 2026"
    # provider_event_id stable across re-projection.
    _, event_rec_again = announcement_to_records(
        march_release, spec=spec, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_announcement_to_records_anchors_iip_with_two_month_lag() -> None:
    """IIP release on 2026-03-02 covers *January* 2026 data — IIP's
    publication lag isn't fixed (Jan 28 release covers Dec data;
    Mar 2 covers Jan; Mar 30 covers Feb), so the parser reads the
    explicit ``"for the month of"`` marker out of the row's
    description rather than walking back a fixed number of months."""
    announcements = parse_release_calendar(_year_2026_json())
    spec = INDICATOR_REGISTRY["INDUSTRIAL_PRODUCTION"]
    march_release = [
        a for a in announcements
        if announcement_matches_spec(a, spec)
        and a.month == 3 and a.day == 2
    ][0]
    _, event_rec = announcement_to_records(
        march_release, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.reference_date == "2026-01-01"
    assert event_rec.reference_label == "January 2026"
    assert event_rec.title == "India Index of Industrial Production"


def test_iip_releases_in_same_publication_month_get_distinct_event_ids() -> None:
    """Regression: the 2026 fixture has IIP releases on Jan 28 (Dec
    2025 data), Mar 2 (Jan 2026 data), and Mar 30 (Feb 2026 data).
    A fixed-lag heuristic (``release_month - 2``) collapses the Mar
    2 and Mar 30 releases onto the same January 2026 reference, so
    their synthesized ``provider_event_id``s collide and the second
    upsert silently overwrites the first cal_econ_event row.
    Reading the period from the row text keeps each release on its
    own row."""
    announcements = parse_release_calendar(_year_2026_json())
    spec = INDICATOR_REGISTRY["INDUSTRIAL_PRODUCTION"]
    iip_releases = sorted(
        [a for a in announcements if announcement_matches_spec(a, spec)],
        key=lambda a: (a.month, a.day),
    )
    assert len(iip_releases) == 3
    event_ids = []
    reference_dates = []
    for release in iip_releases:
        _, event = announcement_to_records(
            release, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
        )
        event_ids.append(event.provider_event_id)
        reference_dates.append(event.reference_date)
    assert reference_dates == ["2025-12-01", "2026-01-01", "2026-02-01"]
    assert len(set(event_ids)) == 3


def test_fetch_mospi_calendar_writes_distinct_rows_per_unique_period(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end check that the 9 matched events project to 9
    distinct cal_econ_event rows — collision regression guard."""
    def fetcher(year: int) -> str:
        return _year_2026_json()
    with store._connection(commit=True) as conn:
        fetch_mospi_calendar(
            conn,
            years=[2026],
            dry_run=False,
            json_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider='mospi'",
        ).fetchone()
    assert rows[0] == 9


def test_monthly_reschedule_keeps_event_id_stable() -> None:
    """An upstream reschedule of the April CPI release from the 13th
    to the 14th must keep the same ``provider_event_id`` — a release-
    date anchor would spawn a stale-date duplicate row in
    ``cal_econ_event``. Monthly indicators anchor on the reference
    period (March 2026 in both cases)."""
    spec = INDICATOR_REGISTRY["CPI"]
    base_payload = """{
        "success": true, "code": 200,
        "data": [
            {"id": 1, "title": "All India Consumer Price Index (CPI)",
             "year": 2026, "month": 4, "day": 13, "level": "day"}
        ]
    }"""
    rescheduled_payload = """{
        "success": true, "code": 200,
        "data": [
            {"id": 1, "title": "All India Consumer Price Index (CPI)",
             "year": 2026, "month": 4, "day": 14, "level": "day"}
        ]
    }"""
    base_announcements = parse_release_calendar(base_payload)
    rescheduled_announcements = parse_release_calendar(rescheduled_payload)
    _, base_event = announcement_to_records(
        base_announcements[0], spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    _, rescheduled_event = announcement_to_records(
        rescheduled_announcements[0], spec=spec, snapshot_epoch_ms=1_800_000_000_001,
    )
    assert base_event.provider_event_id == rescheduled_event.provider_event_id
    # The event time DOES move (anchored on the new release date).
    assert base_event.event_time_utc.startswith("2026-04-13")
    assert rescheduled_event.event_time_utc.startswith("2026-04-14")


def test_announcement_to_records_anchors_quarterly_gdp_on_prior_quarter_end() -> None:
    """GDP First Advance Estimate released 2026-01-06 anchors on the
    most recent quarter-end strictly before release — Dec 31 2025
    (Q4 calendar 2025 = Q3 of FY 2025-26 in India). The provider_
    event_id keys on the *release* date so the two GDP releases in
    this fixture (FAE on Jan 6 + new-series press note on Feb 27)
    stay distinct rows even though both anchor on Q4 2025."""
    announcements = parse_release_calendar(_year_2026_json())
    spec = INDICATOR_REGISTRY["GDP"]
    matches = sorted(
        [a for a in announcements if announcement_matches_spec(a, spec)],
        key=lambda a: (a.month, a.day),
    )
    assert len(matches) == 2
    fae_release, press_note = matches[0], matches[1]
    fae_raw, fae_event = announcement_to_records(
        fae_release, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    pn_raw, pn_event = announcement_to_records(
        press_note, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert fae_event.reference_date == "2025-12-31"
    assert fae_event.reference_label == "Q4 2025"
    assert pn_event.reference_date == "2025-12-31"
    # Stage-distinct identity — two GDP releases for Q4 2025 stay
    # in separate cal_econ_event rows.
    assert fae_event.provider_event_id != pn_event.provider_event_id


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_mospi_calendar_writes_event_per_matched_indicator(
    store: SQLiteEngineStore,
) -> None:
    """The 2026 capture should produce 4 CPI + 3 IIP + 2 GDP = 9 events."""
    def fetcher(year: int) -> str:
        assert year == 2026
        return _year_2026_json()

    with store._connection(commit=True) as conn:
        summary = fetch_mospi_calendar(
            conn,
            years=[2026],
            dry_run=False,
            json_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.announcements_seen == 9
    assert summary.events_upserted == 9
    assert set(summary.indicators_ok) == {
        "CPI", "INDUSTRIAL_PRODUCTION", "GDP",
    }
    assert summary.indicators_empty == []


def test_fetch_mospi_calendar_isolates_per_indicator_absence(
    store: SQLiteEngineStore,
) -> None:
    """When a year carries no matching releases for an indicator,
    the fetcher must report it on ``indicators_empty`` rather than
    flag a connector-wide failure."""
    payload = """{
        "success": true, "code": 200,
        "data": [
            {"id": 1, "title": "All India Consumer Price Index (CPI)",
             "year": 2026, "month": 4, "day": 13, "level": "day"}
        ]
    }"""
    def fetcher(year: int) -> str:
        return payload
    with store._connection(commit=True) as conn:
        summary = fetch_mospi_calendar(
            conn,
            years=[2026],
            dry_run=False,
            json_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert "CPI" in summary.indicators_ok
    assert set(summary.indicators_empty) == {"INDUSTRIAL_PRODUCTION", "GDP"}
    assert summary.events_upserted == 1


def test_fetch_mospi_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    """A network failure must mark the run with ``fetch_error`` and
    write zero rows; every planned indicator lands on
    ``series_failed`` so the next sweep re-attempts."""
    def broken(year: int) -> str:
        raise RuntimeError("simulated 503 from MoSPI")

    with store._connection(commit=True) as conn:
        summary = fetch_mospi_calendar(
            conn, years=[2026], dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0
    failed_keys = {k for k, _ in summary.series_failed}
    assert failed_keys == set(INDICATOR_REGISTRY.keys())


def test_fetch_mospi_calendar_records_parse_error_on_layout_drift(
    store: SQLiteEngineStore,
) -> None:
    """A response that returns 200 but parses zero rows must trip the
    connector's loud-failure path so the operator can triage layout
    drift."""
    def empty(year: int) -> str:
        return '{"success": true, "data": []}'

    with store._connection(commit=True) as conn:
        summary = fetch_mospi_calendar(
            conn, years=[2026], dry_run=False, json_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_mospi_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_mospi_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())
    # Default years = current calendar year (single-element list).
    assert len(summary.years_planned) == 1


def test_fetch_mospi_calendar_iterates_multiple_years(
    store: SQLiteEngineStore,
) -> None:
    """The years argument should drive one POST per year; both years
    contribute to the merged result set."""
    payload_2025 = """{
        "success": true, "code": 200, "data": [
            {"id": 200, "title": "All India Consumer Price Index (CPI)",
             "year": 2025, "month": 1, "day": 12, "level": "day"}
        ]
    }"""
    payload_2026 = """{
        "success": true, "code": 200, "data": [
            {"id": 201, "title": "All India Consumer Price Index (CPI)",
             "year": 2026, "month": 1, "day": 12, "level": "day"}
        ]
    }"""
    fetched_years: list[int] = []
    def fetcher(year: int) -> str:
        fetched_years.append(year)
        return {2025: payload_2025, 2026: payload_2026}[year]
    with store._connection(commit=True) as conn:
        summary = fetch_mospi_calendar(
            conn,
            years=[2025, 2026],
            dry_run=False,
            json_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert fetched_years == [2025, 2026]
    assert summary.events_upserted == 2


# ── scheduler + agency wiring ───────────────────────────────────


def test_mospi_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "mospi" in ALL_CONNECTORS
    assert "mospi" in ALL_VALUE_SIDE_CONNECTORS


def test_mospi_agency_attribution_provider_only_in_p1() -> None:
    """MoSPI owns provider attribution for IN statistical releases,
    but the parity whitelist stays empty in P1 — schedule-only events
    have ``actual=NULL``, so registering an indicator would trip the
    parity comparator's parse_failed-on-missing-actual path on every
    release. Same deferral pattern as the ABS / BoC slice."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    mospi_agency = provider_to_agency("mospi")
    assert mospi_agency is not None and mospi_agency.agency_id == "MOSPI"
    assert mospi_agency.indicators == frozenset()
    assert agency_for("IN", "CPI") is None
    assert agency_for("IN", "INDUSTRIAL_PRODUCTION") is None
    assert agency_for("IN", "GDP") is None


def test_mospi_canonicalize_aliases_resolve_india_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("India Inflation Rate") == "CPI"
    assert canonicalize_indicator("India CPI") == "CPI"
    assert canonicalize_indicator("All India Consumer Price Index") == "CPI"
    assert canonicalize_indicator("India GDP") == "GDP"
    assert canonicalize_indicator("India GDP Growth Rate") == "GDP"
    assert canonicalize_indicator(
        "India Index of Industrial Production",
    ) == "INDUSTRIAL_PRODUCTION"
    assert canonicalize_indicator("India IIP") == "INDUSTRIAL_PRODUCTION"


def test_india_country_filter_resolves_through_alias_map() -> None:
    """``country='India'`` on the calendar query helpers must route to
    ``country_code='IN'``, and stored ``IN`` rows must render as
    ``"India"`` in the response — without these two aliases, MoSPI /
    RBI rows are invisible to ``list_recent_events(country='India')``
    and present as the bare ISO code in event payloads."""
    from storage.queries.calendar import (
        _calendar_country_code,
        _calendar_country_display,
    )
    assert _calendar_country_code("India") == "IN"
    assert _calendar_country_code("IND") == "IN"
    assert _calendar_country_code("IN") == "IN"
    assert _calendar_country_display("IN") == "India"
