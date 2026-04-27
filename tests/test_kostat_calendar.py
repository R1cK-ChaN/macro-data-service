"""Mocked tests for the KOSTAT calendar connector (issue #55 P1).

The captured fixture
``tests/fixtures/kostat_release_calendar/release_schedule.html`` was
recorded live on 2026-04-27 from
``mods.go.kr/menu.es?mid=a20301000000``. It carries the full 2026
release schedule of three monthly indicators (CPI, Industrial
Production, Economically Active Population) plus a handful of other
non-target rows that the title-substring matcher excludes.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.kostat_api import (
    INDICATOR_REGISTRY,
    KOSTATCalendarParseError,
    announcement_to_records,
    fetch_kostat_calendar,
    parse_release_schedule,
)
from ingestion.calendar.kostat_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "kostat_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _schedule_html() -> str:
    return (FIXTURE_DIR / "release_schedule.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_schedule_extracts_2026_year_from_heading() -> None:
    """The fixture's ``<h3>2026 Schedule</h3>`` heading sets the
    publication year for every parsed row."""
    announcements = parse_release_schedule(_schedule_html())
    assert announcements
    assert {a.schedule_year for a in announcements} == {2026}


def test_parse_release_schedule_finds_all_three_p1_indicator_titles() -> None:
    """Each P1 indicator title appears at least 11 times in the 2026
    schedule (12 monthly publications minus any partial-year header)."""
    announcements = parse_release_schedule(_schedule_html())
    cpi = [a for a in announcements if "Consumer Price Index" in a.title]
    iip = [a for a in announcements if "Monthly Industrial Statistics" in a.title]
    eap = [
        a for a in announcements
        if "Economically Active Population Survey" in a.title
    ]
    # Each of the three monthly series publishes every month; the
    # 2026 schedule lists every release once. Allow ≥11 to absorb a
    # missing-Jan or missing-Dec edge case if KOSTAT prunes the page
    # mid-year. The point of the test is "all three families parse".
    assert len(cpi) >= 11
    assert len(iip) >= 11
    assert len(eap) >= 11


def test_parse_release_schedule_extracts_reference_period_from_title() -> None:
    """``"... in <Month> <Year>"`` substring must populate the
    ``reference_year`` / ``reference_month`` fields."""
    announcements = parse_release_schedule(_schedule_html())
    march_cpi = next(
        a for a in announcements
        if a.title == "The Consumer Price Index in March 2026"
    )
    assert march_cpi.reference_year == 2026
    assert march_cpi.reference_month == 3
    assert march_cpi.publication_month == 4
    assert march_cpi.publication_day == 2


def test_parse_release_schedule_handles_cross_year_publication() -> None:
    """The first row of the 2026 schedule is the December 2025
    Economically Active Population Survey published Jan. 14 (Wed.).
    Reference year / month must be 2025 / 12; publication year (from
    the page heading) is 2026."""
    announcements = parse_release_schedule(_schedule_html())
    dec_eap = next(
        a for a in announcements
        if a.title == "The Economically Active Population Survey in December 2025"
    )
    assert dec_eap.schedule_year == 2026
    assert dec_eap.publication_month == 1
    assert dec_eap.publication_day == 14
    assert dec_eap.reference_year == 2025
    assert dec_eap.reference_month == 12


def test_parse_release_schedule_raises_on_missing_heading() -> None:
    with pytest.raises(KOSTATCalendarParseError, match="missing"):
        parse_release_schedule(
            "<html><body>nothing schedule-related here</body></html>",
        )


def test_parse_release_schedule_raises_on_zero_rows() -> None:
    """Heading present but no ``<td class="AGL">`` rows follow it —
    layout-drift signal."""
    html = (
        "<html><body>"
        "<h3>2030 Schedule </h3>"
        "<table><tbody><tr><td>placeholder</td></tr></tbody></table>"
        "</body></html>"
    )
    with pytest.raises(KOSTATCalendarParseError, match="zero"):
        parse_release_schedule(html)


# ── projection ───────────────────────────────────────────────────


def test_announcement_to_records_anchors_cpi_at_kst_window() -> None:
    """CPI default release time is 08:00 KST (= 23:00 UTC of the prior
    day; KST is UTC+09:00 year-round)."""
    announcements = parse_release_schedule(_schedule_html())
    march_cpi = next(
        a for a in announcements
        if a.title == "The Consumer Price Index in March 2026"
    )
    spec = INDICATOR_REGISTRY["CPI"]
    raw_rec, event_rec = announcement_to_records(
        march_cpi, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "KR"
    assert event_rec.title == "South Korea Consumer Price Index"
    assert event_rec.currency == "KRW"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "datetime"
    # Apr 02 08:00 KST = Apr 01 23:00 UTC.
    assert event_rec.event_time_utc.startswith("2026-04-01T23:00:00")
    assert event_rec.reference_date == "2026-03-01"
    assert event_rec.reference_label == "March 2026"
    # provider_event_id stable across re-projection.
    _, event_rec_again = announcement_to_records(
        march_cpi, spec=spec, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_announcement_to_records_employment_anchors_at_noon_kst() -> None:
    """Economically Active Population Survey publishes at 12:00 KST
    (= 03:00 UTC)."""
    announcements = parse_release_schedule(_schedule_html())
    feb_eap = next(
        a for a in announcements
        if a.title == "The Economically Active Population Survey in January 2026"
    )
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    _, event_rec = announcement_to_records(
        feb_eap, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    # Feb 11 12:00 KST = Feb 11 03:00 UTC.
    assert event_rec.event_time_utc.startswith("2026-02-11T03:00:00")
    assert event_rec.reference_date == "2026-01-01"
    assert event_rec.reference_label == "January 2026"
    assert event_rec.unit == "percent"


def test_announcement_to_records_distinct_provider_ids_per_reference() -> None:
    """Twelve CPI publications cover twelve distinct reference months
    so the synthesizer hashes to twelve unique provider_event_ids."""
    announcements = parse_release_schedule(_schedule_html())
    cpi_announcements = [
        a for a in announcements if "Consumer Price Index" in a.title
    ]
    spec = INDICATOR_REGISTRY["CPI"]
    ids = {
        announcement_to_records(a, spec=spec, snapshot_epoch_ms=1_800_000_000_000)[1]
        .provider_event_id
        for a in cpi_announcements
    }
    assert len(ids) == len(cpi_announcements)


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_kostat_calendar_writes_events_for_three_p1_indicators(
    store: SQLiteEngineStore,
) -> None:
    """The fixture's three families × ~12 publications/year should
    write ≥30 events combined."""
    def fetcher() -> str:
        return _schedule_html()

    with store._connection(commit=True) as conn:
        summary = fetch_kostat_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert set(summary.indicators_ok) == {
        "CPI", "INDUSTRIAL_PRODUCTION", "UNEMPLOYMENT_RATE",
    }
    assert summary.indicators_empty == []
    # ≥30 = 3 families × ≥10 monthly publications. Loose bound so a
    # mid-year fixture refresh doesn't churn the test on a row count.
    assert summary.events_upserted >= 30


def test_fetch_kostat_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RuntimeError("simulated 503 from KOSTAT")

    with store._connection(commit=True) as conn:
        summary = fetch_kostat_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0
    # Every planned indicator lands in series_failed so the operator
    # card surfaces a per-indicator failure rather than a single
    # connector-wide ``fetch_error`` only.
    assert {ind for ind, _ in summary.series_failed} == set(INDICATOR_REGISTRY)


def test_fetch_kostat_calendar_records_parse_error_on_layout_drift(
    store: SQLiteEngineStore,
) -> None:
    def empty() -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_kostat_calendar(
            conn, dry_run=False, html_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_kostat_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_kostat_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY)


def test_fetch_kostat_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """The provider_event_id is stable per (indicator, reference month)
    so a second sweep over the same schedule writes zero new rows."""
    def fetcher() -> str:
        return _schedule_html()
    with store._connection(commit=True) as conn:
        first = fetch_kostat_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        second = fetch_kostat_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    assert first.events_upserted >= 30
    assert first.events_upserted == second.events_upserted
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()
    assert rows[0] == first.events_upserted


# ── scheduler + agency wiring ───────────────────────────────────


def test_kostat_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "kostat" in ALL_CONNECTORS
    assert "kostat" in ALL_VALUE_SIDE_CONNECTORS


def test_kostat_agency_attribution_provider_only_in_p1() -> None:
    """KOSTAT owns provider attribution for KR macro indicators, but
    the P1 slice ships schedule-only events (``actual=NULL``); wiring
    ``(KR, …)`` into the parity whitelist would trip the
    parse_failed-on-missing-actual path on every release. P2 adds the
    per-release press-release scrape for the value side."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    kostat_agency = provider_to_agency("kostat")
    assert kostat_agency is not None and kostat_agency.agency_id == "KOSTAT"
    assert kostat_agency.indicators == frozenset()
    assert agency_for("KR", "CPI") is None
    assert agency_for("KR", "INDUSTRIAL_PRODUCTION") is None
    assert agency_for("KR", "UNEMPLOYMENT_RATE") is None


def test_kostat_canonicalize_aliases_resolve_release_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator(
        "South Korea Consumer Price Index",
    ) == "CPI"
    assert canonicalize_indicator(
        "South Korea Industrial Production",
    ) == "INDUSTRIAL_PRODUCTION"
    assert canonicalize_indicator(
        "South Korea Unemployment Rate",
    ) == "UNEMPLOYMENT_RATE"
