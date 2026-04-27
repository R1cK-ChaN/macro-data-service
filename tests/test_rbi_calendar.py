"""Mocked tests for the RBI calendar connector (issue #54 P1).

The captured fixture
``tests/fixtures/rbi_annualpolicy/annualpolicy.html`` was recorded
live on 2026-04-27 from
``rbi.org.in/scripts/annualpolicy.aspx``. It includes the inline
"Meeting Schedule of the Monetary Policy Committee for 2026-2027"
press release with six bi-monthly meeting date triples.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.rbi_api import (
    INDICATOR_REGISTRY,
    RBIMeetingScheduleParseError,
    fetch_rbi_calendar,
    meeting_to_records,
    parse_meeting_schedule,
)
from ingestion.calendar.rbi_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "rbi_annualpolicy"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _annualpolicy_html() -> str:
    return (FIXTURE_DIR / "annualpolicy.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_meeting_schedule_extracts_six_fy_2026_meetings() -> None:
    """The 2026-04-27 capture lists the full FY 2026-2027 schedule:
    April 6-8, June 3-5, August 3-5, October 5-7, December 2-4 (all
    2026), plus February 3-5, 2027."""
    meetings = parse_meeting_schedule(_annualpolicy_html())
    assert len(meetings) == 6
    closing_dates = [m.announcement_date.isoformat() for m in meetings]
    assert closing_dates == [
        "2026-04-08",
        "2026-06-05",
        "2026-08-05",
        "2026-10-07",
        "2026-12-04",
        "2027-02-05",
    ]
    # Three-day meetings — all six triples carry the full (d1, d2, d3) shape.
    assert all(len(m.days) == 3 for m in meetings)
    assert all(m.fiscal_year == "2026-2027" for m in meetings)


def test_parse_meeting_schedule_anchors_on_closing_day() -> None:
    """RBI announces the policy repo rate at 10:00 IST on the third
    (closing) day of each three-day meeting. The parsed
    announcement_date must match the meeting's last day."""
    meetings = parse_meeting_schedule(_annualpolicy_html())
    apr_meeting = meetings[0]
    assert apr_meeting.month_token == "April"
    assert apr_meeting.days == (6, 7, 8)
    assert apr_meeting.announcement_date.day == 8


def test_parse_meeting_schedule_drops_archive_widget_dates() -> None:
    """The page footer carries a fiscal-year archive widget repeating
    the FY label and earlier years. The parser must bound its triple
    search to the schedule body so dates inside the archive widget
    don't get treated as meetings."""
    meetings = parse_meeting_schedule(_annualpolicy_html())
    # Six and only six — if the archive widget bled into the parse,
    # the extra "January 1, 2 and 3, 2025" / similar fragments would
    # push the count higher.
    assert len(meetings) == 6


def test_parse_meeting_schedule_raises_on_missing_heading() -> None:
    with pytest.raises(RBIMeetingScheduleParseError, match="schedule heading"):
        parse_meeting_schedule(
            "<html><body>nothing meeting-related here</body></html>",
        )


def test_parse_meeting_schedule_raises_on_zero_triples() -> None:
    """Heading present but no triples follow it — layout drift signal."""
    html = (
        "<html><body>"
        "Meeting Schedule of the Monetary Policy Committee for 2030-2031 "
        "no actual dates yet "
        "2030-2031 2029-2030"
        "</body></html>"
    )
    with pytest.raises(RBIMeetingScheduleParseError, match="zero meeting triples"):
        parse_meeting_schedule(html)


def test_parse_meeting_schedule_tolerates_two_day_meetings() -> None:
    """RBI has historically held three-day MPC meetings, but the
    parser accepts the shorter ``Month D1 and D3, YYYY`` shape
    defensively in case the format ever shortens."""
    html = (
        "<html><body>"
        "Meeting Schedule of the Monetary Policy Committee for 2030-2031 "
        "April 6 and 8, 2030 "
        "2030-2031 archive"
        "</body></html>"
    )
    meetings = parse_meeting_schedule(html)
    assert len(meetings) == 1
    assert meetings[0].days == (6, 8)
    assert meetings[0].announcement_date.isoformat() == "2030-04-08"


def test_parse_meeting_schedule_handles_cross_month_meeting() -> None:
    """RBI's FY 2025-2026 schedule had ``September 29, 30 and October
    1, 2025`` — the closing day belongs to October even though the
    opening days are September. The parser must anchor the
    announcement on the October 1 closing day, not September 1."""
    html = (
        "<html><body>"
        "Meeting Schedule of the Monetary Policy Committee for 2025-2026 "
        "September 29, 30 and October 1, 2025 "
        "December 3, 4 and 5, 2025 "
        "2025-2026 archive"
        "</body></html>"
    )
    meetings = parse_meeting_schedule(html)
    assert len(meetings) == 2
    assert meetings[0].announcement_date.isoformat() == "2025-10-01"
    assert meetings[0].month_token == "September"
    assert meetings[0].days == (29, 30, 1)
    assert meetings[1].announcement_date.isoformat() == "2025-12-05"


# ── projection ───────────────────────────────────────────────────


def test_meeting_to_records_synthesizes_event_at_ist_window() -> None:
    """The April 6-8 2026 meeting must project a calendar event
    anchored on April 8 at 10:00 IST (= 04:30 UTC)."""
    meetings = parse_meeting_schedule(_annualpolicy_html())
    apr_meeting = meetings[0]
    raw_rec, event_rec = meeting_to_records(
        apr_meeting, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "IN"
    assert event_rec.title == "RBI Interest Rate Decision"
    assert event_rec.currency == "INR"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "datetime"
    # 10:00 IST = 04:30 UTC (IST is UTC+05:30 year-round).
    assert event_rec.event_time_utc.startswith("2026-04-08T04:30:00")
    assert event_rec.reference_date == "2026-04-08"
    assert event_rec.reference_label == "April 2026"
    # provider_event_id stable across re-projection.
    _, event_rec_again = meeting_to_records(
        apr_meeting, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_meeting_to_records_distinct_provider_ids_per_meeting() -> None:
    """Six meetings → six distinct provider_event_ids — each anchors
    on a unique closing date so the synthesizer hashes to a unique
    value."""
    meetings = parse_meeting_schedule(_annualpolicy_html())
    ids = {
        meeting_to_records(m, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for m in meetings
    }
    assert len(ids) == 6


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_rbi_calendar_writes_one_event_per_meeting(
    store: SQLiteEngineStore,
) -> None:
    """The fixture's six-meeting schedule should write six events."""
    def fetcher() -> str:
        return _annualpolicy_html()

    with store._connection(commit=True) as conn:
        summary = fetch_rbi_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.meetings_parsed == 6
    assert summary.events_upserted == 6


def test_fetch_rbi_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RuntimeError("simulated 503 from RBI")

    with store._connection(commit=True) as conn:
        summary = fetch_rbi_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_rbi_calendar_records_parse_error_on_layout_drift(
    store: SQLiteEngineStore,
) -> None:
    def empty() -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_rbi_calendar(
            conn, dry_run=False, html_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_rbi_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_rbi_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["RBI_RATE"]


def test_fetch_rbi_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """The provider_event_id is stable per meeting closing day, so
    a second sweep over the same schedule writes zero new rows."""
    def fetcher() -> str:
        return _annualpolicy_html()
    with store._connection(commit=True) as conn:
        first = fetch_rbi_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        second = fetch_rbi_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    assert first.events_upserted == 6
    assert second.meetings_parsed == 6
    # Re-projection touches the same six rows; the upsert returns the
    # rowcount of the operation, not "new rows" only — so events_upserted
    # is allowed to be non-zero on the second pass. The invariant we
    # care about is that no *additional* cal_econ_event rows appear.
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider='rbi'",
        ).fetchone()
    assert rows[0] == 6


# ── scheduler + agency wiring ───────────────────────────────────


def test_rbi_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "rbi" in ALL_CONNECTORS
    assert "rbi" in ALL_VALUE_SIDE_CONNECTORS


def test_rbi_agency_attribution_provider_only_in_p1() -> None:
    """RBI owns provider attribution for IN rate decisions, but the
    P1 slice ships schedule-only events (``actual=NULL``); wiring
    ``("IN", "RBI_RATE")`` into the parity whitelist would trip the
    parse_failed-on-missing-actual path on every meeting. P2 adds
    the per-meeting Resolution scrape for the value side."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    rbi_agency = provider_to_agency("rbi")
    assert rbi_agency is not None and rbi_agency.agency_id == "RBI"
    assert rbi_agency.indicators == frozenset()
    assert agency_for("IN", "RBI_RATE") is None


def test_rbi_canonicalize_aliases_resolve_central_bank_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator(
        "RBI Interest Rate Decision",
    ) == "RBI_RATE"
    assert canonicalize_indicator(
        "Reserve Bank of India Interest Rate Decision",
    ) == "RBI_RATE"
    assert canonicalize_indicator("India Repo Rate") == "RBI_RATE"
    assert canonicalize_indicator("Policy Repo Rate") == "RBI_RATE"
