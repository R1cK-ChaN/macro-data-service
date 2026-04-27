"""Mocked tests for the BOK calendar connector (issue #55 P1).

The captured fixture
``tests/fixtures/bok_meeting_dates/meeting_dates.html`` was recorded
live on 2026-04-27 from
``bok.or.kr/eng/main/contents.do?menuNo=400020``. It carries the
inline ``Schedule of the MPB's policy-setting meetings`` block with
year-headed sub-tables for 2025, 2024, …, 2011. 2026 dates currently
live only as a ``.hwp``/``.pdf`` attachment on a separate news
article and are not on this page yet — the connector picks them up
when BOK adds the inline ``<h3>2026</h3>`` block.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.bok_api import (
    BOKMeetingScheduleParseError,
    fetch_bok_calendar,
    meeting_to_records,
    parse_meeting_schedule,
)
from ingestion.calendar.bok_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bok_meeting_dates"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _meeting_dates_html() -> str:
    return (FIXTURE_DIR / "meeting_dates.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_meeting_schedule_extracts_2025_full_year() -> None:
    """BOK's MPB met eight times in 2025 — Jan/Feb/Apr/May/Jul/Aug/Oct/Nov.
    The fixture's 2025 sub-table must yield exactly those eight cells."""
    meetings = parse_meeting_schedule(_meeting_dates_html())
    meetings_2025 = [m for m in meetings if m.year == 2025]
    closing_dates = [m.announcement_date.isoformat() for m in meetings_2025]
    assert closing_dates == [
        "2025-01-16",
        "2025-02-25",
        "2025-04-17",
        "2025-05-29",
        "2025-07-10",
        "2025-08-28",
        "2025-10-23",
        "2025-11-27",
    ]


def test_parse_meeting_schedule_extracts_2024_full_year() -> None:
    """Same eight-meeting cadence holds for 2024 and earlier."""
    meetings = parse_meeting_schedule(_meeting_dates_html())
    meetings_2024 = [m for m in meetings if m.year == 2024]
    closing_dates = [m.announcement_date.isoformat() for m in meetings_2024]
    assert closing_dates == [
        "2024-01-11",
        "2024-02-22",
        "2024-04-12",
        "2024-05-23",
        "2024-07-11",
        "2024-08-22",
        "2024-10-11",
        "2024-11-28",
    ]


def test_parse_meeting_schedule_orders_by_announcement_date() -> None:
    """Output must be sorted by announcement_date ascending so a
    downstream caller can paginate or eyeball the upcoming entries
    without re-sorting."""
    meetings = parse_meeting_schedule(_meeting_dates_html())
    iso_list = [m.announcement_date.isoformat() for m in meetings]
    assert iso_list == sorted(iso_list)


def test_parse_meeting_schedule_handles_nbsp_between_day_and_paren() -> None:
    """Some 2025 cells use ``Jan.16&nbsp;(Thu)`` — the parser must
    tolerate the non-breaking space after HTML unescape."""
    html = (
        "<html><body>"
        "<h2>Schedule of the MPB's policy-setting meetings</h2>"
        "<h3>2030</h3>"
        "<table><tbody><tr><td>"
        "<p>Jan.16&nbsp;(Thu)</p></td><td><p>Feb.25 (Tue)</p></td>"
        "</tr></tbody></table>"
        "</body></html>"
    )
    meetings = parse_meeting_schedule(html)
    assert [m.announcement_date.isoformat() for m in meetings] == [
        "2030-01-16",
        "2030-02-25",
    ]


def test_parse_meeting_schedule_anchors_on_h2_heading_to_skip_nav() -> None:
    """The full BOK page carries unrelated ``<h3>YYYY</h3>``-shaped
    markers in the navigation footer (search-results blocks, archive
    widgets). The parser anchors on the ``Schedule of the MPB's
    policy-setting meetings`` heading so those markers don't become
    phantom meetings."""
    html = (
        "<html><body>"
        "<h3>1999</h3>"  # decoy outside the schedule block
        "<table><tbody><tr><td><p>Jan.01 (Fri)</p></td></tr></tbody></table>"
        "<h2>Schedule of the MPB's policy-setting meetings</h2>"
        "<h3>2030</h3>"
        "<table><tbody><tr><td><p>Jan.16 (Thu)</p></td></tr></tbody></table>"
        "</body></html>"
    )
    meetings = parse_meeting_schedule(html)
    assert [m.announcement_date.isoformat() for m in meetings] == [
        "2030-01-16",
    ]


def test_parse_meeting_schedule_raises_on_missing_year_headings() -> None:
    """Schedule heading present but no inline year sub-tables — outage
    or layout-drift signal."""
    with pytest.raises(BOKMeetingScheduleParseError, match="year headings"):
        parse_meeting_schedule(
            "<html><body>"
            "<h2>Schedule of the MPB's policy-setting meetings</h2>"
            "no inline schedule yet"
            "</body></html>",
        )


def test_parse_meeting_schedule_raises_on_missing_schedule_heading() -> None:
    """The schedule ``<h2>`` heading is mandatory. Without it the
    parser would otherwise sweep the entire document and a year-
    shaped ``<h3>YYYY</h3>`` widget anywhere on the page would
    produce phantom meetings."""
    html = (
        "<html><body>"
        "<h3>2030</h3>"  # year-shaped block outside any schedule
        "<table><tbody><tr><td><p>Jan.16 (Thu)</p></td></tr></tbody></table>"
        "</body></html>"
    )
    with pytest.raises(
        BOKMeetingScheduleParseError, match="missing the ``Schedule",
    ):
        parse_meeting_schedule(html)


def test_parse_meeting_schedule_stops_at_next_section_h2() -> None:
    """A future page revision that adds a year-shaped widget *after*
    the schedule (in a sibling section opened by ``<h2>...</h2>``)
    must not bleed into the schedule walk. The parser slices the body
    at the next ``<h1>``/``<h2>`` boundary so cells in the sibling
    section never get matched."""
    html = (
        "<html><body>"
        "<h2>Schedule of the MPB's policy-setting meetings</h2>"
        "<h3>2030</h3>"
        "<table><tbody><tr><td><p>Jan.16 (Thu)</p></td></tr></tbody></table>"
        "<h2>Recently viewed content</h2>"
        "<h3>2099</h3>"  # year-shaped widget in the next section
        "<table><tbody><tr><td><p>Jul.04 (Wed)</p></td></tr></tbody></table>"
        "</body></html>"
    )
    meetings = parse_meeting_schedule(html)
    assert [m.announcement_date.isoformat() for m in meetings] == [
        "2030-01-16",
    ]


def test_parse_meeting_schedule_raises_on_zero_cells() -> None:
    """Year heading present but no parseable date cells follow."""
    html = (
        "<html><body>"
        "<h2>Schedule of the MPB's policy-setting meetings</h2>"
        "<h3>2030</h3>"
        "<table><tbody><tr><td>placeholder</td></tr></tbody></table>"
        "</body></html>"
    )
    with pytest.raises(BOKMeetingScheduleParseError, match="zero meeting cells"):
        parse_meeting_schedule(html)


# ── projection ───────────────────────────────────────────────────


def test_meeting_to_records_anchors_at_kst_window() -> None:
    """The Jan 16 2025 meeting must project a calendar event anchored
    on Jan 16 at 09:50 KST (= 00:50 UTC; KST is UTC+09:00 year-round)."""
    meetings = parse_meeting_schedule(_meeting_dates_html())
    jan_2025 = next(
        m for m in meetings if m.announcement_date.isoformat() == "2025-01-16"
    )
    raw_rec, event_rec = meeting_to_records(
        jan_2025, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "KR"
    assert event_rec.title == "BOK Interest Rate Decision"
    assert event_rec.currency == "KRW"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "datetime"
    # Jan 16 09:50 KST = Jan 16 00:50 UTC.
    assert event_rec.event_time_utc.startswith("2025-01-16T00:50:00")
    assert event_rec.reference_date == "2025-01-16"
    assert event_rec.reference_label == "January 2025"
    # provider_event_id stable across re-projection.
    _, event_rec_again = meeting_to_records(
        jan_2025, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_meeting_to_records_distinct_provider_ids_per_meeting() -> None:
    """Each parsed meeting anchors on a unique closing date → unique
    provider_event_id."""
    meetings = parse_meeting_schedule(_meeting_dates_html())
    ids = {
        meeting_to_records(m, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for m in meetings
    }
    assert len(ids) == len(meetings)


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_bok_calendar_writes_one_event_per_meeting(
    store: SQLiteEngineStore,
) -> None:
    """The fixture lists 15 inline years × 8 meetings = 120 meetings
    (modulo any year that BOK has historically held an extra
    intra-year meeting — keep the assertion as ≥ to absorb that)."""
    def fetcher() -> str:
        return _meeting_dates_html()

    with store._connection(commit=True) as conn:
        summary = fetch_bok_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.meetings_parsed >= 120
    assert summary.events_upserted == summary.meetings_parsed


def test_fetch_bok_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RuntimeError("simulated 503 from BOK")

    with store._connection(commit=True) as conn:
        summary = fetch_bok_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_bok_calendar_records_parse_error_on_layout_drift(
    store: SQLiteEngineStore,
) -> None:
    def empty() -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_bok_calendar(
            conn, dry_run=False, html_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_bok_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_bok_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOK_RATE"]


def test_fetch_bok_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """The provider_event_id is stable per meeting closing day, so a
    second sweep over the same schedule writes zero new rows."""
    def fetcher() -> str:
        return _meeting_dates_html()
    with store._connection(commit=True) as conn:
        first = fetch_bok_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_bok_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()
    assert rows[0] == first.events_upserted


# ── scheduler + agency wiring ───────────────────────────────────


def test_bok_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "bok" in ALL_CONNECTORS
    assert "bok" in ALL_VALUE_SIDE_CONNECTORS


def test_bok_agency_attribution_provider_only_in_p1() -> None:
    """BOK owns provider attribution for KR rate decisions, but the
    P1 slice ships schedule-only events (``actual=NULL``); wiring
    ``("KR", "BOK_RATE")`` into the parity whitelist would trip the
    parse_failed-on-missing-actual path on every meeting."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    bok_agency = provider_to_agency("bok")
    assert bok_agency is not None and bok_agency.agency_id == "BOK"
    assert bok_agency.indicators == frozenset()
    assert agency_for("KR", "BOK_RATE") is None


def test_bok_canonicalize_aliases_resolve_central_bank_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator(
        "BOK Interest Rate Decision",
    ) == "BOK_RATE"
    assert canonicalize_indicator(
        "Bank of Korea Interest Rate Decision",
    ) == "BOK_RATE"
    assert canonicalize_indicator("Korea Base Rate") == "BOK_RATE"
