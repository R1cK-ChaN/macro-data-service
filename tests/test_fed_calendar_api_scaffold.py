"""Mocked tests for the Fed calendar connector (issue #9 P4).

Fixture HTML lives in ``tests/fixtures/fed_fomc_calendar/`` — slices
of the real ``federalreserve.gov/monetarypolicy/fomccalendars.htm``
page captured 2026-04-21. No real HTTP in CI.

Covers:

- Parser: meeting rows extracted correctly (single-day, two-day,
  SEP asterisk); closing day resolves to the second number in the
  date range and DST is applied against that date via 14:00 ET.
- ``meeting_entry_to_records``: ``provider_event_id`` anchors on
  the closing-date ISO string so the id survives a value-side
  upgrade; ``has_sep=True`` surfaces in the title.
- Projector: schedule rows land with ``precision='datetime'`` and
  ``actual=NULL``; upsert is idempotent.
- Service op ``calendar_econ_fetch_fed`` — dry_run plan, fixture
  injection via the ``html_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.fed_api import (
    FOMC_CALENDAR_URL,
    INDICATOR_REGISTRY,
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    FomcCalendarParseError,
    FomcMeetingEntry,
    fetch_fed_calendar,
    meeting_entry_to_records,
    parse_fomc_calendar_html,
    project_events,
    store_raw,
)
from ingestion.calendar.fed_api.parser import PROVIDER, _content_hash
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_fomc_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_keeps_fomc_rate_anchor() -> None:
    """FOMC_RATE is still in the registry after P4a — we only added
    new indicators for the releasedates.htm surface, we didn't
    remove or rename the original anchor."""
    assert "FOMC_RATE" in INDICATOR_REGISTRY
    spec = INDICATOR_REGISTRY["FOMC_RATE"]
    assert spec.country_code == "US"
    assert spec.importance == "high"
    assert spec.unit == "percent"
    assert "FOMC" in spec.title


def test_registry_includes_p4a_additions() -> None:
    """P4a adds three releasedates-surface indicators."""
    assert {"BEIGE_BOOK", "FED_H41", "FED_H8"} <= set(
        INDICATOR_REGISTRY.keys()
    )


# ──────────────────────────────────────────────────────────────────────────
# parse_fomc_calendar_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_2026_panel_extracts_meetings() -> None:
    entries = parse_fomc_calendar_html(_fixture_html("fomc_2026.html"))
    # The 2026 fixture carries eight scheduled meetings — the Fed's
    # standard year has eight, and 2026 is no exception.
    assert len(entries) == 8
    for entry in entries:
        assert entry.year == 2026


def test_parse_resolves_closing_day_for_two_day_meeting() -> None:
    entries = parse_fomc_calendar_html(_fixture_html("fomc_2026.html"))
    jan = [e for e in entries if e.month_name == "January"][0]
    # January 2026 meeting: "27-28" → closing day is 28.
    assert jan.date_cell == "27-28"
    assert jan.closing_date == date(2026, 1, 28)
    assert jan.has_sep is False


def test_parse_flags_sep_meetings() -> None:
    entries = parse_fomc_calendar_html(_fixture_html("fomc_2026.html"))
    # March and June / September / December are the standard SEP meetings.
    mar = [e for e in entries if e.month_name == "March"][0]
    assert mar.date_cell.endswith("*")
    assert mar.has_sep is True
    # Non-SEP meetings must not be flagged.
    jan = [e for e in entries if e.month_name == "January"][0]
    assert jan.has_sep is False


def test_parse_future_year_still_extracts_meetings() -> None:
    # 2027 panel: future meetings with empty statement/minutes blocks.
    # The parser must still recover month + date, independent of the
    # filled-in linkage state.
    entries = parse_fomc_calendar_html(_fixture_html("fomc_2027.html"))
    assert len(entries) >= 4
    jan_2027 = [
        e for e in entries if e.year == 2027 and e.month_name == "January"
    ]
    assert jan_2027 and jan_2027[0].closing_date == date(2027, 1, 27)


def test_parse_ignores_non_meeting_panels() -> None:
    # A panel that isn't a "YYYY FOMC Meetings" panel must be skipped
    # — otherwise the FOMC Search panel on the live page would be
    # mistaken for a year block.
    html = """
    <div class="panel panel-default">
      <div class="panel-heading"><h5 class="panel-title">FOMC Search</h5></div>
    </div>
    <div class="panel panel-default">
      <div class="panel-heading"><h4><a id="x">2025 FOMC Meetings</a></h4></div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>July</strong></div>
        <div class="fomc-meeting__date">29-30</div>
      </div>
    </div>
    """
    entries = parse_fomc_calendar_html(html)
    assert len(entries) == 1
    assert entries[0].closing_date == date(2025, 7, 30)


def test_parse_raises_on_malformed_date() -> None:
    html = """
    <div class="panel panel-default">
      <div class="panel-heading"><h4>2025 FOMC Meetings</h4></div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>July</strong></div>
        <div class="fomc-meeting__date">garbage</div>
      </div>
    </div>
    """
    with pytest.raises(FomcCalendarParseError):
        parse_fomc_calendar_html(html)


def test_parse_cross_month_meeting_resolves_to_second_month() -> None:
    # Real Fed history carries Jan/Feb and Oct/Nov pairs (e.g. the
    # Jan 31 - Feb 1 2023 meeting). The closing day belongs to the
    # *second* month — the rate decision is announced on day 1 of
    # the following month.
    html = """
    <div class="panel panel-default">
      <div class="panel-heading"><h4>2023 FOMC Meetings</h4></div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>Jan/Feb</strong></div>
        <div class="fomc-meeting__date">31-1</div>
      </div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>Oct/Nov</strong></div>
        <div class="fomc-meeting__date">31-1</div>
      </div>
    </div>
    """
    entries = parse_fomc_calendar_html(html)
    assert len(entries) == 2
    jan_feb = [e for e in entries if e.month_name == "Jan/Feb"][0]
    assert jan_feb.closing_date == date(2023, 2, 1)
    oct_nov = [e for e in entries if e.month_name == "Oct/Nov"][0]
    assert oct_nov.closing_date == date(2023, 11, 1)


def test_parse_cross_month_dec_jan_bumps_year() -> None:
    # Not a shape the Fed has used recently, but the scaffold must
    # handle the year boundary correctly if a future calendar does
    # (Dec/Jan 31-1 means the decision announces on Jan 1 of year+1).
    html = """
    <div class="panel panel-default">
      <div class="panel-heading"><h4>2024 FOMC Meetings</h4></div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>Dec/Jan</strong></div>
        <div class="fomc-meeting__date">31-1</div>
      </div>
    </div>
    """
    entries = parse_fomc_calendar_html(html)
    assert len(entries) == 1
    assert entries[0].closing_date == date(2025, 1, 1)


def test_parse_raises_on_unknown_month() -> None:
    html = """
    <div class="panel panel-default">
      <div class="panel-heading"><h4>2025 FOMC Meetings</h4></div>
      <div class="row fomc-meeting">
        <div class="fomc-meeting__month"><strong>Sol</strong></div>
        <div class="fomc-meeting__date">1-2</div>
      </div>
    </div>
    """
    with pytest.raises(FomcCalendarParseError):
        parse_fomc_calendar_html(html)


# ──────────────────────────────────────────────────────────────────────────
# meeting_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def _entry(
    *,
    year: int = 2026,
    month: str = "January",
    date_cell: str = "27-28",
    closing: date = date(2026, 1, 28),
    sep: bool = False,
) -> FomcMeetingEntry:
    return FomcMeetingEntry(
        year=year,
        month_name=month,
        date_cell=date_cell,
        closing_date=closing,
        has_sep=sep,
    )


def test_meeting_record_applies_2pm_eastern_time() -> None:
    # January 28 is EST (UTC-5) → 14:00 ET = 19:00 UTC.
    raw, event = meeting_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    assert event.event_time_precision == "datetime"
    assert event.event_time_utc.endswith("+00:00")
    assert event.event_time_utc.startswith("2026-01-28T19:00")


def test_meeting_record_dst_flip_summer() -> None:
    # June meeting sits in EDT (UTC-4) → 14:00 ET = 18:00 UTC.
    raw, event = meeting_entry_to_records(
        _entry(
            month="June",
            date_cell="16-17",
            closing=date(2026, 6, 17),
        ),
        snapshot_epoch_ms=1_700_000_000,
    )
    assert event.event_time_utc.startswith("2026-06-17T18:00")


def test_sep_meeting_title_flags_projections() -> None:
    raw, event = meeting_entry_to_records(
        _entry(date_cell="17-18*", closing=date(2026, 3, 18), sep=True),
        snapshot_epoch_ms=1_700_000_000,
    )
    assert "SEP" in event.title


def test_provider_event_id_anchors_on_closing_date() -> None:
    """Same meeting must hash to the same id whether the scrape came
    before or after SEP metadata was attached — otherwise an asterisk
    toggle would duplicate the row rather than revise it."""
    raw_a, event_a = meeting_entry_to_records(
        _entry(closing=date(2026, 3, 18), sep=False),
        snapshot_epoch_ms=1_700_000_000,
    )
    raw_b, event_b = meeting_entry_to_records(
        _entry(closing=date(2026, 3, 18), sep=True, date_cell="17-18*"),
        snapshot_epoch_ms=1_700_000_000,
    )
    assert raw_a.provider_event_id == raw_b.provider_event_id
    assert event_a.provider_event_id == event_b.provider_event_id


def test_record_shape_is_schedule_only() -> None:
    raw, event = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    assert raw.provider == PROVIDER
    assert event.provider == PROVIDER
    assert event.country_code == "US"
    assert event.source == "Federal Reserve"
    assert event.source_url == FOMC_CALENDAR_URL
    assert event.currency == "USD"
    # Schedule-only: no value-side fields populated yet.
    assert event.actual is None
    assert event.previous is None
    assert event.forecast is None
    assert event.consensus_forecast is None


def test_content_hash_changes_when_sep_flag_flips() -> None:
    payload_a = {
        "closing_date": "2026-03-18",
        "date_cell":    "17-18",
        "has_sep":      False,
        "event_time_utc": "2026-03-18T18:00:00+00:00",
    }
    payload_b = {**payload_a, "has_sep": True, "date_cell": "17-18*"}
    assert _content_hash(payload_a) != _content_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(store: SQLiteEngineStore) -> None:
    raw, _ = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])
    assert first == 1
    assert second == 0


def test_project_events_inserts_single_row(store: SQLiteEngineStore) -> None:
    _, event = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    with store._connection(commit=True) as conn:
        changed = project_events(conn, [event])
    assert changed == 1
    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()[0]
    assert total == 1


def test_project_events_respects_observed_at_monotonicity(
    store: SQLiteEngineStore,
) -> None:
    _, newer = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=2_000_000_000, observed_at_epoch_ms=2_000_000_000,
    )
    _, older = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=1_000_000_000, observed_at_epoch_ms=1_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [newer])
        project_events(conn, [older])
        observed = conn.execute(
            "SELECT observed_at_epoch_ms FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert observed == 2_000_000_000


def test_project_events_preserves_datetime_on_merge(
    store: SQLiteEngineStore,
) -> None:
    """Safety rail — if a future pass writes an ``approximate`` row for
    the same meeting, the datetime-precision row already in the DB
    must survive."""
    _, scheduled = meeting_entry_to_records(
        _entry(), snapshot_epoch_ms=1_500_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [scheduled])

    # Construct an ``approximate``-precision variant with the same id.
    approx = FedCalendarEventRecord(
        provider=scheduled.provider,
        provider_event_id=scheduled.provider_event_id,
        event_time_utc="2026-01-28T00:00:00+00:00",
        event_time_precision="approximate",
        reference_date=scheduled.reference_date,
        reference_label=scheduled.reference_label,
        country_code=scheduled.country_code,
        indicator_id=None,
        category=scheduled.category,
        title=scheduled.title,
        importance=scheduled.importance,
        currency=scheduled.currency,
        unit=scheduled.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source=scheduled.source,
        source_url=scheduled.source_url,
        content_hash=scheduled.content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=2_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [approx])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert row[1] == "datetime"
    assert row[0] == scheduled.event_time_utc


# ──────────────────────────────────────────────────────────────────────────
# Fetcher
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_indicator_plan(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_fed_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    # FOMC scraper owns FOMC_RATE only; the P4a releasedates-surface
    # indicators (BEIGE_BOOK / FED_H41 / FED_H8) ship via
    # ``fetch_fed_releasedates``, not here.
    assert summary.indicators_planned == ["FOMC_RATE"]
    assert summary.meetings_parsed == 0


def test_fetch_projects_fixture_into_events(store: SQLiteEngineStore) -> None:
    def _fake_fetcher() -> str:
        return _fixture_html("fomc_2026.html")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_calendar(
            conn, dry_run=False, html_fetcher=_fake_fetcher,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert summary.dry_run is False
    assert summary.meetings_parsed == 8
    assert summary.rows_raw_inserted == 8
    assert summary.events_upserted == 8
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert rows == 8


def test_fetch_twice_is_idempotent(store: SQLiteEngineStore) -> None:
    def _fake_fetcher() -> str:
        return _fixture_html("fomc_2026.html")

    with store._connection(commit=True) as conn:
        fetch_fed_calendar(
            conn, dry_run=False, html_fetcher=_fake_fetcher,
            snapshot_epoch_ms=1_700_000_000,
        )
        # Second run with the same snapshot must not grow the raw lane.
        second = fetch_fed_calendar(
            conn, dry_run=False, html_fetcher=_fake_fetcher,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert second.rows_raw_inserted == 0


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_raises_when_parse_yields_zero_meetings(
    store: SQLiteEngineStore,
) -> None:
    """Safety rail — an empty parse in execute mode must fail loudly
    rather than commit a no-op. Cloudflare-style 200 interstitials and
    upstream DOM drift both produce an empty selector match; the
    calendar page itself never legitimately carries zero meetings."""
    def _empty_page() -> str:
        return "<html><body>Access Denied</body></html>"

    with store._connection(commit=True) as conn:
        with pytest.raises(FomcCalendarParseError):
            fetch_fed_calendar(
                conn, dry_run=False, html_fetcher=_empty_page,
                snapshot_epoch_ms=1_700_000_000,
            )


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_fed", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == ["FOMC_RATE"]
