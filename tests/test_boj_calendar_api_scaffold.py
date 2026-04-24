"""Mocked tests for the BoJ calendar connector (issue #14 P1).

Fixture HTML lives in ``tests/fixtures/boj_mpm_calendar/`` and
``tests/fixtures/boj_statements/`` — slices of the real
``boj.or.jp`` pages captured 2026-04-24. No real HTTP in CI.

Covers:

- Schedule parser: MPM rows extracted correctly (closing date is the
  second day; cross-month pairs like "Apr. 30, May 1" resolve to
  May 1; Dec/Jan wrap bumps the year).
- ``mpm_entry_to_records``: ``provider_event_id`` anchors on the
  closing-date ISO string so the id survives the value-side upgrade;
  12:00 JST → 03:00 UTC.
- Statement parser: target policy-rate number extracted from the
  "encourage the uncollateralized overnight call rate ... at around
  X.X percent" sentence.
- Projector: schedule rows land with ``precision='datetime'`` and
  ``actual=NULL``; the value-side write fills ``actual``.
- Service ops ``calendar_econ_fetch_boj`` and
  ``calendar_econ_fetch_boj_values`` — dry-run plans.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.boj_api import (
    BOJ_MPM_CALENDAR_URL,
    INDICATOR_REGISTRY,
    BojMpmCalendarParseError,
    BojMpmEntry,
    BojStatementParseError,
    fetch_boj_calendar,
    fetch_boj_statement_values,
    mpm_entry_to_records,
    parse_boj_mpm_calendar_html,
    parse_statement_html,
    project_events,
    statement_value_to_records,
    store_raw,
)
from ingestion.calendar.boj_api.parser import PROVIDER, _content_hash
from ingestion.calendar.boj_api.statements import build_statement_url
from storage.sqlite import SQLiteEngineStore


CAL_FIXTURES = Path(__file__).parent / "fixtures" / "boj_mpm_calendar"
STMT_FIXTURES = Path(__file__).parent / "fixtures" / "boj_statements"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _cal_fixture() -> str:
    return (CAL_FIXTURES / "mpm_schedule.html").read_text(encoding="utf-8")


def _stmt_fixture(name: str) -> str:
    return (STMT_FIXTURES / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_includes_boj_rate_anchor() -> None:
    assert "BOJ_RATE" in INDICATOR_REGISTRY
    spec = INDICATOR_REGISTRY["BOJ_RATE"]
    assert spec.country_code == "JP"
    assert spec.importance == "high"
    assert spec.unit == "percent"
    assert "BoJ" in spec.title


# ──────────────────────────────────────────────────────────────────────────
# parse_boj_mpm_calendar_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_extracts_all_meetings_from_fixture() -> None:
    entries = parse_boj_mpm_calendar_html(_cal_fixture())
    # 8 meetings per year × 2 years (2026 + 2025) = 16.
    assert len(entries) == 16
    years = {e.year for e in entries}
    assert years == {2025, 2026}


def test_parse_closing_day_is_second_date() -> None:
    entries = parse_boj_mpm_calendar_html(_cal_fixture())
    jan_2026 = [e for e in entries if e.year == 2026 and "Jan." in e.date_cell][0]
    # "Jan. 22 (Thurs.), 23 (Fri.)" — closing day 23.
    assert jan_2026.closing_date == date(2026, 1, 23)


def test_parse_cross_month_resolves_to_second_month() -> None:
    entries = parse_boj_mpm_calendar_html(_cal_fixture())
    may_2025 = [
        e for e in entries
        if e.year == 2025 and "May" in e.date_cell
    ][0]
    # "Apr. 30 (Wed.), May 1 (Thurs.)" — closing day in May.
    assert may_2025.closing_date == date(2025, 5, 1)


def test_parse_bare_second_day_inherits_first_month() -> None:
    html = """
    <h2 id="p2024">2024</h2>
    <div class="tbl-box"><table><tbody>
    <tr><td>July 30 (Tues.), 31 (Wed.)</td><td>-</td><td>-</td><td>-</td></tr>
    </tbody></table></div>
    """
    entries = parse_boj_mpm_calendar_html(html)
    assert len(entries) == 1
    assert entries[0].closing_date == date(2024, 7, 31)


def test_parse_dec_jan_wrap_bumps_year() -> None:
    # BoJ has not scheduled a Dec→Jan wrap recently but the parser
    # must handle it if one appears (e.g. a Dec. 30, Jan. 1 pair).
    html = """
    <h2 id="p2024">2024</h2>
    <div class="tbl-box"><table><tbody>
    <tr><td>Dec. 30 (Mon.), Jan. 1 (Wed.)</td><td>-</td><td>-</td><td>-</td></tr>
    </tbody></table></div>
    """
    entries = parse_boj_mpm_calendar_html(html)
    assert len(entries) == 1
    assert entries[0].closing_date == date(2025, 1, 1)


def test_parse_ignores_non_year_heading() -> None:
    # The live page carries a "Past Monetary Policy Meetings" heading
    # at ``id="p01"`` that doesn't precede a table we want to parse.
    # The parser must skip it silently.
    html = """
    <h2 id="p01">Past Monetary Policy Meetings</h2>
    <div class="tbl-box"><table><tbody>
    <tr><td>Archive link placeholder</td></tr>
    </tbody></table></div>
    <h2 id="p2025">2025</h2>
    <div class="tbl-box"><table><tbody>
    <tr><td>Jan. 23 (Thurs.), 24 (Fri.)</td><td>-</td><td>-</td><td>-</td></tr>
    </tbody></table></div>
    """
    entries = parse_boj_mpm_calendar_html(html)
    assert len(entries) == 1
    assert entries[0].closing_date == date(2025, 1, 24)


def test_parse_raises_on_malformed_cell() -> None:
    html = """
    <h2 id="p2025">2025</h2>
    <div class="tbl-box"><table><tbody>
    <tr><td>garbage</td><td>-</td><td>-</td><td>-</td></tr>
    </tbody></table></div>
    """
    with pytest.raises(BojMpmCalendarParseError):
        parse_boj_mpm_calendar_html(html)


# ──────────────────────────────────────────────────────────────────────────
# mpm_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def _entry(
    *,
    year: int = 2026,
    date_cell: str = "Jan. 22 (Thurs.), 23 (Fri.)",
    closing: date = date(2026, 1, 23),
) -> BojMpmEntry:
    return BojMpmEntry(year=year, date_cell=date_cell, closing_date=closing)


def test_record_uses_noon_jst_release_convention() -> None:
    # 12:00 JST = 03:00 UTC (JST has no DST).
    raw, event = mpm_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    assert event.event_time_precision == "datetime"
    assert event.event_time_utc.startswith("2026-01-23T03:00")


def test_record_shape_is_schedule_only() -> None:
    raw, event = mpm_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    assert raw.provider == PROVIDER == "boj"
    assert event.country_code == "JP"
    assert event.currency == "JPY"
    assert event.source == "Bank of Japan"
    assert event.source_url == BOJ_MPM_CALENDAR_URL
    # Schedule-only — no value fields populated.
    assert event.actual is None
    assert event.forecast is None


def test_provider_event_id_anchors_on_closing_date() -> None:
    """The schedule-side id and the statement-side id must match so the
    value upserts onto the existing row rather than duplicating."""
    _, event_sched = mpm_entry_to_records(
        _entry(closing=date(2025, 3, 19)),
        snapshot_epoch_ms=1_700_000_000,
    )
    from ingestion.calendar.boj_api.statements import StatementValue
    value = StatementValue(
        closing_date=date(2025, 3, 19), rate=0.5, rate_text="0.5",
    )
    _, event_val = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert event_sched.provider_event_id == event_val.provider_event_id


def test_content_hash_changes_when_closing_date_revises() -> None:
    payload_a = {
        "closing_date": "2026-01-23",
        "date_cell":    "Jan. 22 (Thurs.), 23 (Fri.)",
        "event_time_utc": "2026-01-23T03:00:00+00:00",
    }
    payload_b = {**payload_a, "closing_date": "2026-01-24"}
    assert _content_hash(payload_a) != _content_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# Statement parser
# ──────────────────────────────────────────────────────────────────────────


def test_statement_parses_hold_rate() -> None:
    value = parse_statement_html(
        _stmt_fixture("k250319a.htm"), closing_date=date(2025, 3, 19),
    )
    assert value.rate == 0.5
    assert value.rate_text == "0.5"


def test_statement_parses_hike_rate() -> None:
    # 2024-07-31 hike from ~0% to 0.25%. The sentence uses "remain"
    # forward-looking even for a hike; the parser extracts the number.
    value = parse_statement_html(
        _stmt_fixture("k240731a.htm"), closing_date=date(2024, 7, 31),
    )
    assert value.rate == 0.25
    assert value.rate_text == "0.25"


def test_statement_parses_release_time_from_page() -> None:
    """BoJ statements carry a "Release dates and times" block whose
    first entry is the statement itself; the parser lifts that
    HH:MM so the value-side write stamps the real publish time
    rather than the 12:00 JST placeholder. Fixture sample spans
    11:25 → 12:56 JST across the four captured meetings."""
    observed = {
        "k250319a.htm": "11:25",
        "k250501a.htm": "12:02",
        "k250731a.htm": "11:57",
        "k240731a.htm": "12:56",
    }
    for name, expected in observed.items():
        closing_iso = f"20{name[1:3]}-{name[3:5]}-{name[5:7]}"
        value = parse_statement_html(
            _stmt_fixture(name), closing_date=date.fromisoformat(closing_iso),
        )
        assert value.release_time_local == expected, name


def test_statement_value_stamps_actual_publish_time() -> None:
    """11:25 JST → 02:25 UTC (JST has no DST)."""
    value = parse_statement_html(
        _stmt_fixture("k250319a.htm"), closing_date=date(2025, 3, 19),
    )
    _, event = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert event.event_time_utc.startswith("2025-03-19T02:25")


def test_statement_content_hash_changes_when_publish_time_revises() -> None:
    """Raw revision model must capture publish-time corrections.
    Rate alone isn't enough — if BoJ later corrects the "Release
    dates and times" block from 12:56 to 12:55, the raw lane would
    otherwise drop the snapshot as a duplicate and the PIT row's
    datetime would change without an audit trail."""
    from ingestion.calendar.boj_api.statements import (
        StatementValue,
        statement_value_to_records,
    )
    v_a = StatementValue(
        closing_date=date(2025, 3, 19), rate=0.5, rate_text="0.5",
        release_time_local="11:25",
    )
    v_b = StatementValue(
        closing_date=date(2025, 3, 19), rate=0.5, rate_text="0.5",
        release_time_local="11:26",
    )
    raw_a, _ = statement_value_to_records(v_a, snapshot_epoch_ms=1_700_000_000)
    raw_b, _ = statement_value_to_records(v_b, snapshot_epoch_ms=1_700_000_000)
    assert raw_a.content_hash != raw_b.content_hash


def test_statement_falls_back_to_noon_when_release_line_missing() -> None:
    """The projection still works when the statement page doesn't
    carry the schedule block (older statements, emergency drafts).
    Falls through to the 12:00 JST convention."""
    html = """
    <html><body>
      <p>The Bank will encourage the uncollateralized overnight
         call rate to remain at around 0.5 percent.</p>
    </body></html>
    """
    value = parse_statement_html(html, closing_date=date(2025, 3, 19))
    assert value.release_time_local is None
    _, event = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert event.event_time_utc.startswith("2025-03-19T03:00")


def test_statement_parse_raises_on_missing_sentence() -> None:
    html = "<html><body>No policy sentence here.</body></html>"
    with pytest.raises(BojStatementParseError):
        parse_statement_html(html, closing_date=date(2025, 3, 19))


def test_statement_value_writes_actual_with_two_decimals() -> None:
    value = parse_statement_html(
        _stmt_fixture("k250319a.htm"), closing_date=date(2025, 3, 19),
    )
    _, event = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert event.actual == "0.50"
    assert event.source_url == build_statement_url(date(2025, 3, 19))


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(store: SQLiteEngineStore) -> None:
    raw, _ = mpm_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])
    assert first == 1
    assert second == 0


def test_statement_value_upserts_actual_onto_schedule_row(
    store: SQLiteEngineStore,
) -> None:
    # First write the schedule row with ``actual=None``.
    entry = _entry(closing=date(2025, 3, 19), date_cell="Mar. 18, 19")
    raw, event = mpm_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)
    from ingestion.calendar.boj_api import project_schedule_events
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_schedule_events(conn, [event])

    # Now write the value-side record; it should upsert onto the same
    # row (not create a duplicate) and fill ``actual``.
    value = parse_statement_html(
        _stmt_fixture("k250319a.htm"), closing_date=date(2025, 3, 19),
    )
    raw_v, event_v = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_001,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_v])
        changed = project_events(conn, [event_v])
    assert changed == 1

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT actual, event_time_precision FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2025-03-19"),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "0.50"
    assert rows[0][1] == "datetime"


# ──────────────────────────────────────────────────────────────────────────
# Fetcher
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_indicator_plan(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boj_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOJ_RATE"]
    assert summary.meetings_parsed == 0


def test_fetch_projects_fixture_into_events(store: SQLiteEngineStore) -> None:
    html = _cal_fixture()

    with store._connection(commit=True) as conn:
        summary = fetch_boj_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert summary.dry_run is False
    assert summary.meetings_parsed == 16
    assert summary.rows_raw_inserted == 16
    assert summary.events_upserted == 16
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()[0]
    assert rows == 16


def test_fetch_raises_when_parse_yields_zero_meetings(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        with pytest.raises(BojMpmCalendarParseError):
            fetch_boj_calendar(
                conn, dry_run=False,
                html_fetcher=lambda: "<html><body>Access Denied</body></html>",
                snapshot_epoch_ms=1_700_000_000,
            )


def test_fetch_values_discovers_pending_rows(store: SQLiteEngineStore) -> None:
    """Schedule write first, then auto-discovery in dry-run mode must
    return the discovered closings without hitting any statement page."""
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    # Snapshot far in the future so every schedule row counts as past.
    far_future_ms = 4_000_000_000_000
    with store._connection(commit=False) as conn:
        summary = fetch_boj_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=far_future_ms,
        )
    assert summary.meetings_planned == 16


def test_fetch_values_respects_release_buffer(store: SQLiteEngineStore) -> None:
    """Auto-discovery must not queue a meeting whose scheduled event
    time is less than 1h in the past. Without the buffer, a cron
    sweep that fires between 12:00 and ~12:56 JST on a closing day
    would 404 the statement page three times in a row, tripping the
    circuit breaker and delaying the eventual ``actual`` write.
    """
    from ingestion.calendar.boj_api import mpm_entry_to_records, project_schedule_events
    from datetime import datetime, timezone
    entry = _entry(
        year=2025, date_cell="Mar. 18, 19", closing=date(2025, 3, 19),
    )
    raw, event = mpm_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)
    with store._connection(commit=True) as conn:
        from ingestion.calendar.boj_api import store_raw as _store_raw
        _store_raw(conn, [raw])
        project_schedule_events(conn, [event])

    # Scheduled event_time_utc for 2025-03-19 12:00 JST → 2025-03-19T03:00:00+00:00.
    # "Just after noon JST" poll: as_of = 03:30 UTC = 12:30 JST.
    too_early = int(datetime(
        2025, 3, 19, 3, 30, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        early_summary = fetch_boj_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=too_early,
        )
    assert early_summary.meetings_planned == 0

    # "One hour past noon JST" poll: as_of = 04:01 UTC = 13:01 JST.
    # Every observed publish time falls before 13:00 JST, so the
    # statement page is reliably up by the first poll.
    safe = int(datetime(
        2025, 3, 19, 4, 1, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        safe_summary = fetch_boj_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=safe,
        )
    assert safe_summary.meetings_planned == 1


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_boj", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == ["BOJ_RATE"]


def test_service_op_values_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_boj_values", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == ["BOJ_RATE"]
