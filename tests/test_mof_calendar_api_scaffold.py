"""Mocked tests for the MoF Trade Statistics calendar connector
(issue #14 P4).

Fixture HTML / XML live in ``tests/fixtures/mof_calendar/`` and
``tests/fixtures/mof_trade_reports/`` — slices of the real
``customs.go.jp`` surfaces captured 2026-04-24. No real HTTP in CI.

Covers:

- Schedule parser: release-calendar rows extracted correctly
  (column 3 "Monthly Data"; Dec/Jan wrap rule when release month
  < reference month); Fiscal/Calendar-Year aggregation rows
  filtered out; explicit-year override on rowspan year-change.
- ``schedule_entry_to_records``: ``provider_event_id`` anchors on
  ``(indicator, reference_date)``; 08:50 JST → 23:50 UTC prior
  day; schedule source_url points at the per-release XML URL.
- Report (value) parser: headline trade balance extracted from
  ``<sashihiki><sogakutonen>``; deficit triangles (``△``) render
  as signed integers.
- Projector: schedule rows land with ``precision='datetime'`` and
  ``actual=NULL``; the value-side write fills ``actual`` without
  clobbering the stored datetime.
- Service ops ``calendar_econ_fetch_mof`` and
  ``calendar_econ_fetch_mof_values`` — dry-run plans.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.mof_api import (
    ALL_INDICATORS,
    INDICATOR_REGISTRY,
    MOF_CALENDAR_URL,
    MofCalendarEntry,
    MofCalendarParseError,
    MofTradeReportParseError,
    PROVIDER,
    TradeReportValue,
    build_trade_report_url,
    fetch_mof_calendar,
    fetch_mof_trade_values,
    parse_mof_calendar_html,
    parse_trade_report_xml,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    store_raw,
    trade_report_to_records,
)
from ingestion.calendar.mof_api.reports import (
    _content_hash as _report_hash,
    _parse_yen_amount,
)
from ingestion.calendar.mof_api.scraper import _content_hash as _schedule_hash
from storage.sqlite import SQLiteEngineStore


CAL_FIXTURES = Path(__file__).parent / "fixtures" / "mof_calendar"
REPORT_FIXTURES = Path(__file__).parent / "fixtures" / "mof_trade_reports"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _cal_fixture() -> str:
    return (CAL_FIXTURES / "calend_e.htm").read_text(encoding="utf-8")


def _report_fixture(name: str) -> str:
    return (REPORT_FIXTURES / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_includes_balance_of_trade() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {"TRADE_BALANCE"}
    spec = INDICATOR_REGISTRY["TRADE_BALANCE"]
    assert spec.country_code == "JP"
    assert spec.importance == "high"
    assert spec.unit == "JPY Million"
    assert "Balance of Trade" in spec.title


def test_all_indicators_is_sorted_list() -> None:
    assert ALL_INDICATORS == sorted(INDICATOR_REGISTRY.keys())


# ──────────────────────────────────────────────────────────────────────────
# Yen amount parser
# ──────────────────────────────────────────────────────────────────────────


def test_yen_amount_parses_positive() -> None:
    assert _parse_yen_amount("666,977") == 666_977
    assert _parse_yen_amount("105,693") == 105_693


def test_yen_amount_parses_japanese_deficit_triangle() -> None:
    """MoF uses △ (U+25B3) as the deficit sign — plain ASCII minus is
    never used on the trade XML. The parser must normalise to a
    signed integer."""
    assert _parse_yen_amount("△234,620") == -234_620
    assert _parse_yen_amount("△637,610") == -637_610


def test_yen_amount_raises_on_garbage() -> None:
    with pytest.raises(MofTradeReportParseError):
        _parse_yen_amount("n/a")


# ──────────────────────────────────────────────────────────────────────────
# parse_mof_calendar_html
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_parser_extracts_all_monthly_rows() -> None:
    entries = parse_mof_calendar_html(_cal_fixture())
    # Dec 2025 (1) + Jan-Dec 2026 (12) + Jan-Mar 2027 (3) = 16 rows.
    # Fiscal Year and Calendar Year aggregation rows are dropped.
    assert len(entries) == 16
    labels = [e.reference_label for e in entries]
    assert "December 2025" in labels
    assert "March 2026" in labels
    assert "March 2027" in labels


def test_schedule_parser_drops_aggregation_rows() -> None:
    """Fiscal Year and Calendar Year rows would duplicate a matching
    monthly row on the same release day; the parser must skip them."""
    entries = parse_mof_calendar_html(_cal_fixture())
    labels = [e.reference_label for e in entries]
    for label in labels:
        assert "Fiscal Year" not in label
        assert "Calendar Year" not in label


def test_schedule_parser_december_release_wraps_next_year() -> None:
    """Dec.2025 Monthly Data releases on Jan.22 — no explicit year in
    the cell — and since the release month (Jan) precedes the
    reference month (Dec), the release year must bump to 2026."""
    entries = parse_mof_calendar_html(_cal_fixture())
    by_label = {e.reference_label: e for e in entries}
    assert by_label["December 2025"].release_date == date(2026, 1, 22)


def test_schedule_parser_march_release_uses_row_year() -> None:
    """Mar.2026 release "Apr.22" without explicit year → 2026."""
    entries = parse_mof_calendar_html(_cal_fixture())
    by_label = {e.reference_label: e for e in entries}
    assert by_label["March 2026"].release_date == date(2026, 4, 22)


def test_schedule_parser_explicit_year_override() -> None:
    """Jan.2027 release "Feb.17,2027" carries an explicit year which
    wins over the implicit rowspan year rule."""
    entries = parse_mof_calendar_html(_cal_fixture())
    by_label = {e.reference_label: e for e in entries}
    assert by_label["January 2027"].release_date == date(2027, 2, 17)


def test_schedule_parser_builds_per_release_report_url() -> None:
    entries = parse_mof_calendar_html(_cal_fixture())
    by_label = {e.reference_label: e for e in entries}
    assert by_label["March 2026"].report_url == (
        "https://www.customs.go.jp/toukei/shinbun/trade-st_e/2026/2026034e.xml"
    )
    assert by_label["December 2025"].report_url == (
        "https://www.customs.go.jp/toukei/shinbun/trade-st_e/2025/2025124e.xml"
    )


def test_schedule_parser_raises_on_unrecognised_month_cell() -> None:
    """Codex P4 round 1: a DOM shift that switches ``Feb.`` to the
    full ``February`` would otherwise cause the row to silently
    drop while the fetch reports success. Must fail loud."""
    html = """
    <table><caption>Trade Statistics (Provisional)</caption>
    <tbody>
      <tr>
        <th rowspan="1">2026</th>
        <th>February</th>
        <td>Feb.26</td><td>Mar.6</td><td>Mar.18</td><td>Mar.27</td>
      </tr>
    </tbody></table>
    """
    with pytest.raises(MofCalendarParseError):
        parse_mof_calendar_html(html)


def test_schedule_parser_raises_on_unparseable_monthly_cell() -> None:
    """DOM shift that replaces the Monthly Data cell with an
    announcement ("TBD"): the parser must raise rather than let
    the release silently disappear from the calendar."""
    html = """
    <table><caption>Trade Statistics (Provisional)</caption>
    <tbody>
      <tr>
        <th rowspan="1">2026</th>
        <th>Mar.</th>
        <td>Mar.27</td><td>Apr.7</td><td>TBD</td><td>Apr.28</td>
      </tr>
    </tbody></table>
    """
    with pytest.raises(MofCalendarParseError):
        parse_mof_calendar_html(html)


def test_report_parser_raises_on_missing_export_block() -> None:
    """Codex P4 round 1: if MoF drops the ``<export>`` block on one
    release, ``_first_child_text(None, ...)`` would raise
    ``AttributeError`` and the sweep would abort mid-run instead of
    collecting a parse failure. Must surface as
    ``MofTradeReportParseError`` so the fetcher catches it."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <hodoxml>
      <sogakutsuki name="pg1">
        <kohyoymd>April 22, 2026</kohyoymd>
        <taishoymtonen>March 2026</taishoymtonen>
        <import><sogakutonen>10,336,342</sogakutonen></import>
        <sashihiki>
          <sogakutonen>666,977</sogakutonen>
          <sogakuzennen>529,809</sogakuzennen>
        </sashihiki>
      </sogakutsuki>
    </hodoxml>
    """
    with pytest.raises(MofTradeReportParseError):
        parse_trade_report_xml(xml, reference_date=date(2026, 3, 1))


def test_fetch_values_isolates_one_malformed_report(
    store: SQLiteEngineStore,
) -> None:
    """Regression for Codex P4: a single malformed XML (missing
    <export>) must surface as a parse failure without aborting the
    whole sweep; the next release in the planned list still lands."""
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    broken_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <hodoxml>
      <sogakutsuki name="pg1">
        <kohyoymd>January 22, 2026</kohyoymd>
        <taishoymtonen>December 2025</taishoymtonen>
        <import><sogakutonen>10,000,000</sogakutonen></import>
        <sashihiki>
          <sogakutonen>100,000</sogakutonen>
          <sogakuzennen>95,000</sogakuzennen>
        </sashihiki>
      </sogakutsuki>
    </hodoxml>
    """
    fixture_map = {
        date(2025, 12, 1): broken_xml,
        date(2026, 3, 1):  _report_fixture("2026034e.xml"),
    }

    def _local_fetcher(ref: date) -> str:
        return fixture_map[ref]

    far_future_ms = 4_000_000_000_000
    with store._connection(commit=True) as conn:
        summary = fetch_mof_trade_values(
            conn, dry_run=False,
            snapshot_epoch_ms=far_future_ms,
            reference_dates=[date(2025, 12, 1), date(2026, 3, 1)],
            xml_fetcher=_local_fetcher,
        )
    # One malformed + one OK → one parse failure + one successful fetch.
    assert summary.releases_fetched == 1
    assert len(summary.parse_failures) == 1
    assert summary.parse_failures[0][0] == "2025-12-01"


def test_schedule_parser_raises_on_missing_provisional_table() -> None:
    html = """
    <html><body>
      <h1>MoF Release Calendar</h1>
      <table><caption>Other Trade Related Statistics</caption></table>
    </body></html>
    """
    with pytest.raises(MofCalendarParseError):
        parse_mof_calendar_html(html)


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def _entry(
    *,
    reference: date = date(2026, 3, 1),
    release: date = date(2026, 4, 22),
) -> MofCalendarEntry:
    return MofCalendarEntry(
        reference_date=reference,
        reference_label=reference.strftime("%B %Y"),
        release_date=release,
        report_url=build_trade_report_url(reference),
    )


def test_schedule_record_uses_0850_jst_convention() -> None:
    records = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    assert len(records) == 1
    raw, event = records[0]
    assert event.event_time_precision == "datetime"
    # 2026-04-22 08:50 JST → 2026-04-21 23:50 UTC (JST has no DST).
    assert event.event_time_utc.startswith("2026-04-21T23:50")


def test_schedule_record_shape_is_schedule_only() -> None:
    entry = _entry()
    raw, event = schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)[0]
    assert raw.provider == PROVIDER == "mof-jp"
    assert event.country_code == "JP"
    assert event.currency == "JPY"
    assert event.unit == "JPY Million"
    assert event.title == "Balance of Trade"
    assert event.source == "Ministry of Finance Japan"
    # source_url points at the per-release XML, not the calendar
    # index — same provenance-stability pattern as Tankan P1a.
    assert event.source_url == entry.report_url
    assert event.actual is None


def test_schedule_and_value_ids_align() -> None:
    sched_event = schedule_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )[0][1]
    value = TradeReportValue(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 22),
        balance_million_jpy=666_977,
        export_million_jpy=11_003_319,
        import_million_jpy=10_336_342,
    )
    _, value_event = trade_report_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert sched_event.provider_event_id == value_event.provider_event_id


def test_schedule_content_hash_changes_when_release_slips() -> None:
    payload_a = {
        "reference_date": "2026-03-01",
        "release_date":   "2026-04-22",
        "event_time_utc": "2026-04-21T23:50:00+00:00",
    }
    payload_b = {**payload_a, "release_date": "2026-04-23"}
    assert _schedule_hash(payload_a) != _schedule_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# parse_trade_report_xml
# ──────────────────────────────────────────────────────────────────────────


def test_report_parser_extracts_surplus() -> None:
    value = parse_trade_report_xml(
        _report_fixture("2026034e.xml"),
        reference_date=date(2026, 3, 1),
    )
    assert value.balance_million_jpy == 666_977
    assert value.export_million_jpy == 11_003_319
    assert value.import_million_jpy == 10_336_342
    assert value.release_date == date(2026, 4, 22)
    assert value.reference_label == "March 2026"


def test_report_parser_extracts_deficit() -> None:
    """September 2025 saw a trade deficit; the XML prints △234,620
    and the parser must produce −234,620."""
    value = parse_trade_report_xml(
        _report_fixture("2025094e.xml"),
        reference_date=date(2025, 9, 1),
    )
    assert value.balance_million_jpy == -234_620


def test_report_parser_handles_multiple_reference_months() -> None:
    """Table shape must hold across captured fixtures — a per-month
    DOM change that only affects one fixture would leak through
    without this sweep."""
    expected = {
        "2026034e.xml": (date(2026, 3, 1),  666_977),
        "2025124e.xml": (date(2025, 12, 1), 105_693),
        "2025094e.xml": (date(2025, 9, 1),  -234_620),
        "2025054e.xml": (date(2025, 5, 1),  -637_610),
    }
    for name, (ref, expected_balance) in expected.items():
        value = parse_trade_report_xml(_report_fixture(name), reference_date=ref)
        assert value.balance_million_jpy == expected_balance, name
        assert value.reference_date == ref


def test_report_parser_raises_on_reference_mismatch() -> None:
    """If the caller's ``reference_date`` disagrees with the XML's
    ``<taishoymtonen>`` cell, the parser must surface the mismatch
    rather than silently writing the wrong month's DI onto a schedule
    row keyed on the caller's reference."""
    with pytest.raises(MofTradeReportParseError):
        parse_trade_report_xml(
            _report_fixture("2026034e.xml"),
            reference_date=date(2026, 2, 1),
        )


def test_report_parser_raises_on_missing_sashihiki() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <hodoxml>
      <sogakutsuki name="pg1">
        <kohyoymd>April 22, 2026</kohyoymd>
        <taishoymtonen>March 2026</taishoymtonen>
        <export><sogakutonen>11,003,319</sogakutonen></export>
        <import><sogakutonen>10,336,342</sogakutonen></import>
      </sogakutsuki>
    </hodoxml>
    """
    with pytest.raises(MofTradeReportParseError):
        parse_trade_report_xml(xml, reference_date=date(2026, 3, 1))


def test_report_parser_raises_on_malformed_xml() -> None:
    with pytest.raises(MofTradeReportParseError):
        parse_trade_report_xml(
            "<hodoxml><not closed>",
            reference_date=date(2026, 3, 1),
        )


# ──────────────────────────────────────────────────────────────────────────
# trade_report_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_value_record_fills_actual_from_balance() -> None:
    value = parse_trade_report_xml(
        _report_fixture("2026034e.xml"),
        reference_date=date(2026, 3, 1),
    )
    raw, event = trade_report_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    # Current-month balance in millions of yen.
    assert event.actual == "666977"
    assert event.currency == "JPY"
    assert event.unit == "JPY Million"
    assert event.source_url == build_trade_report_url(date(2026, 3, 1))


def test_value_record_signs_deficit() -> None:
    value = parse_trade_report_xml(
        _report_fixture("2025054e.xml"),
        reference_date=date(2025, 5, 1),
    )
    _, event = trade_report_to_records(value, snapshot_epoch_ms=1_700_000_000)
    assert event.actual == "-637610"


def test_value_record_uses_event_time_override_when_supplied() -> None:
    value = parse_trade_report_xml(
        _report_fixture("2026034e.xml"),
        reference_date=date(2026, 3, 1),
    )
    override = "2026-04-21T23:50:00+00:00"
    _, event = trade_report_to_records(
        value, snapshot_epoch_ms=1_700_000_000, event_time_utc=override,
    )
    assert event.event_time_utc == override


def test_value_record_falls_back_to_xml_release_date() -> None:
    """Without caller-supplied override or release_date, the value
    writer projects event_time from the XML's own ``<kohyoymd>``
    cell — April 22, 2026 for March 2026 reference."""
    value = parse_trade_report_xml(
        _report_fixture("2026034e.xml"),
        reference_date=date(2026, 3, 1),
    )
    _, event = trade_report_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    assert event.event_time_utc.startswith("2026-04-21T23:50")


def test_value_content_hash_changes_on_revision() -> None:
    payload_a = {
        "indicator":      "TRADE_BALANCE",
        "balance":        666_977,
        "event_time_utc": "2026-04-21T23:50:00+00:00",
    }
    payload_b = {**payload_a, "balance": 700_000}
    assert _report_hash(payload_a) != _report_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(store: SQLiteEngineStore) -> None:
    raw, _ = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)[0]
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])
    assert first == 1
    assert second == 0


def test_value_upserts_actual_onto_schedule_row(
    store: SQLiteEngineStore,
) -> None:
    entry = _entry()
    raw, event = schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)[0]
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_schedule_events(conn, [event])

    value = parse_trade_report_xml(
        _report_fixture("2026034e.xml"),
        reference_date=date(2026, 3, 1),
    )
    raw_v, event_v = trade_report_to_records(
        value,
        snapshot_epoch_ms=1_700_000_001,
        event_time_utc="2026-04-21T23:50:00+00:00",
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_v])
        changed = project_events(conn, [event_v])
    assert changed == 1

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT actual, event_time_utc, source_url "
            "FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "666977"
    assert rows[0][1].startswith("2026-04-21T23:50")
    # source_url is the same on both sides → re-seed never flips it.
    assert rows[0][2] == build_trade_report_url(date(2026, 3, 1))


# ──────────────────────────────────────────────────────────────────────────
# Fetcher
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_indicator_plan(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_mof_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == list(ALL_INDICATORS)
    assert summary.releases_parsed == 0


def test_fetch_projects_fixture_into_events(store: SQLiteEngineStore) -> None:
    html = _cal_fixture()

    with store._connection(commit=True) as conn:
        summary = fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert summary.dry_run is False
    assert summary.releases_parsed == 16
    # One indicator × 16 releases = 16 rows.
    assert summary.rows_raw_inserted == 16
    assert summary.events_upserted == 16
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event "
            "WHERE provider=? AND title=?",
            (PROVIDER, "Balance of Trade"),
        ).fetchone()[0]
    assert rows == 16


def test_fetch_raises_when_parse_yields_zero_releases(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        with pytest.raises(MofCalendarParseError):
            fetch_mof_calendar(
                conn, dry_run=False,
                html_fetcher=lambda: "<html><body>Access Denied</body></html>",
                snapshot_epoch_ms=1_700_000_000,
            )


def test_fetch_values_discovers_pending_rows(store: SQLiteEngineStore) -> None:
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    far_future_ms = 4_000_000_000_000
    with store._connection(commit=False) as conn:
        summary = fetch_mof_trade_values(
            conn, dry_run=True, snapshot_epoch_ms=far_future_ms,
        )
    # All 16 past schedule rows are up for discovery (no buffer
    # clips because far_future >> every release date).
    assert summary.releases_planned == 16


def test_fetch_values_respects_release_buffer(store: SQLiteEngineStore) -> None:
    """Auto-discovery must not queue a release whose scheduled event
    time is less than 1h in the past. Parallels the Tankan pattern."""
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    # March 2026 event_time_utc = 2026-04-21T23:50:00+00:00.
    # "40 minutes past release" poll: too early (< 1h buffer).
    too_early = int(datetime(
        2026, 4, 22, 0, 30, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        early_summary = fetch_mof_trade_values(
            conn, dry_run=True, snapshot_epoch_ms=too_early,
        )
    # March 2026 is blocked by buffer; earlier rows (Dec 2025 / Jan / Feb)
    # are well past and remain.
    assert early_summary.releases_planned == 3

    # "1h past release" poll: March 2026 admitted.
    safe = int(datetime(
        2026, 4, 22, 1, 1, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        safe_summary = fetch_mof_trade_values(
            conn, dry_run=True, snapshot_epoch_ms=safe,
        )
    assert safe_summary.releases_planned == 4


def test_fetch_values_end_to_end_upsert(store: SQLiteEngineStore) -> None:
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    fixture_map = {
        date(2026, 3, 1):  _report_fixture("2026034e.xml"),
        date(2025, 12, 1): _report_fixture("2025124e.xml"),
        date(2025, 9, 1):  _report_fixture("2025094e.xml"),
        date(2025, 5, 1):  _report_fixture("2025054e.xml"),
    }

    def _local_fetcher(ref: date) -> str:
        return fixture_map[ref]

    far_future_ms = 4_000_000_000_000
    with store._connection(commit=True) as conn:
        summary = fetch_mof_trade_values(
            conn, dry_run=False,
            snapshot_epoch_ms=far_future_ms,
            reference_dates=[date(2026, 3, 1)],
            xml_fetcher=_local_fetcher,
        )
    assert summary.releases_fetched == 1
    assert summary.events_upserted == 1
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT actual, event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    assert rows[0][0] == "666977"
    # Schedule-side event_time preserved via override passthrough.
    assert rows[0][1].startswith("2026-04-21T23:50")


def test_fetch_values_manual_replay_preserves_schedule_event_time(
    store: SQLiteEngineStore,
) -> None:
    """Parallels the Tankan P1a regression: a manual replay must not
    shift the stored datetime to the 20th-of-next-month fallback."""
    html = _cal_fixture()
    with store._connection(commit=True) as conn:
        fetch_mof_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    # The schedule write stamped Dec 2025 row at 2026-01-21T23:50
    # (Jan 22 JST 08:50). Fallback release day would be Jan 20, 2026
    # (20th-of-next-month), shifting the stamp to 2026-01-19T23:50.
    fixture_map = {date(2025, 12, 1): _report_fixture("2025124e.xml")}

    def _local_fetcher(ref: date) -> str:
        return fixture_map[ref]

    with store._connection(commit=True) as conn:
        fetch_mof_trade_values(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_001,
            reference_dates=[date(2025, 12, 1)],
            xml_fetcher=_local_fetcher,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2025-12-01"),
        ).fetchall()
    assert rows[0][0].startswith("2026-01-21T23:50")


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_mof", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == list(ALL_INDICATORS)


def test_service_op_values_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_mof_values", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == list(ALL_INDICATORS)


def test_total_outage_detection_covers_releases_counters() -> None:
    """MoF value-side summary uses ``releases_*`` counters identical
    to the Tankan shape — the breaker-total-outage detection must
    recognise it."""
    from ingestion.calendar.scheduler import _summary_is_total_outage
    from ingestion.calendar.mof_api.fetcher import TradeValuesRunSummary

    every_xml_404 = TradeValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=False,
        releases_planned=3,
        releases_fetched=0,
        fetch_failures=[("2026-03-01", "404"), ("2025-12-01", "404")],
    )
    assert _summary_is_total_outage(every_xml_404) is True
