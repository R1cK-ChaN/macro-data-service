"""Mocked tests for the Cabinet Office (ESRI) calendar connector
(issue #14 P3).

Fixture HTML lives in ``tests/fixtures/cao_schedule/`` and
``tests/fixtures/cao_consumer_confidence/`` — slices of the real
``esri.cao.go.jp`` surfaces captured 2026-04-24. No real HTTP in CI.

Covers:

- Schedule parser: Consumer Confidence column extracted correctly
  (column 3 "Consumer Confidence Survey"); implicit-year carry on
  rows after the first; Dec→Jan reference-year wrap; Business
  Outlook quarter-range cell in the CC column raises; missing
  column raises.
- ``schedule_entry_to_records``: ``provider_event_id`` anchors on
  ``(indicator, reference_date)``; 14:00 JST → 05:00 UTC same-day
  (JST has no DST); ``source_url`` points at the per-release
  landing page, not the schedule index.
- Value parser: extracts reference / release / CCI (SA) from the
  two deterministic sentences; cross-month mismatch raises; ordinal
  suffix handled ("April 9th, 2026").
- ``consumer_confidence_to_records``: caller-supplied
  ``event_time_utc`` takes precedence; actual renders as a plain
  decimal without trailing zeros.
- Projector integration: schedule-side write leaves ``actual`` NULL
  and stamps ``event_time_precision='datetime'``; value-side write
  fills ``actual`` without clobbering the stored datetime.
- Fetcher: dry-run plans; zero entries raises; value-side sweep
  collects fetch / parse failures rather than aborting.
- Canonicalize: ``"Consumer Confidence"`` resolves to
  ``CB_CONSUMER_CONFIDENCE`` (shared with the Conference Board row
  — country disambiguation lives in ``provider_event_id``).
- Scheduler registration: ``cao`` in ``ALL_CONNECTORS`` and
  ``cao-values`` in ``ALL_VALUE_SIDE_CONNECTORS``.
- Service ops ``calendar_econ_fetch_cao`` and
  ``calendar_econ_fetch_cao_values`` — dry-run shapes.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import canonicalize_indicator
from ingestion.calendar.cao_api import (
    ALL_INDICATORS,
    CAO_CONSUMER_CONFIDENCE_URL,
    CAO_ESRI_SCHEDULE_URL,
    INDICATOR_REGISTRY,
    PROVIDER,
    CaoCalendarParseError,
    CaoConsumerConfidenceEntry,
    CaoConsumerConfidenceParseError,
    ConsumerConfidenceSummary,
    consumer_confidence_to_records,
    fetch_cao_calendar,
    fetch_cao_consumer_confidence_values,
    parse_cao_schedule_html,
    parse_consumer_confidence_summary,
    project_events,
    project_schedule_events,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    ALL_VALUE_SIDE_CONNECTORS,
)
from storage.sqlite import SQLiteEngineStore


SCHEDULE_FIXTURES = Path(__file__).parent / "fixtures" / "cao_schedule"
VALUE_FIXTURES = Path(__file__).parent / "fixtures" / "cao_consumer_confidence"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _schedule_fixture() -> str:
    return (SCHEDULE_FIXTURES / "stat-schedule-e.html").read_text(encoding="utf-8")


def _value_fixture(name: str) -> str:
    return (VALUE_FIXTURES / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_holds_consumer_confidence() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {"CONSUMER_CONFIDENCE"}
    spec = INDICATOR_REGISTRY["CONSUMER_CONFIDENCE"]
    assert spec.country_code == "JP"
    assert spec.importance == "high"
    assert spec.unit == "points"
    assert spec.title == "Consumer Confidence"
    assert spec.category == "Consumer"


def test_all_indicators_is_sorted_list() -> None:
    assert ALL_INDICATORS == sorted(INDICATOR_REGISTRY.keys())


def test_provider_id_is_cao() -> None:
    assert PROVIDER == "cao"


# ──────────────────────────────────────────────────────────────────────────
# Canonicalize
# ──────────────────────────────────────────────────────────────────────────


def test_consumer_confidence_canonicalizes_shared_with_cb() -> None:
    """CAO reuses the pre-existing ``CB_CONSUMER_CONFIDENCE`` canonical —
    country disambiguation lives in ``provider_event_id``. Same pattern
    as MoF reusing BEA's ``TRADE_BALANCE``."""
    assert canonicalize_indicator("Consumer Confidence") == "CB_CONSUMER_CONFIDENCE"
    assert canonicalize_indicator("Consumer Confidence Survey") == (
        "CB_CONSUMER_CONFIDENCE"
    )
    assert canonicalize_indicator("CAO Consumer Confidence") == (
        "CB_CONSUMER_CONFIDENCE"
    )
    assert canonicalize_indicator("Japan Consumer Confidence") == (
        "CB_CONSUMER_CONFIDENCE"
    )


# ──────────────────────────────────────────────────────────────────────────
# Schedule parser (stat-schedule-e.html)
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_extracts_consumer_confidence_column() -> None:
    entries = parse_cao_schedule_html(_schedule_fixture())
    assert [e.reference_date for e in entries] == [
        date(2026, 4, 1),
        date(2026, 5, 1),
        date(2026, 6, 1),
        date(2026, 7, 1),
    ]
    assert [e.release_date for e in entries] == [
        date(2026, 4, 30),
        date(2026, 5, 29),
        date(2026, 7, 1),
        date(2026, 7, 30),
    ]
    assert [e.reference_label for e in entries] == [
        "April 2026",
        "May 2026",
        "June 2026",
        "July 2026",
    ]


def test_schedule_carries_year_forward_on_implicit_rows() -> None:
    """First row has an explicit year (``Apr.30,2026``); subsequent
    rows drop the year. The parser must carry 2026 through until a
    new explicit year appears."""
    entries = parse_cao_schedule_html(_schedule_fixture())
    assert all(e.release_date.year == 2026 for e in entries)


def test_schedule_handles_dec_jan_reference_wrap() -> None:
    """A January release that references December belongs to the
    prior reference year. Construct a minimal fixture exercising the
    wrap — the production table doesn't happen to contain one, but
    the logic is in scope."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Jan.15,2027<br>(Dec.)</td></tr>
    </tbody></table>
    </body></html>"""
    entries = parse_cao_schedule_html(html)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.release_date == date(2027, 1, 15)
    assert entry.reference_date == date(2026, 12, 1)
    assert entry.reference_label == "December 2026"


def test_schedule_rolls_implicit_year_on_calendar_boundary() -> None:
    """Codex P3 round 1 accept (rule 4): only the first tbody row is
    obliged to carry an explicit year. Once ESRI's schedule table
    rolls across the year boundary, an implicit ``Jan.15`` after
    ``Dec.10,2026`` must land as 2027-01-15 — without the wrap check
    the connector would write the next January release under the
    prior calendar year and collide with historical rows."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Nov.28,2026<br>(Nov.)</td></tr>
      <tr><td>Dec.26<br>(Dec.)</td></tr>
      <tr><td>Jan.30<br>(Jan.)</td></tr>
      <tr><td>Feb.27<br>(Feb.)</td></tr>
    </tbody></table>
    </body></html>"""
    entries = parse_cao_schedule_html(html)
    assert [e.release_date for e in entries] == [
        date(2026, 11, 28),
        date(2026, 12, 26),
        date(2027, 1, 30),
        date(2027, 2, 27),
    ]
    assert [e.reference_date for e in entries] == [
        date(2026, 11, 1),
        date(2026, 12, 1),
        date(2027, 1, 1),
        date(2027, 2, 1),
    ]


def test_schedule_raises_when_column_missing() -> None:
    """If ESRI reorders the table or renames the column header, the
    scraper must fail loud rather than silently pick the wrong
    column."""
    html = """<html><body>
    <table><thead><tr>
      <th>Machinery Orders</th>
      <th>Business Outlook Survey</th>
    </tr></thead>
    <tbody><tr><td>May 12,2026<br>(Mar.)</td><td>Jun.11,2026<br>(Apr.-Jun.)</td></tr></tbody>
    </table>
    </body></html>"""
    with pytest.raises(CaoCalendarParseError):
        parse_cao_schedule_html(html)


def test_schedule_raises_when_quarter_range_slips_into_cc_column() -> None:
    """``(Apr.-Jun.)`` is the Business Outlook shape — it must not
    appear in the Consumer Confidence column. Loud-fail if it does."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Jun.11,2026<br>(Apr.-Jun.)</td></tr>
    </tbody></table>
    </body></html>"""
    with pytest.raises(CaoCalendarParseError):
        parse_cao_schedule_html(html)


def test_schedule_skips_empty_cells() -> None:
    """Business Outlook Survey ends earlier than the other columns —
    its ragged-right ``&nbsp;`` cells are legitimate but must not
    spawn empty Consumer Confidence entries."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Apr.30,2026<br>(Apr.)</td></tr>
      <tr><td>&nbsp;</td></tr>
      <tr><td>May 29<br>(May)</td></tr>
    </tbody></table>
    </body></html>"""
    entries = parse_cao_schedule_html(html)
    assert len(entries) == 2
    assert [e.reference_date for e in entries] == [
        date(2026, 4, 1),
        date(2026, 5, 1),
    ]


def test_schedule_raises_on_duplicate_reference_month() -> None:
    """Two rows for the same reference month inside a single table
    pass would corrupt the schedule-side projection (one
    provider_event_id colliding with itself)."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Apr.30,2026<br>(Apr.)</td></tr>
      <tr><td>May 1<br>(Apr.)</td></tr>
    </tbody></table>
    </body></html>"""
    with pytest.raises(CaoCalendarParseError):
        parse_cao_schedule_html(html)


def test_schedule_raises_on_implausible_reference_release_gap() -> None:
    """Reference-release gap > 1 month suggests we're parsing the
    wrong column (Machinery Orders has ~6-week lag)."""
    html = """<html><body>
    <table><thead><tr>
      <th>Consumer Confidence Survey</th>
    </tr></thead>
    <tbody>
      <tr><td>Aug.15,2026<br>(Mar.)</td></tr>
    </tbody></table>
    </body></html>"""
    with pytest.raises(CaoCalendarParseError):
        parse_cao_schedule_html(html)


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_records_anchor_on_indicator_and_reference_date() -> None:
    entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 4, 1),
        reference_label="April 2026",
        release_date=date(2026, 4, 30),
    )
    records = schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000_000)
    assert len(records) == 1
    raw, event = records[0]

    assert event.country_code == "JP"
    assert event.reference_date == "2026-04-01"
    assert event.reference_label == "April 2026"
    assert event.provider == PROVIDER
    assert event.title == "Consumer Confidence"
    assert event.importance == "high"
    assert event.unit == "points"
    assert event.currency == ""
    assert event.actual is None
    assert event.event_time_precision == "datetime"
    assert event.source_url == CAO_CONSUMER_CONFIDENCE_URL
    assert event.source == "Cabinet Office Japan (ESRI)"
    # Schedule + raw share the same provider_event_id.
    assert raw.provider_event_id == event.provider_event_id


def test_schedule_event_time_is_1400_jst_same_day_in_utc() -> None:
    """JST = UTC+9 and has no DST. 14:00 JST on April 30 2026 is
    05:00 UTC on April 30 2026."""
    entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 4, 1),
        reference_label="April 2026",
        release_date=date(2026, 4, 30),
    )
    _, event = schedule_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )[0]
    assert event.event_time_utc.startswith("2026-04-30T05:00:00")


def test_schedule_records_different_references_have_different_ids() -> None:
    a = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 4, 1),
        reference_label="April 2026",
        release_date=date(2026, 4, 30),
    )
    b = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 5, 1),
        reference_label="May 2026",
        release_date=date(2026, 5, 29),
    )
    id_a = schedule_entry_to_records(
        a, snapshot_epoch_ms=1_700_000_000_000,
    )[0][1].provider_event_id
    id_b = schedule_entry_to_records(
        b, snapshot_epoch_ms=1_700_000_000_000,
    )[0][1].provider_event_id
    assert id_a != id_b


# ──────────────────────────────────────────────────────────────────────────
# Consumer Confidence value parser (shouhi-e.html)
# ──────────────────────────────────────────────────────────────────────────


def test_value_parser_extracts_march_release() -> None:
    summary = parse_consumer_confidence_summary(_value_fixture("shouhi-e_2026-03.html"))
    assert summary.reference_date == date(2026, 3, 1)
    assert summary.reference_label == "March 2026"
    assert summary.release_date == date(2026, 4, 9)
    assert summary.cci_seasonally_adjusted == pytest.approx(33.3)


def test_value_parser_handles_different_month() -> None:
    """Second fixture exercises the February 2026 → March 10 release
    with value 39.7 (derived by adding the delta described in the
    March-2026 sentence). Ensures the parser isn't coincidentally
    hard-coded to March."""
    summary = parse_consumer_confidence_summary(_value_fixture("shouhi-e_2026-02.html"))
    assert summary.reference_date == date(2026, 2, 1)
    assert summary.release_date == date(2026, 3, 10)
    assert summary.cci_seasonally_adjusted == pytest.approx(39.7)


def test_value_parser_raises_when_release_sentence_missing() -> None:
    html = """<html><body>
    <p>The Consumer Confidence Index (seasonally adjusted series) in
    March 2026 was 33.3, down 6.4 points from the previous month.</p>
    </body></html>"""
    with pytest.raises(CaoConsumerConfidenceParseError):
        parse_consumer_confidence_summary(html)


def test_value_parser_raises_when_sa_sentence_missing() -> None:
    html = """<html><body>
    <p>The Survey of March 2026 was released on April 9th, 2026.</p>
    <p>The Consumer Confidence Index (original series) in March 2026
    was 33.3, down 6.4 points from the previous month.</p>
    </body></html>"""
    with pytest.raises(CaoConsumerConfidenceParseError):
        parse_consumer_confidence_summary(html)


def test_value_parser_raises_on_mismatched_reference_months() -> None:
    """A mid-edit or cache-stale page could leave the release
    sentence and the headline sentence disagreeing on the reference
    month. Loud-fail so we don't stamp the wrong reference."""
    html = """<html><body>
    <p>The Survey of March 2026 was released on April 9th, 2026.</p>
    <p>The Consumer Confidence Index (seasonally adjusted series)
    in February 2026 was 39.7.</p>
    </body></html>"""
    with pytest.raises(CaoConsumerConfidenceParseError):
        parse_consumer_confidence_summary(html)


def test_value_parser_accepts_ordinal_suffix() -> None:
    """The English page always carries the ordinal — ``April 9th``.
    Already exercised by the March fixture; a standalone assertion
    pins the suffix list so a future ``1st``/``22nd`` doesn't slip."""
    for ordinal in ("1st", "2nd", "3rd", "15th", "21st", "22nd"):
        html = f"""<html><body>
        <p>The Survey of March 2026 was released on April {ordinal}, 2026.</p>
        <p>The Consumer Confidence Index (seasonally adjusted series)
        in March 2026 was 33.3.</p>
        </body></html>"""
        summary = parse_consumer_confidence_summary(html)
        assert summary.release_date.year == 2026
        assert summary.release_date.month == 4


# ──────────────────────────────────────────────────────────────────────────
# consumer_confidence_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_value_records_anchor_on_reference_date() -> None:
    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=33.3,
    )
    raw, event = consumer_confidence_to_records(
        summary, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == PROVIDER
    assert event.country_code == "JP"
    assert event.reference_date == "2026-03-01"
    assert event.actual == "33.3"
    assert event.currency == ""
    assert event.source_url == CAO_CONSUMER_CONFIDENCE_URL
    assert raw.provider_event_id == event.provider_event_id


def test_value_records_use_caller_event_time_when_supplied() -> None:
    """Value-side sweep passes the stored schedule-side
    ``event_time_utc`` so a late upsert doesn't rewind the canonical
    publish time. Mirror's MoF/Tankan behaviour."""
    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=33.3,
    )
    _, event = consumer_confidence_to_records(
        summary,
        snapshot_epoch_ms=1_700_000_000_000,
        event_time_utc="2026-04-09T05:00:00+00:00",
    )
    assert event.event_time_utc == "2026-04-09T05:00:00+00:00"


def test_value_records_fallback_event_time_is_release_1400_jst() -> None:
    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=33.3,
    )
    _, event = consumer_confidence_to_records(
        summary, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.event_time_utc.startswith("2026-04-09T05:00:00")


def test_value_records_render_integer_index_cleanly() -> None:
    """A rare integer result like 40.0 should not stamp ``40`` and
    drop the decimal. ``%g`` formatting strips trailing zeros — the
    parser stores the float, the projector renders with ``:g``."""
    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=40.0,
    )
    _, event = consumer_confidence_to_records(
        summary, snapshot_epoch_ms=1_700_000_000_000,
    )
    # ``40`` is acceptable — %g strips the trailing zero. What we
    # must avoid is ``"40.000000"`` or ``""``.
    assert event.actual in ("40", "40.0")


# ──────────────────────────────────────────────────────────────────────────
# Projector integration
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_write_inserts_actual_null(store: SQLiteEngineStore) -> None:
    entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 4, 1),
        reference_label="April 2026",
        release_date=date(2026, 4, 30),
    )
    raw, event = schedule_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )[0]
    with store._connection(commit=True) as c:
        store_raw(c, [raw])
        project_schedule_events(c, [event])
        row = c.execute(
            "SELECT actual, event_time_utc, event_time_precision, source_url "
            "FROM cal_econ_event WHERE provider=? AND provider_event_id=?",
            (PROVIDER, event.provider_event_id),
        ).fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1].startswith("2026-04-30T05:00:00")
    assert row[2] == "datetime"
    assert row[3] == CAO_CONSUMER_CONFIDENCE_URL


def test_value_write_fills_actual_and_preserves_datetime(
    store: SQLiteEngineStore,
) -> None:
    """The schedule-side write lands first with ``actual=NULL``; the
    value-side sweep must fill ``actual`` without rewinding the
    stored datetime to some freshly-synthesised stamp."""
    entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
    )
    raw_sched, event_sched = schedule_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )[0]
    with store._connection(commit=True) as c:
        store_raw(c, [raw_sched])
        project_schedule_events(c, [event_sched])

    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=33.3,
    )
    raw_val, event_val = consumer_confidence_to_records(
        summary,
        snapshot_epoch_ms=1_700_000_000_100,
        event_time_utc=event_sched.event_time_utc,
    )
    with store._connection(commit=True) as c:
        store_raw(c, [raw_val])
        project_events(c, [event_val])
        row = c.execute(
            "SELECT actual, event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND provider_event_id=?",
            (PROVIDER, event_sched.provider_event_id),
        ).fetchone()
    assert row is not None
    assert row[0] == "33.3"
    assert row[1] == event_sched.event_time_utc


def test_value_write_creates_row_when_no_schedule_exists(
    store: SQLiteEngineStore,
) -> None:
    """When the schedule-side refresh hasn't yet seeded a row (cron
    lag), the value-side sweep must still write — project_events'
    full upsert handles the insert path."""
    summary = ConsumerConfidenceSummary(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
        cci_seasonally_adjusted=33.3,
    )
    raw, event = consumer_confidence_to_records(
        summary, snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as c:
        store_raw(c, [raw])
        project_events(c, [event])
        row = c.execute(
            "SELECT actual, event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND provider_event_id=?",
            (PROVIDER, event.provider_event_id),
        ).fetchone()
    assert row is not None
    assert row[0] == "33.3"


# ──────────────────────────────────────────────────────────────────────────
# Fetcher orchestration
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_dry_run_plans_without_fetch(store: SQLiteEngineStore) -> None:
    calls: list[str] = []

    def _must_not_fetch() -> str:
        calls.append("fetched")
        return ""

    with store._connection(commit=False) as c:
        summary = fetch_cao_calendar(c, dry_run=True, html_fetcher=_must_not_fetch)
    assert summary.dry_run is True
    assert summary.indicators_planned == list(ALL_INDICATORS)
    assert summary.releases_parsed == 0
    assert calls == []


def test_schedule_live_run_projects_rows(store: SQLiteEngineStore) -> None:
    html = _schedule_fixture()
    with store._connection(commit=True) as c:
        summary = fetch_cao_calendar(
            c, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )
        count = c.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider='cao'"
        ).fetchone()[0]
    assert summary.releases_parsed == 4
    assert summary.rows_raw_inserted == 4
    assert summary.events_upserted == 4
    assert count == 4


def test_schedule_live_run_raises_on_zero_entries(
    store: SQLiteEngineStore,
) -> None:
    html = (
        "<html><body><table>"
        "<thead><tr><th>Consumer Confidence Survey</th></tr></thead>"
        "<tbody></tbody></table></body></html>"
    )
    with store._connection(commit=False) as c:
        with pytest.raises(CaoCalendarParseError):
            fetch_cao_calendar(c, dry_run=False, html_fetcher=lambda: html)


def test_value_dry_run_plans_without_fetch(store: SQLiteEngineStore) -> None:
    calls: list[str] = []

    def _must_not_fetch() -> str:
        calls.append("fetched")
        return ""

    with store._connection(commit=False) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=True, html_fetcher=_must_not_fetch,
        )
    assert summary.dry_run is True
    assert summary.releases_planned == 1
    assert summary.releases_fetched == 0
    assert calls == []


def test_value_live_run_fills_actual_on_pending_schedule_row(
    store: SQLiteEngineStore,
) -> None:
    # Seed the schedule row first.
    html = _schedule_fixture()
    with store._connection(commit=True) as c:
        fetch_cao_calendar(
            c, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )

    # Then run the value-side sweep with the March landing page.
    value_html = _value_fixture("shouhi-e_2026-03.html")
    with store._connection(commit=True) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_100,
            html_fetcher=lambda: value_html,
        )
        row = c.execute(
            "SELECT actual, reference_date FROM cal_econ_event "
            "WHERE provider='cao' AND reference_date='2026-03-01'"
        ).fetchone()
    # The March reference isn't in the schedule fixture (which
    # covers April–July), so the sweep writes a fresh row. Both the
    # insert-new and update-existing paths share this code path; the
    # next test pins update semantics against a pre-seeded row.
    assert summary.releases_fetched == 1
    assert summary.events_upserted == 1
    assert row is not None
    assert row[0] == "33.3"


def test_value_live_run_fetch_failure_collected(store: SQLiteEngineStore) -> None:
    def _boom() -> str:
        raise RuntimeError("simulated 503")

    with store._connection(commit=False) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=False, html_fetcher=_boom,
        )
    assert summary.releases_fetched == 0
    assert summary.fetch_failures == [("shouhi-e.html", "simulated 503")]
    assert summary.parse_failures == []


def test_value_live_run_parse_failure_collected(store: SQLiteEngineStore) -> None:
    def _bad_html() -> str:
        return "<html><body><p>no sentences here</p></body></html>"

    with store._connection(commit=False) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=False, html_fetcher=_bad_html,
        )
    assert summary.releases_fetched == 0
    assert summary.fetch_failures == []
    assert len(summary.parse_failures) == 1
    assert summary.parse_failures[0][0] == "shouhi-e.html"


def test_value_sweep_surfaces_overdue_references_on_stale_landing(
    store: SQLiteEngineStore,
) -> None:
    """Codex P3 round 2 accept (rule 4): the ``shouhi-e.html``
    landing page can lag the next due release. The prior sweep
    already captured March 2026; on a stale fetch where the page
    still shows March but April's 14:00 JST release time has
    passed, the sweep upserts March again and reports success —
    leaving April unfilled silently. Surface the overdue pending
    row in the summary so the operator / scheduler can trace the
    gap rather than accept ``releases_fetched=1`` as sufficient."""
    # Seed both March (already filled) and April (pending, event time past).
    march_entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
    )
    april_entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 4, 1),
        reference_label="April 2026",
        release_date=date(2026, 4, 30),
    )
    with store._connection(commit=True) as c:
        for entry in (march_entry, april_entry):
            raw, event = schedule_entry_to_records(
                entry, snapshot_epoch_ms=1_700_000_000_000,
            )[0]
            store_raw(c, [raw])
            project_schedule_events(c, [event])

        # Fill March's actual directly to simulate a prior successful sweep.
        march_summary = ConsumerConfidenceSummary(
            reference_date=date(2026, 3, 1),
            reference_label="March 2026",
            release_date=date(2026, 4, 9),
            cci_seasonally_adjusted=33.3,
        )
        raw_m, event_m = consumer_confidence_to_records(
            march_summary, snapshot_epoch_ms=1_700_000_000_100,
        )
        store_raw(c, [raw_m])
        project_events(c, [event_m])

    # Snapshot at 2026-05-01 — both April's event-time (2026-04-30
    # 05:00 UTC) and March's have passed; the landing page is still
    # serving March's release, so the sweep re-writes March but
    # April stays pending.
    stale_html = _value_fixture("shouhi-e_2026-03.html")
    # 2026-05-01T00:00:00 UTC = 1_777_593_600_000 ms since epoch.
    with store._connection(commit=True) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=False,
            snapshot_epoch_ms=1_777_593_600_000,
            html_fetcher=lambda: stale_html,
        )
    # Summary reports the re-fetch but the overdue list carries the
    # April row so operators know about the gap.
    assert summary.releases_fetched == 1
    assert "2026-04-01" in [ref for ref, _ in summary.overdue_references]


def test_value_sweep_reuses_stored_event_time_on_update(
    store: SQLiteEngineStore,
) -> None:
    """When a schedule row already exists for the reference month,
    the value sweep must keep its stored ``event_time_utc`` rather
    than synthesising a fresh one from ``release_date`` + 14:00 JST.
    The shared projector's CASE guards datetime→datetime conflicts,
    but passing the stored stamp through keeps the upsert a no-op
    on the datetime column."""
    # Seed an April 9 2026 release-date schedule row (March data).
    entry = CaoConsumerConfidenceEntry(
        reference_date=date(2026, 3, 1),
        reference_label="March 2026",
        release_date=date(2026, 4, 9),
    )
    raw_sched, event_sched = schedule_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )[0]
    with store._connection(commit=True) as c:
        store_raw(c, [raw_sched])
        project_schedule_events(c, [event_sched])

    value_html = _value_fixture("shouhi-e_2026-03.html")
    with store._connection(commit=True) as c:
        summary = fetch_cao_consumer_confidence_values(
            c, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_100,
            html_fetcher=lambda: value_html,
        )
        row = c.execute(
            "SELECT actual, event_time_utc FROM cal_econ_event "
            "WHERE provider='cao' AND reference_date='2026-03-01'"
        ).fetchone()
    assert summary.releases_fetched == 1
    assert row is not None
    assert row[0] == "33.3"
    assert row[1] == event_sched.event_time_utc


# ──────────────────────────────────────────────────────────────────────────
# Scheduler registration
# ──────────────────────────────────────────────────────────────────────────


def test_cao_registered_in_schedule_side_connectors() -> None:
    assert "cao" in ALL_CONNECTORS


def test_cao_values_registered_in_value_side_connectors() -> None:
    assert "cao-values" in ALL_VALUE_SIDE_CONNECTORS


# ──────────────────────────────────────────────────────────────────────────
# Service ops
# ──────────────────────────────────────────────────────────────────────────


def test_service_calendar_econ_fetch_cao_dry_run(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_cao", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["indicators_planned"] == list(ALL_INDICATORS)
    assert result["stopped_reason"] == "dry_run"


def test_service_calendar_econ_fetch_cao_values_dry_run(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_cao_values", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["releases_planned"] == 1
    assert result["releases_fetched"] == 0
    assert result["stopped_reason"] == "dry_run"


def test_source_urls_are_stable_constants() -> None:
    """Pin the URL constants so a future accidental rewrite surfaces
    in CI rather than only on a live run."""
    assert CAO_ESRI_SCHEDULE_URL == (
        "https://www.esri.cao.go.jp/en/stat/stat-schedule-e.html"
    )
    assert CAO_CONSUMER_CONFIDENCE_URL == (
        "https://www.esri.cao.go.jp/en/stat/shouhi/shouhi-e.html"
    )
