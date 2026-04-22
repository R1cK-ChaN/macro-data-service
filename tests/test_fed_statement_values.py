"""Mocked tests for the Fed FOMC statement value-side scrape (issue #9 P4b-values).

Fixture HTML lives in ``tests/fixtures/fed_fomc_statements/`` — trimmed
copies of four real FOMC statement pages selected to cover every
decision shape the parser must handle: hold, 25-basis-point hike,
50-basis-point cut, and the zero-lower-bound emergency cut (2020-03-15,
``0 to 1/4 percent``). No real HTTP in CI.

Covers:

- :func:`parse_statement_html` — extracts the target range for each
  decision direction and preserves the "by N percentage point(s)"
  phrase on rate changes; normalizes non-breaking hyphens so the
  ``&#8209;`` variant the Fed emits inside ``4&#8209;1/2`` parses
  identically to the ASCII hyphen form.
- :func:`statement_value_to_records` — ``provider_event_id`` matches
  the schedule-side ``meeting_entry_to_records`` output for the same
  closing date, so the value row upserts onto the existing schedule
  row.
- Projector merge — value lands on top of an existing schedule row
  (``actual`` fills, datetime precision survives the merge CASE), and
  a later schedule re-scrape does **not** null the ``actual`` out
  (the P4b-values rationale for flipping the FOMC calendar writer
  to :func:`project_schedule_events`).
- Top-level :func:`fetch_fed_statement_values` — dry-run plan comes
  from ``cal_econ_event`` auto-discovery, execute mode populates the
  value column, per-URL fetch / parse failures land in the summary
  without aborting the run.
- Service op ``calendar_econ_fetch_fed_values`` — dry-run returns the
  planned list, execute mode runs end-to-end against fixtures.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.fed_api import (
    FOMC_STATEMENT_URL_TEMPLATE,
    INDICATOR_REGISTRY,
    FomcMeetingEntry,
    FomcStatementParseError,
    StatementValue,
    build_statement_url,
    fetch_fed_calendar,
    fetch_fed_statement_values,
    meeting_entry_to_records,
    parse_statement_html,
    project_events,
    project_schedule_events,
    statement_value_to_records,
    store_raw,
)
from ingestion.calendar.fed_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_fomc_statements"
FOMC_CAL_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_fomc_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# build_statement_url
# ──────────────────────────────────────────────────────────────────────────


def test_build_statement_url_matches_fed_convention() -> None:
    """URL pattern is ``monetary<YYYYMMDD>a.htm`` under
    ``newsevents/pressreleases/``."""
    assert build_statement_url(date(2025, 1, 29)) == (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20250129a.htm"
    )
    assert build_statement_url(date(2020, 3, 15)) == (
        FOMC_STATEMENT_URL_TEMPLATE.format(yyyymmdd="20200315")
    )


# ──────────────────────────────────────────────────────────────────────────
# parse_statement_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_hold_with_nbh_variant() -> None:
    """January 2025: committee held the range at 4-1/4 to 4-1/2.

    The fixture uses the non-breaking hyphen (``&#8209;``) inside
    ``4-1/2`` to stress-test unicode normalization; the Fed's actual
    pages routinely emit that entity. Parser must recover 4.25-4.50.
    """
    value = parse_statement_html(
        _fixture_html("monetary_20250129.html"),
        closing_date=date(2025, 1, 29),
    )
    assert value.direction == "maintain"
    assert value.range_lower == 4.25
    assert value.range_upper == 4.50
    assert value.by_text is None
    assert value.closing_date == date(2025, 1, 29)


def test_parse_50bp_cut() -> None:
    """September 2024: 50-basis-point cut to 4-3/4 to 5 percent."""
    value = parse_statement_html(
        _fixture_html("monetary_20240918.html"),
        closing_date=date(2024, 9, 18),
    )
    assert value.direction == "lower"
    assert value.range_lower == 4.75
    assert value.range_upper == 5.00
    assert value.by_text is not None
    assert "1/2 percentage point" in value.by_text


def test_parse_25bp_hike() -> None:
    """July 2023: 25-basis-point hike to 5-1/4 to 5-1/2 percent."""
    value = parse_statement_html(
        _fixture_html("monetary_20230726.html"),
        closing_date=date(2023, 7, 26),
    )
    assert value.direction == "raise"
    assert value.range_lower == 5.25
    assert value.range_upper == 5.50
    assert value.by_text is not None
    assert "1/4 percentage point" in value.by_text


def test_parse_zero_lower_bound() -> None:
    """March 2020 emergency cut: ``0 to 1/4 percent`` (no whole-number
    prefix on either endpoint; the upper endpoint is a bare fraction)."""
    value = parse_statement_html(
        _fixture_html("monetary_20200315.html"),
        closing_date=date(2020, 3, 15),
    )
    assert value.direction == "lower"
    assert value.range_lower == 0.0
    assert value.range_upper == 0.25


def test_parse_keep_hold_verb() -> None:
    """Fix for Codex P2 round 2 finding #1: pre-2022 hold statements
    use ``"decided to keep the target range ... at 0 to 1/4 percent"``
    rather than ``"maintain"``. Dropping this verb would leave every
    auto-discovered 2021 / early-2022 FOMC row as a parse failure,
    defeating the point of auto-discovery."""
    value = parse_statement_html(
        _fixture_html("monetary_20211215.html"),
        closing_date=date(2021, 12, 15),
    )
    assert value.direction == "keep"
    assert value.range_lower == 0.0
    assert value.range_upper == 0.25


def test_parse_captures_release_time_for_emergency_statement() -> None:
    """Fix for Codex P2 round 2 finding #2: emergency statements such
    as the 2020-03-15 inter-meeting cut publish at 5:00 p.m. EDT, not
    the standing 14:00 ET slot. The page carries a ``"For release at
    X:XX p.m. …"`` line; parser lifts it into
    :attr:`StatementValue.release_time_local` so the value record can
    honor the published clock. Normal scheduled statements
    (``"For release at 2:00 p.m. EST"``) round-trip identically."""
    emergency = parse_statement_html(
        _fixture_html("monetary_20200315.html"),
        closing_date=date(2020, 3, 15),
    )
    assert emergency.release_time_local is not None
    assert emergency.release_time_local.lower().startswith("5:00")

    normal = parse_statement_html(
        _fixture_html("monetary_20250129.html"),
        closing_date=date(2025, 1, 29),
    )
    assert normal.release_time_local is not None
    assert normal.release_time_local.lower().startswith("2:00")


def test_value_record_honors_emergency_release_time() -> None:
    """End-to-end: emergency statement's 5:00 p.m. EDT lands on the
    event row's ``event_time_utc`` (21:00 UTC in March = EDT's UTC-4
    offset). A scheduled-meeting value record still lands at 19:00
    UTC (14:00 ET EST = UTC-5)."""
    emergency = parse_statement_html(
        _fixture_html("monetary_20200315.html"),
        closing_date=date(2020, 3, 15),
    )
    _, rec = statement_value_to_records(
        emergency, snapshot_epoch_ms=1_800_000_000,
    )
    assert rec.event_time_utc.startswith("2020-03-15T21:00:00")

    scheduled = parse_statement_html(
        _fixture_html("monetary_20250129.html"),
        closing_date=date(2025, 1, 29),
    )
    _, rec2 = statement_value_to_records(
        scheduled, snapshot_epoch_ms=1_800_000_000,
    )
    assert rec2.event_time_utc.startswith("2025-01-29T19:00:00")


def test_parse_raises_on_unknown_body() -> None:
    """An HTML blob that doesn't carry the target-range sentence must
    raise — silent ``None`` here would let a drift in the statement
    layout null out a published rate decision on the existing row."""
    with pytest.raises(FomcStatementParseError):
        parse_statement_html(
            "<html><body><p>Access Denied</p></body></html>",
            closing_date=date(2025, 1, 29),
        )


# ──────────────────────────────────────────────────────────────────────────
# statement_value_to_records
# ──────────────────────────────────────────────────────────────────────────


def _fomc_entry(closing: date = date(2025, 1, 29)) -> FomcMeetingEntry:
    """Build an anchor FomcMeetingEntry for ``closing``."""
    return FomcMeetingEntry(
        year=closing.year,
        month_name=closing.strftime("%B"),
        date_cell=closing.strftime("%d"),
        closing_date=closing,
        has_sep=False,
    )


def test_provider_event_id_matches_schedule_side() -> None:
    """The value-side id must match the schedule-side id for the same
    closing date — both anchor on ``synthesize_event_id(..., closing.iso)``
    so the value row upserts onto the existing schedule row."""
    closing = date(2024, 9, 18)
    _, schedule_rec = meeting_entry_to_records(
        _fomc_entry(closing), snapshot_epoch_ms=1_700_000_000,
    )
    value = parse_statement_html(
        _fixture_html("monetary_20240918.html"), closing_date=closing,
    )
    _, value_rec = statement_value_to_records(
        value, snapshot_epoch_ms=1_800_000_000,
    )
    assert value_rec.provider_event_id == schedule_rec.provider_event_id
    assert value_rec.event_time_utc == schedule_rec.event_time_utc
    assert value_rec.actual == "4.75-5.00"


def test_value_record_writes_two_decimal_range() -> None:
    value = parse_statement_html(
        _fixture_html("monetary_20200315.html"), closing_date=date(2020, 3, 15),
    )
    _, rec = statement_value_to_records(
        value, snapshot_epoch_ms=1_700_000_000,
    )
    # Zero lower bound — two-decimal format still applies.
    assert rec.actual == "0.00-0.25"


# ──────────────────────────────────────────────────────────────────────────
# Projector merge
# ──────────────────────────────────────────────────────────────────────────


def test_value_lands_on_existing_schedule_row(store: SQLiteEngineStore) -> None:
    """End-to-end merge: schedule write first (actual=NULL), then
    statement-value write. The existing row keeps its datetime
    precision and gains ``actual``."""
    closing = date(2025, 1, 29)
    raw_s, event_s = meeting_entry_to_records(
        _fomc_entry(closing), snapshot_epoch_ms=1_700_000_000,
    )
    value = parse_statement_html(
        _fixture_html("monetary_20250129.html"), closing_date=closing,
    )
    raw_v, event_v = statement_value_to_records(
        value, snapshot_epoch_ms=1_800_000_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_s])
        project_schedule_events(conn, [event_s])
        store_raw(conn, [raw_v])
        project_events(conn, [event_v])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual, "
            "observed_at_epoch_ms "
            "FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert row[1] == "datetime"
    assert row[0] == event_s.event_time_utc  # datetime precision survived
    assert row[2] == "4.25-4.50"
    assert row[3] == 1_800_000_000  # observed_at advanced to value write


def test_value_write_preserves_sep_marker_on_quarterly_meetings(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1: the value-side ``project_events``
    upsert overwrites ``title``, so a schedule row that carries ``"+
    SEP"`` (quarterly projection-materials meetings — March, June,
    September, December) must keep that suffix after the statement
    value lands. The driver reads ``has_sep`` from the existing
    schedule row's title; :func:`statement_value_to_records` adds the
    suffix back when constructing the value row."""
    closing = date(2025, 1, 29)
    sep_entry = FomcMeetingEntry(
        year=closing.year,
        month_name=closing.strftime("%B"),
        date_cell="28-29*",
        closing_date=closing,
        has_sep=True,
    )
    raw_s, event_s = meeting_entry_to_records(
        sep_entry, snapshot_epoch_ms=1_700_000_000,
    )
    assert event_s.title == "FOMC Rate Decision + SEP"

    def _fetcher(c: date) -> str:
        return _fixture_html("monetary_20250129.html")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_s])
        project_schedule_events(conn, [event_s])
        summary = fetch_fed_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
            html_fetcher=_fetcher,
        )
        title = conn.execute(
            "SELECT title FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert summary.meetings_fetched == 1
    assert title == "FOMC Rate Decision + SEP"


def test_schedule_rescrape_after_value_preserves_actual(
    store: SQLiteEngineStore,
) -> None:
    """The P4b-values rationale for flipping the FOMC calendar writer
    to ``project_schedule_events``: after a statement value lands, a
    later schedule re-scrape (``actual=NULL`` by construction) must
    not clobber the published rate decision. ``project_schedule_events``
    leaves the value columns and the ``observed_at`` guard alone."""
    closing = date(2025, 1, 29)
    raw_s, event_s = meeting_entry_to_records(
        _fomc_entry(closing), snapshot_epoch_ms=1_700_000_000,
    )
    value = parse_statement_html(
        _fixture_html("monetary_20250129.html"), closing_date=closing,
    )
    raw_v, event_v = statement_value_to_records(
        value, snapshot_epoch_ms=1_800_000_000,
    )
    # Fresher schedule re-scrape — simulates the scheduler hitting the
    # FOMC calendar page the next day.
    _, event_s2 = meeting_entry_to_records(
        _fomc_entry(closing),
        snapshot_epoch_ms=1_900_000_000,
        observed_at_epoch_ms=1_900_000_000,
    )

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_s])
        project_schedule_events(conn, [event_s])
        store_raw(conn, [raw_v])
        project_events(conn, [event_v])
        # Fresher schedule write, ``actual=None`` by construction.
        project_schedule_events(conn, [event_s2])
        actual = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert actual == "4.25-4.50"


# ──────────────────────────────────────────────────────────────────────────
# fetch_fed_statement_values
# ──────────────────────────────────────────────────────────────────────────


def _prime_schedule(store: SQLiteEngineStore, closings: list[date]) -> None:
    """Prime ``cal_econ_event`` with schedule-side rows for the given
    closing dates — mirrors the state after :func:`fetch_fed_calendar`
    would have run."""
    raw_recs = []
    event_recs = []
    for c in closings:
        raw, event = meeting_entry_to_records(
            _fomc_entry(c), snapshot_epoch_ms=1_700_000_000,
        )
        raw_recs.append(raw)
        event_recs.append(event)
    with store._connection(commit=True) as conn:
        store_raw(conn, raw_recs)
        project_schedule_events(conn, event_recs)


def test_dry_run_discovers_pending_closings(store: SQLiteEngineStore) -> None:
    """Dry-run auto-discovers FOMC rows where ``actual IS NULL`` and
    ``event_time_utc < now``."""
    past = [date(2023, 7, 26), date(2024, 9, 18), date(2025, 1, 29)]
    _prime_schedule(store, past)

    # Snapshot is 2026-04-22 — every primed closing is in the past.
    snapshot = 1_777_000_000_000
    with store._connection(commit=False) as conn:
        summary = fetch_fed_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=snapshot,
        )
    assert summary.dry_run is True
    assert summary.meetings_planned == 3
    assert summary.indicators_planned == ["FOMC_RATE"]


def test_dry_run_excludes_future_meetings(store: SQLiteEngineStore) -> None:
    """Future meetings haven't published a statement yet — exclude from
    the plan to keep the op from 404ing against pages that don't exist."""
    _prime_schedule(
        store, [date(2025, 1, 29), date(2099, 12, 15)],
    )
    # Snapshot anchored 2026-04-22.
    snapshot = 1_777_000_000_000
    with store._connection(commit=False) as conn:
        summary = fetch_fed_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=snapshot,
        )
    assert summary.meetings_planned == 1


def test_execute_populates_actual_for_every_closing(
    store: SQLiteEngineStore,
) -> None:
    closings = [date(2023, 7, 26), date(2024, 9, 18), date(2025, 1, 29)]
    _prime_schedule(store, closings)

    by_date = {
        date(2023, 7, 26): _fixture_html("monetary_20230726.html"),
        date(2024, 9, 18): _fixture_html("monetary_20240918.html"),
        date(2025, 1, 29): _fixture_html("monetary_20250129.html"),
    }

    def _fetcher(closing: date) -> str:
        return by_date[closing]

    snapshot = 1_777_000_000_000
    with store._connection(commit=True) as conn:
        summary = fetch_fed_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=snapshot,
            html_fetcher=_fetcher,
        )
        rows = dict(conn.execute(
            "SELECT reference_date, actual FROM cal_econ_event "
            "WHERE provider=? ORDER BY reference_date",
            (PROVIDER,),
        ).fetchall())
    assert summary.meetings_fetched == 3
    assert summary.rows_raw_inserted == 3
    assert summary.events_upserted == 3
    assert rows["2023-07-26"] == "5.25-5.50"
    assert rows["2024-09-18"] == "4.75-5.00"
    assert rows["2025-01-29"] == "4.25-4.50"


def test_execute_collects_fetch_and_parse_failures(
    store: SQLiteEngineStore,
) -> None:
    """A single 404 or layout drift for one closing must not abort the
    whole run — it's collected onto the summary so the scheduler can
    surface it while the other pages still land."""
    good = date(2025, 1, 29)
    http_err = date(2024, 9, 18)
    parse_err = date(2023, 7, 26)
    _prime_schedule(store, [good, http_err, parse_err])

    def _fetcher(closing: date) -> str:
        if closing == http_err:
            raise RuntimeError("HTTP 404")
        if closing == parse_err:
            return "<html><body>unrecognised layout</body></html>"
        return _fixture_html("monetary_20250129.html")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_777_000_000_000,
            html_fetcher=_fetcher,
        )
    assert summary.meetings_fetched == 1
    assert summary.events_upserted == 1
    assert len(summary.fetch_failures) == 1
    assert summary.fetch_failures[0][0] == "2024-09-18"
    assert "404" in summary.fetch_failures[0][1]
    assert len(summary.parse_failures) == 1
    assert summary.parse_failures[0][0] == "2023-07-26"


def test_execute_accepts_explicit_closing_dates(
    store: SQLiteEngineStore,
) -> None:
    """Explicit ``closing_dates`` override the auto-discovery query —
    lets the operator target a single meeting."""
    _prime_schedule(store, [date(2025, 1, 29)])

    def _fetcher(closing: date) -> str:
        assert closing == date(2025, 1, 29)
        return _fixture_html("monetary_20250129.html")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_777_000_000_000,
            closing_dates=[date(2025, 1, 29)],
            html_fetcher=_fetcher,
        )
    assert summary.meetings_fetched == 1


def test_explicit_closing_dates_still_carry_sep_marker(
    store: SQLiteEngineStore,
) -> None:
    """When the operator passes ``closing_dates`` directly, the SEP
    lookup must still read the stored schedule row's title — dropping
    the marker silently here would reintroduce the Codex round-1
    regression on operator-driven one-meeting runs."""
    closing = date(2025, 3, 19)
    sep_entry = FomcMeetingEntry(
        year=closing.year,
        month_name="March",
        date_cell="18-19*",
        closing_date=closing,
        has_sep=True,
    )
    raw_s, event_s = meeting_entry_to_records(
        sep_entry, snapshot_epoch_ms=1_700_000_000,
    )

    def _fetcher(c: date) -> str:
        return _fixture_html("monetary_20250129.html")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_s])
        project_schedule_events(conn, [event_s])
        fetch_fed_statement_values(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_800_000_000_000,
            closing_dates=[closing],
            html_fetcher=_fetcher,
        )
        title = conn.execute(
            "SELECT title FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert title == "FOMC Rate Decision + SEP"


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    _prime_schedule(store, [date(2024, 9, 18), date(2025, 1, 29)])

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_fed_values", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == ["FOMC_RATE"]
    # Auto-discovery ran against the primed schedule rows; both are in
    # the past relative to the default "now" snapshot.
    assert result["meetings_planned"] == 2


def test_full_cycle_fomc_calendar_then_values(
    store: SQLiteEngineStore,
) -> None:
    """Exercises the schedule-then-value flow end-to-end through the
    FOMC calendar fixture that already ships with the scaffold."""
    cal_fixture = (FOMC_CAL_FIXTURE_DIR / "fomc_2026.html").read_text(
        encoding="utf-8",
    )

    def _cal_fetcher() -> str:
        return cal_fixture

    with store._connection(commit=True) as conn:
        cal_summary = fetch_fed_calendar(
            conn, dry_run=False, html_fetcher=_cal_fetcher,
            snapshot_epoch_ms=1_700_000_000_000,
        )

    assert cal_summary.meetings_parsed == 8
    # Fed calendar fixture writes 8 schedule rows for 2026. As of the
    # 2026-04-22 snapshot, only the January 28 and March 18 meetings
    # are in the past — the other six are filtered out of the plan.
    with store._connection(commit=False) as conn:
        values_plan = fetch_fed_statement_values(
            conn, dry_run=True, snapshot_epoch_ms=1_777_000_000_000,
        )
    assert values_plan.meetings_planned == 2


def test_registry_still_declares_percent_unit() -> None:
    """Sanity rail for the ``actual`` format — the FOMC_RATE unit
    stayed ``"percent"`` after P4b-values so downstream consumers of
    the ``X.XX-Y.YY`` range string keep their unit assumption."""
    assert INDICATOR_REGISTRY["FOMC_RATE"].unit == "percent"


def test_statement_value_dataclass_is_frozen() -> None:
    value = StatementValue(
        closing_date=date(2025, 1, 29),
        range_lower=4.25,
        range_upper=4.50,
        direction="maintain",
        by_text=None,
    )
    with pytest.raises(Exception):
        value.range_lower = 5.0  # type: ignore[misc]
