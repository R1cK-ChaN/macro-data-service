"""Mocked tests for the TCMB calendar connector (issue #86 P1).

The captured fixture
``tests/fixtures/tcmb_rate/1hafta_repo.html`` was recorded live on
2026-04-27 from the TCMB 1-Week Repo Auction Rate page. It carries
the full history of PPK rate-change announcements since
20 May 2010 (when the 1-week repo became the policy rate).

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.tcmb_api import (
    INDICATOR_REGISTRY,
    TCMBRateHistoryParseError,
    decision_to_records,
    fetch_tcmb_calendar,
    parse_rate_history,
)
from ingestion.calendar.tcmb_api.parser import PROVIDER, TCMB_RATE_HISTORY_URL
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "tcmb_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _rate_html() -> str:
    return (FIXTURE_DIR / "1hafta_repo.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_rate_history_returns_decisions_most_recent_first() -> None:
    decisions = parse_rate_history(_rate_html())
    assert decisions
    # Decisions arrive newest-first.
    for prev, curr in zip(decisions, decisions[1:]):
        assert prev.effective_date >= curr.effective_date


def test_parse_rate_history_anchors_first_decision_at_2010_05_20() -> None:
    """The 1-Week Repo became TCMB's operational policy rate on
    20 May 2010 — the earliest ``Tarih`` (effective date) on the
    page. The parser stores it verbatim; reconstructing the exact
    PPK announcement date is deferred to P2."""
    decisions = parse_rate_history(_rate_html())
    earliest = min(d.effective_date for d in decisions)
    assert earliest == date(2010, 5, 20)


def test_parse_rate_history_recovers_latest_known_rate() -> None:
    """The 23 January 2026 row is the effective date of the rate cut to
    37.00% (announced at the 22 Jan 2026 PPK meeting; takes effect the
    following business day) — captured live on 2026-04-27."""
    decisions = parse_rate_history(_rate_html())
    latest = decisions[0]
    assert latest.effective_date == date(2026, 1, 23)
    assert latest.rate == "37.00"
    # The previous-rate chain steps back to the prior decision.
    assert latest.previous_rate == "38.00"


def test_parse_rate_history_first_decision_has_no_previous_rate() -> None:
    decisions = parse_rate_history(_rate_html())
    first = min(decisions, key=lambda d: d.effective_date)
    assert first.rate == "7.00"
    assert first.previous_rate is None


def test_parse_rate_history_dashed_borrowing_column_returns_none() -> None:
    """``Borç Alma`` is dashed on the 1-Week Repo page — the parser
    must round-trip the dash to None rather than mis-coerce to 0."""
    decisions = parse_rate_history(_rate_html())
    assert all(d.borrowing_rate is None for d in decisions)


def test_parse_rate_history_rejects_page_without_midtable() -> None:
    with pytest.raises(TCMBRateHistoryParseError):
        parse_rate_history("<html><body><p>maintenance window</p></body></html>")


def test_parse_rate_history_rejects_table_with_no_rate_rows() -> None:
    empty_table = """
        <html><body>
        <table id="midTable">
          <tbody>
            <tr><td>Tarih</td><td>Borç Alma</td><td>Borç Verme</td></tr>
          </tbody>
        </table>
        </body></html>
    """
    with pytest.raises(TCMBRateHistoryParseError):
        parse_rate_history(empty_table)


def test_parse_rate_history_skips_corrupted_row_without_aborting() -> None:
    """A row with a non-decimal rate must not nuke the whole list —
    the parser logs and walks past it (mirrors the BCB / RBA defensive
    shape)."""
    # Hand-rolled mini fixture: one valid row + one corrupted row.
    mini = """
        <html><body>
        <table id="midTable">
          <tbody>
            <tr><td>Tarih</td><td>Borç Alma</td><td>Borç Verme</td></tr>
            <tr><td>20.05.2010</td><td>-</td><td>7.00</td></tr>
            <tr><td>17.12.2010</td><td>-</td><td>BUSTED</td></tr>
            <tr><td>21.01.2011</td><td>-</td><td>6.25</td></tr>
          </tbody>
        </table>
        </body></html>
    """
    decisions = parse_rate_history(mini)
    dates = sorted(d.effective_date for d in decisions)
    assert date(2010, 5, 20) in dates
    assert date(2011, 1, 21) in dates
    # The corrupted 17.12.2010 row is skipped.
    assert date(2010, 12, 17) not in dates


# ── projection ────────────────────────────────────────────────────


def test_decision_to_records_anchors_event_on_effective_date() -> None:
    decisions = parse_rate_history(_rate_html())
    latest = decisions[0]
    raw, event = decision_to_records(
        latest, snapshot_epoch_ms=1_700_000_000_000,
    )
    # Announcement was 2026-01-23; Türkiye sits at UTC+3 year-round
    # since 2016, so 14:00 TRT == 11:00 UTC.
    assert event.event_time_utc == "2026-01-23T11:00:00+00:00"
    assert event.reference_date == "2026-01-23"
    assert event.actual == "37.00"
    assert event.previous == "38.00"
    assert event.country_code == "TR"
    assert event.currency == "TRY"
    assert event.title == "TCMB Interest Rate Decision"
    assert event.source_url == TCMB_RATE_HISTORY_URL


def test_decision_to_records_handles_pre_2017_dst_backfill() -> None:
    """Türkiye observed DST (UTC+2 winter / UTC+3 summer) until
    September 2016. ``Europe/Istanbul`` resolves the historical 2010-
    2016 wall-clock offsets correctly."""
    decisions = parse_rate_history(_rate_html())
    # Pick a known winter (DST off) decision and a known summer (DST
    # on) decision from the corpus.
    winter = next(d for d in decisions if d.effective_date == date(2014, 1, 29))
    summer = next(d for d in decisions if d.effective_date == date(2010, 5, 20))

    _, winter_ev = decision_to_records(
        winter, snapshot_epoch_ms=1_700_000_000_000,
    )
    _, summer_ev = decision_to_records(
        summer, snapshot_epoch_ms=1_700_000_000_000,
    )
    # Winter wall-clock 14:00 in 2014 maps to UTC+2 → 12:00 UTC.
    assert winter_ev.event_time_utc == "2014-01-29T12:00:00+00:00"
    # Summer wall-clock 14:00 in 2010 maps to UTC+3 → 11:00 UTC.
    assert summer_ev.event_time_utc == "2010-05-20T11:00:00+00:00"


def test_decision_payload_includes_borrowing_column_verbatim() -> None:
    decisions = parse_rate_history(_rate_html())
    latest = decisions[0]
    raw, _event = decision_to_records(
        latest, snapshot_epoch_ms=1_700_000_000_000,
    )
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "tcmb_rate_decision"
    assert payload["rate"] == "37.00"
    assert payload["borrowing_rate"] is None  # dashed on this surface


# ── fetcher integration ──────────────────────────────────────────


def test_fetch_tcmb_calendar_dry_run_returns_plan(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_tcmb_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["TCMB_RATE"]
    assert summary.events_upserted == 0


def test_fetch_tcmb_calendar_writes_events_for_each_change(store) -> None:
    html = _rate_html()
    with store._connection(commit=True) as conn:
        summary = fetch_tcmb_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )
    assert summary.fetch_error is None
    # The 2026-04-27 fixture has 57 rate-change rows (verified at
    # capture time). The fetcher writes one event per row.
    assert summary.decisions_parsed == 57
    assert summary.events_upserted == 57
    assert summary.rows_raw_inserted == 57

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == 57


def test_fetch_tcmb_calendar_idempotent_on_repeat(store) -> None:
    html = _rate_html()
    with store._connection(commit=True) as conn:
        first = fetch_tcmb_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )
        second = fetch_tcmb_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            html_fetcher=lambda: html,
        )
    assert first.events_upserted == 57
    # Raw rows collapse to zero on the second pass; event count is
    # the same upsert hit count, table cardinality stays at 57.
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted
    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert total[0] == 57


def test_fetch_tcmb_calendar_records_fetch_error_on_outage(store) -> None:
    import requests

    def broken() -> str:
        raise requests.exceptions.ConnectionError("simulated TCMB outage")

    with store._connection(commit=True) as conn:
        summary = fetch_tcmb_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_tcmb_calendar_records_parse_error_on_blank_page(store) -> None:
    def blank() -> str:
        return "<html><body>maintenance window</body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_tcmb_calendar(
            conn, dry_run=False, html_fetcher=blank,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ─────────────────────────────────────


def test_tcmb_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "tcmb" in ALL_CONNECTORS
    assert "tcmb" in ALL_VALUE_SIDE_CONNECTORS


def test_tcmb_parity_whitelist_empty_in_p1() -> None:
    """TCMB stays out of the parity whitelist in P1 — same deferral
    pattern as the BoC Valet connector. Two reasons: (a) the
    rate-history page's ``Tarih`` column is the rate's effective
    date, off-by-one from TE's announcement-date convention, and
    (b) coverage is change-only, so TE's hold-meeting rows have no
    agency counterpart. The P2 per-meeting press-release scrape will
    give us authoritative announcement dates AND hold coverage."""
    from ingestion.calendar.agency_registry import AGENCIES

    tcmb_decl = next(a for a in AGENCIES if a.agency_id == "TCMB")
    assert tcmb_decl.indicators == frozenset()
