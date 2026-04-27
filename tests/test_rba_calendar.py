"""Mocked tests for the RBA calendar connector (issue #53 P1).

Fixture captured live on 2026-04-27 from
``https://www.rba.gov.au/statistics/cash-rate/`` — the full MPB
decisions table. No real HTTP in CI — every test injects the
``html_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.rba_api import (
    INDICATOR_REGISTRY,
    RBARateDecision,
    RBACashRateParseError,
    decision_to_records,
    fetch_rba_calendar,
    parse_cash_rate_table,
)
from ingestion.calendar.rba_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "rba_cash_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _cash_rate_html() -> str:
    return (FIXTURE_DIR / "cash_rate_table.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_cash_rate_table_returns_recent_decisions_first() -> None:
    decisions = parse_cash_rate_table(_cash_rate_html())
    # Live capture covers every MPB decision back to August 1990 (~384
    # rows). Assert the most-recent few against published RBA history.
    # Effective date 18 Mar 2026 = announcement Tue 17 Mar 2026 14:30 AEDT;
    # the parser pulls the announcement date from the minutes URL embedded
    # in the row's Related Documents cell.
    assert decisions[0].announcement_date == date(2026, 3, 17)
    assert decisions[0].effective_date == date(2026, 3, 18)
    assert decisions[0].rate == "4.10"
    assert decisions[0].change == "+0.25"
    assert decisions[0].previous_rate == "3.85"
    # Statement / minutes URLs absolutise against the RBA host.
    assert decisions[0].statement_url == (
        "https://www.rba.gov.au/media-releases/2026/mr-26-08.html"
    )
    # Hold rows are present (unlike BoC's change-only Valet pattern).
    holds = [d for d in decisions if d.change == "0.00"]
    assert len(holds) > 0


def test_parse_cash_rate_table_includes_hold_decisions() -> None:
    """The RBA table publishes hold (no-change) rows in the same shape
    as moves, so the parity whitelist can light up in P1 without a
    separate fixed-announcement-dates feed (the BoC P2 follow-up)."""
    decisions = parse_cash_rate_table(_cash_rate_html())
    # 10 December 2025 effective date = announcement Tue 9 Dec 2025
    # (per the minutes URL embedded in the Related Documents cell).
    dec_9 = [d for d in decisions if d.announcement_date == date(2025, 12, 9)]
    assert len(dec_9) == 1
    assert dec_9[0].effective_date == date(2025, 12, 10)
    assert dec_9[0].change == "0.00"
    assert dec_9[0].rate == "3.60"
    assert dec_9[0].previous_rate == "3.60"


def test_parse_cash_rate_table_falls_back_to_previous_business_day() -> None:
    """When the row has no minutes URL (oldest 1990–early 1991 entries),
    the parser anchors ``announcement_date`` on the previous business day
    relative to the effective date — mirrors RBA's documented next-
    business-day convention without needing the embedded link."""
    # Synthetic row: 4-cell row with empty Related Documents cell, plus a
    # 3-cell row with no Related Documents column at all (matches the
    # earliest historical entries).
    html = (
        '<html><body><table id="datatable"><tbody>'
        # Effective Wed 18 Mar 2026 with no links → announcement Tue 17 Mar.
        '<tr><th scope="row">18 Mar 2026</th><td>+0.25</td><td>4.10</td>'
        '<td></td></tr>'
        # 3-cell row with no Related Documents cell (1990 shape) —
        # effective Wed 5 Sep 1990 → announcement Tue 4 Sep 1990.
        '<tr><th scope="row">5 Sep 1990</th><td>+1.00</td><td>14.00</td></tr>'
        '</tbody></table></body></html>'
    )
    decisions = parse_cash_rate_table(html)
    by_eff = {d.effective_date: d for d in decisions}
    assert by_eff[date(2026, 3, 18)].announcement_date == date(2026, 3, 17)
    assert by_eff[date(1990, 9, 5)].announcement_date == date(1990, 9, 4)


def test_parse_cash_rate_table_skips_post_weekend_to_friday() -> None:
    """An effective Monday must roll the announcement back to the prior
    Friday — Tue/Wed/Thu/Fri all roll back one calendar day, only Mon
    needs the 3-day skip."""
    html = (
        '<html><body><table id="datatable"><tbody>'
        # Effective Mon 6 Apr 2026 → announcement Fri 3 Apr 2026.
        '<tr><th scope="row">6 Apr 2026</th><td>0.00</td><td>4.10</td>'
        '<td></td></tr>'
        '</tbody></table></body></html>'
    )
    decisions = parse_cash_rate_table(html)
    assert decisions[0].announcement_date == date(2026, 4, 3)


def test_parse_cash_rate_table_derives_previous_rate_from_chronology() -> None:
    """``previous_rate`` is the rate from the prior decision in
    chronological order — for the oldest row in the page it is None."""
    decisions = parse_cash_rate_table(_cash_rate_html())
    oldest = decisions[-1]
    assert oldest.previous_rate is None


def test_parse_cash_rate_table_raises_on_missing_table() -> None:
    with pytest.raises(RBACashRateParseError, match="missing 'datatable'"):
        parse_cash_rate_table("<html><body><p>no table</p></body></html>")


def test_parse_cash_rate_table_raises_on_empty_tbody() -> None:
    html = (
        '<html><body>'
        '<table id="datatable"><tbody></tbody></table>'
        '</body></html>'
    )
    with pytest.raises(RBACashRateParseError, match="zero decisions"):
        parse_cash_rate_table(html)


def test_parse_cash_rate_table_skips_malformed_rows() -> None:
    """A truncated row with a malformed date / rate must not nuke the
    whole list — skip and keep walking. Mirrors the BoC parser's
    defensive shape so a single bad cell can't take down a daily sweep."""
    html = (
        '<html><body><table id="datatable"><tbody>'
        '<tr><th scope="row">18 Mar 2026</th><td>+0.25</td><td>4.10</td>'
        '<td><a href="/x">Statement</a></td></tr>'
        '<tr><th scope="row">garbage row</th><td>nope</td><td>—</td><td></td></tr>'
        '<tr><th scope="row">4 Feb 2026</th><td>+0.25</td><td>3.85</td>'
        '<td><a href="/y">Statement</a></td></tr>'
        '</tbody></table></body></html>'
    )
    decisions = parse_cash_rate_table(html)
    assert {d.rate for d in decisions} == {"4.10", "3.85"}


# ── projection ───────────────────────────────────────────────────


def test_decision_to_records_synthesizes_event_at_aedt_for_summer_meeting() -> None:
    decision = RBARateDecision(
        announcement_date=date(2026, 3, 17),
        effective_date=date(2026, 3, 18),
        rate="4.10",
        change="+0.25",
        previous_rate="3.85",
        statement_url="https://www.rba.gov.au/media-releases/2026/mr-26-08.html",
        minutes_url=None,
    )
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "AU"
    assert event_rec.actual == "4.10"
    assert event_rec.previous == "3.85"
    assert event_rec.title == "RBA Interest Rate Decision"
    assert event_rec.currency == "AUD"
    # March 17 sits inside AEDT (UTC+11) → 14:30 AEDT = 03:30 UTC.
    # event_time_utc anchors on announcement day, not the table's
    # next-business-day effective date, so parity buckets line up
    # with TE / Bloomberg / Reuters convention.
    assert event_rec.event_time_utc.startswith("2026-03-17T03:30:00")
    assert event_rec.event_time_precision == "datetime"
    assert event_rec.reference_date == "2026-03-17"
    # Source URL prefers the official statement when present, falling
    # back to the cash-rate page only when the row carries no anchor.
    assert event_rec.source_url == decision.statement_url
    # provider_event_id stable across re-projection.
    _, event_rec_again = decision_to_records(
        decision, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_decision_to_records_handles_aest_winter_meeting() -> None:
    """August 2025 sits inside AEST (UTC+10) → 14:30 AEST = 04:30 UTC."""
    decision = RBARateDecision(
        announcement_date=date(2025, 8, 12),
        effective_date=date(2025, 8, 13),
        rate="3.60",
        change="-0.25",
        previous_rate="3.85",
        statement_url=None,
        minutes_url=None,
    )
    _, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.event_time_utc.startswith("2025-08-12T04:30:00")
    # Without a statement URL on the row, the projector falls back
    # to the cash-rate page so the source link remains usable.
    from ingestion.calendar.rba_api import RBA_CASH_RATE_URL
    assert event_rec.source_url == RBA_CASH_RATE_URL


def test_decision_to_records_emits_hold_with_change_zero() -> None:
    """Hold decisions ship as events with ``actual = previous`` and
    ``change = "0.00"``. The parity whitelist depends on this being
    the same shape as a move row so TE's hold rows match."""
    decision = RBARateDecision(
        announcement_date=date(2025, 12, 9),
        effective_date=date(2025, 12, 10),
        rate="3.60",
        change="0.00",
        previous_rate="3.60",
        statement_url=None,
        minutes_url=None,
    )
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.actual == "3.60"
    assert event_rec.previous == "3.60"
    # Audit payload preserves both dates so a downstream consumer can
    # see the full announce-vs-effective split if needed.
    import json as _json
    payload = _json.loads(raw_rec.payload_json)
    assert payload["announcement_date"] == "2025-12-09"
    assert payload["effective_date"] == "2025-12-10"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_rba_calendar_writes_one_event_per_decision(
    store: SQLiteEngineStore,
) -> None:
    payload = _cash_rate_html()
    with store._connection(commit=True) as conn:
        summary = fetch_rba_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda: payload,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.decisions_parsed > 100
    assert summary.events_upserted == summary.decisions_parsed


def test_fetch_rba_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_rba_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["RBA_RATE"]


def test_fetch_rba_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RBACashRateParseError("zero decisions")

    with store._connection(commit=True) as conn:
        summary = fetch_rba_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_rba_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "rba" in ALL_CONNECTORS
    assert "rba" in ALL_VALUE_SIDE_CONNECTORS


def test_rba_agency_attribution_includes_rba_rate() -> None:
    """RBA owns ``(AU, RBA_RATE)`` in the parity whitelist — unlike
    the BoC pattern, the cash-rate page publishes hold decisions in
    the same row format as moves, so the daily comparator can rely
    on matched rows for every scheduled MPB announcement."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    rba = provider_to_agency("rba")
    assert rba is not None and rba.agency_id == "RBA"
    assert agency_for("AU", "RBA_RATE") is rba


def test_rba_canonicalize_aliases_resolve_rate_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("RBA Interest Rate Decision") == "RBA_RATE"
    assert canonicalize_indicator(
        "Reserve Bank of Australia Interest Rate Decision",
    ) == "RBA_RATE"
    assert canonicalize_indicator("Australia Interest Rate Decision") == "RBA_RATE"
    assert canonicalize_indicator("RBA Cash Rate Target") == "RBA_RATE"
