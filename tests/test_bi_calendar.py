"""Mocked tests for the Bank Indonesia calendar connector (issue #92 P1).

The captured fixture
``tests/fixtures/bi_rate/bi_rate.html`` was recorded live on 2026-04-28
from ``bi.go.id/en/statistik/indikator/bi-rate.aspx``. It carries the
most recent 10 BI Board of Governors decisions back to 16 July 2025
(page 1 of the SharePoint-paginated history table).

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.bi_api import (
    INDICATOR_REGISTRY,
    BIRateHistoryParseError,
    decision_to_records,
    fetch_bi_calendar,
    parse_rate_history,
)
from ingestion.calendar.bi_api.parser import (
    BI_BASE_URL,
    BI_RATE_HISTORY_URL,
    PROVIDER,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bi_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _rate_html() -> str:
    return (FIXTURE_DIR / "bi_rate.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_returns_decisions_most_recent_first() -> None:
    decisions = parse_rate_history(_rate_html())
    assert decisions
    for prev, curr in zip(decisions, decisions[1:]):
        assert prev.announcement_date >= curr.announcement_date


def test_parse_recovers_latest_known_rate() -> None:
    """The 22 April 2026 row is the BI Board of Governors decision
    holding the BI-Rate at 4.75% — captured live on 2026-04-28."""
    decisions = parse_rate_history(_rate_html())
    latest = decisions[0]
    assert latest.announcement_date == date(2026, 4, 22)
    assert latest.rate == "4.75"
    # Previous chain steps back to the prior decision (also 4.75% — a
    # streak of holds in the captured corpus).
    assert latest.previous_rate == "4.75"


def test_parse_anchors_oldest_decision_at_2025_07_16() -> None:
    """Page 1 of the BI rate table goes back to 16 July 2025 at
    fixture-capture; the oldest visible row carries no chronological
    predecessor."""
    decisions = parse_rate_history(_rate_html())
    earliest = min(decisions, key=lambda d: d.announcement_date)
    assert earliest.announcement_date == date(2025, 7, 16)
    assert earliest.rate == "5.25"
    assert earliest.previous_rate is None


def test_parse_recovers_change_decisions_in_chain() -> None:
    """The BI Board of Governors cut from 5.25 → 5.00 in September
    2025 — every change must propagate cleanly through ``previous_rate``."""
    decisions = parse_rate_history(_rate_html())
    # Find the row whose rate is 5.00 (the cut from 5.25 → 5.00).
    sept_cut = next(
        (d for d in decisions if d.rate == "5.00"), None,
    )
    assert sept_cut is not None
    assert sept_cut.previous_rate == "5.25"


def test_parse_quantises_rate_to_two_decimals() -> None:
    decisions = parse_rate_history(_rate_html())
    for d in decisions:
        assert "." in d.rate
        assert d.rate == f"{float(d.rate):.2f}"


def test_parse_extracts_press_release_url() -> None:
    """Each BI decision carries a press-release link for the deferred
    P2 minute-scrape audit trail."""
    decisions = parse_rate_history(_rate_html())
    assert all(d.press_release_url for d in decisions)
    assert all(d.press_release_url.startswith(BI_BASE_URL) for d in decisions)


def test_parse_accepts_bytes_payload() -> None:
    raw = (FIXTURE_DIR / "bi_rate.html").read_bytes()
    decisions = parse_rate_history(raw)
    assert decisions


def test_parse_rejects_page_without_rate_table() -> None:
    with pytest.raises(BIRateHistoryParseError):
        parse_rate_history(
            "<html><body><p>maintenance window</p></body></html>",
        )


def test_parse_rejects_table_with_no_data_rows() -> None:
    empty = """
    <html><body>
    <table class="table table-striped table-no-bordered table-lg">
      <thead>
        <tr class="table-header">
          <th>No</th><th>Period</th><th>BI-Rate</th><th>Press Release Link</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
    </body></html>
    """
    with pytest.raises(BIRateHistoryParseError):
        parse_rate_history(empty)


def test_parse_skips_corrupted_row_without_aborting() -> None:
    """A row with a non-decimal rate must not nuke the whole list —
    the parser logs and walks past it (mirrors the BCB / Banxico
    parser defensive shape)."""
    mini = """
    <html><body>
    <table class="table table-striped table-no-bordered table-lg">
      <thead><tr><th>No</th><th>Period</th><th>BI-Rate</th><th>Link</th></tr></thead>
      <tbody>
        <tr><th>1</th><td>22 April 2026</td><td>4.75 %</td>
            <td><a href="/x.aspx">View</a></td></tr>
        <tr><th>2</th><td>17 March 2026</td><td>BUSTED</td>
            <td><a href="/y.aspx">View</a></td></tr>
        <tr><th>3</th><td>19 February 2026</td><td>4.75 %</td>
            <td><a href="/z.aspx">View</a></td></tr>
      </tbody>
    </table>
    </body></html>
    """
    decisions = parse_rate_history(mini)
    dates = sorted(d.announcement_date for d in decisions)
    assert date(2026, 4, 22) in dates
    assert date(2026, 2, 19) in dates
    # The corrupted row is skipped.
    assert date(2026, 3, 17) not in dates


# ── projection ────────────────────────────────────────────────────


def test_decision_to_records_anchors_event_on_announcement_date() -> None:
    """22 April 2026 announcement at 14:00 WIB → 07:00 UTC (UTC+7
    year-round, no DST)."""
    decisions = parse_rate_history(_rate_html())
    latest = decisions[0]
    raw, event = decision_to_records(
        latest, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.event_time_utc == "2026-04-22T07:00:00+00:00"
    assert event.reference_date == "2026-04-22"
    assert event.actual == "4.75"
    assert event.previous == "4.75"
    assert event.country_code == "ID"
    assert event.currency == "IDR"
    assert event.title == "Bank Indonesia Interest Rate Decision"
    assert event.source_url == BI_RATE_HISTORY_URL


def test_decision_payload_includes_press_release_url() -> None:
    decisions = parse_rate_history(_rate_html())
    raw, _event = decision_to_records(
        decisions[0], snapshot_epoch_ms=1_700_000_000_000,
    )
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "bi_rate_decision"
    assert payload["rate"] == "4.75"
    assert "sp_288426.aspx" in payload["press_release_url"]


def test_decision_to_records_uses_indicator_registry_default() -> None:
    spec = INDICATOR_REGISTRY["BI_RATE"]
    assert spec.country_code == "ID"
    assert spec.unit == "percent"


# ── fetcher integration ──────────────────────────────────────────


def test_fetch_bi_calendar_dry_run_returns_plan(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_bi_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BI_RATE"]
    assert summary.events_upserted == 0


def test_fetch_bi_calendar_writes_event_per_decision(store) -> None:
    html = _rate_html()
    with store._connection(commit=True) as conn:
        summary = fetch_bi_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )
    assert summary.fetch_error is None
    # Captured fixture has 10 decisions (page 1 of the table).
    assert summary.decisions_parsed == 10
    assert summary.events_upserted == 10
    assert summary.rows_raw_inserted == 10

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == 10


def test_fetch_bi_calendar_idempotent_on_repeat(store) -> None:
    html = _rate_html()
    with store._connection(commit=True) as conn:
        first = fetch_bi_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: html,
        )
        second = fetch_bi_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            html_fetcher=lambda: html,
        )
    assert first.events_upserted == 10
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted

    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert total[0] == 10


def test_fetch_bi_calendar_records_fetch_error_on_outage(store) -> None:
    import requests

    def broken() -> str:
        raise requests.exceptions.ConnectionError("simulated BI outage")

    with store._connection(commit=True) as conn:
        summary = fetch_bi_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_bi_calendar_records_parse_error_on_blank_page(store) -> None:
    def blank() -> str:
        return "<html><body>maintenance</body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_bi_calendar(
            conn, dry_run=False, html_fetcher=blank,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ─────────────────────────────────────


def test_bi_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "bank-indonesia" in ALL_CONNECTORS
    assert "bank-indonesia" in ALL_VALUE_SIDE_CONNECTORS


def test_bi_parity_whitelist_contains_id_bi_rate() -> None:
    """BI publishes every meeting (change OR hold) with the new rate
    inline, so the parity whitelist lights up in P1 — RBA / BCB /
    Banxico-style coverage."""
    from ingestion.calendar.agency_registry import AGENCIES

    bi_decl = next(a for a in AGENCIES if a.agency_id == "BANK_INDONESIA")
    assert bi_decl.indicators == frozenset({("ID", "BI_RATE")})
    assert bi_decl.providers == ("bank-indonesia",)


def test_bi_listed_in_parity_official_providers() -> None:
    from ingestion.calendar.parity import OFFICIAL_PROVIDERS
    assert "bank-indonesia" in OFFICIAL_PROVIDERS


def test_canonicalize_routes_te_country_agnostic_title_to_bi_rate() -> None:
    """TE supplies Indonesia rate-decision rows with the country-
    agnostic title ``"Interest Rate Decision"`` which canonicalizes to
    ``FOMC_RATE`` by default. With ``country='ID'`` the parity bucket
    key must remap to ``BI_RATE`` so TE rows and BI rows land in the
    same bucket — without this the daily comparator silently misses
    every Indonesia rate decision."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    # Country-agnostic call (no country) keeps the legacy behaviour.
    assert canonicalize_indicator("Interest Rate Decision") == "FOMC_RATE"
    # Country-aware call routes to the per-country central-bank canonical.
    assert canonicalize_indicator(
        "Interest Rate Decision", country="ID",
    ) == "BI_RATE"
    # US keeps FOMC_RATE — the override only fires for countries with
    # an entry in the rate-override map.
    assert canonicalize_indicator(
        "Interest Rate Decision", country="US",
    ) == "FOMC_RATE"
    # Every value-bearing parity-whitelisted central bank must route
    # cleanly under the override.
    assert canonicalize_indicator(
        "Interest Rate Decision", country="MX",
    ) == "BANXICO_RATE"
    assert canonicalize_indicator(
        "Interest Rate Decision", country="BR",
    ) == "BCB_RATE"
    assert canonicalize_indicator(
        "Interest Rate Decision", country="AU",
    ) == "RBA_RATE"


def test_canonicalize_country_override_does_not_affect_non_rate_titles() -> None:
    """The country override only fires for the ``FOMC_RATE`` canonical
    (the alias for the country-agnostic ``"Interest Rate Decision"``).
    Other titles must keep their existing canonical regardless of
    country — CPI, GDP, etc. already disambiguate via their own
    aliases."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    assert canonicalize_indicator(
        "Consumer Price Index", country="ID",
    ) == "CPI"
    assert canonicalize_indicator(
        "GDP Growth Rate", country="ID",
    ) == "GDP"
