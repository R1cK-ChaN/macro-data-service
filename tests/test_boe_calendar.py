"""Mocked tests for the BoE calendar connector (issue #51 P1).

Fixture captured live on 2026-04-26 from
``https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp``.
No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.boe_api import (
    BoEMpcDecision,
    BoERatePageParseError,
    INDICATOR_REGISTRY,
    decision_to_records,
    fetch_boe_calendar,
    parse_bank_rate_html,
)
from ingestion.calendar.boe_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "boe_bank_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _bank_rate_html() -> str:
    return (FIXTURE_DIR / "bank_rate_history.html").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_bank_rate_html_returns_recent_decisions() -> None:
    decisions = parse_bank_rate_html(_bank_rate_html())
    assert len(decisions) >= 10
    # Most recent first; live capture ends with the December 2025 cut.
    assert decisions[0].effective_date == date(2025, 12, 18)
    assert decisions[0].rate == "3.75"
    assert decisions[1].effective_date == date(2025, 8, 7)
    assert decisions[1].rate == "4.00"


def test_parse_bank_rate_html_resolves_two_digit_year_pivot() -> None:
    """The table prints two-digit years (``18 Dec 25``); the parser
    must place 70-99 in the 1900s and 00-69 in the 2000s, otherwise
    historical rows from 1975-1999 would slide into 2075-2099."""
    decisions = parse_bank_rate_html(_bank_rate_html())
    years = {d.effective_date.year for d in decisions}
    # The captured fixture spans roughly 1980 to 2025.
    assert all(1970 <= y <= 2099 for y in years)
    assert 2025 in years


def test_parse_bank_rate_html_raises_on_missing_table() -> None:
    with pytest.raises(BoERatePageParseError, match="zero decision rows"):
        parse_bank_rate_html("<html><body><p>nothing here</p></body></html>")


# ── projection ───────────────────────────────────────────────────


def test_decision_to_records_synthesizes_event_at_uk_12_00() -> None:
    decision = BoEMpcDecision(effective_date=date(2025, 12, 18), rate="3.75")
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "UK"
    assert event_rec.actual == "3.75"
    assert event_rec.title == "BoE Interest Rate Decision"
    # December lives in GMT (UTC+0) → 12:00 UK local = 12:00 UTC.
    assert event_rec.event_time_utc.startswith("2025-12-18T12:00:00")
    assert event_rec.event_time_precision == "datetime"
    # provider_event_id stable across re-projection.
    _, event_rec_again = decision_to_records(
        decision, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_decision_to_records_handles_bst_summer_meeting() -> None:
    decision = BoEMpcDecision(effective_date=date(2025, 8, 7), rate="4.00")
    _, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    # August lives in BST (UTC+1) → 12:00 UK local = 11:00 UTC.
    assert event_rec.event_time_utc.startswith("2025-08-07T11:00:00")


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_boe_calendar_writes_one_event_per_decision(
    store: SQLiteEngineStore,
) -> None:
    html = _bank_rate_html()

    with store._connection(commit=True) as conn:
        summary = fetch_boe_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda: html,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.decisions_parsed >= 10
    assert summary.events_upserted == summary.decisions_parsed


def test_fetch_boe_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boe_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOE_RATE"]


def test_fetch_boe_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise BoERatePageParseError("zero decision rows")

    with store._connection(commit=True) as conn:
        summary = fetch_boe_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_boe_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "boe" in ALL_CONNECTORS
    assert "boe" in ALL_VALUE_SIDE_CONNECTORS


def test_boe_agency_attribution_includes_bank_rate() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    boe = provider_to_agency("boe")
    assert boe is not None and boe.agency_id == "BOE"
    assert agency_for("UK", "BOE_RATE") is boe


def test_boe_canonicalize_aliases_resolve_rate_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("BoE Interest Rate Decision") == "BOE_RATE"
    assert canonicalize_indicator("Bank of England Interest Rate Decision") == "BOE_RATE"
    assert canonicalize_indicator("UK Bank Rate") == "BOE_RATE"
