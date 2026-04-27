"""Mocked tests for the BoC calendar connector (issue #52 P1).

Fixture captured live on 2026-04-27 from
``https://www.bankofcanada.ca/valet/observations/V39079/json?start_date=2024-01-01``.
No real HTTP in CI — every test injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.boc_api import (
    BoCRateDecision,
    BoCValetParseError,
    INDICATOR_REGISTRY,
    decision_to_records,
    fetch_boc_calendar,
    parse_overnight_rate_observations,
)
from ingestion.calendar.boc_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "boc_valet"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _valet_json() -> str:
    return (FIXTURE_DIR / "overnight_rate_v39079.json").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_overnight_rate_observations_returns_recent_changes() -> None:
    decisions = parse_overnight_rate_observations(_valet_json())
    # Live capture spans 9 rate-change days from June 2024 through
    # October 2025; assert structure (most-recent-first) and a couple
    # of well-known anchor decisions. The Valet ``d`` is the *effective*
    # day; BoC's announcement was the prior business day (09:45 ET).
    # Both dates are exposed on the dataclass.
    assert len(decisions) == 9
    assert decisions[0].effective_date == date(2025, 10, 30)
    assert decisions[0].announcement_date == date(2025, 10, 29)
    assert decisions[0].previous_rate == "2.50"
    assert decisions[0].rate == "2.25"
    # Mid-window: V39079 transition 2025-09-18 ⇒ announcement 2025-09-17.
    assert decisions[1].effective_date == date(2025, 9, 18)
    assert decisions[1].announcement_date == date(2025, 9, 17)
    # The last (oldest) decision is the start of the easing cycle.
    assert decisions[-1].effective_date == date(2024, 6, 6)
    assert decisions[-1].announcement_date == date(2024, 6, 5)
    assert decisions[-1].previous_rate == "5.00"
    assert decisions[-1].rate == "4.75"


def test_parse_overnight_rate_observations_only_emits_change_days() -> None:
    """Hold (no-change) days must NOT produce a decision row — the
    daily series shows the same rate before and after a hold, so the
    diff-based detector skips them. The change-only window between
    March 2025 and September 2025 has zero decisions."""
    decisions = parse_overnight_rate_observations(_valet_json())
    march_to_sept = [
        d for d in decisions
        if date(2025, 3, 14) <= d.effective_date <= date(2025, 9, 17)
    ]
    assert march_to_sept == []


def test_parse_overnight_rate_observations_raises_on_missing_observations() -> None:
    with pytest.raises(BoCValetParseError, match="missing 'observations'"):
        parse_overnight_rate_observations('{"terms": {}}')


def test_parse_overnight_rate_observations_raises_on_flat_history() -> None:
    """A payload with observations but no value transitions must be
    surfaced — this is the symptom we'd see if the Valet API drifted
    to returning a single-day window."""
    flat = {
        "observations": [
            {"d": "2026-04-20", "V39079": {"v": "2.25"}},
            {"d": "2026-04-21", "V39079": {"v": "2.25"}},
        ],
    }
    with pytest.raises(BoCValetParseError, match="zero rate-change decisions"):
        parse_overnight_rate_observations(flat)


# ── projection ───────────────────────────────────────────────────


def test_decision_to_records_synthesizes_event_at_et_09_45_edt() -> None:
    decision = BoCRateDecision(
        announcement_date=date(2025, 10, 29),
        effective_date=date(2025, 10, 30),
        rate="2.25",
        previous_rate="2.50",
    )
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "CA"
    assert event_rec.actual == "2.25"
    assert event_rec.previous == "2.50"
    assert event_rec.title == "BoC Interest Rate Decision"
    assert event_rec.currency == "CAD"
    # October 29 sits inside EDT (UTC-4) → 09:45 ET = 13:45 UTC.
    # event_time_utc anchors on announcement day, not Valet's effective day.
    assert event_rec.event_time_utc.startswith("2025-10-29T13:45:00")
    assert event_rec.event_time_precision == "datetime"
    # reference_date follows announcement so parity buckets line up
    # with TE's "BoC Interest Rate Decision" rows (which use the
    # announcement day, matching Bloomberg / Reuters convention).
    assert event_rec.reference_date == "2025-10-29"
    # provider_event_id stable across re-projection.
    _, event_rec_again = decision_to_records(
        decision, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_decision_to_records_handles_est_winter_meeting() -> None:
    decision = BoCRateDecision(
        announcement_date=date(2025, 1, 29),
        effective_date=date(2025, 1, 30),
        rate="3.00",
        previous_rate="3.25",
    )
    _, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    # January lives in EST (UTC-5) → 09:45 ET = 14:45 UTC.
    assert event_rec.event_time_utc.startswith("2025-01-29T14:45:00")
    assert event_rec.reference_date == "2025-01-29"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_boc_calendar_writes_one_event_per_change(
    store: SQLiteEngineStore,
) -> None:
    payload = _valet_json()

    with store._connection(commit=True) as conn:
        summary = fetch_boc_calendar(
            conn,
            dry_run=False,
            json_fetcher=lambda: payload,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.decisions_parsed == 9
    assert summary.events_upserted == summary.decisions_parsed


def test_fetch_boc_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boc_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOC_RATE"]


def test_fetch_boc_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise BoCValetParseError("zero rate-change decisions")

    with store._connection(commit=True) as conn:
        summary = fetch_boc_calendar(
            conn, dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_boc_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "boc" in ALL_CONNECTORS
    assert "boc" in ALL_VALUE_SIDE_CONNECTORS


def test_boc_agency_attribution_owns_canadian_rate() -> None:
    """BoC owns provider attribution for Canadian rate decisions, but
    the parity whitelist is intentionally empty — Valet only exposes
    *change* days, while TE carries every scheduled announcement
    (including holds), so registering the indicator before the BoC
    fixed-announcement-dates source ships would flag every hold day
    as a missing-release anomaly. Same deferral pattern as the BoE
    Bank-Rate page."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    boc = provider_to_agency("boc")
    assert boc is not None and boc.agency_id == "BOC"
    assert boc.indicators == frozenset()
    assert agency_for("CA", "BOC_RATE") is None


def test_boc_canonicalize_aliases_resolve_rate_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("BoC Interest Rate Decision") == "BOC_RATE"
    assert canonicalize_indicator(
        "Bank of Canada Interest Rate Decision",
    ) == "BOC_RATE"
    assert canonicalize_indicator("Canada Interest Rate Decision") == "BOC_RATE"
    assert canonicalize_indicator("Target for the Overnight Rate") == "BOC_RATE"
