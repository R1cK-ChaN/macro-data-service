"""Mocked tests for the SARB calendar connector (issue #90 P1).

The captured fixture
``tests/fixtures/sarb_rate/repo_rate_history.json`` was recorded live
on 2026-04-28 from
``custom.resbank.co.za/SarbWebApi/WebIndicators/Shared/GetTimeseriesObservations/MRDREPOR``.
It carries 25 historical repo-rate change events back to 2017-07-21.

No real HTTP in CI — every test injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.sarb_api import (
    INDICATOR_REGISTRY,
    SARBRateHistoryParseError,
    decision_to_records,
    fetch_sarb_calendar,
    parse_repo_rate_history,
)
from ingestion.calendar.sarb_api.parser import (
    PROVIDER,
    SARB_PUBLIC_HISTORY_URL,
    SARB_RATE_HISTORY_URL,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sarb_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _rate_payload() -> list[dict]:
    return json.loads(
        (FIXTURE_DIR / "repo_rate_history.json").read_text(encoding="utf-8")
    )


# ── parser ───────────────────────────────────────────────────────


def test_parse_returns_decisions_most_recent_first() -> None:
    decisions = parse_repo_rate_history(_rate_payload())
    assert decisions
    for prev, curr in zip(decisions, decisions[1:]):
        assert prev.effective_date >= curr.effective_date


def test_parse_recovers_latest_known_rate() -> None:
    """The 21 November 2025 row is the effective date of the rate cut to
    6.75% (announced at the 20 Nov 2025 SARB MPC meeting; takes effect
    the following business day) — captured live on 2026-04-28."""
    decisions = parse_repo_rate_history(_rate_payload())
    latest = decisions[0]
    assert latest.effective_date == date(2025, 11, 21)
    assert latest.rate == "6.75"
    # Previous chain steps back to the prior rate (7.00 effective 1 Aug 2025).
    assert latest.previous_rate == "7.00"


def test_parse_anchors_first_decision_at_2017_07_21() -> None:
    decisions = parse_repo_rate_history(_rate_payload())
    earliest = min(decisions, key=lambda d: d.effective_date)
    assert earliest.effective_date == date(2017, 7, 21)
    assert earliest.rate == "6.75"
    assert earliest.previous_rate is None


def test_parse_quantises_rate_to_two_decimals() -> None:
    """SARB returns rates as JSON numbers (``6.75`` not ``"6.75"``).
    The parser rounds the cell through Decimal quantised at 0.01 so
    downstream comparisons stay stable on a canonical 2-decimal string."""
    decisions = parse_repo_rate_history(_rate_payload())
    for decision in decisions:
        assert "." in decision.rate
        assert decision.rate == f"{float(decision.rate):.2f}"


def test_parse_accepts_string_payload() -> None:
    raw_text = (FIXTURE_DIR / "repo_rate_history.json").read_text(encoding="utf-8")
    decisions_from_str = parse_repo_rate_history(raw_text)
    decisions_from_list = parse_repo_rate_history(_rate_payload())
    assert len(decisions_from_str) == len(decisions_from_list)


def test_parse_accepts_bytes_payload() -> None:
    raw_bytes = (FIXTURE_DIR / "repo_rate_history.json").read_bytes()
    decisions = parse_repo_rate_history(raw_bytes)
    assert decisions


def test_parse_rejects_non_array_payload() -> None:
    with pytest.raises(SARBRateHistoryParseError):
        parse_repo_rate_history('{"error": "maintenance"}')


def test_parse_rejects_unparseable_json() -> None:
    with pytest.raises(SARBRateHistoryParseError):
        parse_repo_rate_history("<html>not JSON</html>")


def test_parse_rejects_empty_array() -> None:
    """An empty MRDREPOR response is itself a drift signal — the
    timeseries has been populated since the modern repo regime."""
    with pytest.raises(SARBRateHistoryParseError):
        parse_repo_rate_history("[]")


def test_parse_skips_corrupted_row_without_aborting() -> None:
    """A row with a non-decimal value must not nuke the whole list."""
    mini = [
        {"Period": "2017-07-21T00:00:00", "Value": 6.75},
        {"Period": "2018-03-29T00:00:00", "Value": "BUSTED"},
        {"Period": "2018-11-23T00:00:00", "Value": 6.75},
    ]
    decisions = parse_repo_rate_history(mini)
    dates = sorted(d.effective_date for d in decisions)
    assert date(2017, 7, 21) in dates
    assert date(2018, 11, 23) in dates
    # The corrupted row is skipped.
    assert date(2018, 3, 29) not in dates


def test_parse_skips_row_with_missing_period() -> None:
    mini = [
        {"Period": "2017-07-21T00:00:00", "Value": 6.75},
        {"Value": 6.50},  # missing Period
    ]
    decisions = parse_repo_rate_history(mini)
    assert len(decisions) == 1


def test_parse_accepts_bare_date_period() -> None:
    """Defensive against adjacent SARB timeseries that ship dates as
    ``YYYY-MM-DD`` rather than the full ISO datetime."""
    mini = [{"Period": "2017-07-21", "Value": 6.75}]
    decisions = parse_repo_rate_history(mini)
    assert len(decisions) == 1
    assert decisions[0].effective_date == date(2017, 7, 21)


# ── projection ────────────────────────────────────────────────────


def test_decision_to_records_anchors_event_on_effective_date() -> None:
    decisions = parse_repo_rate_history(_rate_payload())
    latest = decisions[0]
    raw, event = decision_to_records(
        latest, snapshot_epoch_ms=1_700_000_000_000,
    )
    # Effective 2025-11-21; SARB sits at UTC+2 year-round (no DST), so
    # 15:00 SAST == 13:00 UTC.
    assert event.event_time_utc == "2025-11-21T13:00:00+00:00"
    assert event.reference_date == "2025-11-21"
    assert event.actual == "6.75"
    assert event.previous == "7.00"
    assert event.country_code == "ZA"
    assert event.currency == "ZAR"
    assert event.title == "SARB Interest Rate Decision"
    assert event.source_url == SARB_PUBLIC_HISTORY_URL


def test_decision_payload_includes_rate_and_ts_code() -> None:
    decisions = parse_repo_rate_history(_rate_payload())
    raw, _event = decision_to_records(
        decisions[0], snapshot_epoch_ms=1_700_000_000_000,
    )
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "sarb_rate_decision"
    assert payload["rate"] == "6.75"
    assert payload["ts_code"] == "MRDREPOR"
    assert payload["source_url"] == SARB_RATE_HISTORY_URL


def test_decision_to_records_uses_indicator_registry_default() -> None:
    spec = INDICATOR_REGISTRY["SARB_RATE"]
    assert spec.country_code == "ZA"
    assert spec.unit == "percent"


# ── fetcher integration ──────────────────────────────────────────


def test_fetch_sarb_calendar_dry_run_returns_plan(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_sarb_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["SARB_RATE"]
    assert summary.events_upserted == 0


def test_fetch_sarb_calendar_writes_event_per_change(store) -> None:
    payload = _rate_payload()
    with store._connection(commit=True) as conn:
        summary = fetch_sarb_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda: payload,
        )
    assert summary.fetch_error is None
    # Captured fixture has 25 rate-change rows.
    assert summary.decisions_parsed == 25
    assert summary.events_upserted == 25
    assert summary.rows_raw_inserted == 25

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == 25


def test_fetch_sarb_calendar_idempotent_on_repeat(store) -> None:
    payload = _rate_payload()
    with store._connection(commit=True) as conn:
        first = fetch_sarb_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda: payload,
        )
        second = fetch_sarb_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            json_fetcher=lambda: payload,
        )
    assert first.events_upserted == 25
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted
    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert total[0] == 25


def test_fetch_sarb_calendar_records_fetch_error_on_outage(store) -> None:
    import requests

    def broken() -> list[dict]:
        raise requests.exceptions.ConnectionError("simulated SARB outage")

    with store._connection(commit=True) as conn:
        summary = fetch_sarb_calendar(
            conn, dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_sarb_calendar_records_parse_error_on_drift(store) -> None:
    def drifted() -> list[dict]:
        return []  # empty array trips the layout-drift guard

    with store._connection(commit=True) as conn:
        summary = fetch_sarb_calendar(
            conn, dry_run=False, json_fetcher=drifted,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ─────────────────────────────────────


def test_sarb_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "sarb" in ALL_CONNECTORS
    assert "sarb" in ALL_VALUE_SIDE_CONNECTORS


def test_sarb_parity_whitelist_empty_in_p1() -> None:
    """SARB stays out of the parity whitelist in P1 — same deferral
    pattern as the BoC Valet / TCMB connectors. Two reasons: (a) the
    MRDREPOR ``Period`` is the rate's effective date, off-by-one from
    TE's announcement-date convention, and (b) coverage is change-only,
    so TE's hold-meeting rows have no agency counterpart. The P2 MPC-
    statement scrape will give us authoritative announcement dates AND
    hold coverage."""
    from ingestion.calendar.agency_registry import AGENCIES

    sarb_decl = next(a for a in AGENCIES if a.agency_id == "SARB")
    assert sarb_decl.indicators == frozenset()
    assert sarb_decl.providers == ("sarb",)


def test_sarb_listed_in_parity_official_providers() -> None:
    from ingestion.calendar.parity import OFFICIAL_PROVIDERS
    assert "sarb" in OFFICIAL_PROVIDERS
