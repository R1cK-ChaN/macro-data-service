"""Mocked tests for the ONS calendar connector (issue #51 P1).

Fixtures captured live on 2026-04-26 from the ONS public
timeseries JSON endpoints (CPI ``d7g7`` / unemployment ``mgsx`` /
GDP ``ihyq``). No real HTTP in CI — every test injects the
``json_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.ons_api import (
    INDICATOR_REGISTRY,
    ONSIndicatorSpec,
    ONSTimeseriesParseError,
    fetch_ons_calendar,
    parse_timeseries_json,
    value_observation_to_records,
)
from ingestion.calendar.ons_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ons_timeseries"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_cpi_latest_observation_extracted_from_real_json() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    obs = parse_timeseries_json(_fixture("cpi_d7g7.json"), spec=spec)
    assert obs.indicator == "CPI"
    assert obs.value == "3.3"
    assert obs.reference_date == "2026-03-01"
    assert obs.reference_label == "March 2026"
    # updateDate is ``2026-04-21T23:00:00.000Z`` → ``2026-04-22`` UK local.
    assert obs.release_date == "2026-04-22"
    assert "/timeseries/d7g7/mm23" in obs.source_url


def test_unemployment_latest_observation_extracted() -> None:
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    obs = parse_timeseries_json(_fixture("unemployment_mgsx.json"), spec=spec)
    assert obs.value == "4.9"
    assert obs.reference_date == "2026-01-01"
    assert obs.release_date == "2026-04-21"
    assert obs.indicator == "UNEMPLOYMENT_RATE"


def test_gdp_quarterly_observation_extracted() -> None:
    spec = INDICATOR_REGISTRY["GDP"]
    obs = parse_timeseries_json(_fixture("gdp_ihyq.json"), spec=spec)
    assert obs.value == "0.1"
    # Q4 2025 → first day October 2025.
    assert obs.reference_date == "2025-10-01"
    assert obs.reference_label == "Q4 2025"
    # 2026-03-30T23:00:00Z → 2026-03-31 BST (DST already started Mar 29).
    assert obs.release_date == "2026-03-31"


def test_parser_raises_on_empty_payload() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    with pytest.raises(ONSTimeseriesParseError):
        parse_timeseries_json('{"months": []}', spec=spec)


def test_parser_skips_value_blank_observations() -> None:
    """Mid-history rows with no value (forecasts, gap-filled holes)
    must not break the parser — the latest *populated* observation
    is the headline."""
    payload = {
        "months": [
            {"date": "2026 JAN", "value": "3.0", "updateDate": "2026-02-18T00:00:00Z"},
            {"date": "2026 FEB", "value": "3.0", "updateDate": "2026-03-20T00:00:00Z"},
            # Gap-filled placeholder — must be skipped.
            {"date": "2026 MAR", "value": "", "updateDate": "2026-04-21T23:00:00Z"},
        ],
    }
    spec = INDICATOR_REGISTRY["CPI"]
    obs = parse_timeseries_json(payload, spec=spec)
    assert obs.reference_date == "2026-02-01"
    assert obs.value == "3.0"


# ── projection ───────────────────────────────────────────────────


def test_value_observation_to_records_synthesizes_event_at_uk_07_00() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    obs = parse_timeseries_json(_fixture("cpi_d7g7.json"), spec=spec)
    raw_rec, event_rec = value_observation_to_records(
        obs, snapshot_epoch_ms=1_800_000_000_000, spec=spec,
    )
    assert event_rec.country_code == "UK"
    assert event_rec.actual == "3.3"
    assert event_rec.title == "UK Inflation Rate"
    # April 22 sits inside BST (UTC+1) → 07:00 UK local = 06:00 UTC.
    assert event_rec.event_time_utc.startswith("2026-04-22T06:00:00")
    assert event_rec.event_time_precision == "datetime"
    # provider_event_id stable across re-fetch — keyed on reference_date.
    obs_again = parse_timeseries_json(_fixture("cpi_d7g7.json"), spec=spec)
    _, event_rec_again = value_observation_to_records(
        obs_again, snapshot_epoch_ms=2_000_000_000_000, spec=spec,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_ons_calendar_writes_event_per_indicator(
    store: SQLiteEngineStore,
) -> None:
    payloads = {
        "CPI": _fixture("cpi_d7g7.json"),
        "UNEMPLOYMENT_RATE": _fixture("unemployment_mgsx.json"),
        "GDP": _fixture("gdp_ihyq.json"),
    }

    def fetcher(spec: ONSIndicatorSpec) -> str:
        return payloads[spec.indicator]

    with store._connection(commit=True) as conn:
        summary = fetch_ons_calendar(
            conn,
            dry_run=False,
            json_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )

    assert summary.fetch_error is None
    assert summary.observations_seen == 3
    assert summary.events_upserted == 3
    assert set(summary.indicators_ok) == {"CPI", "UNEMPLOYMENT_RATE", "GDP"}
    assert summary.series_failed == []


def test_fetch_ons_calendar_isolates_per_indicator_failure(
    store: SQLiteEngineStore,
) -> None:
    """A 503 on one indicator must not roll back the others."""
    cpi = _fixture("cpi_d7g7.json")

    def fetcher(spec: ONSIndicatorSpec) -> str:
        if spec.indicator == "GDP":
            raise RuntimeError("simulated 503")
        return cpi if spec.indicator == "CPI" else _fixture("unemployment_mgsx.json")

    with store._connection(commit=True) as conn:
        summary = fetch_ons_calendar(
            conn, dry_run=False, json_fetcher=fetcher,
        )
    failed_keys = {k for k, _ in summary.series_failed}
    assert "GDP" in failed_keys
    assert summary.events_upserted == 2
    assert set(summary.indicators_ok) == {"CPI", "UNEMPLOYMENT_RATE"}


def test_fetch_ons_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_ons_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())


# ── scheduler + agency wiring ───────────────────────────────────


def test_ons_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "ons" in ALL_CONNECTORS
    assert "ons" in ALL_VALUE_SIDE_CONNECTORS


def test_ons_agency_attribution_includes_uk_indicators() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    ons = provider_to_agency("ons")
    assert ons is not None and ons.agency_id == "ONS"
    assert agency_for("UK", "CPI") is ons
    assert agency_for("UK", "UNEMPLOYMENT_RATE") is ons
    assert agency_for("UK", "GDP") is ons


def test_ons_canonicalize_aliases_resolve_uk_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("United Kingdom Inflation Rate") == "CPI"
    assert canonicalize_indicator("United Kingdom Unemployment Rate") == "UNEMPLOYMENT_RATE"
    assert canonicalize_indicator("United Kingdom GDP Growth Rate") == "GDP"
