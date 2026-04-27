"""Mocked tests for the StatCan calendar connector (issue #52 P1).

Fixtures captured live on 2026-04-27 from the StatCan WDS endpoint
``getDataFromVectorsAndLatestNPeriods`` for vectors ``41690973``
(CPI all-items index), ``2062815`` (Unemployment Rate), and
``65201210`` (Monthly real GDP). No real HTTP in CI — every test
injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.calendar.statcan_api import (
    INDICATOR_REGISTRY,
    StatCanIndicatorSpec,
    StatCanWDSParseError,
    fetch_statcan_calendar,
    parse_vector_response,
    value_observation_to_records,
)
from ingestion.calendar.statcan_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "statcan_wds"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_cpi_latest_observation_extracted_from_real_payload() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    obs = parse_vector_response(_fixture("cpi_v41690973.json"), spec=spec)
    assert obs.indicator == "CPI"
    assert obs.value == "167.4"
    assert obs.reference_date == "2026-03-01"
    assert obs.reference_label == "March 2026"
    # CPI for March 2026 was published 2026-04-20 at 08:30 ET.
    assert obs.release_date == "2026-04-20"
    assert obs.release_time_local == "08:30"
    assert obs.product_id == 18100004
    assert "pid=18100004" in obs.source_url


def test_unemployment_latest_observation_extracted() -> None:
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    obs = parse_vector_response(_fixture("unemployment_v2062815.json"), spec=spec)
    assert obs.indicator == "UNEMPLOYMENT_RATE"
    assert obs.value == "6.7"
    assert obs.reference_date == "2026-03-01"
    assert obs.release_date == "2026-04-10"
    assert obs.release_time_local == "08:30"


def test_gdp_latest_observation_extracted() -> None:
    spec = INDICATOR_REGISTRY["GDP"]
    obs = parse_vector_response(_fixture("gdp_v65201210.json"), spec=spec)
    assert obs.indicator == "GDP"
    # Monthly real GDP at annual rates is a millions-CAD level —
    # ensure the parser preserves the raw number without scaling.
    assert obs.value == "2342804.0"
    assert obs.reference_date == "2026-01-01"
    assert obs.reference_label == "January 2026"


def test_parser_raises_on_missing_vector_in_response() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    # Build a single-vector payload that doesn't carry CPI's vector id —
    # the parser must surface this rather than silently picking the wrong
    # entry.
    other = json.loads(_fixture("unemployment_v2062815.json"))
    with pytest.raises(StatCanWDSParseError, match="not in response"):
        parse_vector_response(other, spec=spec)


def test_parser_raises_on_failed_status() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    payload = [{"status": "FAILED", "object": {"vectorId": spec.vector_id}}]
    with pytest.raises(StatCanWDSParseError, match="WDS status"):
        parse_vector_response(payload, spec=spec)


def test_parser_raises_on_empty_data_points() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    payload = [{
        "status": "SUCCESS",
        "object": {"vectorId": spec.vector_id, "vectorDataPoint": []},
    }]
    with pytest.raises(StatCanWDSParseError, match="empty vectorDataPoint"):
        parse_vector_response(payload, spec=spec)


def test_parser_skips_value_blank_observations() -> None:
    """Mid-history rows with no ``value`` (suppressed / forecast) must
    not break the parser — the latest *populated* observation is the
    headline."""
    spec = INDICATOR_REGISTRY["CPI"]
    payload = [{
        "status": "SUCCESS",
        "object": {
            "vectorId": spec.vector_id,
            "productId": 18100004,
            "vectorDataPoint": [
                {"refPer": "2026-01-01", "value": 165.0,
                 "releaseTime": "2026-02-17T08:30"},
                {"refPer": "2026-02-01", "value": 166.5,
                 "releaseTime": "2026-03-16T08:30"},
                # Suppressed rolling row — must be skipped.
                {"refPer": "2026-03-01", "value": None,
                 "releaseTime": "2026-04-20T08:30"},
            ],
        },
    }]
    obs = parse_vector_response(payload, spec=spec)
    assert obs.reference_date == "2026-02-01"
    assert obs.value == "166.5"


def test_parser_skips_status_code_or_empty_string_observations() -> None:
    """``statusCode != 0`` marks suppressed / confidential / preliminary
    observations on WDS; an empty-string ``value`` carries the same
    semantic. Either case must fall through to the latest *valid*
    observation rather than publishing the suppressed row."""
    spec = INDICATOR_REGISTRY["CPI"]
    payload = [{
        "status": "SUCCESS",
        "object": {
            "vectorId": spec.vector_id,
            "productId": 18100004,
            "vectorDataPoint": [
                {"refPer": "2026-01-01", "value": 165.0,
                 "statusCode": 0,
                 "releaseTime": "2026-02-17T08:30"},
                {"refPer": "2026-02-01", "value": 166.5,
                 "statusCode": 0,
                 "releaseTime": "2026-03-16T08:30"},
                # Confidential / suppressed — non-zero statusCode.
                {"refPer": "2026-03-01", "value": 0,
                 "statusCode": 4,
                 "releaseTime": "2026-04-20T08:30"},
                # Withdrawn estimate — empty string value.
                {"refPer": "2026-04-01", "value": "",
                 "statusCode": 0,
                 "releaseTime": "2026-05-18T08:30"},
            ],
        },
    }]
    obs = parse_vector_response(payload, spec=spec)
    assert obs.reference_date == "2026-02-01"
    assert obs.value == "166.5"


# ── projection ───────────────────────────────────────────────────


def test_value_observation_to_records_synthesizes_event_at_et_08_30() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    obs = parse_vector_response(_fixture("cpi_v41690973.json"), spec=spec)
    raw_rec, event_rec = value_observation_to_records(
        obs, snapshot_epoch_ms=1_800_000_000_000, spec=spec,
    )
    assert event_rec.country_code == "CA"
    assert event_rec.actual == "167.4"
    assert event_rec.title == "Canada Consumer Price Index"
    assert event_rec.currency == "CAD"
    # April 20 sits inside EDT (UTC-4) → 08:30 ET = 12:30 UTC.
    assert event_rec.event_time_utc.startswith("2026-04-20T12:30:00")
    assert event_rec.event_time_precision == "datetime"
    # provider_event_id stable across re-fetch — keyed on reference_date.
    obs_again = parse_vector_response(_fixture("cpi_v41690973.json"), spec=spec)
    _, event_rec_again = value_observation_to_records(
        obs_again, snapshot_epoch_ms=2_000_000_000_000, spec=spec,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_statcan_calendar_writes_event_per_indicator(
    store: SQLiteEngineStore,
) -> None:
    # Build one combined-batch payload mirroring the real WDS shape:
    # ``getDataFromVectorsAndLatestNPeriods`` returns a single JSON
    # array carrying all requested vectors in one round-trip.
    combined: list = []
    for fname in (
        "cpi_v41690973.json",
        "unemployment_v2062815.json",
        "gdp_v65201210.json",
    ):
        combined.extend(json.loads(_fixture(fname)))

    def fetcher(specs: list[StatCanIndicatorSpec]) -> str:
        return json.dumps(combined)

    with store._connection(commit=True) as conn:
        summary = fetch_statcan_calendar(
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


def test_fetch_statcan_calendar_isolates_per_indicator_failure(
    store: SQLiteEngineStore,
) -> None:
    """A single vector returned with status=FAILED must not roll back the
    others — the connector projects the SUCCESS rows and reports the
    failure on ``series_failed``."""
    cpi = json.loads(_fixture("cpi_v41690973.json"))
    unemp = json.loads(_fixture("unemployment_v2062815.json"))
    failed_gdp = [{
        "status": "FAILED",
        "object": {"vectorId": INDICATOR_REGISTRY["GDP"].vector_id,
                   "vectorDataPoint": []},
    }]
    combined = cpi + unemp + failed_gdp

    def fetcher(specs: list[StatCanIndicatorSpec]) -> str:
        return json.dumps(combined)

    with store._connection(commit=True) as conn:
        summary = fetch_statcan_calendar(
            conn, dry_run=False, json_fetcher=fetcher,
        )
    failed_keys = {k for k, _ in summary.series_failed}
    assert "GDP" in failed_keys
    assert summary.events_upserted == 2
    assert set(summary.indicators_ok) == {"CPI", "UNEMPLOYMENT_RATE"}


def test_fetch_statcan_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    """A whole-batch network failure must mark the run with
    ``fetch_error`` and write zero rows — the per-indicator failure
    list also gets every planned indicator so the next sweep can
    retry."""
    def broken(specs: list[StatCanIndicatorSpec]) -> str:
        raise RuntimeError("simulated 503")

    with store._connection(commit=True) as conn:
        summary = fetch_statcan_calendar(
            conn, dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0
    failed_keys = {k for k, _ in summary.series_failed}
    assert failed_keys == set(INDICATOR_REGISTRY.keys())


def test_fetch_statcan_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_statcan_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())


# ── scheduler + agency wiring ───────────────────────────────────


def test_statcan_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "statcan" in ALL_CONNECTORS
    assert "statcan" in ALL_VALUE_SIDE_CONNECTORS


def test_statcan_agency_attribution_includes_unemployment_rate() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    statcan = provider_to_agency("statcan")
    assert statcan is not None and statcan.agency_id == "STATCAN"
    # Only Unemployment Rate is parity-comparable today (BLS pattern):
    # CPI/GDP store native units that don't match TE's rate display.
    assert agency_for("CA", "UNEMPLOYMENT_RATE") is statcan
    assert agency_for("CA", "CPI") is None
    assert agency_for("CA", "GDP") is None


def test_statcan_canonicalize_aliases_resolve_canada_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("Canada Inflation Rate") == "CPI"
    assert canonicalize_indicator("Canada Unemployment Rate") == "UNEMPLOYMENT_RATE"
    assert canonicalize_indicator("Canada GDP Growth Rate") == "GDP"
    assert canonicalize_indicator("Canadian CPI") == "CPI"
