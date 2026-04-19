"""Tests for the VIX regime classifier + obs_enrichment sidecar
(issue #3 item 5)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.timeseries.regimes import (
    VIX_LOW_THRESHOLD,
    VIX_STRESSED_THRESHOLD,
    classify_vix_regime,
)
from ingestion.types import RawObservation, RawSeries
from storage.sqlite import IndicatorObservationRecord, SQLiteEngineStore


# ── classify_vix_regime ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "value, expected",
    [
        (0.0, "low"),
        (10.0, "low"),
        (VIX_LOW_THRESHOLD - 0.001, "low"),
        (VIX_LOW_THRESHOLD, "elevated"),
        (20.0, "elevated"),
        (VIX_STRESSED_THRESHOLD - 0.001, "elevated"),
        (VIX_STRESSED_THRESHOLD, "stressed"),
        (40.0, "stressed"),
        (80.0, "stressed"),
    ],
)
def test_vix_regime_thresholds(value: float, expected: str) -> None:
    assert classify_vix_regime(value) == expected


@pytest.mark.parametrize(
    "value",
    [None, float("nan"), float("inf"), -float("inf"), "not a number", "15"[:0]],
)
def test_vix_regime_invalid_inputs_return_none(value) -> None:
    assert classify_vix_regime(value) is None


def test_vix_regime_accepts_string_number() -> None:
    # Some stores coerce float → str on read; classifier should still work.
    assert classify_vix_regime("18.5") == "elevated"


# ── obs_enrichment CRUD ─────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_obs_enrichment_schema_created(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        row = c.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='obs_enrichment'"
        ).fetchone()
    assert row is not None


def test_set_and_get_obs_enrichment_round_trip(store: SQLiteEngineStore) -> None:
    store.set_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-19",
        key="regime", value="elevated",
    )
    got = store.get_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-19", key="regime",
    )
    assert got == "elevated"
    # Missing row returns None
    assert store.get_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-20", key="regime",
    ) is None


def test_set_obs_enrichment_is_upsert(store: SQLiteEngineStore) -> None:
    store.set_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-19",
        key="regime", value="low",
    )
    store.set_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-19",
        key="regime", value="elevated",
    )
    assert store.get_obs_enrichment(
        obs_family_id="us.markets.vix", date="2026-04-19", key="regime",
    ) == "elevated"


def test_list_obs_enrichment_orders_by_date_desc(store: SQLiteEngineStore) -> None:
    for date, label in [
        ("2026-04-17", "low"),
        ("2026-04-18", "elevated"),
        ("2026-04-19", "stressed"),
    ]:
        store.set_obs_enrichment(
            obs_family_id="us.markets.vix", date=date,
            key="regime", value=label,
        )
    rows = store.list_obs_enrichment_for_family("us.markets.vix")
    assert [r[0] for r in rows] == ["2026-04-19", "2026-04-18", "2026-04-17"]
    assert [r[2] for r in rows] == ["stressed", "elevated", "low"]


def test_list_obs_enrichment_filter_by_key(store: SQLiteEngineStore) -> None:
    store.set_obs_enrichment(obs_family_id="us.markets.vix",
                             date="2026-04-19", key="regime", value="elevated")
    store.set_obs_enrichment(obs_family_id="us.markets.vix",
                             date="2026-04-19", key="percentile", value="62")
    regime_only = store.list_obs_enrichment_for_family(
        "us.markets.vix", key="regime",
    )
    assert [r[1] for r in regime_only] == ["regime"]


# ── refresh_vix_regime (end-to-end) ─────────────────────────────────────


def _seed_vix(store: SQLiteEngineStore, values: list[tuple[str, float]]) -> None:
    for date, value in values:
        store.upsert_indicator_observation(
            IndicatorObservationRecord(
                series_id="VIXCLS",
                source="fred",
                date=date,
                value=value,
                metadata={},
            )
        )


def test_refresh_vix_regime_classifies_every_observation(
    store: SQLiteEngineStore,
) -> None:
    _seed_vix(store, [
        ("2026-04-15", 10.0),   # low
        ("2026-04-16", 20.0),   # elevated
        ("2026-04-17", 45.0),   # stressed
    ])

    count = store.refresh_vix_regime()
    assert count == 3

    rows = store.list_obs_enrichment_for_family("us.markets.vix", key="regime")
    got = {d: v for d, _k, v in rows}
    assert got == {
        "2026-04-15": "low",
        "2026-04-16": "elevated",
        "2026-04-17": "stressed",
    }


def test_refresh_vix_regime_empty_store_writes_nothing(
    store: SQLiteEngineStore,
) -> None:
    """Guard for the 'no data yet' branch: refresh against an empty
    indicators table returns 0 and leaves obs_enrichment untouched."""
    assert store.refresh_vix_regime() == 0
    assert store.list_obs_enrichment_for_family("us.markets.vix") == []


def test_refresh_vix_regime_is_idempotent_on_second_call(
    store: SQLiteEngineStore,
) -> None:
    _seed_vix(store, [("2026-04-15", 12.0), ("2026-04-16", 30.0)])
    assert store.refresh_vix_regime() == 2
    # Second invocation overwrites the same (family, date, key) rows; the
    # write count stays 2 but no duplicates accumulate.
    assert store.refresh_vix_regime() == 2
    rows = store.list_obs_enrichment_for_family("us.markets.vix", key="regime")
    assert len(rows) == 2


# ── concept / subject bridge ────────────────────────────────────────────


def test_vix_us_bridges_to_vol_vix_subject(store: SQLiteEngineStore) -> None:
    """Regression glue: VIX_US concept → VIXCLS series → vol.vix subject
    through subject_aliases.fred_series, so /items?subject=vol.vix will
    eventually surface VIX observations alongside documents."""
    from storage.subjects import sync_from_yaml
    store.seed_concept_map()
    sync_from_yaml(store)
    subs = store.resolve_subjects_for_concept("VIX_US")
    assert "vol.vix" in subs


def test_vixcls_registered_in_fred_family_map(store: SQLiteEngineStore) -> None:
    store.seed_obs_sources_and_families()
    fam = store.get_obs_family("us.markets.vix")
    assert fam is not None
    assert fam.source_id == "fred"
    assert fam.provider_series_id == "VIXCLS"
    assert fam.unit == "index"


def test_vixcls_listed_in_fred_macro_series() -> None:
    """Regression for codex P2: FredFetcher.fetch_series reads MACRO_SERIES
    directly, so a series missing from that dict is silently skipped by
    fred_daily / fred_full — leaving VIX_US empty and refresh_vix_regime
    with nothing to classify. Asserting presence keeps the VIX ingestion
    wiring complete end to end."""
    from ingestion.timeseries._config import MACRO_SERIES
    assert "VIXCLS" in MACRO_SERIES
    assert MACRO_SERIES["VIXCLS"]["freq"] == "daily"
