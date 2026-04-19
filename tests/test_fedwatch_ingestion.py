"""Tests for the FedWatch / rate_probability ingestion path (issue #3 item 3)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers.rateprobability import (
    FedMeetingProbability,
    FedRateProbability,
)
from ingestion.sources import IngestionOrchestrator
from storage.sqlite import SQLiteEngineStore


def _fake_probability() -> FedRateProbability:
    return FedRateProbability(
        as_of="2026-04-19T12:00:00Z",
        current_band="4.25-4.50",
        midpoint=4.375,
        effr=4.33,
        meetings=[
            FedMeetingProbability(
                meeting_date="2026-05-07",
                implied_rate=4.25,
                prob_move_pct=85.0,
                is_cut=True,
                num_moves=1,
                change_bps=-12.5,
            ),
            FedMeetingProbability(
                meeting_date="2026-06-18",
                implied_rate=4.00,
                prob_move_pct=95.0,
                is_cut=True,
                num_moves=2,
                change_bps=-37.5,
            ),
        ],
    )


# ── Observation fetch ────────────────────────────────────────────────────


def _orchestrator_with_prob(
    prob: FedRateProbability, store: SQLiteEngineStore | None = None,
) -> IngestionOrchestrator:
    client = Mock()
    client.fetch_probabilities.return_value = prob
    if store is None:
        # In-memory-style sink for pure-fetch tests; real store is injected
        # in the end-to-end persistence test.
        store = Mock()
    return IngestionOrchestrator(store=store, rate_probability=client)


def test_rate_probability_fetch_emits_midpoint_and_meetings() -> None:
    orch = _orchestrator_with_prob(_fake_probability())
    observations = orch._fetch_rate_probability_observations()
    series_ids = [o.series_id for o in observations]

    assert "FEDWATCH_MIDPOINT" in series_ids
    assert "FEDPROB_2026-05-07" in series_ids
    assert "FEDPROB_2026-06-18" in series_ids

    midpoint = next(o for o in observations if o.series_id == "FEDWATCH_MIDPOINT")
    assert midpoint.value == 4.375
    assert midpoint.date == "2026-04-19"
    assert midpoint.source == "rateprobability"
    assert midpoint.metadata["current_band"] == "4.25-4.50"


def test_rate_probability_fetch_skips_midpoint_when_missing() -> None:
    prob = FedRateProbability(
        as_of="2026-04-19T12:00:00Z",
        current_band="4.25-4.50",
        midpoint=None,  # upstream returned no value
        effr=4.33,
        meetings=_fake_probability().meetings,
    )
    orch = _orchestrator_with_prob(prob)
    series_ids = [o.series_id for o in orch._fetch_rate_probability_observations()]
    assert "FEDWATCH_MIDPOINT" not in series_ids
    # Per-meeting data still flows so the forward curve remains available.
    assert "FEDPROB_2026-05-07" in series_ids


@pytest.mark.parametrize(
    "payload",
    [
        # Missing key entirely
        {"today": {"as_of": "2026-04-19T12:00:00Z", "current band": "4.25-4.50",
                   "most_recent_effr": 4.33, "rows": []}},
        # Explicit null
        {"today": {"as_of": "2026-04-19T12:00:00Z", "current band": "4.25-4.50",
                   "midpoint": None, "most_recent_effr": 4.33, "rows": []}},
        # Non-numeric garbage
        {"today": {"as_of": "2026-04-19T12:00:00Z", "current band": "4.25-4.50",
                   "midpoint": "n/a", "most_recent_effr": 4.33, "rows": []}},
    ],
)
def test_rate_probability_parser_preserves_missing_midpoint_as_none(
    payload: dict,
) -> None:
    """Regression for codex P2: the parser must NOT coerce missing /
    null / non-numeric midpoint values to 0.0, or downstream ingestion
    publishes a synthetic 0% FEDWATCH_US rate for that snapshot."""
    from ingestion.scrapers.rateprobability import RateProbabilityClient
    client = RateProbabilityClient()
    parsed = client._parse(payload)
    assert parsed.midpoint is None


def test_end_to_end_missing_midpoint_does_not_publish_zero(
    store: SQLiteEngineStore,
) -> None:
    """Full ingest with a missing midpoint: no FEDWATCH_MIDPOINT row
    should land, so resolve_indicator returns None instead of 0.0."""
    prob = FedRateProbability(
        as_of="2026-04-19T12:00:00Z",
        current_band="4.25-4.50",
        midpoint=None,
        effr=4.33,
        meetings=_fake_probability().meetings,
    )
    client = Mock()
    client.fetch_probabilities.return_value = prob
    orch = IngestionOrchestrator(store=store, rate_probability=client)
    store.seed_obs_sources_and_families()
    store.seed_concept_map()

    orch.run_source("rate_probability")
    assert store.resolve_indicator("FEDWATCH_US") is None


# ── Concept + subject bridge ─────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_seed_registers_fedwatch_obs_family(store: SQLiteEngineStore) -> None:
    store.seed_obs_sources_and_families()
    fam = store.get_obs_family("us.rates.fedwatch_midpoint")
    assert fam is not None
    assert fam.source_id == "rateprobability"
    assert fam.provider_series_id == "FEDWATCH_MIDPOINT"
    assert fam.unit == "percent"
    assert fam.frequency == "daily"


def test_concept_map_has_fedwatch_us(store: SQLiteEngineStore) -> None:
    store.seed_concept_map()
    rows = store.get_concept_series("FEDWATCH_US")
    assert len(rows) == 1
    row = rows[0]
    assert row.source_id == "rateprobability"
    assert row.provider_series_id == "FEDWATCH_MIDPOINT"
    assert row.obs_family_id == "us.rates.fedwatch_midpoint"
    assert row.role == "primary"


def test_fedwatch_us_bridges_to_rate_us_fedwatch_subject(
    store: SQLiteEngineStore,
) -> None:
    """FEDWATCH_US concept → FEDWATCH_MIDPOINT series → rate.us.fedwatch
    subject through the subject_aliases join, so downstream /items can
    resolve either vocabulary."""
    from storage.subjects import sync_from_yaml
    store.seed_concept_map()
    sync_from_yaml(store)
    subs = store.resolve_subjects_for_concept("FEDWATCH_US")
    assert "rate.us.fedwatch" in subs


# ── End-to-end run_source ────────────────────────────────────────────────


def test_run_rate_probability_source_persists_midpoint(
    store: SQLiteEngineStore, tmp_path: Path,
) -> None:
    """Full ingest path: observations are written to the indicators
    table and the concept resolver returns the midpoint value."""
    prob = _fake_probability()
    client = Mock()
    client.fetch_probabilities.return_value = prob
    orch = IngestionOrchestrator(store=store, rate_probability=client)
    store.seed_obs_sources_and_families()
    store.seed_concept_map()

    result = orch.run_source("rate_probability")
    assert result is not None

    resolved = store.resolve_indicator("FEDWATCH_US")
    assert resolved is not None
    assert resolved.value == 4.375
    assert resolved.source_id == "rateprobability"
    assert resolved.provider_series_id == "FEDWATCH_MIDPOINT"
