"""Tests for `scripts/backfill_concept.py` — cursor + dispatch (issue #114 P2)."""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tests rely on the engine.db storage layer, so prepend src/ before importing
# the script (which itself does the same).
sys.path.insert(0, str(REPO_ROOT / "src"))


def _load_script(monkeypatch=None):
    spec = importlib.util.spec_from_file_location(
        "backfill_concept_script",
        REPO_ROOT / "scripts" / "backfill_concept.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def script(tmp_path, monkeypatch):
    mod = _load_script()
    # Redirect cursor to a tmp path so each test starts clean.
    monkeypatch.setattr(mod, "CURSOR_PATH", tmp_path / "backfill_cursor.json")
    return mod


def test_cursor_save_load_roundtrip(script):
    state = {
        "fred::CPIAUCSL": {"status": "completed", "rows_written": 3098},
        "bls::CUUR0000SA0": {"status": "failed", "error": "rate limit"},
    }
    script.save_cursor(state)
    loaded = script.load_cursor()
    assert loaded == state


def test_cursor_load_missing_returns_empty(script):
    assert script.load_cursor() == {}


def test_cursor_load_corrupt_returns_empty(script):
    script.CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    script.CURSOR_PATH.write_text("{not valid json")
    assert script.load_cursor() == {}


def test_cursor_key_format(script):
    assert script.cursor_key("fred", "CPIAUCSL") == "fred::CPIAUCSL"


def test_dispatch_routes_per_source(script):
    # Build a row for each source family — verify we hit the right
    # handler. We do not actually call upstreams; each handler short-
    # circuits on missing config / dry_run with a recognisable error.
    from storage.sqlite import SQLiteEngineStore

    store = SQLiteEngineStore(Path(tempfile.mkdtemp()) / "test.db")

    cases = [
        # (source_id, provider_series_id, expected handler attribute name)
        ("rateprobability", "FEDWATCH_MIDPOINT", "backfill_rateprobability"),
        ("not_a_known_source", "X", None),
    ]
    for src, sid, expected in cases:
        row = script.BackfillRow(
            concept_id="X", source_id=src, provider_series_id=sid,
            obs_family_id="",
        )
        result = script.dispatch(store, row, dry_run=True)
        if expected is None:
            assert result.skipped
            assert "no backfill handler wired" in result.error
        else:
            # rateprobability handler always returns skipped with its
            # snapshot-only message — confirms the dispatch landed there.
            assert result.skipped
            assert "current-snapshot" in result.error


def test_list_backfill_rows_filters(script):
    from storage.sqlite import SQLiteEngineStore

    with tempfile.TemporaryDirectory() as td:
        store = SQLiteEngineStore(Path(td) / "test.db")
        store.seed_concept_map()
        all_rows = script.list_backfill_rows(
            store, concept_filter=None, source_filter=None,
        )
        assert len(all_rows) > 50
        cpi_rows = script.list_backfill_rows(
            store, concept_filter="CPI_US", source_filter=None,
        )
        sources = {r.source_id for r in cpi_rows}
        assert sources == {"bls", "fred"}
        bls_rows = script.list_backfill_rows(
            store, concept_filter=None, source_filter="bls",
        )
        assert all(r.source_id == "bls" for r in bls_rows)


def test_coerce_value_reads_rate_field(script):
    """NY Fed records expose ``rate`` not ``value`` — make sure the
    helper finds it."""
    from ingestion.timeseries.scrapers.nyfed import NYFedRate

    rec = NYFedRate(date="2024-01-01", type="EFFR", rate=5.33)
    assert script._coerce_value(rec) == 5.33


def test_coerce_value_reads_value_field(script):
    from ingestion.timeseries.scrapers.fred import FredObservation

    rec = FredObservation(series_id="X", date="2024-01-01", value=42.0)
    assert script._coerce_value(rec) == 42.0


def test_coerce_value_returns_none_when_no_field(script):
    class Bare: pass
    assert script._coerce_value(Bare()) is None


def test_single_observation_uses_observation_date_for_idempotency(script, tmp_path):
    """Issue #114 P2: single_observation rows use observation_date as
    the vintage_date so reruns collide on the PK and ``INSERT OR IGNORE``
    keeps the database stable."""
    from ingestion.timeseries.scrapers.fred import FredObservation
    from storage.sqlite import SQLiteEngineStore

    store = SQLiteEngineStore(tmp_path / "test.db")
    obs = [
        FredObservation(series_id="X", date="2024-01-01", value=1.0),
        FredObservation(series_id="X", date="2024-02-01", value=2.0),
    ]
    row = script.BackfillRow(
        concept_id="C", source_id="fred",
        provider_series_id="X", obs_family_id="",
    )
    # First run.
    r1 = script._write_single_observations(
        store, row, obs, source="fred", dry_run=False,
    )
    assert r1.rows_written == 2
    rows1 = store.get_vintages_for_series("X")
    assert len(rows1) == 2
    # Vintage_date == observation_date, so re-running is idempotent.
    for v in rows1:
        assert v.vintage_date == v.observation_date

    # Second run — same observations. PK collisions ignored, no new rows.
    script._write_single_observations(
        store, row, obs, source="fred", dry_run=False,
    )
    rows2 = store.get_vintages_for_series("X")
    assert len(rows2) == 2  # unchanged


def test_dispatch_handles_snapshot_only_sources(script):
    """aisi/ism/sentix are seeded in concept_map but only publish the
    current period — explicit skip-with-reason, not generic
    ``no handler`` so the operator gets an honest signal."""
    from storage.sqlite import SQLiteEngineStore

    with tempfile.TemporaryDirectory() as td:
        store = SQLiteEngineStore(Path(td) / "test.db")
        for src, sid in (
            ("aisi", "AISI_RAW_STEEL_PRODUCTION_US"),
            ("ism", "ISM_MFG_PMI_US"),
            ("sentix", "SENTIX_US_HEADLINE"),
        ):
            row = script.BackfillRow(
                concept_id="X", source_id=src,
                provider_series_id=sid, obs_family_id="",
            )
            result = script.dispatch(store, row, dry_run=True)
            assert result.skipped, f"{src} should be skipped"
            assert "current period" in result.error
