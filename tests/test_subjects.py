"""Tests for the unified subject vocabulary (issue #2)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from storage.sqlite import SQLiteEngineStore
from storage.subjects import (
    STRUCTURED_CONFIDENCE,
    TITLE_CONFIDENCE,
    SubjectTagger,
    load_subjects_yaml,
    sync_from_yaml,
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _count(store: SQLiteEngineStore, table: str) -> int:
    with store._connection(commit=False) as c:
        return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_schema_creates_subject_tables(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        names = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {"subjects", "subject_aliases", "item_subjects"} <= names


def test_sync_from_yaml_loads_default_vocab(store: SQLiteEngineStore) -> None:
    n = sync_from_yaml(store)
    assert n >= 20
    assert _count(store, "subjects") == n
    assert _count(store, "subject_aliases") > n  # each subject has several aliases


def test_sync_is_idempotent(store: SQLiteEngineStore) -> None:
    a = sync_from_yaml(store)
    before_aliases = _count(store, "subject_aliases")
    b = sync_from_yaml(store)
    after_aliases = _count(store, "subject_aliases")
    assert a == b
    assert before_aliases == after_aliases


def test_list_subjects_sorted(store: SQLiteEngineStore) -> None:
    sync_from_yaml(store)
    rows = store.list_subjects()
    ids = [r["subject_id"] for r in rows]
    assert ids == sorted(ids)
    assert "econ.cpi" in ids
    assert "rate.us.sofr" in ids


def test_get_subject_aliases_filtered(store: SQLiteEngineStore) -> None:
    sync_from_yaml(store)
    fred = store.get_subject_aliases("econ.cpi", alias_type="fred_series")
    assert set(fred) == {"CPIAUCSL", "CPILFESL", "CUUR0000SA0"}
    cal = store.get_subject_aliases("econ.cpi", alias_type="calendar_indicator")
    assert "CPI" in cal


def test_tagger_title_regex(store: SQLiteEngineStore) -> None:
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    hits = tagger.tag_text("Hot CPI print shakes markets")
    assert hits == [("econ.cpi", TITLE_CONFIDENCE)]
    hits = tagger.tag_text("Nonfarm payrolls beat forecasts")
    assert ("econ.us.nfp", TITLE_CONFIDENCE) in hits


def test_tagger_structured_alias(store: SQLiteEngineStore) -> None:
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)

    # FRED series (macro-data-service native)
    assert tagger.tag_alias("fred_series", "CPIAUCSL") == [
        ("econ.cpi", STRUCTURED_CONFIDENCE)
    ]
    # NY Fed series (information-layer addition)
    assert tagger.tag_alias("ny_fed_series", "SOFR") == [
        ("rate.us.sofr", STRUCTURED_CONFIDENCE)
    ]
    # Calendar indicator (case-insensitive)
    assert tagger.tag_alias("calendar_indicator", "core cpi") == [
        ("econ.cpi", STRUCTURED_CONFIDENCE)
    ]
    # Unknown alias → empty
    assert tagger.tag_alias("fred_series", "DOES_NOT_EXIST") == []


def test_tagger_structured_dedups_across_alias_types(
    store: SQLiteEngineStore,
) -> None:
    sync_from_yaml(store)
    with store._connection(commit=False) as c:
        tagger = SubjectTagger(c)
    # FRED + calendar both point to econ.cpi → one tuple back
    hits = tagger.tag_structured(
        fred_series="CPIAUCSL",
        calendar_indicator="CPI",
    )
    assert hits == [("econ.cpi", STRUCTURED_CONFIDENCE)]


@pytest.mark.parametrize(
    "concept_id, expected_subject",
    [
        # FRED-sourced bridges
        ("CPI_US", "econ.cpi"),
        ("CORE_PCE_US", "econ.us.core_pce"),
        ("UNEMP_US", "econ.unemployment"),
        ("GDP_REAL_US", "econ.gdp"),
        ("RETAIL_SALES_US", "econ.retail_sales"),
        ("TREASURY_2Y_US", "rate.us.2y"),
        ("TREASURY_10Y_US", "rate.us.10y"),
        ("DOLLAR_INDEX_US", "fx.dxy"),
        # BLS-sourced bridges (would break without bls_series aliases)
        ("NFP_US", "econ.us.nfp"),
        ("PPI_US", "econ.ppi"),
        # EIA-sourced bridges (would break without eia_series aliases)
        ("WTI_CRUDE", "commodity.wti"),
        ("BRENT_CRUDE", "commodity.brent"),
        # NY Fed bridges (the dual-form alias fix)
        ("SOFR_US", "rate.us.sofr"),
        ("OBFR_US", "rate.us.obfr"),
    ],
)
def test_resolve_subjects_for_concept_bridges_vocabularies(
    store: SQLiteEngineStore,
    concept_id: str,
    expected_subject: str,
) -> None:
    """concept_map provider_series_id ↔ subject_aliases.alias_value is the
    only bridge between the timeseries vocabulary and the subject vocabulary.
    Every concept shipped in _CONCEPT_MAP_DEFS whose subject exists in the
    yaml must resolve through this join.
    """
    store.seed_concept_map()
    sync_from_yaml(store)
    subs = store.resolve_subjects_for_concept(concept_id)
    assert expected_subject in subs, (
        f"{concept_id} did not bridge to {expected_subject}; got {subs}"
    )


def test_load_subjects_yaml_shape() -> None:
    rows = load_subjects_yaml()
    assert isinstance(rows, list)
    assert all("id" in r and "display" in r for r in rows)
