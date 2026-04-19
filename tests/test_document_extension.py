"""Tests for the information-layer document extension: 17-field columns +
FTS5 (issue #3 item 4 + port of doc_parser / gov_report / news)."""

from __future__ import annotations

from pathlib import Path

import pytest

from storage.sqlite import DocumentRecord, SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    s = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    # A doc_source row is required by the document.source_id FK.
    with s._connection(commit=True) as c:
        c.execute(
            "INSERT INTO doc_source(source_id, source_code, source_name, "
            "source_type, country_code, is_active, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("us.bls", "BLS", "US Bureau of Labor Statistics",
             "government_agency", "US", 1, "2026-04-19", "2026-04-19"),
        )
    return s


def _doc(**overrides) -> DocumentRecord:
    base = dict(
        document_id="doc-1",
        release_family_id="",
        source_id="us.bls",
        canonical_url="https://bls.gov/cpi/2026-04.htm",
        title="CPI rose 0.5% in March",
        subtitle="",
        document_type="release",
        mime_type="text/html",
        language_code="en",
        country_code="US",
        topic_code="inflation",
        published_date="2026-04-10",
        published_at="",
        status="published",
        version_no=1,
        parent_document_id="",
        hash_sha256="a" * 64,
        created_at="2026-04-10T12:00:00Z",
        updated_at="2026-04-10T12:00:00Z",
    )
    base.update(overrides)
    return DocumentRecord(**base)


def test_schema_has_17_field_columns(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        cols = {r["name"] for r in c.execute("PRAGMA table_info(document)")}
    for col in (
        "institution", "authors", "data_period", "market", "asset_class",
        "sector", "event_type", "impact_level", "contains_commentary",
        "confidence", "subject_freetext",
    ):
        assert col in cols, f"missing column: {col}"


def test_filter_indexes_created(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        names = {r["name"] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
    assert {"idx_document_impact_level",
            "idx_document_asset_class",
            "idx_document_event_type"} <= names


def test_documents_fts_virtual_table_exists(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as c:
        row = c.execute(
            "SELECT name FROM sqlite_master WHERE name='documents_fts'"
        ).fetchone()
    assert row is not None


def test_extended_fields_round_trip(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc(
        institution="US Bureau of Labor Statistics",
        authors="BLS Commissioner",
        data_period="2026-03",
        market="US",
        asset_class="macro",
        sector="consumer_prices",
        event_type="data_release",
        impact_level="high",
        contains_commentary=True,
        confidence=0.95,
        subject_freetext="consumer inflation",
    ))
    got = store.get_document("doc-1")
    assert got is not None
    assert got.institution == "US Bureau of Labor Statistics"
    assert got.authors == "BLS Commissioner"
    assert got.data_period == "2026-03"
    assert got.market == "US"
    assert got.asset_class == "macro"
    assert got.sector == "consumer_prices"
    assert got.event_type == "data_release"
    assert got.impact_level == "high"
    assert got.contains_commentary is True
    assert got.confidence == 0.95
    assert got.subject_freetext == "consumer inflation"


def test_defaults_preserved_for_legacy_callers(store: SQLiteEngineStore) -> None:
    """Ingestion paths that don't populate the 17-field surface still work;
    missing fields come back as '' / 0 / False rather than None."""
    store.upsert_document(_doc())
    got = store.get_document("doc-1")
    assert got is not None
    assert got.institution == ""
    assert got.impact_level == ""
    assert got.contains_commentary is False
    assert got.confidence == 0.0
    assert got.subject_freetext == ""


def test_fts_indexes_title_and_body(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc())
    store.upsert_document_fts(
        document_id="doc-1",
        title="CPI rose 0.5% in March",
        body="Consumer prices rose 0.5 percent in March after a flat February.",
    )
    hits = store.search_documents("CPI")
    assert [h.document_id for h in hits] == ["doc-1"]
    # BM25 match inside the body, not just title
    hits = store.search_documents("consumer prices")
    assert [h.document_id for h in hits] == ["doc-1"]


def test_fts_upsert_replaces_existing_row(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc())
    store.upsert_document_fts(document_id="doc-1", title="first", body="alpha")
    store.upsert_document_fts(document_id="doc-1", title="second", body="beta")
    # Old terms gone, new terms hit
    assert store.search_documents("alpha") == []
    hits = store.search_documents("beta")
    assert [h.document_id for h in hits] == ["doc-1"]


def test_fts_delete_removes_row(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc())
    store.upsert_document_fts(document_id="doc-1", title="x", body="unobtainium")
    assert len(store.search_documents("unobtainium")) == 1
    store.delete_document_fts("doc-1")
    assert store.search_documents("unobtainium") == []


def test_search_documents_empty_query_returns_empty(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc())
    store.upsert_document_fts(document_id="doc-1", title="CPI", body="")
    assert store.search_documents("") == []
    assert store.search_documents("   ") == []


@pytest.mark.parametrize(
    "unsafe_query",
    [
        "0.5%",           # percent sign
        "CPI - March",    # hyphen (FTS5 NOT operator)
        "CPI:",           # colon (FTS5 column filter)
        'Fed "hawkish',   # unmatched double quote
        "AA/BBB",         # slash
        "*asterisk*",     # leading/trailing asterisk
        "(parens)",       # grouping chars
        "OR AND NOT",     # reserved keywords
    ],
)
def test_search_documents_tolerates_punctuation(
    store: SQLiteEngineStore, unsafe_query: str
) -> None:
    """FTS5 raises on raw user input containing its syntax characters.
    search_documents() must either return results or an empty list — it
    must never raise sqlite3.OperationalError to the caller.
    """
    store.upsert_document(_doc())
    store.upsert_document_fts(
        document_id="doc-1",
        title="CPI rose 0.5% in March",
        body="",
    )
    # Either returns hits or [] — but must not raise.
    result = store.search_documents(unsafe_query)
    assert isinstance(result, list)


def test_search_documents_ranks_by_bm25(store: SQLiteEngineStore) -> None:
    store.upsert_document(_doc(document_id="doc-1"))
    store.upsert_document(_doc(
        document_id="doc-2", canonical_url="https://bls.gov/cpi/2026-03.htm"
    ))
    # doc-2 has the target term multiple times → ranks above doc-1
    store.upsert_document_fts(
        document_id="doc-1", title="Markets react", body="CPI mentioned once",
    )
    store.upsert_document_fts(
        document_id="doc-2", title="CPI report",
        body="CPI rose 0.5% CPI headline CPI core",
    )
    ranked = store.search_documents("CPI")
    assert [d.document_id for d in ranked[:2]] == ["doc-2", "doc-1"]
