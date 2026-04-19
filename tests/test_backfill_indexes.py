"""Tests for the one-shot document-index backfills (Step 9 review fix).

Covers the upgrade path: a DB that accumulated document rows before
documents_fts / item_subjects were populated by ingestion should still
become queryable once backfill_document_indexes runs."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from macro_data.service import LocalMacroDataService
from storage.sqlite import (
    DocumentBlobRecord,
    DocumentRecord,
    SQLiteEngineStore,
)


def _seed_doc_source(store: SQLiteEngineStore) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with store._connection(commit=True) as c:
        c.execute(
            "INSERT OR IGNORE INTO doc_source "
            "(source_id, source_code, source_name, source_type, "
            " country_code, is_active, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
            ("news", "NEWS", "News feeds", "news_agency", "US", now, now),
        )


def _write_legacy_doc(
    store: SQLiteEngineStore,
    *,
    doc_id: str,
    title: str,
    body: str,
    published_date: str = "2026-04-10",
) -> None:
    """Write a document WITHOUT calling upsert_document_fts / set_document_
    subjects — simulates a row that landed before those sidecars existed."""
    now_iso = datetime.now(timezone.utc).isoformat()
    store.upsert_document(DocumentRecord(
        document_id=doc_id,
        release_family_id="",
        source_id="news",
        canonical_url=f"https://example.com/{doc_id}",
        title=title,
        subtitle="",
        document_type="report",
        mime_type="text/html",
        language_code="en",
        country_code="US",
        topic_code="markets",
        published_date=published_date,
        published_at=published_date,
        status="published",
        version_no=1,
        parent_document_id="",
        hash_sha256=doc_id + "0" * (64 - len(doc_id)),
        created_at=now_iso,
        updated_at=now_iso,
    ))
    store.upsert_document_blob(DocumentBlobRecord(
        document_blob_id=f"{doc_id}_md",
        document_id=doc_id,
        blob_role="markdown",
        storage_path="",
        content_text=body,
        content_bytes=None,
        byte_size=len(body.encode("utf-8")),
        encoding="utf-8",
        parser_name="legacy",
        parser_version="",
        extracted_at=now_iso,
    ))


@pytest.fixture()
def legacy_store(tmp_path: Path) -> SQLiteEngineStore:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    _seed_doc_source(store)
    _write_legacy_doc(store, doc_id="d_cpi",
                      title="Hot CPI print shakes markets",
                      body="Headline CPI rose 0.5 percent in March.")
    _write_legacy_doc(store, doc_id="d_nfp",
                      title="Nonfarm payrolls beat forecasts",
                      body="NFP added 300k jobs.",
                      published_date="2026-04-08")
    _write_legacy_doc(store, doc_id="d_misc",
                      title="Boring regulatory memo",
                      body="Nothing matches the subject vocabulary here.")
    return store


# ── backfill_documents_fts ──────────────────────────────────────────────


def test_legacy_documents_are_invisible_to_fts_before_backfill(
    legacy_store: SQLiteEngineStore,
) -> None:
    """Baseline: documents written without calling upsert_document_fts
    don't appear in search_documents — demonstrates the bug."""
    assert legacy_store.search_documents("CPI") == []


def test_backfill_documents_fts_populates_index(
    legacy_store: SQLiteEngineStore,
) -> None:
    written = legacy_store.backfill_documents_fts()
    assert written == 3
    hits = legacy_store.search_documents("CPI")
    assert [h.document_id for h in hits] == ["d_cpi"]
    # Body content is indexed too
    hits = legacy_store.search_documents("300k jobs")
    assert [h.document_id for h in hits] == ["d_nfp"]


def test_backfill_documents_fts_is_idempotent(
    legacy_store: SQLiteEngineStore,
) -> None:
    assert legacy_store.backfill_documents_fts() == 3
    # Second call sees no missing rows → writes nothing
    assert legacy_store.backfill_documents_fts() == 0
    # Index still intact
    assert len(legacy_store.search_documents("CPI")) == 1


# ── backfill_document_subjects ──────────────────────────────────────────


def test_legacy_documents_have_no_subject_tags_before_backfill(
    legacy_store: SQLiteEngineStore,
) -> None:
    assert legacy_store.list_document_subjects("d_cpi") == []


def test_backfill_document_subjects_tags_by_title_regex(
    legacy_store: SQLiteEngineStore,
) -> None:
    # Vocabulary needs to exist before tagging runs.
    from storage.subjects import sync_from_yaml
    sync_from_yaml(legacy_store)

    tagged = legacy_store.backfill_document_subjects()
    # d_cpi → econ.cpi, d_nfp → econ.us.nfp. d_misc has no matching regex.
    assert tagged == 2
    assert dict(legacy_store.list_document_subjects("d_cpi")) == {"econ.cpi": 0.8}
    assert dict(legacy_store.list_document_subjects("d_nfp")) == {"econ.us.nfp": 0.8}
    assert legacy_store.list_document_subjects("d_misc") == []


def test_backfill_document_subjects_is_idempotent(
    legacy_store: SQLiteEngineStore,
) -> None:
    from storage.subjects import sync_from_yaml
    sync_from_yaml(legacy_store)
    legacy_store.backfill_document_subjects()
    # Already-tagged documents are excluded from the second pass.
    assert legacy_store.backfill_document_subjects() == 0


# ── service op wrapper ─────────────────────────────────────────────────


def test_backfill_op_runs_both_and_makes_list_items_work(
    legacy_store: SQLiteEngineStore,
) -> None:
    """End-to-end: after running the op, list_items(subject=...) and
    list_items(q=...) both return the legacy rows they previously missed."""
    svc = LocalMacroDataService(store=legacy_store)

    resp = svc.invoke("backfill_document_indexes", {})
    assert resp == {"fts_rows_written": 3, "documents_subject_tagged": 2}

    by_text = svc.invoke("list_items", {"q": "CPI"})
    assert [i["document_id"] for i in by_text["items"]] == ["d_cpi"]

    by_subject = svc.invoke("list_items", {"subject": "econ.cpi"})
    assert [i["document_id"] for i in by_subject["items"]] == ["d_cpi"]


def test_backfill_op_on_fresh_db_is_a_noop(tmp_path: Path) -> None:
    """Calling the op on a DB with no documents must not error and
    reports zero work done."""
    store = SQLiteEngineStore(db_path=tmp_path / "fresh.db")
    svc = LocalMacroDataService(store=store)
    resp = svc.invoke("backfill_document_indexes", {})
    assert resp == {"fts_rows_written": 0, "documents_subject_tagged": 0}
