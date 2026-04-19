"""Tests for the research-notes ingestion module (issue #3 item 6)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.notes.ingest import (
    NoteFrontmatter,
    _ensure_notes_doc_source,
    ingest_notes,
    parse_note,
)
from storage.sqlite import SQLiteEngineStore


_SAMPLE = """---
title: "Thoughts on Q3 CPI"
date: "2026-04-19"
subject_id: econ.cpi
author: ewan
---

# Body

Inflation looks sticky in services.
"""


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


# ── parse_note ──────────────────────────────────────────────────────────


def test_parse_note_happy_path(tmp_path: Path) -> None:
    p = _write(tmp_path / "n.md", _SAMPLE)
    fm, body = parse_note(p)
    assert isinstance(fm, NoteFrontmatter)
    assert fm.title == "Thoughts on Q3 CPI"
    assert fm.publish_date == "2026-04-19"
    assert fm.subject_id == "econ.cpi"
    assert fm.author == "ewan"
    assert "Inflation looks sticky" in body


def test_parse_note_missing_frontmatter(tmp_path: Path) -> None:
    p = _write(tmp_path / "n.md", "no frontmatter here")
    with pytest.raises(ValueError, match="missing YAML frontmatter"):
        parse_note(p)


def test_parse_note_unterminated_frontmatter(tmp_path: Path) -> None:
    p = _write(tmp_path / "n.md", "---\ntitle: x\n")
    with pytest.raises(ValueError, match="unterminated frontmatter"):
        parse_note(p)


def test_parse_note_missing_required_field(tmp_path: Path) -> None:
    text = "---\ntitle: foo\ndate: 2026-01-01\n---\nbody"
    p = _write(tmp_path / "n.md", text)
    with pytest.raises(ValueError, match="subject_id"):
        parse_note(p)


def test_parse_note_defaults_country_and_language(tmp_path: Path) -> None:
    text = '---\ntitle: t\ndate: "2026-04-10"\nsubject_id: econ.cpi\n---\nb'
    p = _write(tmp_path / "n.md", text)
    fm, _ = parse_note(p)
    assert fm.country == "XX"
    assert fm.language == "en"


# ── ingest_notes ─────────────────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_ingest_notes_writes_document_and_fts(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "notes_input"
    notes_dir.mkdir()
    _write(notes_dir / "q3_cpi.md", _SAMPLE)

    stats = ingest_notes(notes_dir, store=store)
    assert stats == {"ingested": 1, "skipped": 0, "failed": 0}

    docs = store.list_documents(source_id="notes", limit=5)
    assert len(docs) == 1
    doc = docs[0]
    assert doc.title == "Thoughts on Q3 CPI"
    assert doc.document_type == "report"
    assert doc.event_type == "Research Note"
    assert doc.subject_freetext == "econ.cpi"
    assert doc.confidence == 1.0
    assert doc.authors == "ewan"
    assert doc.institution == "ewan"

    # FTS indexed
    hits = store.search_documents("sticky")
    assert [h.document_id for h in hits] == [doc.document_id]


def test_ingest_notes_tags_subject_at_full_confidence(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    _write(notes_dir / "a.md", _SAMPLE)
    ingest_notes(notes_dir, store=store)

    doc = store.list_documents(source_id="notes", limit=1)[0]
    tags = dict(store.list_document_subjects(doc.document_id))
    assert tags == {"econ.cpi": 1.0}


def test_ingest_notes_is_idempotent_by_sha(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    _write(notes_dir / "a.md", _SAMPLE)

    first = ingest_notes(notes_dir, store=store)
    second = ingest_notes(notes_dir, store=store)
    assert first == {"ingested": 1, "skipped": 0, "failed": 0}
    assert second == {"ingested": 0, "skipped": 1, "failed": 0}


def test_ingest_notes_reingests_on_body_change(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    path = _write(notes_dir / "a.md", _SAMPLE)
    ingest_notes(notes_dir, store=store)

    # Edit body → sha256 over the file changes → new row gets ingested.
    path.write_text(_SAMPLE + "\n\nAdditional observation.\n", encoding="utf-8")
    stats = ingest_notes(notes_dir, store=store)
    assert stats["ingested"] == 1

    docs = store.list_documents(source_id="notes", limit=5)
    assert len(docs) == 2  # original + updated both stored


def test_ingest_notes_counts_malformed_as_failed(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    _write(notes_dir / "good.md", _SAMPLE)
    _write(notes_dir / "bad.md", "no frontmatter at all")
    _write(notes_dir / "missing_subject.md",
           "---\ntitle: t\ndate: 2026-01-01\n---\nbody")

    stats = ingest_notes(notes_dir, store=store)
    assert stats == {"ingested": 1, "skipped": 0, "failed": 2}


def test_ingest_notes_seeds_notes_doc_source(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    _ensure_notes_doc_source(store)
    _ensure_notes_doc_source(store)  # idempotent

    with store._connection(commit=False) as c:
        row = c.execute(
            "SELECT source_id, source_type, country_code "
            "FROM doc_source WHERE source_id = 'notes'"
        ).fetchone()
    assert row is not None
    assert row["source_type"] == "news_agency"
    assert row["country_code"] == "XX"


def test_ingest_notes_empty_dir_is_noop(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    notes_dir = tmp_path / "empty"
    notes_dir.mkdir()
    stats = ingest_notes(notes_dir, store=store)
    assert stats == {"ingested": 0, "skipped": 0, "failed": 0}


def test_ingest_notes_dedupes_duplicate_bytes_under_different_names(
    tmp_path: Path, store: SQLiteEngineStore,
) -> None:
    """Regression for codex P2: when two files contain identical bytes
    under different filenames they share the same sha256 and therefore
    the same doc_id. Previously the second file slipped past
    document_exists() (which used filename-qualified canonical_url) and
    INSERT OR REPLACE overwrote the first row. canonical_url must be
    sha-only so both files dedupe to one document."""
    notes_dir = tmp_path / "in"
    notes_dir.mkdir()
    _write(notes_dir / "a.md", _SAMPLE)
    _write(notes_dir / "b.md", _SAMPLE)  # identical bytes

    stats = ingest_notes(notes_dir, store=store)
    assert stats == {"ingested": 1, "skipped": 1, "failed": 0}

    docs = store.list_documents(source_id="notes", limit=5)
    assert len(docs) == 1
    # And the persisted row still points at the original first-seen file.
    assert docs[0].subtitle == "a.md"
