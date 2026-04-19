"""Ingest research notes from a markdown input folder.

Each input file must start with a YAML frontmatter block containing at
minimum ``title``, ``date``, and ``subject_id``. The body below the
second ``---`` fence is the note content. Ingesting a note:

  * ensures the ``notes`` doc_source row exists,
  * writes the note into ``document`` (document_type='report',
    source_id='notes', event_type='Research Note'),
  * stores the markdown body in ``document_blob``,
  * indexes title + body into ``documents_fts``,
  * tags ``item_subjects`` with the frontmatter ``subject_id`` at
    confidence 1.0 (author already picked the canonical subject, so no
    regex matching is needed).

Already-ingested notes (same sha256 over the raw file) are skipped.
Editing any part of the file — frontmatter or body — forces a fresh
row because the hash changes.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from storage.sqlite import (
    DocumentBlobRecord,
    DocumentRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)

_FRONTMATTER_FENCE = "---"
_NOTES_DOC_SOURCE_ID = "notes"
# 'report' is the closest existing document_type allowed by the CHECK
# constraint. event_type='Research Note' keeps notes distinguishable from
# press releases and gov reports.
_NOTES_DOCUMENT_TYPE = "report"
_NOTES_EVENT_TYPE = "Research Note"


@dataclass(frozen=True)
class NoteFrontmatter:
    title: str
    publish_date: str
    subject_id: str
    author: str = ""
    country: str = "XX"
    language: str = "en"


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


def parse_note(path: Path) -> tuple[NoteFrontmatter, str]:
    """Split a note into ``(frontmatter, body_markdown)``.

    Raises ``ValueError`` if frontmatter is missing or the required
    fields (``title``, ``date``, ``subject_id``) aren't present.
    """
    text = path.read_text(encoding="utf-8")
    if not text.startswith(_FRONTMATTER_FENCE):
        raise ValueError(f"{path}: missing YAML frontmatter fence")
    rest = text[len(_FRONTMATTER_FENCE):].lstrip("\n")
    try:
        end = rest.index(f"\n{_FRONTMATTER_FENCE}")
    except ValueError as exc:
        raise ValueError(f"{path}: unterminated frontmatter") from exc
    fm_text = rest[:end]
    body = rest[end + len(_FRONTMATTER_FENCE) + 1:].lstrip("\n")

    try:
        fm: Any = yaml.safe_load(fm_text) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"{path}: malformed YAML frontmatter: {exc}") from exc
    if not isinstance(fm, dict):
        raise ValueError(f"{path}: frontmatter must be a mapping")

    for required in ("title", "date", "subject_id"):
        if not fm.get(required):
            raise ValueError(f"{path}: frontmatter is missing '{required}'")

    return (
        NoteFrontmatter(
            title=str(fm["title"]),
            publish_date=str(fm["date"])[:10],
            subject_id=str(fm["subject_id"]),
            author=str(fm.get("author") or ""),
            country=(str(fm.get("country") or "XX")[:2].upper() or "XX"),
            language=(str(fm.get("language") or "en")[:5]),
        ),
        body,
    )


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def _ensure_notes_doc_source(store: SQLiteEngineStore) -> None:
    """Idempotently seed the ``notes`` row in ``doc_source``.

    ``source_type='news_agency'`` keeps the value inside the existing
    CHECK allowlist — research notes aren't a central bank or gov
    agency, and broadening the CHECK would cost a full table rebuild
    (unnecessary for an ingestion category).
    """
    with store._connection(commit=True) as c:
        now = datetime.now(timezone.utc).isoformat()
        c.execute(
            "INSERT OR IGNORE INTO doc_source "
            "(source_id, source_code, source_name, source_type, "
            " country_code, is_active, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
            (_NOTES_DOC_SOURCE_ID, "NOTES", "Research notes",
             "news_agency", "XX", now, now),
        )


def _compute_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ingest_one(
    store: SQLiteEngineStore,
    *,
    path: Path,
    fm: NoteFrontmatter,
    body: str,
    sha: str,
) -> bool:
    """Write one note into the unified document surface.

    Returns ``True`` if the note was persisted, ``False`` if a document
    with the same canonical URL already existed (upstream dedup by sha).
    """
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    now_epoch_ms = int(now_dt.timestamp() * 1000)
    try:
        published_epoch_ms = int(datetime.fromisoformat(
            fm.publish_date + "T00:00:00+00:00"
        ).timestamp() * 1000)
    except ValueError:
        published_epoch_ms = now_epoch_ms

    doc_id = sha[:16]
    # canonical_url is derived from the content hash alone so two files
    # with identical bytes but different names dedupe correctly — both
    # map to the same document row instead of the second overwriting the
    # first via INSERT OR REPLACE on document_id.
    canonical_url = f"note:///{sha}"
    if store.document_exists(canonical_url):
        return False

    doc = DocumentRecord(
        document_id=doc_id,
        release_family_id="",
        source_id=_NOTES_DOC_SOURCE_ID,
        canonical_url=canonical_url,
        title=fm.title,
        subtitle=path.name,
        document_type=_NOTES_DOCUMENT_TYPE,
        mime_type="text/markdown",
        language_code=fm.language,
        country_code=fm.country,
        topic_code="research_note",
        published_date=fm.publish_date,
        published_at=fm.publish_date,
        published_precision="date_only",
        status="published",
        version_no=1,
        parent_document_id="",
        hash_sha256=sha,
        created_at=now_iso,
        updated_at=now_iso,
        published_epoch_ms=published_epoch_ms,
        created_epoch_ms=now_epoch_ms,
        updated_epoch_ms=now_epoch_ms,
        institution=fm.author,
        authors=fm.author,
        event_type=_NOTES_EVENT_TYPE,
        # Author pre-resolved the subject, so confidence is the
        # structured-alias value (1.0) downstream.
        confidence=1.0,
        subject_freetext=fm.subject_id,
    )
    store.upsert_document(doc)

    if body.strip():
        blob = DocumentBlobRecord(
            document_blob_id=f"{doc_id}_md",
            document_id=doc_id,
            blob_role="markdown",
            storage_path=str(path),
            content_text=body,
            content_bytes=None,
            byte_size=len(body.encode("utf-8")),
            encoding="utf-8",
            parser_name="notes.ingest",
            parser_version="",
            extracted_at=now_iso,
        )
        store.upsert_document_blob(blob)

    store.upsert_document_fts(
        document_id=doc_id,
        title=fm.title,
        body=body,
    )

    # Author-assigned subject: straight 1.0, no tagger regex round-trip.
    store.set_document_subjects(doc_id, {fm.subject_id: 1.0})
    return True


def ingest_notes(
    input_dir: Path,
    *,
    store: SQLiteEngineStore,
) -> dict[str, int]:
    """Ingest every ``*.md`` file under ``input_dir``.

    Returns ``{"ingested": N, "skipped": M, "failed": K}``. Already-
    stored notes (same sha256) count as ``skipped``; files with missing
    or malformed frontmatter count as ``failed``.
    """
    _ensure_notes_doc_source(store)

    stats = {"ingested": 0, "skipped": 0, "failed": 0}
    for md in sorted(Path(input_dir).glob("*.md")):
        try:
            fm, body = parse_note(md)
        except ValueError as exc:
            logger.error("skip %s: %s", md, exc)
            stats["failed"] += 1
            continue

        sha = _compute_sha(md)
        try:
            persisted = _ingest_one(store, path=md, fm=fm, body=body, sha=sha)
        except Exception:  # noqa: BLE001 — log & continue so one bad file doesn't halt the batch
            logger.warning("note storage failed: %s", md, exc_info=True)
            stats["failed"] += 1
            continue
        if persisted:
            stats["ingested"] += 1
        else:
            stats["skipped"] += 1
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingest research notes into the engine store.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Folder containing *.md notes with YAML frontmatter.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite path (defaults to .macro-data/engine.db).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if not args.input.exists():
        print(f"input dir does not exist: {args.input}", file=sys.stderr)
        return 2

    store = SQLiteEngineStore(db_path=args.db)
    stats = ingest_notes(args.input, store=store)
    print(
        f"ingested={stats['ingested']} skipped={stats['skipped']}"
        f" failed={stats['failed']}"
    )
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
