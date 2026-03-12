"""Ingestion bridge — reads SQLite records, chunks, embeds, inserts to vector store.

Provides incremental sync via ``rag_sync_watermarks`` table.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from .bm25 import BM25Stats, bm25_sparse_vector, save_stats
from .chunker import (
    RawChunk,
    chunk_central_bank_comm,
    chunk_news_article,
    chunk_research_artifact,
)
from .config import RAGConfig
from .embeddings import Embedder
from .text_utils import tokenize
from .vector_store import VectorStore

log = logging.getLogger(__name__)

_EMBED_BATCH_SIZE = 32
_INSERT_BATCH_SIZE = 64


class MacroIngestionBridge:
    def __init__(
        self,
        store: VectorStore,
        embedder: Embedder,
        cfg: RAGConfig,
        db_path: str | None = None,
    ) -> None:
        self.store = store
        self.embedder = embedder
        self.cfg = cfg
        self._db_path = db_path

    def _get_sqlite_conn(self):
        import sqlite3

        path = self._db_path
        if not path:
            from analyst.storage.sqlite import default_engine_db_path

            path = str(default_engine_db_path())
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Full calibrate
    # ------------------------------------------------------------------

    def calibrate(self) -> Dict[str, int]:
        """Drop collection, rebuild from all SQLite data."""
        log.info("rag_calibrate_start")
        t0 = time.perf_counter()

        self.store.drop_collection()
        self.store.init_collection()

        conn = self._get_sqlite_conn()
        try:
            all_chunks: list[RawChunk] = []
            news_chunks, _ = self._read_news(conn)
            all_chunks.extend(news_chunks)
            fed_chunks, _ = self._read_fed_comms(conn)
            all_chunks.extend(fed_chunks)
            res_chunks, _ = self._read_research(conn)
            all_chunks.extend(res_chunks)

            inserted = self._embed_and_insert(all_chunks)

            # Reset watermarks
            self._reset_watermarks(conn)
        finally:
            conn.close()

        elapsed = time.perf_counter() - t0
        log.info(
            "rag_calibrate_done chunks=%d inserted=%d elapsed_sec=%.1f",
            len(all_chunks),
            inserted,
            elapsed,
        )
        return {"chunks": len(all_chunks), "inserted": inserted}

    # ------------------------------------------------------------------
    # Incremental sync
    # ------------------------------------------------------------------

    def sync(self) -> Dict[str, int]:
        """Sync new/updated records since last watermark."""
        log.info("rag_sync_start")
        t0 = time.perf_counter()

        conn = self._get_sqlite_conn()
        try:
            watermarks = self._load_watermarks(conn)
            all_chunks: list[RawChunk] = []
            new_watermarks: dict[str, int] = {}

            # News
            news_wm = watermarks.get("news_article", 0)
            news_chunks, news_max_id = self._read_news(conn, min_id=news_wm)
            all_chunks.extend(news_chunks)
            if news_max_id > news_wm:
                new_watermarks["news_article"] = news_max_id

            # Fed comms
            fed_wm = watermarks.get("central_bank_comm", 0)
            fed_chunks, fed_max_id = self._read_fed_comms(conn, min_id=fed_wm)
            all_chunks.extend(fed_chunks)
            if fed_max_id > fed_wm:
                new_watermarks["central_bank_comm"] = fed_max_id

            # Research
            res_wm = watermarks.get("research_artifact", 0)
            res_chunks, res_max_id = self._read_research(conn, min_id=res_wm)
            all_chunks.extend(res_chunks)
            if res_max_id > res_wm:
                new_watermarks["research_artifact"] = res_max_id

            inserted = 0
            if all_chunks:
                # Delete stale doc_ids
                doc_ids = list({c.doc_id for c in all_chunks if c.doc_id})
                if doc_ids:
                    self.store.delete_by_doc_ids(doc_ids, flush=False)
                inserted = self._embed_and_insert(all_chunks)

            # Update watermarks
            for source_type, max_id in new_watermarks.items():
                self._save_watermark(conn, source_type, max_id)
            conn.commit()
        finally:
            conn.close()

        elapsed = time.perf_counter() - t0
        log.info(
            "rag_sync_done chunks=%d inserted=%d elapsed_sec=%.1f",
            len(all_chunks),
            inserted,
            elapsed,
        )
        return {"chunks": len(all_chunks), "inserted": inserted}

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        conn = self._get_sqlite_conn()
        try:
            watermarks = self._load_watermarks(conn)
        finally:
            conn.close()
        entity_count = 0
        try:
            entity_count = self.store.entity_count()
        except Exception:
            pass
        return {
            "collection": self.store.collection_name,
            "entity_count": entity_count,
            "watermarks": watermarks,
        }

    # ------------------------------------------------------------------
    # SQLite readers
    # ------------------------------------------------------------------

    def _read_news(
        self, conn, min_id: int = 0
    ) -> tuple[list[RawChunk], int]:
        cursor = conn.execute(
            "SELECT rowid, * FROM news_articles WHERE rowid > ? ORDER BY rowid",
            (min_id,),
        )
        chunks: list[RawChunk] = []
        max_id = min_id
        for row in cursor:
            row_dict = dict(row)
            rid = row_dict.get("rowid", 0)
            if rid > max_id:
                max_id = rid
            chunks.extend(chunk_news_article(row_dict))
        return chunks, max_id

    def _read_fed_comms(
        self, conn, min_id: int = 0
    ) -> tuple[list[RawChunk], int]:
        cursor = conn.execute(
            "SELECT rowid, * FROM central_bank_comms WHERE rowid > ? ORDER BY rowid",
            (min_id,),
        )
        chunks: list[RawChunk] = []
        max_id = min_id
        for row in cursor:
            row_dict = dict(row)
            rid = row_dict.get("rowid", 0)
            if rid > max_id:
                max_id = rid
            chunks.extend(chunk_central_bank_comm(row_dict))
        return chunks, max_id

    def _read_research(
        self, conn, min_id: int = 0
    ) -> tuple[list[RawChunk], int]:
        cursor = conn.execute(
            "SELECT rowid, * FROM research_artifacts WHERE rowid > ? ORDER BY rowid",
            (min_id,),
        )
        chunks: list[RawChunk] = []
        max_id = min_id
        for row in cursor:
            row_dict = dict(row)
            rid = row_dict.get("rowid", 0)
            if rid > max_id:
                max_id = rid
            chunks.extend(chunk_research_artifact(row_dict))
        return chunks, max_id

    # ------------------------------------------------------------------
    # Embed + insert
    # ------------------------------------------------------------------

    def _embed_and_insert(self, chunks: List[RawChunk]) -> int:
        if not chunks:
            return 0

        # Build BM25 stats
        bm25_stats = BM25Stats()
        for c in chunks:
            tokens = tokenize(c.text)
            bm25_stats.update(tokens)

        # Save stats
        if self.cfg.bm25_stats_dir:
            os.makedirs(self.cfg.bm25_stats_dir, exist_ok=True)
            save_stats(f"{self.cfg.bm25_stats_dir}/bm25_stats.json", bm25_stats)

        inserted = 0
        for batch_start in range(0, len(chunks), _EMBED_BATCH_SIZE):
            batch = chunks[batch_start : batch_start + _EMBED_BATCH_SIZE]
            texts = [c.text for c in batch]
            embeddings = self.embedder.embed_many(texts)

            rows: list[dict[str, Any]] = []
            for c, emb in zip(batch, embeddings):
                sparse = bm25_sparse_vector(c.text, bm25_stats)
                rows.append({
                    "chunk_id": c.chunk_id,
                    "text": c.text[:8192],
                    "source_type": c.source_type,
                    "source_id": c.source_id,
                    "section_path": c.section_path,
                    "content_type": c.content_type,
                    "country": c.country,
                    "indicator_group": c.indicator_group,
                    "impact_level": c.impact_level,
                    "data_source": c.data_source,
                    "updated_at": c.updated_at,
                    "content_hash": c.content_hash_val,
                    "doc_id": c.doc_id,
                    "chunk_index": c.chunk_index,
                    "chunk_total": c.chunk_total,
                    "dense_vec": emb,
                    "sparse_vec": sparse,
                })

            for ins_start in range(0, len(rows), _INSERT_BATCH_SIZE):
                ins_batch = rows[ins_start : ins_start + _INSERT_BATCH_SIZE]
                self.store.insert(ins_batch, flush=False)
                inserted += len(ins_batch)

        self.store.flush()
        return inserted

    # ------------------------------------------------------------------
    # Watermarks
    # ------------------------------------------------------------------

    def _load_watermarks(self, conn) -> Dict[str, int]:
        try:
            cursor = conn.execute(
                "SELECT source_type, last_synced_id FROM rag_sync_watermarks"
            )
            return {row["source_type"]: row["last_synced_id"] for row in cursor}
        except Exception:
            return {}

    def _save_watermark(self, conn, source_type: str, last_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO rag_sync_watermarks (source_type, last_synced_id, last_synced_at)
               VALUES (?, ?, ?)
               ON CONFLICT(source_type) DO UPDATE SET
                   last_synced_id = excluded.last_synced_id,
                   last_synced_at = excluded.last_synced_at""",
            (source_type, last_id, now),
        )

    def _reset_watermarks(self, conn) -> None:
        try:
            conn.execute("DELETE FROM rag_sync_watermarks")
            conn.commit()
        except Exception:
            pass
