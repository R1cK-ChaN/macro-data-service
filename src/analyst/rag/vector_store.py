"""SQLite + numpy vector store for macro-economic content.

Lightweight replacement for Milvus — brute-force inner-product search on an
in-memory numpy matrix loaded lazily from SQLite.  Suitable for MVP-scale
collections (< 100 k chunks on a single VPS).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np

from .config import RAGConfig

log = logging.getLogger(__name__)

_METADATA_COLS = [
    "chunk_id",
    "source_type",
    "source_id",
    "section_path",
    "content_type",
    "country",
    "indicator_group",
    "impact_level",
    "data_source",
    "updated_at",
    "content_hash",
    "doc_id",
    "chunk_index",
    "chunk_total",
]

_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS rag_chunks (
    chunk_id        TEXT PRIMARY KEY,
    text            TEXT NOT NULL DEFAULT '',
    source_type     TEXT NOT NULL DEFAULT '',
    source_id       TEXT NOT NULL DEFAULT '',
    section_path    TEXT NOT NULL DEFAULT '',
    content_type    TEXT NOT NULL DEFAULT '',
    country         TEXT NOT NULL DEFAULT '',
    indicator_group TEXT NOT NULL DEFAULT '',
    impact_level    TEXT NOT NULL DEFAULT '',
    data_source     TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT '',
    content_hash    TEXT NOT NULL DEFAULT '',
    doc_id          TEXT NOT NULL DEFAULT '',
    chunk_index     INTEGER NOT NULL DEFAULT 0,
    chunk_total     INTEGER NOT NULL DEFAULT 0,
    dense_vec       BLOB,
    sparse_vec      TEXT DEFAULT '{}'
)"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_rag_doc_id ON rag_chunks(doc_id)",
    "CREATE INDEX IF NOT EXISTS idx_rag_source_type ON rag_chunks(source_type)",
    "CREATE INDEX IF NOT EXISTS idx_rag_country ON rag_chunks(country)",
    "CREATE INDEX IF NOT EXISTS idx_rag_indicator_group ON rag_chunks(indicator_group)",
    "CREATE INDEX IF NOT EXISTS idx_rag_impact_level ON rag_chunks(impact_level)",
]


# ------------------------------------------------------------------
# Dense vector serialisation (float32 array ↔ BLOB)
# ------------------------------------------------------------------


def _vec_to_blob(vec: list[float]) -> bytes:
    """Pack a list of floats into a compact binary BLOB (4 bytes each)."""
    return struct.pack(f"{len(vec)}f", *vec)


def _blob_to_vec(blob: bytes) -> np.ndarray:
    n = len(blob) // 4
    return np.array(struct.unpack(f"{n}f", blob), dtype=np.float32)


# ------------------------------------------------------------------
# SearchHit — thin shim so pipeline.py can use ``hit.score`` / ``hit.entity``
# ------------------------------------------------------------------


@dataclass
class SearchHit:
    score: float
    entity: Dict[str, Any]


# ------------------------------------------------------------------
# VectorStore
# ------------------------------------------------------------------


class VectorStore:
    """SQLite-backed vector store with numpy brute-force search."""

    def __init__(self, cfg: RAGConfig) -> None:
        self.cfg = cfg
        self._db_path = cfg.db_path
        self._conn: sqlite3.Connection | None = None

        # In-memory index (lazy-loaded)
        self._ids: list[str] = []
        self._meta: list[dict[str, Any]] = []
        self._matrix: np.ndarray | None = None  # shape (N, dim), float32
        self._sparse: list[dict[int, float]] = []
        self._dirty = True

    @property
    def collection_name(self) -> str:
        return "rag_chunks"

    # ------------------------------------------------------------------
    # Connection / lifecycle
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
        return self._conn

    def init_collection(self) -> None:
        conn = self._get_conn()
        conn.execute(_CREATE_TABLE)
        for idx_sql in _CREATE_INDEXES:
            conn.execute(idx_sql)
        conn.commit()
        self._dirty = True

    def drop_collection(self) -> None:
        conn = self._get_conn()
        conn.execute("DROP TABLE IF EXISTS rag_chunks")
        conn.commit()
        self._ids = []
        self._meta = []
        self._matrix = None
        self._sparse = []
        self._dirty = True

    def _load_index(self) -> None:
        """Load all vectors + metadata into memory for brute-force search."""
        if not self._dirty:
            return
        conn = self._get_conn()
        meta_cols = ", ".join(_METADATA_COLS[1:])  # skip chunk_id, handled separately
        cursor = conn.execute(
            f"SELECT chunk_id, {meta_cols}, dense_vec, sparse_vec FROM rag_chunks"
        )

        ids: list[str] = []
        meta: list[dict[str, Any]] = []
        vecs: list[np.ndarray] = []
        sparse: list[dict[int, float]] = []

        for row in cursor:
            rd = dict(row)
            ids.append(rd["chunk_id"])
            meta.append({col: rd[col] for col in _METADATA_COLS})

            blob = rd.get("dense_vec")
            if blob:
                vecs.append(_blob_to_vec(blob))
            else:
                vecs.append(np.zeros(self.cfg.embedding_dim, dtype=np.float32))

            sp_json = rd.get("sparse_vec") or "{}"
            sparse.append({int(k): float(v) for k, v in json.loads(sp_json).items()})

        self._ids = ids
        self._meta = meta
        self._sparse = sparse
        self._matrix = np.vstack(vecs) if vecs else np.zeros(
            (0, self.cfg.embedding_dim), dtype=np.float32
        )
        self._dirty = False
        log.info("rag_index_loaded chunks=%d", len(ids))

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def insert(self, rows: List[Dict[str, Any]], *, flush: bool = True) -> None:
        conn = self._get_conn()
        for row in rows:
            dense_blob = _vec_to_blob(row["dense_vec"]) if row.get("dense_vec") else None
            sparse_json = json.dumps(
                {str(k): v for k, v in row.get("sparse_vec", {}).items()}
            )
            conn.execute(
                """INSERT OR REPLACE INTO rag_chunks
                   (chunk_id, text, source_type, source_id, section_path,
                    content_type, country, indicator_group, impact_level,
                    data_source, updated_at, content_hash, doc_id,
                    chunk_index, chunk_total, dense_vec, sparse_vec)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row.get("chunk_id", ""),
                    row.get("text", "")[:8192],
                    row.get("source_type", ""),
                    row.get("source_id", ""),
                    row.get("section_path", ""),
                    row.get("content_type", ""),
                    row.get("country", ""),
                    row.get("indicator_group", ""),
                    row.get("impact_level", ""),
                    row.get("data_source", ""),
                    row.get("updated_at", ""),
                    row.get("content_hash", ""),
                    row.get("doc_id", ""),
                    row.get("chunk_index", 0),
                    row.get("chunk_total", 0),
                    dense_blob,
                    sparse_json,
                ),
            )
        if flush:
            conn.commit()
        self._dirty = True

    def flush(self) -> None:
        if self._conn:
            self._conn.commit()
        self._dirty = True

    def delete_by_doc_id(self, doc_id: str, *, flush: bool = True) -> None:
        conn = self._get_conn()
        conn.execute("DELETE FROM rag_chunks WHERE doc_id = ?", (doc_id,))
        if flush:
            conn.commit()
        self._dirty = True

    def delete_by_doc_ids(
        self, doc_ids: List[str], *, batch_size: int = 128, flush: bool = True
    ) -> None:
        if not doc_ids:
            return
        conn = self._get_conn()
        for i in range(0, len(doc_ids), batch_size):
            batch = doc_ids[i : i + batch_size]
            placeholders = ",".join("?" for _ in batch)
            conn.execute(
                f"DELETE FROM rag_chunks WHERE doc_id IN ({placeholders})", batch
            )
        if flush:
            conn.commit()
        self._dirty = True

    # ------------------------------------------------------------------
    # Filter mask
    # ------------------------------------------------------------------

    def _build_mask(self, filters: Optional[Dict[str, Any]]) -> np.ndarray:
        """Build a boolean mask over ``self._meta`` from a filter dict."""
        n = len(self._ids)
        if n == 0:
            return np.zeros(0, dtype=bool)
        mask = np.ones(n, dtype=bool)
        if not filters:
            return mask

        for key in (
            "source_type", "content_type", "country", "indicator_group", "impact_level"
        ):
            filt = filters.get(key)
            if filt is None:
                continue
            include = filt.get("include") if isinstance(filt, dict) else None
            if not include:
                continue
            include_set = set(include)
            for i, m in enumerate(self._meta):
                if mask[i] and m.get(key, "") not in include_set:
                    mask[i] = False

        updated_after = filters.get("updated_after")
        if updated_after:
            for i, m in enumerate(self._meta):
                if mask[i] and (m.get("updated_at", "") < updated_after):
                    mask[i] = False

        return mask

    # ------------------------------------------------------------------
    # Search — dense (inner product via numpy)
    # ------------------------------------------------------------------

    def search_dense(
        self,
        query_vec: List[float],
        top_k: int,
        filters: Optional[Dict[str, Any]] = None,
        *,
        include_text: bool = False,
    ) -> List[SearchHit]:
        self._load_index()
        if self._matrix is None or len(self._ids) == 0:
            return []

        mask = self._build_mask(filters)
        if not mask.any():
            return []

        qvec = np.array(query_vec, dtype=np.float32)
        scores = self._matrix @ qvec  # (N,)
        scores[~mask] = -np.inf

        k = min(top_k, int(mask.sum()))
        if k <= 0:
            return []

        top_idx = np.argpartition(scores, -k)[-k:]
        top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

        hits: list[SearchHit] = []
        for idx in top_idx:
            s = float(scores[idx])
            if s == -np.inf:
                continue
            entity = dict(self._meta[idx])
            if include_text:
                entity["text"] = self._fetch_text(entity["chunk_id"])
            hits.append(SearchHit(score=s, entity=entity))
        return hits

    # ------------------------------------------------------------------
    # Search — sparse (iterate + inner product)
    # ------------------------------------------------------------------

    def search_sparse(
        self,
        query_vec: Dict[int, float],
        top_k: int,
        filters: Optional[Dict[str, Any]] = None,
        *,
        include_text: bool = False,
    ) -> List[SearchHit]:
        self._load_index()
        if not self._ids:
            return []

        mask = self._build_mask(filters)
        if not mask.any():
            return []

        scored: list[tuple[int, float]] = []
        for i in range(len(self._ids)):
            if not mask[i]:
                continue
            doc_sparse = self._sparse[i]
            dot = sum(qw * doc_sparse[dim] for dim, qw in query_vec.items() if dim in doc_sparse)
            if dot > 0:
                scored.append((i, dot))

        scored.sort(key=lambda x: x[1], reverse=True)
        scored = scored[:top_k]

        hits: list[SearchHit] = []
        for idx, s in scored:
            entity = dict(self._meta[idx])
            if include_text:
                entity["text"] = self._fetch_text(entity["chunk_id"])
            hits.append(SearchHit(score=s, entity=entity))
        return hits

    def _fetch_text(self, chunk_id: str) -> str:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT text FROM rag_chunks WHERE chunk_id = ?", (chunk_id,)
        ).fetchone()
        return str(row["text"]) if row else ""

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def query_by_chunk_ids(
        self,
        chunk_ids: List[str],
        *,
        include_text: bool = True,
        batch_size: int | None = None,
    ) -> List[Dict[str, Any]]:
        if not chunk_ids:
            return []
        conn = self._get_conn()
        cols = list(_METADATA_COLS)
        if include_text:
            cols = ["text"] + cols
        col_str = ", ".join(cols)
        rows: list[dict[str, Any]] = []
        size = batch_size or 64
        unique_ids = list(dict.fromkeys(chunk_ids))
        for i in range(0, len(unique_ids), size):
            batch = unique_ids[i : i + size]
            placeholders = ",".join("?" for _ in batch)
            cursor = conn.execute(
                f"SELECT {col_str} FROM rag_chunks WHERE chunk_id IN ({placeholders})",
                batch,
            )
            rows.extend(dict(r) for r in cursor)
        return rows

    def query_by_doc_and_index(
        self, doc_id: str, indices: List[int], *, include_text: bool = True
    ) -> List[Dict[str, Any]]:
        if not indices:
            return []
        conn = self._get_conn()
        cols = list(_METADATA_COLS)
        if include_text:
            cols = ["text"] + cols
        col_str = ", ".join(cols)
        placeholders = ",".join("?" for _ in indices)
        cursor = conn.execute(
            f"SELECT {col_str} FROM rag_chunks"
            f" WHERE doc_id = ? AND chunk_index IN ({placeholders})",
            [doc_id] + list(indices),
        )
        return [dict(r) for r in cursor]

    def query_neighbors_batch(
        self,
        doc_indices: Dict[str, List[int]],
        *,
        include_text: bool = True,
        batch_docs: int | None = None,
    ) -> List[Dict[str, Any]]:
        if not doc_indices:
            return []
        rows: list[dict[str, Any]] = []
        for doc_id, indices in doc_indices.items():
            deduped = sorted(
                {int(idx) for idx in indices if isinstance(idx, int) and idx >= 0}
            )
            if deduped:
                rows.extend(
                    self.query_by_doc_and_index(doc_id, deduped, include_text=include_text)
                )
        return rows

    def entity_count(self) -> int:
        conn = self._get_conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM rag_chunks").fetchone()
        return row["cnt"] if row else 0
