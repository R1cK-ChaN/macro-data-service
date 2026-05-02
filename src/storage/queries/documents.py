"""Documents-domain query helpers for SQLiteEngineStore.

Covers doc_source + doc_release_family + document + document_blob +
document_extra + documents_fts + item_subjects + the cross-cutting
subject queries (`list_items_combined`, `list_subject_indicators`,
`list_subject_market_bars`) that index into documents and bridge across
indicator + market data.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from contracts import utc_now
from storage.models.documents import (
    DocReleaseFamilyRecord,
    DocSourceRecord,
    DocumentBlobRecord,
    DocumentExtraRecord,
    DocumentRecord,
)
from storage.queries.calendar import (
    _infer_timestamp_precision,
    _safe_epoch_ms,
    _safe_utc_iso,
)


class _DocumentsQueriesMixin:
    def upsert_doc_source(self, record: DocSourceRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO doc_source (
                    source_id, source_code, source_name, source_type,
                    country_code, default_language_code, homepage_url,
                    is_active, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.source_id,
                    record.source_code,
                    record.source_name,
                    record.source_type,
                    record.country_code,
                    record.default_language_code,
                    record.homepage_url,
                    int(record.is_active),
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_doc_source(self, source_id: str) -> DocSourceRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM doc_source WHERE source_id = ?",
                (source_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_doc_source(row)

    def list_doc_sources(self, *, active_only: bool = True) -> list[DocSourceRecord]:
        query = "SELECT * FROM doc_source"
        params: list[Any] = []
        if active_only:
            query += " WHERE is_active = 1"
        query += " ORDER BY source_id"
        with self._connection(commit=False) as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_doc_source(row) for row in rows]

    def _row_to_doc_source(self, row: sqlite3.Row) -> DocSourceRecord:
        return DocSourceRecord(
            source_id=row["source_id"],
            source_code=row["source_code"],
            source_name=row["source_name"],
            source_type=row["source_type"],
            country_code=row["country_code"],
            default_language_code=row["default_language_code"] or "",
            homepage_url=row["homepage_url"] or "",
            is_active=bool(row["is_active"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_doc_release_family(self, record: DocReleaseFamilyRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO doc_release_family (
                    release_family_id, source_id, release_code, release_name,
                    topic_code, country_code, frequency, default_language_code,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.release_family_id,
                    record.source_id,
                    record.release_code,
                    record.release_name,
                    record.topic_code,
                    record.country_code,
                    record.frequency,
                    record.default_language_code,
                    record.created_at,
                    record.updated_at,
                ),
            )

    def get_doc_release_family(self, release_family_id: str) -> DocReleaseFamilyRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM doc_release_family WHERE release_family_id = ?",
                (release_family_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_doc_release_family(row)

    def list_doc_release_families(
        self,
        *,
        source_id: str | None = None,
        country_code: str | None = None,
        topic_code: str | None = None,
    ) -> list[DocReleaseFamilyRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic_code:
            conditions.append("topic_code = ?")
            params.append(topic_code)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"SELECT * FROM doc_release_family {where} ORDER BY release_family_id",
                params,
            ).fetchall()
        return [self._row_to_doc_release_family(row) for row in rows]

    def _row_to_doc_release_family(self, row: sqlite3.Row) -> DocReleaseFamilyRecord:
        return DocReleaseFamilyRecord(
            release_family_id=row["release_family_id"],
            source_id=row["source_id"],
            release_code=row["release_code"],
            release_name=row["release_name"],
            topic_code=row["topic_code"],
            country_code=row["country_code"],
            frequency=row["frequency"] or "",
            default_language_code=row["default_language_code"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def upsert_document(self, record: DocumentRecord) -> None:
        published_precision = record.published_precision or _infer_timestamp_precision(
            record.published_at or record.published_date
        )
        if record.published_at:
            if published_precision == "exact":
                published_at = _safe_utc_iso(record.published_at)
            else:
                published_at = record.published_at[:10]
        elif record.published_date:
            if published_precision == "exact":
                published_at = _safe_utc_iso(record.published_date)
            else:
                published_at = record.published_date
        else:
            published_at = ""
        published_epoch_ms = record.published_epoch_ms or _safe_epoch_ms(published_at or record.published_date)
        created_epoch_ms = record.created_epoch_ms or _safe_epoch_ms(record.created_at)
        updated_epoch_ms = record.updated_epoch_ms or _safe_epoch_ms(record.updated_at)
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document (
                    document_id, release_family_id, source_id, canonical_url,
                    title, subtitle, document_type, mime_type,
                    language_code, country_code, topic_code,
                    published_date, published_at, published_precision, published_epoch_ms, status, version_no,
                    parent_document_id, hash_sha256,
                    created_at, updated_at, created_epoch_ms, updated_epoch_ms,
                    institution, authors, data_period, market, asset_class,
                    sector, event_type, impact_level,
                    contains_commentary, confidence, subject_freetext
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.document_id,
                    record.release_family_id or None,
                    record.source_id,
                    record.canonical_url,
                    record.title,
                    record.subtitle,
                    record.document_type,
                    record.mime_type,
                    record.language_code,
                    record.country_code,
                    record.topic_code,
                    record.published_date,
                    published_at or None,
                    published_precision,
                    published_epoch_ms,
                    record.status,
                    record.version_no,
                    record.parent_document_id or None,
                    record.hash_sha256 or None,
                    record.created_at,
                    record.updated_at,
                    created_epoch_ms,
                    updated_epoch_ms,
                    record.institution,
                    record.authors,
                    record.data_period,
                    record.market,
                    record.asset_class,
                    record.sector,
                    record.event_type,
                    record.impact_level,
                    1 if record.contains_commentary else 0,
                    record.confidence,
                    record.subject_freetext,
                ),
            )

    def get_document(self, document_id: str) -> DocumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document WHERE document_id = ?",
                (document_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document(row)

    def get_document_by_url(self, canonical_url: str) -> DocumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document WHERE canonical_url = ?",
                (canonical_url,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document(row)

    def document_exists(self, canonical_url: str) -> bool:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM document WHERE canonical_url = ? LIMIT 1",
                (canonical_url,),
            ).fetchone()
        return row is not None

    def list_documents(
        self,
        *,
        source_id: str | None = None,
        release_family_id: str | None = None,
        country_code: str | None = None,
        topic_code: str | None = None,
        status: str | None = None,
        document_type: str | None = None,
        limit: int = 50,
        days: int | None = None,
    ) -> list[DocumentRecord]:
        conditions: list[str] = []
        params: list[Any] = []
        if source_id:
            conditions.append("source_id = ?")
            params.append(source_id)
        if release_family_id:
            conditions.append("release_family_id = ?")
            params.append(release_family_id)
        if country_code:
            conditions.append("country_code = ?")
            params.append(country_code)
        if topic_code:
            conditions.append("topic_code = ?")
            params.append(topic_code)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if document_type:
            conditions.append("document_type = ?")
            params.append(document_type)
        if days is not None:
            cutoff = (date.today() - timedelta(days=days)).isoformat()
            cutoff_dt = datetime.fromisoformat(cutoff).replace(tzinfo=timezone.utc)
            cutoff_epoch_ms = int(cutoff_dt.timestamp() * 1000)
            conditions.append("(published_epoch_ms >= ? OR published_date >= ?)")
            params.extend([cutoff_epoch_ms, cutoff])
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM document
                {where}
                ORDER BY published_epoch_ms DESC, published_date DESC, document_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_document(row) for row in rows]

    def _row_to_document(self, row: sqlite3.Row) -> DocumentRecord:
        published_precision = row["published_precision"] or _infer_timestamp_precision(
            row["published_at"] or row["published_date"]
        )
        published_at = row["published_at"] or (
            _safe_utc_iso(row["published_date"]) if published_precision == "exact" else row["published_date"]
        )
        return DocumentRecord(
            document_id=row["document_id"],
            release_family_id=row["release_family_id"] or "",
            source_id=row["source_id"],
            canonical_url=row["canonical_url"],
            title=row["title"],
            subtitle=row["subtitle"] or "",
            document_type=row["document_type"],
            mime_type=row["mime_type"],
            language_code=row["language_code"],
            country_code=row["country_code"],
            topic_code=row["topic_code"],
            published_date=row["published_date"],
            published_at=published_at,
            published_precision=published_precision,
            status=row["status"],
            version_no=int(row["version_no"]),
            parent_document_id=row["parent_document_id"] or "",
            hash_sha256=row["hash_sha256"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            published_epoch_ms=(
                int(row["published_epoch_ms"])
                if row["published_epoch_ms"]
                else _safe_epoch_ms(row["published_at"] or row["published_date"])
            ),
            created_epoch_ms=(
                int(row["created_epoch_ms"])
                if row["created_epoch_ms"]
                else _safe_epoch_ms(row["created_at"])
            ),
            updated_epoch_ms=(
                int(row["updated_epoch_ms"])
                if row["updated_epoch_ms"]
                else _safe_epoch_ms(row["updated_at"])
            ),
            institution=row["institution"] or "",
            authors=row["authors"] or "",
            data_period=row["data_period"] or "",
            market=row["market"] or "",
            asset_class=row["asset_class"] or "",
            sector=row["sector"] or "",
            event_type=row["event_type"] or "",
            impact_level=row["impact_level"] or "",
            contains_commentary=bool(row["contains_commentary"] or 0),
            confidence=float(row["confidence"] or 0),
            subject_freetext=row["subject_freetext"] or "",
        )

    def upsert_document_blob(self, record: DocumentBlobRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document_blob (
                    document_blob_id, document_id, blob_role,
                    storage_path, content_text, content_bytes,
                    byte_size, encoding, parser_name, parser_version,
                    extracted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.document_blob_id,
                    record.document_id,
                    record.blob_role,
                    record.storage_path or None,
                    record.content_text or None,
                    record.content_bytes,
                    record.byte_size,
                    record.encoding or None,
                    record.parser_name or None,
                    record.parser_version or None,
                    record.extracted_at or None,
                ),
            )

    def get_document_blob(
        self,
        document_id: str,
        blob_role: str,
    ) -> DocumentBlobRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document_blob WHERE document_id = ? AND blob_role = ?",
                (document_id, blob_role),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_document_blob(row)

    def list_document_blobs(self, document_id: str) -> list[DocumentBlobRecord]:
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT * FROM document_blob WHERE document_id = ? ORDER BY blob_role",
                (document_id,),
            ).fetchall()
        return [self._row_to_document_blob(row) for row in rows]

    def _row_to_document_blob(self, row: sqlite3.Row) -> DocumentBlobRecord:
        return DocumentBlobRecord(
            document_blob_id=row["document_blob_id"],
            document_id=row["document_id"],
            blob_role=row["blob_role"],
            storage_path=row["storage_path"] or "",
            content_text=row["content_text"] or "",
            content_bytes=row["content_bytes"],
            byte_size=int(row["byte_size"]) if row["byte_size"] is not None else 0,
            encoding=row["encoding"] or "",
            parser_name=row["parser_name"] or "",
            parser_version=row["parser_version"] or "",
            extracted_at=row["extracted_at"] or "",
        )

    def upsert_document_extra(self, record: DocumentExtraRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO document_extra (
                    document_id, extra_json
                ) VALUES (?, ?)
                """,
                (
                    record.document_id,
                    json.dumps(record.extra_json, ensure_ascii=False, sort_keys=True),
                ),
            )

    def get_document_extra(self, document_id: str) -> DocumentExtraRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document_extra WHERE document_id = ?",
                (document_id,),
            ).fetchone()
        if row is None:
            return None
        return DocumentExtraRecord(
            document_id=row["document_id"],
            extra_json=json.loads(row["extra_json"]),
        )

    def _fts5_available(self, connection: sqlite3.Connection) -> bool:
        row = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='documents_fts' LIMIT 1"
        ).fetchone()
        return row is not None

    def upsert_document_fts(
        self, *, document_id: str, title: str, body: str
    ) -> None:
        """Rewrite a document's row in the documents_fts index.

        Contentless FTS5 — the virtual table owns its own copy of
        (document_id, title, body). Callers invoke this after writing the
        document + its markdown blob. No-op if FTS5 is unavailable.
        """
        with self._connection(commit=True) as connection:
            if not self._fts5_available(connection):
                return
            connection.execute(
                "DELETE FROM documents_fts WHERE document_id = ?",
                (document_id,),
            )
            connection.execute(
                "INSERT INTO documents_fts(document_id, title, body) "
                "VALUES (?, ?, ?)",
                (document_id, title or "", body or ""),
            )

    def delete_document_fts(self, document_id: str) -> None:
        with self._connection(commit=True) as connection:
            if not self._fts5_available(connection):
                return
            connection.execute(
                "DELETE FROM documents_fts WHERE document_id = ?",
                (document_id,),
            )

    @staticmethod
    def _quote_fts_query(query: str) -> str:
        """Wrap each whitespace-separated token in double quotes so FTS5
        metacharacters (``-``, ``:``, ``"``, ``/``, ``%``, unmatched quotes,
        etc.) never reach the MATCH parser. Callers get literal phrase
        matching per token joined by implicit AND, which is what a
        user-facing keyword search expects.
        """
        tokens = (query or "").split()
        if not tokens:
            return ""
        return " ".join(f'"{t.replace(chr(34), chr(34) * 2)}"' for t in tokens)

    def search_documents(
        self,
        query: str,
        *,
        limit: int = 50,
    ) -> list[DocumentRecord]:
        """BM25-ranked full-text search across document title + body.

        Falls back to LIKE over title + subtitle if FTS5 is unavailable or
        if the MATCH query still fails after sanitization. Pass an empty
        ``query`` to get the empty list — use :meth:`list_documents` for
        the unfiltered recency feed.
        """
        query = (query or "").strip()
        if not query:
            return []
        with self._connection(commit=False) as connection:
            if self._fts5_available(connection):
                sanitized = self._quote_fts_query(query)
                if sanitized:
                    try:
                        rows = connection.execute(
                            """
                            SELECT document.*
                            FROM documents_fts
                            JOIN document
                              ON document.document_id = documents_fts.document_id
                            WHERE documents_fts MATCH ?
                            ORDER BY rank
                            LIMIT ?
                            """,
                            (sanitized, limit),
                        ).fetchall()
                        return [self._row_to_document(r) for r in rows]
                    except sqlite3.OperationalError:
                        pass  # defensive: fall through to LIKE
            like = f"%{query}%"
            rows = connection.execute(
                """
                SELECT * FROM document
                WHERE title LIKE ? OR subtitle LIKE ?
                ORDER BY published_epoch_ms DESC, published_date DESC
                LIMIT ?
                """,
                (like, like, limit),
            ).fetchall()
        return [self._row_to_document(row) for row in rows]

    def seed_doc_sources_and_families(self, source_configs: dict[str, dict[str, dict[str, Any]]]) -> None:
        """Populate doc_source and doc_release_family from scraper config dicts.

        Args:
            source_configs: Mapping of region label to source_id→config dicts,
                e.g. {"us": {"us_bls_cpi": {...}, ...}, "cn": {...}}.
        """
        now = utc_now().isoformat()
        seen_sources: dict[str, DocSourceRecord] = {}

        for _region, sources in source_configs.items():
            for source_id, cfg in sources.items():
                institution = cfg.get("institution", "")
                country = cfg.get("country", "")
                language = cfg.get("language", "en")

                # Derive source-level key: e.g. "us.bls" from "us_bls_cpi"
                parts = source_id.split("_")
                source_key = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else source_id

                if source_key not in seen_sources:
                    source_type = self._infer_source_type(institution)
                    homepage = cfg.get("url", "")
                    seen_sources[source_key] = DocSourceRecord(
                        source_id=source_key,
                        source_code=parts[1] if len(parts) >= 2 else source_id,
                        source_name=institution,
                        source_type=source_type,
                        country_code=country,
                        default_language_code=language,
                        homepage_url=homepage,
                        is_active=True,
                        created_at=now,
                        updated_at=now,
                    )
                    self.upsert_doc_source(seen_sources[source_key])

                # Release family
                release_code = "_".join(parts[2:]) if len(parts) > 2 else parts[-1]
                data_category = cfg.get("data_category", "")
                frequency = self._infer_frequency(data_category)

                family = DocReleaseFamilyRecord(
                    release_family_id=source_id.replace("_", "."),
                    source_id=source_key,
                    release_code=release_code,
                    release_name=cfg.get("data_category", release_code).replace("_", " ").title(),
                    topic_code=data_category,
                    country_code=country,
                    frequency=frequency,
                    default_language_code=language,
                    created_at=now,
                    updated_at=now,
                )
                self.upsert_doc_release_family(family)

    @staticmethod
    def _infer_source_type(institution: str) -> str:
        lower = institution.lower()
        central_banks = [
            "federal reserve", "pboc", "人民银行", "bank of japan", "boj",
            "ecb", "bank of england",
        ]
        if any(cb in lower for cb in central_banks):
            return "central_bank"
        stats = ["统计局", "eurostat", "census", "cabinet office"]
        if any(s in lower for s in stats):
            return "statistics_bureau"
        intl = ["imf", "world bank", "oecd", "s&p global", "caixin"]
        if any(i in lower for i in intl):
            return "intl_org"
        return "government_agency"

    @staticmethod
    def _infer_frequency(data_category: str) -> str:
        monthly = [
            "inflation", "employment", "consumption", "trade",
            "industrial_production", "monetary", "interest_rate",
            "money_supply", "fx_reserves", "fiscal_policy",
            "bond_issuance", "capital_flows", "housing",
            "consumer_sentiment", "manufacturing",
        ]
        if data_category in monthly:
            return "monthly"
        if data_category in ("gdp", "investment"):
            return "quarterly"
        if data_category in ("monetary_policy", "economic_conditions"):
            return "irregular"
        return "irregular"

    def set_document_subjects(
        self, document_id: str, subjects: dict[str, float]
    ) -> None:
        """Replace the item_subjects rows for ``document_id``.

        ``subjects`` is a ``{subject_id: confidence}`` mapping produced by
        :class:`storage.subjects.SubjectTagger` at ingest time. Rewriting
        on every upsert keeps tagging idempotent.
        """
        with self._connection(commit=True) as connection:
            connection.execute(
                "DELETE FROM item_subjects WHERE item_sha = ?",
                (document_id,),
            )
            if subjects:
                connection.executemany(
                    "INSERT INTO item_subjects "
                    "(item_sha, subject_id, confidence) VALUES (?, ?, ?)",
                    [(document_id, sid, float(c)) for sid, c in subjects.items()],
                )

    def list_document_subjects(self, document_id: str) -> list[tuple[str, float]]:
        """Return ``(subject_id, confidence)`` tags for a document,
        ordered by confidence descending."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT subject_id, confidence FROM item_subjects "
                "WHERE item_sha = ? ORDER BY confidence DESC, subject_id",
                (document_id,),
            ).fetchall()
        return [(r[0], float(r[1])) for r in rows]

    def backfill_documents_fts(self) -> int:
        """Populate ``documents_fts`` for documents missing an index row.

        Needed on upgraded DBs that accumulated rows before Step 2 added
        the virtual table. Rebuilds ``(document_id, title, body)`` from
        ``document`` + the most recent ``document_blob`` markdown per
        document. Idempotent: subsequent calls are no-ops once every
        document has an FTS row.
        """
        with self._connection(commit=False) as connection:
            if not self._fts5_available(connection):
                return 0
            rows = connection.execute(
                """
                SELECT d.document_id, d.title,
                       COALESCE(
                           (SELECT content_text FROM document_blob b
                            WHERE b.document_id = d.document_id
                              AND b.blob_role = 'markdown'
                            ORDER BY b.extracted_at DESC LIMIT 1),
                           ''
                       ) AS body
                FROM document d
                WHERE d.document_id NOT IN (
                    SELECT document_id FROM documents_fts
                )
                """
            ).fetchall()
        written = 0
        for row in rows:
            self.upsert_document_fts(
                document_id=row["document_id"],
                title=row["title"] or "",
                body=row["body"] or "",
            )
            written += 1
        return written

    def backfill_document_subjects(self) -> int:
        """Tag documents that have no ``item_subjects`` rows.

        Runs the current :class:`storage.subjects.SubjectTagger` against
        each untagged document's title and writes any title-regex matches.
        Used by upgraded DBs to fill in subject tags for pre-merge rows;
        new ingestion already tags at write time. Idempotent: documents
        that are already tagged (even with zero matches left after a
        re-tag) are skipped via the NOT IN filter.
        """
        from storage.subjects import SubjectTagger
        with self._connection(commit=False) as connection:
            tagger = SubjectTagger(connection)
            untagged = connection.execute(
                """
                SELECT document_id, title FROM document
                WHERE document_id NOT IN (
                    SELECT item_sha FROM item_subjects
                )
                """
            ).fetchall()
        written = 0
        for row in untagged:
            tags = dict(tagger.tag_text(row["title"] or ""))
            if tags:
                self.set_document_subjects(row["document_id"], tags)
                written += 1
        return written

    _DOCUMENT_FAMILY_SQL: dict[str, tuple[str, tuple[Any, ...]]] = {
        "news":           ("document.source_id = ?",                ("news",)),
        "note":           ("document.source_id = ?",                ("notes",)),
        "calendar":       ("document.source_id = ?",                ("calendar",)),
        "release_report": ("instr(document.source_id, '.') > 0",    ()),
    }

    @classmethod
    def _family_predicate(cls, family: str | None) -> tuple[str, tuple[Any, ...]]:
        """Return ``(sql_fragment, params)`` for a doc family filter, or
        an empty clause if ``family`` isn't a known document family."""
        if not family:
            return "", ()
        return cls._DOCUMENT_FAMILY_SQL.get(family, ("", ()))

    def list_items_for_subject(
        self,
        subject_id: str,
        *,
        limit: int = 50,
        min_confidence: float = 0.0,
        document_type: str | None = None,
        country_code: str | None = None,
        family: str | None = None,
    ) -> list[DocumentRecord]:
        """Return documents tagged with ``subject_id`` (confidence >= the
        filter), most-recent first. Joins item_subjects + document and
        applies document_type / country_code / family predicates in SQL
        so the caller doesn't have to post-filter a capped window."""
        sql = [
            "SELECT document.*",
            "FROM item_subjects",
            "JOIN document",
            "  ON document.document_id = item_subjects.item_sha",
            "WHERE item_subjects.subject_id = ?",
            "  AND item_subjects.confidence >= ?",
        ]
        params: list[Any] = [subject_id, min_confidence]
        if document_type:
            sql.append("  AND document.document_type = ?")
            params.append(document_type)
        if country_code:
            sql.append("  AND document.country_code = ?")
            params.append(country_code)
        family_sql, family_params = self._family_predicate(family)
        if family_sql:
            sql.append(f"  AND {family_sql}")
            params.extend(family_params)
        sql.append("ORDER BY document.published_epoch_ms DESC,")
        sql.append("         document.published_date DESC")
        sql.append("LIMIT ?")
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute("\n".join(sql), params).fetchall()
        return [self._row_to_document(r) for r in rows]

    def list_items_combined(
        self,
        *,
        subject_id: str | None,
        query: str | None,
        limit: int = 50,
        min_confidence: float = 0.0,
        document_type: str | None = None,
        country_code: str | None = None,
        family: str | None = None,
    ) -> list[DocumentRecord]:
        """Return documents matching both a subject tag AND an FTS query.

        Filters are applied in SQL so the limit bounds the *final*
        result set — not a candidate pool that might miss valid matches
        beyond the window. Falls back to LIKE when FTS5 is unavailable
        or the MATCH query fails after quoting. When only one of
        ``subject_id`` / ``query`` is given, routes to
        :meth:`list_items_for_subject` or :meth:`search_documents`
        respectively (with the same extra predicates applied).
        """
        subject_id = (subject_id or "").strip() or None
        query = (query or "").strip() or None

        if subject_id and not query:
            return self.list_items_for_subject(
                subject_id, limit=limit, min_confidence=min_confidence,
                document_type=document_type, country_code=country_code,
                family=family,
            )
        if query and not subject_id:
            return self._search_documents_filtered(
                query, limit=limit,
                document_type=document_type, country_code=country_code,
                family=family,
            )
        if not subject_id and not query:
            family_sql, family_params = self._family_predicate(family)
            if not family_sql:
                return self.list_documents(
                    document_type=document_type, country_code=country_code,
                    limit=limit,
                )
            # Recency feed with a family filter — the LIMIT must bound
            # rows *after* the family predicate applies, so run a direct
            # SQL query rather than calling list_documents and trimming.
            type_clause = " AND document.document_type = ?" if document_type else ""
            country_clause = " AND document.country_code = ?" if country_code else ""
            # Order matches the WHERE clause: family_sql first, then the
            # type_clause and country_clause appended after it.
            direct_params: list[Any] = list(family_params)
            if document_type:
                direct_params.append(document_type)
            if country_code:
                direct_params.append(country_code)
            with self._connection(commit=False) as connection:
                rows = connection.execute(
                    f"""
                    SELECT document.*
                    FROM document
                    WHERE {family_sql}
                      {type_clause}{country_clause}
                    ORDER BY document.published_epoch_ms DESC,
                             document.published_date DESC
                    LIMIT ?
                    """,
                    [*direct_params, limit],
                ).fetchall()
            return [self._row_to_document(r) for r in rows]

        # Both set — combine in one query so the limit isn't eaten by a
        # pre-intersection cap.
        type_clause = " AND document.document_type = ?" if document_type else ""
        country_clause = " AND document.country_code = ?" if country_code else ""
        family_sql, family_params = self._family_predicate(family)
        family_clause = f" AND {family_sql}" if family_sql else ""
        extra_params: list[Any] = []
        if document_type:
            extra_params.append(document_type)
        if country_code:
            extra_params.append(country_code)
        extra_params.extend(family_params)

        with self._connection(commit=False) as connection:
            if self._fts5_available(connection):
                sanitized = self._quote_fts_query(query or "")
                if sanitized:
                    try:
                        rows = connection.execute(
                            f"""
                            SELECT document.*
                            FROM documents_fts
                            JOIN document
                              ON document.document_id = documents_fts.document_id
                            JOIN item_subjects
                              ON item_subjects.item_sha = document.document_id
                            WHERE documents_fts MATCH ?
                              AND item_subjects.subject_id = ?
                              AND item_subjects.confidence >= ?
                              {type_clause}{country_clause}{family_clause}
                            ORDER BY rank
                            LIMIT ?
                            """,
                            [sanitized, subject_id, min_confidence, *extra_params, limit],
                        ).fetchall()
                        return [self._row_to_document(r) for r in rows]
                    except sqlite3.OperationalError:
                        pass  # fall through to LIKE
            like = f"%{query}%"
            rows = connection.execute(
                f"""
                SELECT document.*
                FROM document
                JOIN item_subjects
                  ON item_subjects.item_sha = document.document_id
                WHERE item_subjects.subject_id = ?
                  AND item_subjects.confidence >= ?
                  AND (document.title LIKE ? OR document.subtitle LIKE ?)
                  {type_clause}{country_clause}{family_clause}
                ORDER BY document.published_epoch_ms DESC,
                         document.published_date DESC
                LIMIT ?
                """,
                [subject_id, min_confidence, like, like, *extra_params, limit],
            ).fetchall()
        return [self._row_to_document(r) for r in rows]

    _ALIAS_TYPE_TO_INDICATOR_SOURCE: dict[str, str] = {
        "fred_series": "fred",
        "bls_series": "bls",
        "eia_series": "eia",
        "bundesbank_series": "bundesbank",
        "mof_jp_series": "mof_jp",
        "aisi_series": "aisi",
        "ny_fed_series": "nyfed",
        "fedwatch_series": "rateprobability",
        "imf_series": "imf",
        "ecb_series": "ecb",
        "bis_series": "bis",
        "eurostat_series": "eurostat",
        "oecd_series": "oecd",
        "worldbank_series": "worldbank",
        "treasury_series": "treasury_fiscal",
    }

    def list_subject_indicators(
        self, subject_id: str, *, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return indicator observations reached from ``subject_id``.

        Two branches are unioned and tagged ``family = 'economic_data'``:

        1. **concept_map bridge** — ``subject_aliases → concept_map →
           indicators``, matching ``resolve_indicator``'s live chain.
           Carries the ``concept_id`` so cross-source aliases surface.
        2. **direct alias match** — ``subject_aliases → indicators`` keyed
           by ``(alias_type, alias_value)``. Covers subjects whose alias
           points straight at an ``indicators.(source, series_id)`` row
           without a concept_map entry (``commodity.gold`` →
           ``GOLDAMGBD228NLBM``, etc.).

        Dedup is on ``(source, series_id, date)`` — the concept_map row
        wins when both branches resolve to the same observation, so the
        richer ``concept_id`` annotation is preserved."""
        # Per-query fetch is intentionally wider than `limit`: the final
        # slice applies a per-series cap (see below), and a tight
        # per-query LIMIT would drop older series before the fair-share
        # logic runs. The ceiling keeps worst-case memory bounded.
        internal_cap = max(limit * 10, 500)
        with self._connection(commit=False) as connection:
            # Pivot through concept_id so every (source, provider_series_id)
            # row in concept_map that shares a concept with a subject alias
            # contributes. Stopping at the one matched row would hide the
            # cross-source alternates the concept_map is meant to express
            # (``econ.unemployment`` via FRED ``UNRATE`` still needs to
            # surface the BLS ``LNS14000000`` observations under ``UNEMP_US``).
            #
            # The `cm_in.source_id` CASE mirrors the direct-alias branch:
            # if a provider series id is reused across sources (same id
            # under ``fred`` and ``imf``), a ``fred_series`` alias must not
            # attach to the ``imf`` concept row and fan out through
            # unrelated observations.
            case_branches_concept = " ".join(
                f"WHEN sa.alias_type = '{at}' THEN '{src}'"
                for at, src in self._ALIAS_TYPE_TO_INDICATOR_SOURCE.items()
            )
            concept_rows = connection.execute(
                f"""
                SELECT DISTINCT
                  i.source AS source, i.series_id AS series_id,
                  i.date AS date, i.value AS value,
                  cm_out.concept_id AS concept_id
                FROM subject_aliases sa
                JOIN concept_map cm_in
                  ON cm_in.provider_series_id = sa.alias_value
                 AND cm_in.source_id = (CASE {case_branches_concept} END)
                JOIN concept_map cm_out
                  ON cm_out.concept_id = cm_in.concept_id
                JOIN indicators i
                  ON i.source = cm_out.source_id
                 AND i.series_id = cm_out.provider_series_id
                WHERE sa.subject_id = ?
                ORDER BY i.date DESC
                LIMIT ?
                """,
                (subject_id, internal_cap),
            ).fetchall()
            alias_rows: list[Any] = []
            if self._ALIAS_TYPE_TO_INDICATOR_SOURCE:
                placeholders = ",".join(
                    "?" * len(self._ALIAS_TYPE_TO_INDICATOR_SOURCE)
                )
                # Build a CASE so the JOIN predicate compares the alias to
                # the correct `indicators.source` per alias_type.
                case_branches = " ".join(
                    f"WHEN sa.alias_type = '{at}' THEN '{src}'"
                    for at, src in self._ALIAS_TYPE_TO_INDICATOR_SOURCE.items()
                )
                alias_rows = connection.execute(
                    f"""
                    SELECT DISTINCT
                      i.source AS source, i.series_id AS series_id,
                      i.date AS date, i.value AS value
                    FROM subject_aliases sa
                    JOIN indicators i
                      ON i.series_id = sa.alias_value
                     AND i.source = (CASE {case_branches} END)
                    WHERE sa.subject_id = ?
                      AND sa.alias_type IN ({placeholders})
                    ORDER BY i.date DESC
                    LIMIT ?
                    """,
                    (
                        subject_id,
                        *self._ALIAS_TYPE_TO_INDICATOR_SOURCE.keys(),
                        internal_cap,
                    ),
                ).fetchall()

        # Merge both queries keyed on (source, series_id, date) so the
        # concept row wins on dedup (its concept_id annotation is
        # richer) while direct-only series still contribute.
        merged: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in concept_rows:
            key = (r["source"], r["series_id"], r["date"])
            merged[key] = {
                "family": "economic_data",
                "kind": "indicator",
                "source": r["source"],
                "series_id": r["series_id"],
                "concept_id": r["concept_id"],
                "date": r["date"],
                "value": r["value"],
            }
        for r in alias_rows:
            key = (r["source"], r["series_id"], r["date"])
            if key in merged:
                continue
            merged[key] = {
                "family": "economic_data",
                "kind": "indicator",
                "source": r["source"],
                "series_id": r["series_id"],
                "concept_id": "",
                "date": r["date"],
                "value": r["value"],
            }

        # Group by (source, series_id) so each series gets a fair
        # share of the final limit — a subject whose concept path has
        # 100 recent DFF observations must not bury the direct-only
        # FEDFUNDS series. Per-series cap = ceil(limit / N) with a
        # floor of 1 so even many series each surface at least once.
        by_series: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in merged.values():
            by_series.setdefault((row["source"], row["series_id"]), []).append(row)
        if not by_series:
            return []
        per_series_cap = max(1, -(-limit // len(by_series)))  # ceil div
        kept: list[dict[str, Any]] = []
        for rows in by_series.values():
            rows.sort(key=lambda r: r["date"], reverse=True)
            kept.extend(rows[:per_series_cap])
        kept.sort(
            key=lambda r: (r["date"], r["source"], r["series_id"]),
            reverse=True,
        )
        return kept[:limit]

    def list_subject_market_bars(
        self, subject_id: str, *, limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return market-price bars reached from ``subject_id``.

        A subject links to a market instrument when any of its aliases
        match ``market_instruments.primary_ticker``, ``.instrument_id``,
        or appears as a value in ``.provider_symbols_json`` — the last
        branch covers synthetic macro instruments (e.g.
        ``MACRO_RATES_US_10Y``) whose ``provider_symbols_json`` stores
        the underlying indicator series id (``DGS10``). Rows are tagged
        ``family = 'market_price'``."""
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT
                  mi.instrument_id AS instrument_id,
                  mi.primary_ticker AS primary_ticker,
                  mi.asset_class AS asset_class,
                  bars.date AS date,
                  bars.bar_interval AS bar_interval,
                  bars.open AS open, bars.high AS high,
                  bars.low AS low, bars.close AS close,
                  bars.volume AS volume
                FROM subject_aliases sa
                JOIN market_instruments mi
                  ON mi.primary_ticker = sa.alias_value
                  OR mi.instrument_id = sa.alias_value
                  OR EXISTS (
                    SELECT 1 FROM json_each(mi.provider_symbols_json) je
                    WHERE je.value = sa.alias_value
                  )
                JOIN market_price_bars bars
                  ON bars.instrument_id = mi.instrument_id
                WHERE sa.subject_id = ?
                ORDER BY bars.date DESC
                LIMIT ?
                """,
                (subject_id, limit),
            ).fetchall()
        return [
            {
                "family": "market_price",
                "kind": "market_bar",
                "instrument_id": r["instrument_id"],
                "ticker": r["primary_ticker"],
                "asset_class": r["asset_class"],
                "date": r["date"],
                "bar_interval": r["bar_interval"],
                "open": r["open"], "high": r["high"],
                "low": r["low"], "close": r["close"],
                "volume": r["volume"],
            }
            for r in rows
        ]

    def _search_documents_filtered(
        self,
        query: str,
        *,
        limit: int,
        document_type: str | None,
        country_code: str | None,
        family: str | None = None,
    ) -> list[DocumentRecord]:
        """Like :meth:`search_documents` but with document_type / country
        / family predicates applied in SQL so the limit counts post-filter
        rows."""
        if not (document_type or country_code or family):
            return self.search_documents(query, limit=limit)
        query = query.strip()
        if not query:
            return []
        type_clause = " AND document.document_type = ?" if document_type else ""
        country_clause = " AND document.country_code = ?" if country_code else ""
        family_sql, family_params = self._family_predicate(family)
        family_clause = f" AND {family_sql}" if family_sql else ""
        extra_params: list[Any] = []
        if document_type:
            extra_params.append(document_type)
        if country_code:
            extra_params.append(country_code)
        extra_params.extend(family_params)
        with self._connection(commit=False) as connection:
            if self._fts5_available(connection):
                sanitized = self._quote_fts_query(query)
                if sanitized:
                    try:
                        rows = connection.execute(
                            f"""
                            SELECT document.*
                            FROM documents_fts
                            JOIN document
                              ON document.document_id = documents_fts.document_id
                            WHERE documents_fts MATCH ?
                              {type_clause}{country_clause}{family_clause}
                            ORDER BY rank
                            LIMIT ?
                            """,
                            [sanitized, *extra_params, limit],
                        ).fetchall()
                        return [self._row_to_document(r) for r in rows]
                    except sqlite3.OperationalError:
                        pass
            # LIKE fallback uses unqualified column names (no `document.`
            # alias) because the FROM clause is `document` directly — so
            # the family predicate fragment (which says `document.source_id`)
            # needs stripping of the table prefix.
            like = f"%{query}%"
            like_family_clause = family_clause.replace("document.", "")
            rows = connection.execute(
                f"""
                SELECT * FROM document
                WHERE (title LIKE ? OR subtitle LIKE ?)
                  {type_clause}{country_clause}{like_family_clause}
                ORDER BY published_epoch_ms DESC, published_date DESC
                LIMIT ?
                """,
                [like, like, *extra_params, limit],
            ).fetchall()
        return [self._row_to_document(r) for r in rows]

    def get_document_body(self, document_id: str) -> str:
        """Return the markdown body text for a document, empty string
        when no blob has been persisted yet."""
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT content_text FROM document_blob "
                "WHERE document_id = ? AND blob_role = 'markdown' "
                "ORDER BY extracted_at DESC LIMIT 1",
                (document_id,),
            ).fetchone()
        return row["content_text"] if row and row["content_text"] else ""

    def get_document_by_sha(self, hash_sha256: str) -> DocumentRecord | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM document WHERE hash_sha256 = ? LIMIT 1",
                (hash_sha256,),
            ).fetchone()
        return self._row_to_document(row) if row else None
