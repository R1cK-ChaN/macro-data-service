from __future__ import annotations

from typing import Any

from .base import (
    LocalMacroDataServiceBase,
    _DOCUMENT_FAMILIES,
    _document_family,
)


class DocumentsOpsMixin(LocalMacroDataServiceBase):
    def _op_list_items(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Merged feed across the unified surface: documents + indicator
        observations + market-price bars, all filterable by ``subject``.

        Arguments:
          subject         — subject_id to filter on (e.g. "econ.cpi")
          q               — free-text query (FTS5 over title + body;
                            applies to documents only)
          family          — optional family filter (e.g. "economic_data",
                            "market_price", "news"). When unset, rows
                            from every family are returned.
          min_confidence  — default 0.0; filters item_subjects rows
          limit           — default 50, capped at 500
          document_type   — optional exact match (e.g. "report")
          country_code    — optional 2-letter ISO filter

        Each item carries a ``family`` and ``kind`` tag so callers can
        dispatch without a second lookup. Documents keep the existing
        summary shape; indicator / market-bar rows carry only the
        columns relevant to their type.
        """
        subject = (arguments.get("subject") or "").strip() or None
        query = (arguments.get("q") or "").strip() or None
        family_filter = (arguments.get("family") or "").strip() or None
        document_type = (arguments.get("document_type") or "").strip() or None
        country_code = (arguments.get("country_code") or "").strip() or None
        try:
            limit = int(arguments.get("limit") or 50)
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, 500))
        raw_conf = arguments.get("min_confidence")
        try:
            min_conf = float(raw_conf) if raw_conf is not None else 0.0
        except (TypeError, ValueError):
            min_conf = 0.0

        # Subject-driven branches need the yaml vocabulary + default
        # concept_map loaded. Both helpers are idempotent.
        if subject:
            self._ensure_subject_vocabulary()

        items: list[dict[str, Any]] = []

        # Documents branch. Non-document filters (document_type /
        # country_code) only make sense here — indicator + market-bar
        # rows carry no such metadata, so including them when the
        # caller asked for `document_type="report"` would violate the
        # filter contract.
        want_documents = family_filter is None or family_filter in _DOCUMENT_FAMILIES
        if want_documents:
            # The family predicate runs in SQL now (see
            # SQLiteEngineStore._family_predicate) so the server-side
            # limit bounds the matching document rows directly — no
            # widen-then-post-filter sleight of hand.
            candidates = self._store.list_items_combined(
                subject_id=subject,
                query=query,
                limit=limit,
                min_confidence=min_conf,
                document_type=document_type,
                country_code=country_code,
                family=family_filter if family_filter in _DOCUMENT_FAMILIES else None,
            )
            for doc in candidates:
                summary = self._document_summary(doc)
                summary["family"] = _document_family(doc)
                summary["kind"] = "document"
                items.append(summary)

        # Indicator + market-bar branches only run when the caller is
        # asking by subject. Without a subject the join chain
        # (subject_aliases → concept_map / market_instruments) can't
        # produce meaningful rows, and widening the branches to "all
        # indicators" would explode the result set.
        if subject and not query and not document_type and not country_code:
            if family_filter in (None, "economic_data"):
                items.extend(
                    self._store.list_subject_indicators(subject, limit=limit)
                )
            if family_filter in (None, "market_price"):
                items.extend(
                    self._store.list_subject_market_bars(subject, limit=limit)
                )

        # When a family filter is set, only one branch ran and its own
        # SQL LIMIT has already capped the result to `limit`. Without a
        # filter, every branch returns up to `limit` rows so the
        # envelope can carry up to ``3 * limit`` — intentional, since
        # the callers asked for cross-type visibility and dropping the
        # later branches to fit a global `limit` would re-introduce the
        # crowding bug Codex flagged.
        if family_filter:
            items = items[:limit]
        return {"total": len(items), "items": items}

    def _op_get_document(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Fetch one document (metadata + markdown body + subject tags).

        Accepts either ``document_id`` (internal TEXT id) or
        ``hash_sha256`` (content hash stored on the row).
        """
        document_id = (arguments.get("document_id") or "").strip()
        sha = (arguments.get("hash_sha256") or "").strip()
        if not document_id and not sha:
            return {"error": "document_id or hash_sha256 is required"}
        doc = (
            self._store.get_document(document_id)
            if document_id
            else self._store.get_document_by_sha(sha)
        )
        if doc is None:
            return {"document": None}
        body = self._store.get_document_body(doc.document_id)
        subjects = self._store.list_document_subjects(doc.document_id)
        return {
            "document": self._document_summary(doc),
            "body": body,
            "subjects": [{"subject_id": s, "confidence": c} for s, c in subjects],
        }

    def _op_list_subjects(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """List the subject vocabulary. Seeds the yaml on first call so
        the response is always complete on a fresh DB."""
        del arguments
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass  # best-effort; yaml may be unavailable in test environments
        return {"subjects": self._store.list_subjects()}

    def _op_backfill_document_indexes(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Run the one-shot FTS + subject-tag backfills.

        Needed on DBs that accumulated ``document`` rows before Step 2
        added ``documents_fts`` and Step 3 started calling
        ``set_document_subjects`` at ingest. Both helpers are idempotent,
        so calling this on a fresh DB does no work.
        """
        del arguments
        # Ensure the vocabulary is seeded before tagging runs — otherwise
        # the tagger sees an empty alias list and nothing gets tagged.
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass
        fts_written = self._store.backfill_documents_fts()
        subjects_tagged = self._store.backfill_document_subjects()
        return {
            "fts_rows_written": fts_written,
            "documents_subject_tagged": subjects_tagged,
        }

    @staticmethod
    def _document_summary(doc: Any) -> dict[str, Any]:
        """Shape a DocumentRecord for API responses — omit internal-only
        fields (epoch_ms duplicates, release_family_id) to keep the
        payload lean."""
        return {
            "document_id": doc.document_id,
            "hash_sha256": doc.hash_sha256,
            "title": doc.title,
            "subtitle": doc.subtitle,
            "source_id": doc.source_id,
            "document_type": doc.document_type,
            "country_code": doc.country_code,
            "language_code": doc.language_code,
            "topic_code": doc.topic_code,
            "published_date": doc.published_date,
            "published_at": doc.published_at,
            "institution": doc.institution,
            "authors": doc.authors,
            "data_period": doc.data_period,
            "market": doc.market,
            "asset_class": doc.asset_class,
            "sector": doc.sector,
            "event_type": doc.event_type,
            "impact_level": doc.impact_level,
            "contains_commentary": doc.contains_commentary,
            "confidence": doc.confidence,
            "subject_freetext": doc.subject_freetext,
            "canonical_url": doc.canonical_url,
        }
