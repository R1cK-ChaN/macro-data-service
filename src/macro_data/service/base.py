from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import logging
from typing import Any
from urllib.parse import urlparse

from contracts import format_epoch_iso

logger = logging.getLogger(__name__)

_NEWS_PRESETS: dict[str, tuple[str, ...]] = {
    "all": ("investing", "forexfactory", "tradingeconomics", "reuters", "bloomberg", "ft", "wsj"),
    "premium": ("bloomberg", "ft", "wsj", "reuters"),
    "free": ("investing", "forexfactory", "tradingeconomics"),
}

# Maps a DocumentRecord.source_id to its ingestion family tag. Exact-match
# sources are listed here; `GovReportIngestionClient` writes documents with
# derived source ids like ``us.bls`` / ``cn.stats`` (country.agency), so
# ``_document_family`` also treats any dotted source id as a gov report
# and tags it ``release_report``.
_DOCUMENT_SOURCE_FAMILY: dict[str, str] = {
    "news": "news",
    "notes": "note",
    "calendar": "calendar",
}


def _document_family(doc: Any) -> str:
    source_id = getattr(doc, "source_id", "") or ""
    fam = _DOCUMENT_SOURCE_FAMILY.get(source_id)
    if fam:
        return fam
    # Gov report source ids are derived as "country.agency" — treat any
    # dotted id as a gov release so the family filter actually matches.
    if "." in source_id:
        return "release_report"
    return ""


# Families whose rows live in the document table (as opposed to
# indicators). When the caller's family filter is one of these, the
# indicator branch is skipped. The market-price branch retired in issue
# #118 P4 — market data now lives in ClickHouse and ``list_items`` stays
# macro-line-only.
_DOCUMENT_FAMILIES: frozenset[str] = frozenset(
    {"news", "note", "calendar", "release_report"}
)
_VALID_RATE_TYPES = {"sofr", "effr", "obfr", "all"}
_ARTICLE_DOMAIN_MAP: dict[str, str] = {
    "bloomberg.com": "bloomberg",
    "ft.com": "ft",
    "wsj.com": "wsj",
    "reuters.com": "reuters",
}


def _detect_article_domain(url: str) -> str | None:
    hostname = urlparse(url).hostname or ""
    for domain, key in _ARTICLE_DOMAIN_MAP.items():
        if hostname == domain or hostname.endswith("." + domain):
            return key
    return None


class LocalMacroDataServiceBase:
    def __init__(
        self,
        *,
        store: Any,
        market_store: Any | None = None,
        ingestion: Any | None = None,
        health: Any | None = None,
    ) -> None:
        self._store = store
        self._market_store = market_store
        self._ingestion = ingestion
        self._health = health
        self._ontology_seeded = False
        self._subject_vocabulary_seeded = False

    def invoke(self, operation: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        handler = getattr(self, f"_op_{operation}", None)
        if handler is None:
            raise KeyError(f"unknown macro-data operation: {operation}")
        return handler(arguments or {})

    def _store_is_read_only(self) -> bool:
        return bool(getattr(self._store, "read_only", False))

    def _ensure_structural_ontology(self) -> None:
        if self._ontology_seeded:
            return
        if self._store_is_read_only():
            self._ontology_seeded = True
            return
        seed = getattr(self._store, "seed_structural_ontology", None)
        if callable(seed):
            seed()
        self._ontology_seeded = True

    def _ensure_subject_vocabulary(self) -> None:
        """Seed subject_aliases + concept_map once per process.

        The cross-type ``list_items`` branches (indicators, market bars)
        read ``subject_aliases`` and ``concept_map``. On a DB populated
        only through time-series refresh paths neither table is filled,
        so we load the yaml vocabulary + default concept map on demand
        — both helpers are idempotent so repeat calls are cheap."""
        if self._subject_vocabulary_seeded:
            return
        if self._store_is_read_only():
            self._subject_vocabulary_seeded = True
            return
        try:
            from storage.subjects import sync_from_yaml
            sync_from_yaml(self._store)
        except (AttributeError, TypeError, FileNotFoundError):
            pass
        seed_cm = getattr(self._store, "seed_concept_map", None)
        if callable(seed_cm):
            try:
                seed_cm()
            except Exception:
                logger.warning("seed_concept_map failed", exc_info=True)
        self._subject_vocabulary_seeded = True
