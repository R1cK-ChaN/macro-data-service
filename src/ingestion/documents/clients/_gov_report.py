"""Government report ingestion client."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, datetime

from contracts import normalize_utc_iso, to_epoch_ms
from ingestion.scrapers.gov_report import GovReportClient, GovReportItem
from ingestion.url_canon import canonicalize_url
from storage import (
    DocumentBlobRecord,
    DocumentExtraRecord,
    DocumentRecord,
    NewsArticleRecord,
    SQLiteEngineStore,
)

logger = logging.getLogger(__name__)

_MIN_GOV_REPORT_CONTENT_CHARS = 100


def _infer_publish_precision(value: str | None) -> str:
    if not value:
        return "estimated"
    if re.search(r"[T ]\d{1,2}:\d{2}", value):
        return "exact"
    return "date_only"


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class GovReportIngestionClient:
    """Fetches government reports and stores them into the normalized
    document tables (doc_source / doc_release_family / document /
    document_blob / document_extra) **and** the legacy news_articles
    table so existing consumers keep working."""

    def __init__(self) -> None:
        self._client = GovReportClient()
        self._seeded = False

    def _ensure_seed(self, store: SQLiteEngineStore) -> None:
        if self._seeded:
            return
        from ingestion.scrapers.gov_report import (
            _US_SOURCES,
            _CN_SOURCES,
            _JP_SOURCES,
            _EU_SOURCES,
        )
        store.seed_doc_sources_and_families({
            "us": _US_SOURCES,
            "cn": _CN_SOURCES,
            "jp": _JP_SOURCES,
            "eu": _EU_SOURCES,
        })
        self._seeded = True

    @staticmethod
    def _gov_document_type(data_category: str) -> str:
        """Map data_category to a valid document.document_type value."""
        mapping = {
            "monetary_policy": "statement",
            "economic_conditions": "bulletin",
            "speeches": "speech",
            "press_releases": "press_release",
        }
        return mapping.get(data_category, "release")

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        items = self.fetch_items()
        return RefreshStats(source="gov_reports", count=self.store_items(store, items))

    def fetch_items(self) -> list[GovReportItem]:
        return self._client.fetch_all()

    def store_items(self, store: SQLiteEngineStore, items: list[GovReportItem]) -> int:
        self._ensure_seed(store)
        count = 0
        failures: list[str] = []
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat()
        now_epoch_ms = int(now_dt.timestamp() * 1000)
        for item in items:
            try:
                content = (item.content_markdown or "").strip()
                if len(content) < _MIN_GOV_REPORT_CONTENT_CHARS:
                    content = (item.description or "").strip()
                if len(content) < _MIN_GOV_REPORT_CONTENT_CHARS:
                    logger.info(
                        "Gov report stored with minimal content: %s (%d chars)",
                        item.source_id, len(content),
                    )
                canonical = canonicalize_url(item.url)
                url_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                published_precision = item.published_precision or _infer_publish_precision(item.published_at)
                try:
                    if published_precision == "exact" and item.published_at:
                        published_at = normalize_utc_iso(item.published_at)
                        published_epoch_ms = to_epoch_ms(item.published_at)
                    elif published_precision == "date_only" and item.published_at:
                        published_at = item.published_at[:10]
                        published_epoch_ms = to_epoch_ms(published_at)
                    else:
                        published_at = now_iso
                        published_epoch_ms = now_epoch_ms
                        published_precision = "estimated"
                except ValueError:
                    published_at = now_iso
                    published_epoch_ms = now_epoch_ms
                    published_precision = "estimated"
                published_date = published_at[:10]

                # --- Normalized document storage ---
                if not store.document_exists(canonical):
                    doc_id = url_hash[:16]
                    release_family_id = item.source_id.replace("_", ".")
                    parts = item.source_id.split("_")
                    source_key = f"{parts[0]}.{parts[1]}" if len(parts) >= 2 else item.source_id

                    doc = DocumentRecord(
                        document_id=doc_id,
                        release_family_id=release_family_id,
                        source_id=source_key,
                        canonical_url=canonical,
                        title=item.title,
                        subtitle="",
                        document_type=self._gov_document_type(item.data_category),
                        mime_type="text/html",
                        language_code=item.language,
                        country_code=item.country,
                        topic_code=item.data_category,
                        published_date=published_date,
                        published_at=published_at,
                        published_precision=published_precision,
                        status="published",
                        version_no=1,
                        parent_document_id="",
                        hash_sha256=url_hash,
                        created_at=now_iso,
                        updated_at=now_iso,
                        published_epoch_ms=published_epoch_ms,
                        created_epoch_ms=now_epoch_ms,
                        updated_epoch_ms=now_epoch_ms,
                    )
                    store.upsert_document(doc)

                    if content:
                        blob = DocumentBlobRecord(
                            document_blob_id=f"{doc_id}_md",
                            document_id=doc_id,
                            blob_role="markdown",
                            storage_path="",
                            content_text=content,
                            content_bytes=None,
                            byte_size=len(content.encode("utf-8")),
                            encoding="utf-8",
                            parser_name="markdownify",
                            parser_version="",
                            extracted_at=now_iso,
                        )
                        store.upsert_document_blob(blob)

                    if item.raw_json or item.importance:
                        extra_data = dict(item.raw_json) if item.raw_json else {}
                        extra_data["importance"] = item.importance
                        extra_data["institution"] = item.institution
                        extra_data["description"] = item.description
                        extra_data["published_precision"] = published_precision
                        store.upsert_document_extra(DocumentExtraRecord(
                            document_id=doc_id,
                            extra_json=extra_data,
                        ))

                # --- Legacy news_articles storage ---
                if store.news_article_exists(url_hash):
                    continue
                ts = int(published_epoch_ms / 1000)
                record = NewsArticleRecord(
                    url_hash=url_hash,
                    source_feed=f"gov_{item.source_id}",
                    feed_category="government",
                    title=item.title,
                    url=item.url,
                    timestamp=ts,
                    description=item.description,
                    content_markdown=content,
                    impact_level=item.importance or "medium",
                    finance_category=item.data_category,
                    confidence=0.8,
                    content_fetched=bool(content),
                    institution=item.institution,
                    country=item.country,
                    document_type="government_report",
                    language=item.language,
                )
                store.upsert_news_article(record)
                count += 1
            except Exception as exc:
                logger.warning("Gov report storage failed: %s", item.source_id, exc_info=True)
                failures.append(f"{item.source_id}: {exc}")
                continue
        if failures and count == 0 and items:
            raise RuntimeError(
                f"gov report storage failed for all {len(items)} items; first error: {failures[0]}"
            )
        if failures:
            logger.warning(
                "gov report partial storage failures: %d/%d; first error: %s",
                len(failures),
                len(items),
                failures[0],
            )
        return count
