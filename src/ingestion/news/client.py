"""News ingestion client.

Issue #113 P1 removed the LLM-based metadata extraction from the ingest
path; the data layer writes the slim raw record (title, url, description,
content, language) and downstream services run their own enrichment.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone

import feedparser
import requests
from bs4 import BeautifulSoup

from contracts import format_epoch_iso
from ingestion.news._classify import Deduplicator
from ingestion.news._config import get_feeds
from ingestion.news._fetcher import ArticleFetcher
from ingestion._shared.url_canon import canonicalize_url, content_hash
from ingestion.news._types import PreparedNewsRecord, RawNewsEntry
from storage import (
    DocumentBlobRecord,
    DocumentRecord,
    NewsArticleRecord,
    SQLiteEngineStore,
)
from storage.subjects import SubjectTagger, sync_from_yaml

logger = logging.getLogger(__name__)
_MIN_ARTICLE_CONTENT_CHARS = 100

# News articles share the `document` table with gov reports and notes.
# The document_type CHECK constraint already allows 'report' — news
# articles fit there cleanly; feed-level attribution lives in
# release_family_id (the feed name) and the structured columns.
_NEWS_DOC_SOURCE_ID = "news"
_NEWS_DOCUMENT_TYPE = "report"
# ISO 3166 user-assigned placeholder used when the per-feed config
# doesn't carry a country (global / multi-region wires). 'US' would
# wrongly bucket ECB / BoJ / global news under US filters.
_UNKNOWN_COUNTRY_CODE = "XX"


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


class NewsIngestionClient:
    def __init__(
        self,
        *,
        timeout: int = 15,
        max_items_per_feed: int = 10,
        article_timeout: int = 20,
        max_content_chars: int = 15_000,
    ) -> None:
        self._timeout = timeout
        self._max_items_per_feed = max_items_per_feed
        self._article_fetcher = ArticleFetcher(
            timeout=article_timeout,
            max_content_chars=max_content_chars,
        )
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        })

    @staticmethod
    def _finalize_stage(
        stage: str,
        *,
        failures: list[str],
        successes: int,
        attempted: int,
        subject: str,
    ) -> None:
        if not failures:
            return
        if attempted > 0 and successes == 0:
            raise RuntimeError(
                f"news {stage} failed for all {attempted} {subject}; first error: {failures[0]}"
            )
        logger.warning(
            "news %s partial failures: %d/%d; first error: %s",
            stage,
            len(failures),
            attempted,
            failures[0],
        )

    def refresh(
        self,
        store: SQLiteEngineStore,
        *,
        category: str | None = None,
    ) -> RefreshStats:
        entries = self.fetch_entries(category=category)
        normalized = self.normalize_entries(entries)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(store, valid)
        return RefreshStats(source="news", count=self.store_articles(store, deduplicated))

    def fetch_entries(self, *, category: str | None = None) -> list[RawNewsEntry]:
        entries: list[RawNewsEntry] = []
        attempted_feeds = 0
        successful_feeds = 0
        failures: list[str] = []
        for feed in get_feeds(category):
            attempted_feeds += 1
            try:
                resp = self._session.get(feed.url, timeout=self._timeout)
                resp.raise_for_status()
                parsed = feedparser.parse(resp.text)
            except Exception as exc:
                logger.warning("news feed fetch failed: %s", feed.url, exc_info=True)
                failures.append(f"{feed.name}: {exc}")
                continue
            successful_feeds += 1

            for entry in parsed.entries[: self._max_items_per_feed]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                raw_desc = entry.get("summary", "") or entry.get("description", "")
                description = BeautifulSoup(raw_desc, "html.parser").get_text(" ", strip=True)
                ts = 0
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    ts = int(datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).timestamp())
                if not ts:
                    ts = int(datetime.now(timezone.utc).timestamp())
                entries.append(
                    RawNewsEntry(
                        source_feed=feed.name,
                        feed_category=feed.category,
                        title=title,
                        url=link,
                        description=description,
                        timestamp=ts,
                    )
                )
            time.sleep(0.3)
        self._finalize_stage(
            "feed fetch",
            failures=failures,
            successes=successful_feeds,
            attempted=attempted_feeds,
            subject="feeds",
        )
        return entries

    def normalize_entries(self, entries: list[RawNewsEntry]) -> list[PreparedNewsRecord]:
        normalized: list[PreparedNewsRecord] = []
        for entry in entries:
            try:
                canonical = canonicalize_url(entry.url)
                url_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                title_hash = content_hash(entry.title, entry.timestamp)
                normalized.append(
                    PreparedNewsRecord(
                        source_feed=entry.source_feed,
                        feed_category=entry.feed_category,
                        description=entry.description,
                        timestamp=entry.timestamp,
                        canonical_url=canonical,
                        raw_url=entry.url,
                        raw_title=entry.title,
                        url_hash=url_hash,
                        title_hash=title_hash,
                    )
                )
            except Exception:
                continue
        return normalized

    def validate_entries(self, entries: list[PreparedNewsRecord]) -> list[PreparedNewsRecord]:
        valid: list[PreparedNewsRecord] = []
        for entry in entries:
            if not entry.raw_title.strip():
                continue
            if not entry.raw_url.strip():
                continue
            if not entry.canonical_url.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(
        self,
        store: SQLiteEngineStore,
        entries: list[PreparedNewsRecord],
    ) -> list[PreparedNewsRecord]:
        deduplicator = Deduplicator(threshold=0.6)
        deduplicator.seed(store.get_recent_news_titles(hours=24))
        unique: list[PreparedNewsRecord] = []
        for entry in entries:
            if store.fingerprint_exists(url_hash=entry.url_hash, title_hash=entry.title_hash):
                continue
            if deduplicator.is_duplicate(entry.raw_title):
                continue
            unique.append(entry)
        return unique

    def store_articles(self, store: SQLiteEngineStore, entries: list[PreparedNewsRecord]) -> int:
        # Seed the unified-document plumbing for the news channel and
        # build the subject tagger once per batch. Both paths tolerate
        # mock stores (regression tests) by swallowing the AttributeError
        # / TypeError that Mock()._connection(...) raises when used as a
        # context manager.
        self._ensure_news_doc_source(store)
        try:
            sync_from_yaml(store)
        except (AttributeError, TypeError):
            pass
        tagger: SubjectTagger | None
        try:
            with store._connection(commit=False) as c:
                tagger = SubjectTagger(c)
        except (AttributeError, TypeError):
            tagger = None  # store didn't expose _connection (mock/double)
        count = 0
        failures: list[str] = []
        for entry in entries:
            try:
                article = self._article_fetcher.fetch_article(entry.raw_url, entry.description)
                if len((article.content or "").strip()) < _MIN_ARTICLE_CONTENT_CHARS:
                    logger.info("news article dropped for low content: %s", entry.raw_url)
                    continue
                record = NewsArticleRecord(
                    url_hash=entry.url_hash,
                    source_feed=entry.source_feed,
                    feed_category=entry.feed_category,
                    title=entry.raw_title,
                    url=entry.raw_url,
                    timestamp=entry.timestamp,
                    description=entry.description,
                    content_markdown=article.content,
                    content_fetched=article.fetched,
                )
                store.upsert_news_article(record)
                store.insert_fingerprint(
                    url_hash=entry.url_hash,
                    title_hash=entry.title_hash,
                    canonical_url=entry.canonical_url,
                    raw_url=entry.raw_url,
                    title=entry.raw_title,
                    source_feed=entry.source_feed,
                )
                # Mirror the article into the unified document surface so
                # it's visible to /items and documents_fts alongside gov
                # reports and notes.
                self._mirror_as_document(
                    store=store,
                    tagger=tagger,
                    entry=entry,
                    article_content=article.content or "",
                )
                count += 1
                time.sleep(0.5)
            except Exception as exc:
                logger.warning("news article storage failed: %s", entry.raw_url, exc_info=True)
                failures.append(f"{entry.raw_url}: {exc}")
                continue
        self._finalize_stage(
            "article storage",
            failures=failures,
            successes=count,
            attempted=len(entries),
            subject="articles",
        )
        return count

    # ── Unified document surface ────────────────────────────────────────

    @staticmethod
    def _ensure_news_doc_source(store: SQLiteEngineStore) -> None:
        """Create the 'news' row in doc_source if it isn't there yet.

        Every document.source_id is a FK into doc_source; gov reports seed
        per-agency rows via seed_doc_sources_and_families, but news lives
        under one umbrella 'news' source because individual feeds are
        carried on release_family_id and the structured columns. Mock
        stores used in regression tests raise on context-manager entry —
        swallow and skip, since those tests don't exercise the unified-
        document surface.
        """
        try:
            with store._connection(commit=True) as c:
                now = datetime.now(timezone.utc).isoformat()
                # doc_source.source_type CHECK allowlist: government_agency,
                # central_bank, intl_org, statistics_bureau, news_agency.
                # News feeds land under 'news_agency' so the FK from
                # document.source_id stays valid.
                c.execute(
                    "INSERT OR IGNORE INTO doc_source "
                    "(source_id, source_code, source_name, source_type, "
                    " country_code, is_active, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
                    (_NEWS_DOC_SOURCE_ID, "NEWS", "News feeds (aggregated)",
                     "news_agency", "US", now, now),
                )
        except (AttributeError, TypeError):
            return  # mock store — test harness only, skip

    def _mirror_as_document(
        self,
        *,
        store: SQLiteEngineStore,
        tagger: SubjectTagger | None,
        entry: PreparedNewsRecord,
        article_content: str,
    ) -> None:
        """Write one news article as a row in the document surface.

        Idempotent: if a document with this canonical_url already exists,
        skip the write (news_articles already deduped upstream, but a
        simultaneous gov_report ingest can race on the same URL).
        Issue #113 P1 dropped the LLM-extraction fields; the document row
        carries only what the raw feed provides.
        """
        if store.document_exists(entry.canonical_url):
            return
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        now_epoch_ms = int(now_dt.timestamp() * 1000)
        doc_id = entry.url_hash[:16]
        published_at = format_epoch_iso(entry.timestamp)
        published_date = published_at[:10]
        published_epoch_ms = entry.timestamp * 1000

        doc = DocumentRecord(
            document_id=doc_id,
            release_family_id="",
            source_id=_NEWS_DOC_SOURCE_ID,
            canonical_url=entry.canonical_url,
            title=entry.raw_title,
            subtitle=entry.source_feed,
            document_type=_NEWS_DOCUMENT_TYPE,
            mime_type="text/html",
            language_code="en",
            country_code=_UNKNOWN_COUNTRY_CODE,
            topic_code=entry.feed_category[:50],
            published_date=published_date,
            published_at=published_at,
            published_precision="exact",
            status="published",
            version_no=1,
            parent_document_id="",
            hash_sha256=entry.url_hash,
            created_at=now_iso,
            updated_at=now_iso,
            published_epoch_ms=published_epoch_ms,
            created_epoch_ms=now_epoch_ms,
            updated_epoch_ms=now_epoch_ms,
        )
        store.upsert_document(doc)

        if article_content:
            blob = DocumentBlobRecord(
                document_blob_id=f"{doc_id}_md",
                document_id=doc_id,
                blob_role="markdown",
                storage_path="",
                content_text=article_content,
                content_bytes=None,
                byte_size=len(article_content.encode("utf-8")),
                encoding="utf-8",
                parser_name="news_article_fetcher",
                parser_version="",
                extracted_at=now_iso,
            )
            store.upsert_document_blob(blob)

        store.upsert_document_fts(
            document_id=doc_id,
            title=entry.raw_title,
            body=article_content,
        )

        if tagger is not None:
            tags = dict(tagger.tag_text(entry.raw_title))
            if tags:
                store.set_document_subjects(doc_id, tags)

    def close(self) -> None:
        self._article_fetcher.close()
