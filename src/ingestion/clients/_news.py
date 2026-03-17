"""News ingestion client."""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from contracts import format_epoch_iso, normalize_utc_iso, to_epoch_ms
from env import get_env_value
from ingestion.news_classify import Deduplicator
from ingestion.news_extract import extract_news_metadata
from ingestion.news_feeds import get_feeds
from ingestion.news_fetcher import ArticleFetcher
from ingestion.url_canon import canonicalize_url, content_hash
from storage import NewsArticleRecord, SQLiteEngineStore

logger = logging.getLogger(__name__)


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
        for feed in get_feeds(category):
            try:
                resp = self._session.get(feed.url, timeout=self._timeout)
                resp.raise_for_status()
                parsed = feedparser.parse(resp.text)
            except Exception:
                continue

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
        count = 0
        for entry in entries:
            try:
                article = self._article_fetcher.fetch_article(entry.raw_url, entry.description)
                extraction = extract_news_metadata(
                    title=entry.raw_title,
                    description=entry.description,
                    content_markdown=article.content,
                    source_feed=entry.source_feed,
                    feed_category=entry.feed_category,
                    published_at=format_epoch_iso(entry.timestamp),
                )
                record = NewsArticleRecord(
                    url_hash=entry.url_hash,
                    source_feed=entry.source_feed,
                    feed_category=entry.feed_category,
                    title=extraction.title,
                    url=entry.raw_url,
                    timestamp=entry.timestamp,
                    description=entry.description,
                    content_markdown=article.content,
                    impact_level=extraction.impact_level,
                    finance_category=extraction.finance_category,
                    confidence=extraction.confidence,
                    content_fetched=article.fetched,
                    institution=extraction.institution,
                    country=extraction.country,
                    market=extraction.market,
                    asset_class=extraction.asset_class,
                    sector=extraction.sector,
                    document_type=extraction.document_type,
                    event_type=extraction.event_type,
                    subject=extraction.subject,
                    subject_id=extraction.subject_id,
                    data_period=extraction.data_period,
                    contains_commentary=extraction.contains_commentary,
                    language=extraction.language,
                    authors=extraction.authors,
                    extraction_provider=extraction.extraction_provider,
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
                count += 1
                time.sleep(0.5)
            except Exception:
                continue
        return count

    def close(self) -> None:
        self._article_fetcher.close()


