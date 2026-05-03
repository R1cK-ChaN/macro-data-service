"""News-domain query helpers for SQLiteEngineStore.

Covers news_articles + article_fingerprint plus the news-context
retrieval used by downstream callers.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.

Issue #113 P1 stripped the LLM-enrichment columns from `news_articles`;
the data layer now writes only the raw fetched fields. Downstream
services that want enrichment run their own pipeline against these rows.
"""

from __future__ import annotations

import hashlib
import math
import sqlite3
from datetime import timedelta
from typing import Any

from contracts import (
    epoch_to_datetime,
    format_epoch_iso,
    format_epoch_iso_in_timezone,
    utc_now,
)
from storage.models.news import NewsArticleRecord


class _NewsQueriesMixin:
    # Half-life days used by ``get_news_context``: more recent articles
    # weigh more without any per-article impact-level signal (issue #113 P1).
    _DEFAULT_HALF_LIFE_DAYS = 3

    _TIME_DECAY_MAX_BOOST = 1.5

    _TIME_DECAY_MIN_BOOST = 0.1

    def upsert_news_article(self, article: NewsArticleRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO news_articles (
                    url_hash, source_feed, feed_category, title, url,
                    timestamp, description, content_markdown,
                    content_fetched, language, authors, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    article.url_hash,
                    article.source_feed,
                    article.feed_category,
                    article.title,
                    article.url,
                    article.timestamp,
                    article.description,
                    article.content_markdown,
                    int(article.content_fetched),
                    article.language,
                    article.authors,
                    utc_now().isoformat(),
                ),
            )

    def list_recent_news(
        self,
        *,
        limit: int = 20,
        days: int = 7,
        feed_category: str | None = None,
    ) -> list[NewsArticleRecord]:
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM news_articles
                WHERE {' AND '.join(conditions)}
                ORDER BY timestamp DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_news_article(row) for row in rows]

    def search_news(self, query: str, *, limit: int = 20) -> list[NewsArticleRecord]:
        with self._connection(commit=False) as connection:
            try:
                rows = connection.execute(
                    """
                    SELECT n.* FROM news_articles n
                    JOIN news_fts ON news_fts.rowid = n.id
                    WHERE news_fts MATCH ?
                    ORDER BY n.timestamp DESC, n.id DESC
                    LIMIT ?
                    """,
                    (query, limit),
                ).fetchall()
            except sqlite3.OperationalError:
                pattern = f"%{query}%"
                rows = connection.execute(
                    """
                    SELECT * FROM news_articles
                    WHERE title LIKE ? OR description LIKE ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT ?
                    """,
                    (pattern, pattern, limit),
                ).fetchall()
        return [self._row_to_news_article(row) for row in rows]

    def get_news_context(
        self,
        *,
        query: str | None = None,
        days: int = 7,
        limit: int = 15,
        feed_category: str | None = None,
        display_timezone: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve news with time-decay scoring (no per-article impact weight)."""
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)

        with self._connection(commit=False) as connection:
            if query:
                try:
                    rows = connection.execute(
                        f"""
                        SELECT n.* FROM news_articles n
                        JOIN news_fts ON news_fts.rowid = n.id
                        WHERE news_fts MATCH ? AND {' AND '.join(conditions)}
                        """,
                        [query] + params,
                    ).fetchall()
                except sqlite3.OperationalError:
                    pattern = f"%{query}%"
                    conditions.append("(title LIKE ? OR description LIKE ?)")
                    params.extend([pattern, pattern])
                    rows = connection.execute(
                        f"""
                        SELECT * FROM news_articles
                        WHERE {' AND '.join(conditions)}
                        """,
                        params,
                    ).fetchall()
            else:
                rows = connection.execute(
                    f"""
                    SELECT * FROM news_articles
                    WHERE {' AND '.join(conditions)}
                    """,
                    params,
                ).fetchall()

        now = utc_now()
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            article = self._row_to_news_article(row)
            pub = epoch_to_datetime(article.timestamp)
            age_days = max((now - pub).total_seconds() / 86400, 0.0)
            time_decay = self._TIME_DECAY_MIN_BOOST + (
                (self._TIME_DECAY_MAX_BOOST - self._TIME_DECAY_MIN_BOOST)
                * math.pow(2, -age_days / self._DEFAULT_HALF_LIFE_DAYS)
            )

            desc = article.description
            if len(desc) > 500:
                desc = desc[:500] + "..."
            payload = {
                "source_feed": article.source_feed,
                "feed_category": article.feed_category,
                "title": article.title,
                "url": article.url,
                "timestamp": article.timestamp,
                "published_at": format_epoch_iso(article.timestamp),
                "description": desc,
                "score": round(time_decay, 4),
            }
            if display_timezone:
                try:
                    payload["published_at_local"] = format_epoch_iso_in_timezone(
                        article.timestamp,
                        display_timezone,
                    )
                    payload["published_timezone"] = display_timezone
                except ValueError:
                    pass
            scored.append((time_decay, payload))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for _, item in scored[:limit]]

    def get_recent_news_titles(self, *, hours: int = 24) -> list[str]:
        cutoff = (utc_now() - timedelta(hours=hours)).isoformat()
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                """
                SELECT title FROM news_articles
                WHERE scraped_at >= ?
                ORDER BY id DESC
                """,
                (cutoff,),
            ).fetchall()
        return [row["title"] for row in rows]

    def news_article_exists(self, url_hash: str) -> bool:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM news_articles WHERE url_hash = ? LIMIT 1",
                (url_hash,),
            ).fetchone()
        return row is not None

    def fingerprint_exists(self, *, url_hash: str | None = None, title_hash: str | None = None) -> bool:
        """Return True if a fingerprint with the given url_hash OR title_hash exists."""
        if not url_hash and not title_hash:
            return False
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT 1 FROM article_fingerprint WHERE url_hash = ? OR title_hash = ? LIMIT 1",
                (url_hash or "", title_hash or ""),
            ).fetchone()
        return row is not None

    def insert_fingerprint(
        self,
        url_hash: str,
        title_hash: str,
        canonical_url: str,
        raw_url: str,
        title: str = "",
        source_feed: str = "",
    ) -> None:
        """Insert a fingerprint record. Silently ignores duplicates."""
        now_iso = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO article_fingerprint
                    (url_hash, title_hash, canonical_url, raw_url, title, source_feed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (url_hash, title_hash, canonical_url, raw_url, title, source_feed, now_iso),
            )

    def backfill_fingerprints(self) -> int:
        """One-time migration: compute fingerprints for all existing news_articles."""
        from ingestion.url_canon import canonicalize_url, content_hash

        with self._connection(commit=False) as connection:
            rows = connection.execute(
                "SELECT url_hash, url, title, timestamp FROM news_articles"
            ).fetchall()

        count = 0
        now_iso = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            for row in rows:
                canonical = canonicalize_url(row["url"])
                u_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
                t_hash = content_hash(row["title"], int(row["timestamp"]))
                try:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO article_fingerprint
                            (url_hash, title_hash, canonical_url, raw_url, title, source_feed, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (u_hash, t_hash, canonical, row["url"], row["title"], "", now_iso),
                    )
                    count += 1
                except sqlite3.IntegrityError:
                    pass
        return count

    def _row_to_news_article(self, row: sqlite3.Row) -> NewsArticleRecord:
        return NewsArticleRecord(
            url_hash=row["url_hash"],
            source_feed=row["source_feed"],
            feed_category=row["feed_category"],
            title=row["title"],
            url=row["url"],
            timestamp=int(row["timestamp"]),
            description=row["description"],
            content_markdown=row["content_markdown"],
            content_fetched=bool(row["content_fetched"]),
            language=row["language"] or "en",
            authors=row["authors"] or "",
        )
