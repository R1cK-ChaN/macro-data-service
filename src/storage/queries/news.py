"""News-domain query helpers for SQLiteEngineStore.

Covers news_articles + trend_topics + article_fingerprint plus the
news-context scoring path used by analytical/messaging consumers.

Extracted from storage.sqlite in issue #71 Tier 2.1B-2. Methods rely on
the ``self._connection`` context manager defined on the SQLiteEngineStore
base class — composition wires them together via multiple inheritance.
"""

from __future__ import annotations

import hashlib
import json
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
from storage.models.news import (
    NewsArticleRecord,
    TrendTopicRecord,
)


class _NewsQueriesMixin:
    _IMPACT_HALF_LIFE = {"critical": 7, "high": 5, "medium": 3, "low": 2, "info": 1}

    _IMPACT_WEIGHT = {"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.6, "info": 0.3}

    _TIME_DECAY_MAX_BOOST = 1.5

    _TIME_DECAY_MIN_BOOST = 0.1

    def upsert_news_article(self, article: NewsArticleRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO news_articles (
                    url_hash, source_feed, feed_category, title, url,
                    timestamp, description, content_markdown,
                    impact_level, finance_category, confidence,
                    content_fetched, institution, country, market,
                    asset_class, sector, document_type, event_type,
                    subject, subject_id, data_period,
                    contains_commentary, language, authors,
                    extraction_provider, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    article.impact_level,
                    article.finance_category,
                    article.confidence,
                    int(article.content_fetched),
                    article.institution,
                    article.country,
                    article.market,
                    article.asset_class,
                    article.sector,
                    article.document_type,
                    article.event_type,
                    article.subject,
                    article.subject_id,
                    article.data_period,
                    int(article.contains_commentary),
                    article.language,
                    article.authors,
                    article.extraction_provider,
                    utc_now().isoformat(),
                ),
            )

    def upsert_trend_topic(self, trend: TrendTopicRecord) -> None:
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO trend_topics (
                    trend_id, provider, provider_topic_id, title_raw, topic,
                    summary, keywords_json, category, region, popularity_score,
                    provider_rank, engagement_score, comment_count,
                    observed_at, expires_at, raw_json, normalized_topic_hash,
                    scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trend.trend_id,
                    trend.provider,
                    trend.provider_topic_id,
                    trend.title_raw,
                    trend.topic,
                    trend.summary,
                    json.dumps(trend.keywords, ensure_ascii=True),
                    trend.category,
                    trend.region,
                    float(trend.popularity_score),
                    int(trend.provider_rank),
                    float(trend.engagement_score),
                    int(trend.comment_count),
                    int(trend.observed_at),
                    int(trend.expires_at),
                    json.dumps(trend.raw_json, ensure_ascii=True, sort_keys=True),
                    trend.normalized_topic_hash,
                    utc_now().isoformat(),
                ),
            )

    def list_active_trends(
        self,
        *,
        limit: int = 10,
        hours: int = 48,
        category: str | None = None,
        region: str | None = None,
    ) -> list[TrendTopicRecord]:
        now_ts = int(utc_now().timestamp())
        cutoff = int((utc_now() - timedelta(hours=hours)).timestamp())
        conditions = ["expires_at >= ?", "observed_at >= ?"]
        params: list[Any] = [now_ts, cutoff]
        if category:
            conditions.append("category = ?")
            params.append(category)
        if region:
            conditions.append("region = ?")
            params.append(region)
        params.append(limit)
        with self._connection(commit=False) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM trend_topics
                WHERE {' AND '.join(conditions)}
                ORDER BY popularity_score DESC, observed_at DESC, trend_id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_trend_topic(row) for row in rows]

    def list_recent_news(
        self,
        *,
        limit: int = 20,
        days: int = 7,
        impact_level: str | None = None,
        feed_category: str | None = None,
        finance_category: str | None = None,
        country: str | None = None,
        asset_class: str | None = None,
    ) -> list[NewsArticleRecord]:
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if impact_level:
            conditions.append("impact_level = ?")
            params.append(impact_level)
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)
        if finance_category:
            conditions.append("finance_category = ?")
            params.append(finance_category)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if asset_class:
            conditions.append("asset_class = ?")
            params.append(asset_class)
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
        impact_level: str | None = None,
        feed_category: str | None = None,
        finance_category: str | None = None,
        country: str | None = None,
        asset_class: str | None = None,
        display_timezone: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve news with time-decay + impact-weight composite scoring."""
        cutoff = int((utc_now() - timedelta(days=days)).timestamp())
        conditions = ["timestamp >= ?"]
        params: list[Any] = [cutoff]
        if impact_level:
            conditions.append("impact_level = ?")
            params.append(impact_level)
        if feed_category:
            conditions.append("feed_category = ?")
            params.append(feed_category)
        if finance_category:
            conditions.append("finance_category = ?")
            params.append(finance_category)
        if country:
            conditions.append("country = ?")
            params.append(country)
        if asset_class:
            conditions.append("asset_class = ?")
            params.append(asset_class)

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
            half_life = self._IMPACT_HALF_LIFE.get(article.impact_level, 2)
            time_decay = self._TIME_DECAY_MIN_BOOST + (
                (self._TIME_DECAY_MAX_BOOST - self._TIME_DECAY_MIN_BOOST)
                * math.pow(2, -age_days / half_life)
            )
            impact_w = self._IMPACT_WEIGHT.get(article.impact_level, 0.5)
            composite = time_decay * impact_w

            desc = article.description
            if len(desc) > 500:
                desc = desc[:500] + "..."
            payload = {
                "source_feed": article.source_feed,
                "title": article.title,
                "url": article.url,
                "timestamp": article.timestamp,
                "published_at": format_epoch_iso(article.timestamp),
                "description": desc,
                "impact_level": article.impact_level,
                "finance_category": article.finance_category,
                "country": article.country,
                "asset_class": article.asset_class,
                "subject": article.subject,
                "event_type": article.event_type,
                "score": round(composite, 4),
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
            scored.append((composite, payload))

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
            impact_level=row["impact_level"],
            finance_category=row["finance_category"],
            confidence=float(row["confidence"]),
            content_fetched=bool(row["content_fetched"]),
            institution=row["institution"] or "",
            country=row["country"] or "",
            market=row["market"] or "",
            asset_class=row["asset_class"] or "",
            sector=row["sector"] or "",
            document_type=row["document_type"] or "",
            event_type=row["event_type"] or "",
            subject=row["subject"] or "",
            subject_id=row["subject_id"] or "",
            data_period=row["data_period"] or "",
            contains_commentary=bool(row["contains_commentary"]),
            language=row["language"] or "en",
            authors=row["authors"] or "",
            extraction_provider=row["extraction_provider"] or "keyword",
        )

    def _row_to_trend_topic(self, row: sqlite3.Row) -> TrendTopicRecord:
        try:
            keywords = json.loads(row["keywords_json"] or "[]")
        except json.JSONDecodeError:
            keywords = []
        if not isinstance(keywords, list):
            keywords = []
        try:
            raw_json = json.loads(row["raw_json"] or "{}")
        except json.JSONDecodeError:
            raw_json = {}
        if not isinstance(raw_json, dict):
            raw_json = {}
        return TrendTopicRecord(
            trend_id=row["trend_id"],
            provider=row["provider"],
            provider_topic_id=row["provider_topic_id"],
            title_raw=row["title_raw"],
            topic=row["topic"],
            summary=row["summary"],
            keywords=[str(item) for item in keywords if str(item).strip()],
            category=row["category"] or "",
            region=row["region"] or "global",
            popularity_score=float(row["popularity_score"]),
            provider_rank=int(row["provider_rank"]),
            engagement_score=float(row["engagement_score"]),
            comment_count=int(row["comment_count"]),
            observed_at=int(row["observed_at"]),
            expires_at=int(row["expires_at"]),
            raw_json=raw_json,
            normalized_topic_hash=row["normalized_topic_hash"] or "",
        )
