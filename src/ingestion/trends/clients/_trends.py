"""Trend ingestion clients: Reddit and Weibo."""

from __future__ import annotations

import hashlib
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from ingestion.news._types import RawNewsEntry, PreparedNewsRecord
from ingestion.scrapers.reddit import RedditTrendClient, RedditTrendPost
from ingestion.scrapers.weibo import WeiboTrendClient, WeiboTrendItem
from storage import SQLiteEngineStore, TrendTopicRecord

logger = logging.getLogger(__name__)


class RefreshStats:
    def __init__(self, source: str, count: int) -> None:
        self.source = source
        self.count = count


@dataclass(frozen=True)
class RedditTrendSourceConfig:
    subreddit: str
    category: str
    region: str = "global"


@dataclass(frozen=True)
class RawTrendEntry:
    subreddit: str
    category: str
    region: str
    provider_topic_id: str
    title_raw: str
    url: str
    score: int
    comment_count: int
    provider_rank: int
    observed_at: int
    is_stickied: bool
    is_nsfw: bool
    raw_json: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PreparedTrendRecord:
    trend_id: str
    provider: str
    provider_topic_id: str
    title_raw: str
    topic: str
    summary: str
    keywords: list[str]
    category: str
    region: str
    popularity_score: float
    provider_rank: int
    engagement_score: float
    comment_count: int
    observed_at: int
    expires_at: int
    raw_json: dict[str, Any]
    normalized_topic_hash: str


_REDDIT_TREND_SOURCES: tuple[RedditTrendSourceConfig, ...] = (
    RedditTrendSourceConfig(subreddit="technology", category="technology"),
    RedditTrendSourceConfig(subreddit="artificial", category="technology"),
    RedditTrendSourceConfig(subreddit="economics", category="business"),
    RedditTrendSourceConfig(subreddit="stocks", category="business"),
    RedditTrendSourceConfig(subreddit="investing", category="business"),
    RedditTrendSourceConfig(subreddit="worldnews", category="news"),
    RedditTrendSourceConfig(subreddit="news", category="news"),
    RedditTrendSourceConfig(subreddit="movies", category="entertainment"),
    RedditTrendSourceConfig(subreddit="television", category="entertainment"),
)

_TREND_KEYWORD_STOPWORDS = {
    "about",
    "after",
    "against",
    "also",
    "among",
    "because",
    "before",
    "being",
    "between",
    "could",
    "discussion",
    "from",
    "have",
    "into",
    "just",
    "more",
    "most",
    "news",
    "over",
    "post",
    "reddit",
    "should",
    "some",
    "still",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "today",
    "topic",
    "users",
    "what",
    "when",
    "where",
    "which",
    "while",
    "with",
    "would",
}

_WEIBO_CATEGORY_MAP = {
    "互联网": "technology",
    "体育": "sports",
    "作品衍生": "entertainment",
    "剧集": "entertainment",
    "国内时政": "news",
    "国际时政": "news",
    "幽默": "culture",
    "情感": "lifestyle",
    "民生新闻": "news",
    "汽车": "business",
    "突发/灾害": "news",
    "综艺": "entertainment",
    "美食": "lifestyle",
    "财经": "business",
    "音乐": "entertainment",
}


class RedditTrendIngestionClient:
    def __init__(
        self,
        *,
        client: RedditTrendClient | None = None,
        source_configs: tuple[RedditTrendSourceConfig, ...] = _REDDIT_TREND_SOURCES,
        max_items_per_subreddit: int = 25,
        ttl_hours: int = 48,
    ) -> None:
        self._client = client or RedditTrendClient()
        self._source_configs = source_configs
        self._max_items_per_subreddit = max_items_per_subreddit
        self._ttl_hours = ttl_hours

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        entries = self.fetch_entries()
        normalized = self.normalize_entries(entries)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(valid)
        return RefreshStats(source="reddit_trends", count=self.store_topics(store, deduplicated))

    def fetch_entries(self) -> list[RawTrendEntry]:
        entries: list[RawTrendEntry] = []
        for source in self._source_configs:
            posts = self._client.fetch_hot_posts(
                source.subreddit,
                limit=self._max_items_per_subreddit,
            )
            for provider_rank, post in enumerate(posts, start=1):
                if post.is_stickied or post.is_nsfw or not post.title.strip():
                    continue
                entries.append(self._to_raw_entry(post, source=source, provider_rank=provider_rank))
            time.sleep(0.2)
        return entries

    def normalize_entries(self, entries: list[RawTrendEntry]) -> list[PreparedTrendRecord]:
        normalized: list[PreparedTrendRecord] = []
        now_ts = int(datetime.now(timezone.utc).timestamp())
        ttl_seconds = max(self._ttl_hours, 1) * 3600
        for entry in entries:
            topic = self._clean_title(entry.title_raw)
            normalized_topic = self._normalize_topic_text(topic)
            normalized_hash = hashlib.sha256(normalized_topic.encode("utf-8")).hexdigest()
            observed_at = entry.observed_at if entry.observed_at > 0 else now_ts
            keywords = self._extract_keywords(normalized_topic)
            engagement_score = self._engagement_score(entry)
            popularity_score = self._popularity_score(
                entry,
                engagement_score=engagement_score,
                now_ts=now_ts,
            )
            normalized.append(
                PreparedTrendRecord(
                    trend_id=f"reddit:{normalized_hash}",
                    provider="reddit",
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=topic,
                    summary=self._build_summary(topic, category=entry.category, region=entry.region),
                    keywords=keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=round(popularity_score, 2),
                    provider_rank=entry.provider_rank,
                    engagement_score=round(engagement_score, 2),
                    comment_count=max(entry.comment_count, 0),
                    observed_at=observed_at,
                    expires_at=observed_at + ttl_seconds,
                    raw_json={
                        "subreddit": entry.subreddit,
                        "url": entry.url,
                        "score": entry.score,
                        "comment_count": entry.comment_count,
                        "provider_rank": entry.provider_rank,
                        **entry.raw_json,
                    },
                    normalized_topic_hash=normalized_hash,
                )
            )
        return normalized

    def validate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        valid: list[PreparedTrendRecord] = []
        for entry in entries:
            if len(entry.topic.strip()) < 12:
                continue
            if not entry.provider_topic_id.strip():
                continue
            if not entry.normalized_topic_hash.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        best_by_topic: dict[str, PreparedTrendRecord] = {}
        for entry in entries:
            existing = best_by_topic.get(entry.normalized_topic_hash)
            if existing is None or self._sort_key(entry) > self._sort_key(existing):
                best_by_topic[entry.normalized_topic_hash] = entry
        return sorted(best_by_topic.values(), key=self._sort_key, reverse=True)

    def store_topics(self, store: SQLiteEngineStore, entries: list[PreparedTrendRecord]) -> int:
        for entry in entries:
            store.upsert_trend_topic(
                TrendTopicRecord(
                    trend_id=entry.trend_id,
                    provider=entry.provider,
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=entry.topic,
                    summary=entry.summary,
                    keywords=entry.keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=entry.popularity_score,
                    provider_rank=entry.provider_rank,
                    engagement_score=entry.engagement_score,
                    comment_count=entry.comment_count,
                    observed_at=entry.observed_at,
                    expires_at=entry.expires_at,
                    raw_json=entry.raw_json,
                    normalized_topic_hash=entry.normalized_topic_hash,
                )
            )
        return len(entries)

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _to_raw_entry(
        post: RedditTrendPost,
        *,
        source: RedditTrendSourceConfig,
        provider_rank: int,
    ) -> RawTrendEntry:
        return RawTrendEntry(
            subreddit=source.subreddit,
            category=source.category,
            region=source.region,
            provider_topic_id=post.post_id,
            title_raw=post.title,
            url=post.permalink,
            score=post.score,
            comment_count=post.num_comments,
            provider_rank=provider_rank,
            observed_at=post.created_utc,
            is_stickied=post.is_stickied,
            is_nsfw=post.is_nsfw,
            raw_json=post.raw_json,
        )

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = title.strip()
        while cleaned:
            updated = re.sub(r"^\s*(?:\[[^\]]{1,40}\]|\([^)]{1,40}\)|\{[^}]{1,40}\})\s*", "", cleaned).strip()
            if updated == cleaned:
                break
            cleaned = updated
        cleaned = re.sub(r"([!?.,])\1+", r"\1", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned.strip(" -|:")

    @staticmethod
    def _normalize_topic_text(title: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    @staticmethod
    def _extract_keywords(normalized_topic: str) -> list[str]:
        keywords: list[str] = []
        for token in re.findall(r"[a-z0-9]+", normalized_topic):
            if len(token) < 3 or token.isdigit():
                continue
            if token in _TREND_KEYWORD_STOPWORDS:
                continue
            if token in keywords:
                continue
            keywords.append(token)
            if len(keywords) == 5:
                break
        return keywords

    @staticmethod
    def _build_summary(topic: str, *, category: str, region: str) -> str:
        if region and region.lower() != "global":
            return f"{topic} is drawing heavy discussion in {region.lower()} {category} conversations."
        if category:
            return f"{topic} is drawing heavy discussion in {category} conversations."
        return f"{topic} is drawing heavy discussion right now."

    @staticmethod
    def _engagement_score(entry: RawTrendEntry) -> float:
        score_component = math.log1p(max(entry.score, 0)) * 10.0
        comments_component = math.log1p(max(entry.comment_count, 0)) * 8.0
        return min(55.0, score_component + comments_component)

    @staticmethod
    def _popularity_score(
        entry: RawTrendEntry,
        *,
        engagement_score: float,
        now_ts: int,
    ) -> float:
        rank_bonus = max(0.0, 30.0 - max(entry.provider_rank - 1, 0) * 1.2)
        age_hours = max((now_ts - max(entry.observed_at, 0)) / 3600.0, 0.0)
        recency_bonus = max(0.0, 15.0 - min(age_hours, 15.0))
        return min(100.0, engagement_score + rank_bonus + recency_bonus)

    @staticmethod
    def _sort_key(entry: PreparedTrendRecord) -> tuple[float, int, int, str]:
        return (
            entry.popularity_score,
            entry.observed_at,
            -entry.provider_rank,
            entry.topic.lower(),
        )


class WeiboTrendIngestionClient:
    def __init__(
        self,
        *,
        client: WeiboTrendClient | None = None,
        ttl_hours: int = 48,
    ) -> None:
        self._client = client or WeiboTrendClient()
        self._ttl_hours = ttl_hours

    def refresh(self, store: SQLiteEngineStore) -> RefreshStats:
        items = self.fetch_entries()
        normalized = self.normalize_entries(items)
        valid = self.validate_entries(normalized)
        deduplicated = self.deduplicate_entries(valid)
        return RefreshStats(source="weibo_trends", count=self.store_topics(store, deduplicated))

    def fetch_entries(self) -> list[WeiboTrendItem]:
        return [item for item in self._client.fetch_hot_band() if item.topic_flag != 0]

    def normalize_entries(self, items: list[WeiboTrendItem]) -> list[PreparedTrendRecord]:
        normalized: list[PreparedTrendRecord] = []
        now_ts = int(datetime.now(timezone.utc).timestamp())
        ttl_seconds = max(self._ttl_hours, 1) * 3600
        for item in items:
            topic = self._clean_title(item.note or item.word)
            normalized_topic = self._normalize_topic_text(topic)
            if not normalized_topic:
                continue
            normalized_hash = hashlib.sha256(normalized_topic.encode("utf-8")).hexdigest()
            observed_at = item.onboard_time if item.onboard_time > 0 else now_ts
            popularity_score = self._popularity_score(item, now_ts=now_ts)
            normalized.append(
                PreparedTrendRecord(
                    trend_id=f"weibo:{normalized_hash}",
                    provider="weibo",
                    provider_topic_id=item.word_scheme or item.word,
                    title_raw=item.note or item.word,
                    topic=topic,
                    summary=self._build_summary(topic, category=self._map_category(item.category)),
                    keywords=self._extract_keywords(topic),
                    category=self._map_category(item.category),
                    region="china",
                    popularity_score=round(popularity_score, 2),
                    provider_rank=max(item.realpos, 0),
                    engagement_score=round(self._engagement_score(item), 2),
                    comment_count=0,
                    observed_at=observed_at,
                    expires_at=observed_at + ttl_seconds,
                    raw_json=item.raw_json,
                    normalized_topic_hash=normalized_hash,
                )
            )
        return normalized

    def validate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        valid: list[PreparedTrendRecord] = []
        for entry in entries:
            if len(entry.topic.strip()) < 2:
                continue
            if not entry.provider_topic_id.strip():
                continue
            if not entry.normalized_topic_hash.strip():
                continue
            valid.append(entry)
        return valid

    def deduplicate_entries(self, entries: list[PreparedTrendRecord]) -> list[PreparedTrendRecord]:
        best_by_topic: dict[str, PreparedTrendRecord] = {}
        for entry in entries:
            existing = best_by_topic.get(entry.normalized_topic_hash)
            if existing is None or self._sort_key(entry) > self._sort_key(existing):
                best_by_topic[entry.normalized_topic_hash] = entry
        return sorted(best_by_topic.values(), key=self._sort_key, reverse=True)

    def store_topics(self, store: SQLiteEngineStore, entries: list[PreparedTrendRecord]) -> int:
        for entry in entries:
            store.upsert_trend_topic(
                TrendTopicRecord(
                    trend_id=entry.trend_id,
                    provider=entry.provider,
                    provider_topic_id=entry.provider_topic_id,
                    title_raw=entry.title_raw,
                    topic=entry.topic,
                    summary=entry.summary,
                    keywords=entry.keywords,
                    category=entry.category,
                    region=entry.region,
                    popularity_score=entry.popularity_score,
                    provider_rank=entry.provider_rank,
                    engagement_score=entry.engagement_score,
                    comment_count=entry.comment_count,
                    observed_at=entry.observed_at,
                    expires_at=entry.expires_at,
                    raw_json=entry.raw_json,
                    normalized_topic_hash=entry.normalized_topic_hash,
                )
            )
        return len(entries)

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = title.strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = re.sub(r"[#]+", "", cleaned)
        return cleaned.strip(" -|:")

    @staticmethod
    def _normalize_topic_text(title: str) -> str:
        normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", " ", title)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.casefold().strip()

    @staticmethod
    def _extract_keywords(title: str) -> list[str]:
        keywords: list[str] = []
        for token in re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]{2,}", title):
            normalized = token.casefold()
            if normalized.isascii():
                if len(normalized) < 3 or normalized.isdigit():
                    continue
                if normalized in _TREND_KEYWORD_STOPWORDS:
                    continue
                value = normalized
            else:
                value = token.strip()
                if len(value) < 2:
                    continue
            if value in keywords:
                continue
            keywords.append(value)
            if len(keywords) == 5:
                break
        return keywords or [title[:16]]

    @staticmethod
    def _map_category(category: str) -> str:
        mapped = _WEIBO_CATEGORY_MAP.get(category.strip(), "")
        return mapped or "news"

    @staticmethod
    def _build_summary(topic: str, *, category: str) -> str:
        return f"{topic} is trending on Weibo in China's {category} conversations."

    @staticmethod
    def _engagement_score(item: WeiboTrendItem) -> float:
        hot_component = math.log1p(max(item.num, 0)) * 10.0
        raw_hot_component = math.log1p(max(item.raw_hot, 0)) * 6.0
        label_bonus = 4.0 if item.label_name in {"新", "热", "沸", "爆"} else 0.0
        return min(70.0, hot_component + raw_hot_component + label_bonus)

    @classmethod
    def _popularity_score(cls, item: WeiboTrendItem, *, now_ts: int) -> float:
        engagement_score = cls._engagement_score(item)
        rank_bonus = max(0.0, 28.0 - max(item.realpos - 1, 0) * 1.0)
        age_hours = max((now_ts - max(item.onboard_time, 0)) / 3600.0, 0.0) if item.onboard_time else 0.0
        recency_bonus = max(0.0, 12.0 - min(age_hours, 12.0))
        return min(100.0, engagement_score + rank_bonus + recency_bonus)

    @staticmethod
    def _sort_key(entry: PreparedTrendRecord) -> tuple[float, int, int, str]:
        return (
            entry.popularity_score,
            entry.observed_at,
            -entry.provider_rank,
            entry.topic.casefold(),
        )

