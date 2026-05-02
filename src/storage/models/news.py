"""Storage records — news articles + trend topics.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class NewsArticleRecord:
    url_hash: str
    source_feed: str
    feed_category: str
    title: str
    url: str
    timestamp: int
    description: str
    content_markdown: str
    content_fetched: bool
    language: str = "en"
    authors: str = ""


@dataclass(frozen=True)
class TrendTopicRecord:
    trend_id: str
    provider: str
    provider_topic_id: str
    title_raw: str
    topic: str
    summary: str
    keywords: list[str] = field(default_factory=list)
    category: str = ""
    region: str = "global"
    popularity_score: float = 0.0
    provider_rank: int = 0
    engagement_score: float = 0.0
    comment_count: int = 0
    observed_at: int = 0
    expires_at: int = 0
    raw_json: dict[str, Any] = field(default_factory=dict)
    normalized_topic_hash: str = ""
