"""Storage records — news articles.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass


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
