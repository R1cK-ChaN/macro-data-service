"""News-domain shared dataclasses."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RawNewsEntry:
    source_feed: str
    feed_category: str
    title: str
    url: str
    description: str
    timestamp: int


@dataclass(frozen=True)
class PreparedNewsRecord:
    source_feed: str
    feed_category: str
    description: str
    timestamp: int
    canonical_url: str
    raw_url: str
    raw_title: str
    url_hash: str
    title_hash: str
