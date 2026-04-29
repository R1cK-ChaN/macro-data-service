"""Public surface of the X (Twitter) v2 API ingestion lane (issue #76)."""

from __future__ import annotations

from ingestion.sentiment.x_api._types import (
    XAPIError,
    XAuthError,
    XNotFoundError,
    XPost,
    XRateLimitError,
    XUser,
)
from ingestion.sentiment.x_api.client import (
    KeywordPollResult,
    TimelinePollResult,
    XKeywordSearchIngestor,
    XTimelineIngestor,
)
from ingestion.sentiment.x_api.discovery import (
    DEFAULT_DISCOVERY_QUERY,
    HashtagDiscoveryResult,
    SocialBreakoutInjection,
    XHashtagDiscoveryRunner,
    XSpikeDetector,
)
from ingestion.sentiment.x_api.scrapers import XV2Client

__all__ = [
    "DEFAULT_DISCOVERY_QUERY",
    "HashtagDiscoveryResult",
    "KeywordPollResult",
    "SocialBreakoutInjection",
    "TimelinePollResult",
    "XAPIError",
    "XAuthError",
    "XHashtagDiscoveryRunner",
    "XKeywordSearchIngestor",
    "XNotFoundError",
    "XPost",
    "XRateLimitError",
    "XSpikeDetector",
    "XTimelineIngestor",
    "XUser",
    "XV2Client",
]
