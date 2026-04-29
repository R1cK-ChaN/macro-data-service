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
from ingestion.sentiment.x_api.scrapers import XV2Client

__all__ = [
    "KeywordPollResult",
    "TimelinePollResult",
    "XAPIError",
    "XAuthError",
    "XKeywordSearchIngestor",
    "XNotFoundError",
    "XPost",
    "XRateLimitError",
    "XTimelineIngestor",
    "XUser",
    "XV2Client",
]
