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
    TimelinePollResult,
    XTimelineIngestor,
)
from ingestion.sentiment.x_api.scrapers import XV2Client

__all__ = [
    "TimelinePollResult",
    "XAPIError",
    "XAuthError",
    "XNotFoundError",
    "XPost",
    "XRateLimitError",
    "XTimelineIngestor",
    "XUser",
    "XV2Client",
]
