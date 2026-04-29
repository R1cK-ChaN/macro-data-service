"""Typed DTOs + exceptions for the X (Twitter) v2 API client.

Mirrors the EODHD client layout (``ingestion.market.scrapers._eodhd``) —
errors are HTTP-shape mapped, not transport-level. Issue #76 P1.
"""

from __future__ import annotations

from dataclasses import dataclass


class XAPIError(RuntimeError):
    """Base error for X v2 API failures."""


class XAuthError(XAPIError):
    """Bearer token missing or rejected (HTTP 401/403)."""


class XRateLimitError(XAPIError):
    """Throttled (HTTP 429). Carries the reset epoch for back-off."""

    def __init__(self, message: str, *, reset_epoch: int | None = None) -> None:
        super().__init__(message)
        self.reset_epoch = reset_epoch


class XNotFoundError(XAPIError):
    """Resource missing — handle without an account, or deleted post (HTTP 404)."""


@dataclass(frozen=True)
class XUser:
    """Resolved X user — only the fields we persist into x_tracked_accounts."""

    user_id: str  # numeric id as string (the API ships int, we keep the wider type)
    username: str  # handle without @


@dataclass(frozen=True)
class XPost:
    """Normalized post row — what the timeline + search endpoints both yield.

    Field names match ``x_posts`` columns 1:1 so the persistence layer
    can call ``asdict(post)`` without remapping. ``query_context`` is
    set by the caller (``'timeline'`` for the user-tweets endpoint;
    keyword text for ``search/recent``).
    """

    post_id: str
    author_id: str
    author_handle: str
    text: str
    created_at: str  # ISO-8601 from the API, kept verbatim
    lang: str
    retweet_count: int
    like_count: int
    reply_count: int
    quote_count: int
    query_context: str
    fetched_at: str  # ISO-8601 stamp set by the caller
