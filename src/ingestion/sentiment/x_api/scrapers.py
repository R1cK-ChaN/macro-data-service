"""HTTP client for the X (Twitter) v2 API — issue #76 P1.

Pay-per-use tier endpoints we wire here:

* ``GET /2/users/by/username/:username`` — handle → user_id resolution.
  Used once per seed handle on first bootstrap (the resolved id then
  lives in ``x_tracked_accounts.user_id`` and the timeline endpoint is
  cheaper to call by id).
* ``GET /2/users/:id/tweets`` — incremental timeline. We pass
  ``since_id`` so each refresh only ships the unseen tail.

Auth is the bearer token under ``X_BEARER_TOKEN`` (or ``TWITTER_BEARER_TOKEN``).
The HTTP shape matches EODHD's client (``requests.Session`` injected,
status → typed exception via ``_raise_for_status``) so the test mocks
can use the same ``Mock(spec=requests.Response)`` pattern.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from contracts import utc_now
from env import get_env_value
from ingestion.sentiment.x_api._types import (
    XAPIError,
    XAuthError,
    XNotFoundError,
    XPost,
    XRateLimitError,
    XUser,
)

logger = logging.getLogger(__name__)

_DEFAULT_TWEET_FIELDS = "created_at,public_metrics,lang,author_id"
_DEFAULT_USER_FIELDS = "id,username"


def _raise_for_status(response: requests.Response, *, context: str) -> None:
    if response.status_code == 200:
        return
    if response.status_code in (401, 403):
        raise XAuthError(f"X auth error {response.status_code} for {context}")
    if response.status_code == 404:
        raise XNotFoundError(f"X resource not found: {context}")
    if response.status_code == 429:
        reset = response.headers.get("x-rate-limit-reset")
        reset_epoch = int(reset) if reset and reset.isdigit() else None
        raise XRateLimitError(
            f"X rate limit hit for {context}", reset_epoch=reset_epoch,
        )
    raise XAPIError(
        f"X API error {response.status_code} for {context}: "
        f"{response.text[:200]}"
    )


def _parse_post(
    item: dict[str, Any],
    *,
    author_handle: str,
    query_context: str,
    fetched_at: str,
) -> XPost | None:
    """Project one v2 tweet object into ``XPost``. Returns ``None`` on
    malformed rows so a single bad item doesn't kill the whole batch."""
    post_id = item.get("id")
    author_id = item.get("author_id", "")
    text = item.get("text", "")
    created_at = item.get("created_at", "")
    if not post_id or not author_id or not created_at:
        logger.debug("X post skipped (missing id/author_id/created_at): %s", item)
        return None
    metrics = item.get("public_metrics") or {}
    return XPost(
        post_id=str(post_id),
        author_id=str(author_id),
        author_handle=author_handle,
        text=text,
        created_at=str(created_at),
        lang=str(item.get("lang") or ""),
        retweet_count=int(metrics.get("retweet_count") or 0),
        like_count=int(metrics.get("like_count") or 0),
        reply_count=int(metrics.get("reply_count") or 0),
        quote_count=int(metrics.get("quote_count") or 0),
        query_context=query_context,
        fetched_at=fetched_at,
    )


class XV2Client:
    """Low-level HTTP client for X (Twitter) v2 API."""

    BASE_URL = "https://api.twitter.com/2"

    def __init__(self, bearer_token: str | None = None) -> None:
        self.bearer_token = bearer_token or get_env_value(
            "X_BEARER_TOKEN", "TWITTER_BEARER_TOKEN",
        )
        self.session = requests.Session()

    def _headers(self) -> dict[str, str]:
        if not self.bearer_token:
            raise XAuthError(
                "X bearer token not set "
                "(env: X_BEARER_TOKEN or TWITTER_BEARER_TOKEN)"
            )
        return {"Authorization": f"Bearer {self.bearer_token}"}

    def get_user_by_username(self, username: str) -> XUser:
        """Resolve a handle to its numeric user_id.

        Called once per tracked account on bootstrap. The X API treats
        ``username`` case-insensitively; we keep what the seed list
        wrote (``federalreserve``) so the round-tripped record matches.
        """
        url = f"{self.BASE_URL}/users/by/username/{username}"
        params = {"user.fields": _DEFAULT_USER_FIELDS}
        response = self.session.get(url, params=params, headers=self._headers(), timeout=30)
        _raise_for_status(response, context=f"users/by/username/{username}")
        body = response.json()
        data = body.get("data") or {}
        user_id = data.get("id")
        if not user_id:
            raise XNotFoundError(f"X user '{username}' not found")
        return XUser(user_id=str(user_id), username=str(data.get("username") or username))

    def get_user_tweets(
        self,
        user_id: str,
        *,
        since_id: str = "",
        max_results: int = 100,
        author_handle: str = "",
        max_pages: int = 5,
    ) -> tuple[list[XPost], str]:
        """Fetch the unseen tail of a user's timeline, paginated.

        X returns up to ``max_results`` tweets per page in reverse
        chronological order; when the unseen tail since ``since_id`` is
        longer than one page, the response carries
        ``meta.next_token`` and the older rows live behind it. We must
        drain those pages before advancing the cursor — otherwise the
        first call's ``newest_id`` becomes the new ``since_id`` and the
        intermediate (older) tweets fall *behind* the cursor on the
        next sweep, never to be fetched.

        Returns ``(posts, newest_id)`` where ``newest_id`` is the very
        first page's ``meta.newest_id`` (the head of the unseen tail —
        the cursor the caller persists). On an empty result we keep
        ``since_id``. ``max_pages`` caps a runaway loop in case the
        upstream loops on tokens; the default 5 pages × 100 = 500
        tweets per account per sweep, well above any realistic
        15-minute volume.
        """
        url = f"{self.BASE_URL}/users/{user_id}/tweets"
        base_params: dict[str, str | int] = {
            "tweet.fields": _DEFAULT_TWEET_FIELDS,
            "max_results": max(5, min(max_results, 100)),
        }
        if since_id:
            base_params["since_id"] = since_id

        fetched_at = utc_now().isoformat()
        posts: list[XPost] = []
        first_page_newest_id = since_id
        next_token = ""
        for _ in range(max(1, max_pages)):
            params = dict(base_params)
            if next_token:
                params["pagination_token"] = next_token
            response = self.session.get(
                url, params=params, headers=self._headers(), timeout=30,
            )
            _raise_for_status(response, context=f"users/{user_id}/tweets")
            body = response.json()
            meta = body.get("meta") or {}
            # The very first page's newest_id is the cursor candidate.
            if not next_token:
                first_page_newest_id = str(meta.get("newest_id") or since_id)
            rows = body.get("data") or []
            for item in rows:
                parsed = _parse_post(
                    item,
                    author_handle=author_handle,
                    query_context="timeline",
                    fetched_at=fetched_at,
                )
                if parsed is not None:
                    posts.append(parsed)
            next_token = str(meta.get("next_token") or "")
            if not next_token:
                break
        # If we exit the loop with ``next_token`` still set, we hit the
        # ``max_pages`` cap with older unseen pages still behind it.
        # Advancing the cursor here would make those pages unreachable
        # on later sweeps; preserve the old ``since_id`` instead so the
        # next sweep resumes the drain. Posts already persisted via
        # ``upsert_x_post`` dedupe on conflict, so a partial re-fetch
        # is safe.
        cursor_id = since_id if next_token else first_page_newest_id
        return posts, cursor_id
