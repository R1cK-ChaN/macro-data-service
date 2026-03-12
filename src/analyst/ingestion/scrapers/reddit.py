from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = "AnalystMacroData/0.1 (https://github.com/openai)"


@dataclass(frozen=True)
class RedditTrendPost:
    subreddit: str
    post_id: str
    title: str
    permalink: str
    score: int
    num_comments: int
    created_utc: int
    is_stickied: bool = False
    is_nsfw: bool = False
    raw_json: dict[str, Any] = field(default_factory=dict)


class RedditTrendClient:
    BASE_URL = "https://www.reddit.com"

    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout: int = 15,
    ) -> None:
        self._timeout = timeout
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._session.headers.update({
            "User-Agent": _DEFAULT_USER_AGENT,
            "Accept": "application/json",
        })

    def fetch_hot_posts(self, subreddit: str, *, limit: int = 25) -> list[RedditTrendPost]:
        normalized = subreddit.strip().lstrip("r/").strip("/")
        if not normalized:
            return []
        response = self._session.get(
            f"{self.BASE_URL}/r/{normalized}/hot.json",
            params={"limit": max(1, min(limit, 100)), "raw_json": 1},
            timeout=self._timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return self._parse_listing(payload, subreddit=normalized)

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    @staticmethod
    def _parse_listing(payload: dict[str, Any], *, subreddit: str) -> list[RedditTrendPost]:
        listing = payload.get("data", {})
        children = listing.get("children", [])
        if not isinstance(children, list):
            return []
        posts: list[RedditTrendPost] = []
        for child in children:
            data = child.get("data", {}) if isinstance(child, dict) else {}
            post_id = str(data.get("id") or "").strip()
            title = str(data.get("title") or "").strip()
            permalink = str(data.get("permalink") or "").strip()
            if not post_id or not title or not permalink:
                continue
            try:
                created_utc = int(float(data.get("created_utc", 0) or 0))
            except (TypeError, ValueError):
                created_utc = 0
            try:
                score = int(data.get("score", 0) or 0)
            except (TypeError, ValueError):
                score = 0
            try:
                num_comments = int(data.get("num_comments", 0) or 0)
            except (TypeError, ValueError):
                num_comments = 0
            posts.append(
                RedditTrendPost(
                    subreddit=str(data.get("subreddit") or subreddit),
                    post_id=post_id,
                    title=title,
                    permalink=f"{RedditTrendClient.BASE_URL}{permalink}",
                    score=score,
                    num_comments=num_comments,
                    created_utc=created_utc,
                    is_stickied=bool(data.get("stickied")),
                    is_nsfw=bool(data.get("over_18")),
                    raw_json=data if isinstance(data, dict) else {},
                )
            )
        return posts
