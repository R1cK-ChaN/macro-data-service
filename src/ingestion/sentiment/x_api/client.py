"""X (Twitter) tracked-account timeline ingestor — issue #76 P1.

Wires together the HTTP client (``XV2Client``), the storage mixin
(``_SentimentQueriesMixin``), and the priority-tier scheduler query.
One ``poll_once`` call:

1. Picks accounts whose cooldown has elapsed (DB-driven; tier mapping
   lives on the mixin).
2. Resolves ``user_id`` for any handle that's still on the empty seed
   value.
3. Calls ``GET /2/users/:id/tweets`` with ``since_id``.
4. Upserts each post into ``x_posts`` (engagement metrics on conflict).
5. Persists the new ``since_id`` + ``last_fetched_at`` on the tracked
   row.

Errors short-circuit per account, not per batch — a 429 on one handle
should not stop a higher-priority handle from being polled in the same
sweep. Returns a per-account result list so the caller can log /
persist run statistics elsewhere.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from contracts import utc_now
from ingestion.sentiment.x_api._types import (
    XAPIError,
    XAuthError,
    XNotFoundError,
    XPost,
    XRateLimitError,
)
from ingestion.sentiment.x_api.scrapers import XV2Client
from storage import SQLiteEngineStore, XPostRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimelinePollResult:
    handle: str
    user_id: str
    posts_persisted: int
    new_since_id: str
    error: str = ""


class XTimelineIngestor:
    """Polls tracked-account timelines — one orchestrator entry point.

    Holds an injected ``XV2Client`` so tests can pass a Mock-based
    fake (matching the ``EODHDClient`` test pattern). The store is
    also injected; the ingestor never opens its own connection."""

    def __init__(
        self,
        store: SQLiteEngineStore,
        client: XV2Client | None = None,
    ) -> None:
        self.store = store
        self.client = client or XV2Client()

    def _ensure_user_id(self, handle: str, current_user_id: str) -> str:
        """Resolve ``user_id`` for a handle that's still on the empty
        seed value. No-op when the row already carries a numeric id."""
        if current_user_id:
            return current_user_id
        user = self.client.get_user_by_username(handle)
        self.store.update_x_tracked_account_user_id(
            handle=handle, user_id=user.user_id,
        )
        return user.user_id

    def poll_account(
        self, *, handle: str, user_id: str, since_id: str,
    ) -> TimelinePollResult:
        """Run one timeline fetch + persist cycle for one account.

        Returns the result with ``error`` set on a known X-API failure
        instead of raising — this lets ``poll_once`` keep iterating
        through the rest of the cooldown-expired list."""
        try:
            resolved_user_id = self._ensure_user_id(handle, user_id)
        except XNotFoundError:
            # Stamp last_fetched_at so the row enters its priority-tier
            # cooldown — without this the empty stamp keeps the row in
            # the "due" set and we re-call the paid resolver every sweep.
            self.store.update_x_tracked_account_since_id(
                handle=handle, since_id=since_id or "",
            )
            return TimelinePollResult(
                handle=handle, user_id="", posts_persisted=0,
                new_since_id=since_id, error="handle_not_found",
            )
        except (XAuthError, XRateLimitError, XAPIError) as exc:
            return TimelinePollResult(
                handle=handle, user_id=user_id, posts_persisted=0,
                new_since_id=since_id, error=str(exc),
            )

        try:
            posts, newest_id = self.client.get_user_tweets(
                resolved_user_id,
                since_id=since_id,
                author_handle=handle,
            )
        except XNotFoundError:
            # The handle resolved but the timeline endpoint says no
            # such user — typical when an account is suspended or
            # deactivated between resolution and poll. Cursor advances
            # to keep the row out of the next sweep until manual review.
            self.store.update_x_tracked_account_since_id(
                handle=handle, since_id=since_id or "",
            )
            return TimelinePollResult(
                handle=handle, user_id=resolved_user_id, posts_persisted=0,
                new_since_id=since_id, error="user_unavailable",
            )
        except (XAuthError, XRateLimitError, XAPIError) as exc:
            return TimelinePollResult(
                handle=handle, user_id=resolved_user_id, posts_persisted=0,
                new_since_id=since_id, error=str(exc),
            )

        persisted = self._persist_posts(posts)
        # Even on an empty page the cursor advances — the X meta block
        # ships ``newest_id`` only when there are posts; we keep the
        # old ``since_id`` otherwise (handled in scrapers._parse_post
        # by passing through ``since_id`` as the default).
        self.store.update_x_tracked_account_since_id(
            handle=handle, since_id=newest_id,
        )
        return TimelinePollResult(
            handle=handle, user_id=resolved_user_id, posts_persisted=persisted,
            new_since_id=newest_id,
        )

    def poll_once(
        self, *, limit: int | None = None,
    ) -> list[TimelinePollResult]:
        """Sweep all cooldown-expired accounts once.

        ``limit`` caps the per-sweep account count so a one-shot run
        from cron / systemd doesn't burst through the entire seed list
        in a single tick."""
        accounts = self.store.list_x_tracked_accounts_due_for_polling(
            now_iso=utc_now().isoformat(), limit=limit,
        )
        results: list[TimelinePollResult] = []
        for row in accounts:
            results.append(
                self.poll_account(
                    handle=str(row["handle"]),
                    user_id=str(row.get("user_id") or ""),
                    since_id=str(row.get("since_id") or ""),
                )
            )
        return results

    def _persist_posts(self, posts: list[XPost]) -> int:
        for post in posts:
            self.store.upsert_x_post(
                XPostRecord(
                    post_id=post.post_id,
                    author_id=post.author_id,
                    author_handle=post.author_handle,
                    text=post.text,
                    created_at=post.created_at,
                    lang=post.lang,
                    retweet_count=post.retweet_count,
                    like_count=post.like_count,
                    reply_count=post.reply_count,
                    quote_count=post.quote_count,
                    query_context=post.query_context,
                    fetched_at=post.fetched_at,
                )
            )
        return len(posts)
