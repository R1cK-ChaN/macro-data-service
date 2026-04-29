"""Sentiment-domain query helpers for SQLiteEngineStore.

Covers ``x_tracked_accounts`` + ``x_posts``. Issue #76 P1 ships the
timeline-polling write-path:

* ``upsert_x_post`` — INSERT-or-update on engagement counters; the
  textual columns are immutable from first write so a re-fetch through
  the same ``query_context`` doesn't overwrite a richer earlier copy.
* ``update_x_tracked_account_user_id`` — bootstrap fill after the
  handle resolver runs.
* ``update_x_tracked_account_since_id`` — persist ``newest_id`` after
  every successful timeline call.
* ``list_x_tracked_accounts_due_for_polling`` — priority-tiered
  scheduler query (``priority>=80`` every 15min, ``>=50`` every 30min,
  rest hourly).

Methods rely on the ``self._connection`` context manager defined on
``SQLiteEngineStore`` — composition wires them together via multiple
inheritance, matching the layout shipped in issue #71 Tier 2.1B.
"""

from __future__ import annotations

from datetime import timedelta

from contracts import utc_now
from storage.models.sentiment import XPostRecord


class _SentimentQueriesMixin:
    # Tier-to-cooldown mapping used by both the scheduler query and the
    # write-path's ``last_fetched_at`` stamp. Constants live on the
    # mixin so a future fast/slow-tier shift is one edit, not five.
    _PRIORITY_TIER_COOLDOWN_SECONDS: tuple[tuple[int, int], ...] = (
        (80, 15 * 60),  # priority >= 80 → every 15 min
        (50, 30 * 60),  # priority >= 50 → every 30 min
        (0,  60 * 60),  # rest → hourly
    )

    def upsert_x_post(self, post: XPostRecord) -> None:
        """Insert a new ``x_posts`` row, or update only the engagement
        counters + ``fetched_at`` on an existing row.

        Engagement metrics drift over the first hours after a post —
        re-polling refreshes them. Text / author / created_at are
        treated as immutable; preserving them on conflict means a
        keyword-search-then-timeline re-fetch (different
        ``query_context``) doesn't blow away the original context."""
        now = utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                """
                INSERT INTO x_posts (
                    post_id, author_id, author_handle, text, created_at,
                    lang, retweet_count, like_count, reply_count,
                    quote_count, query_context, fetched_at, is_available,
                    availability_checked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, '')
                ON CONFLICT(post_id) DO UPDATE SET
                    retweet_count = excluded.retweet_count,
                    like_count    = excluded.like_count,
                    reply_count   = excluded.reply_count,
                    quote_count   = excluded.quote_count,
                    fetched_at    = excluded.fetched_at
                """,
                (
                    post.post_id,
                    post.author_id,
                    post.author_handle,
                    post.text,
                    post.created_at,
                    post.lang,
                    int(post.retweet_count),
                    int(post.like_count),
                    int(post.reply_count),
                    int(post.quote_count),
                    post.query_context,
                    post.fetched_at or now,
                ),
            )

    def update_x_tracked_account_user_id(
        self, *, handle: str, user_id: str,
    ) -> None:
        """Set ``user_id`` after the bootstrap resolver succeeds.

        ``handle`` is the PK so this update is unambiguous; the row's
        ``updated_at`` advances so operators can audit when bootstrap
        ran."""
        with self._connection(commit=True) as connection:
            connection.execute(
                "UPDATE x_tracked_accounts "
                "SET user_id = ?, updated_at = ? "
                "WHERE handle = ?",
                (user_id, utc_now().isoformat(), handle),
            )

    def update_x_tracked_account_since_id(
        self, *, handle: str, since_id: str, fetched_at: str | None = None,
    ) -> None:
        """Persist the timeline cursor after a successful poll.

        ``fetched_at`` defaults to ``utc_now()`` — callers may pass
        a stamp captured before the HTTP call to align with the
        ``x_posts.fetched_at`` rows from the same batch."""
        stamp = fetched_at or utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                "UPDATE x_tracked_accounts "
                "SET since_id = ?, last_fetched_at = ?, updated_at = ? "
                "WHERE handle = ?",
                (since_id, stamp, stamp, handle),
            )

    def list_x_tracked_accounts_due_for_polling(
        self, *, now_iso: str | None = None, limit: int | None = None,
    ) -> list[dict[str, object]]:
        """Return active accounts whose ``last_fetched_at`` is older
        than the cooldown for their priority tier.

        Accounts without a resolved ``user_id`` (the fresh-seed state)
        are returned as well — the orchestrator runs the handle
        resolver on those before the timeline call."""
        now = now_iso or utc_now().isoformat()
        # Build a CASE expression mapping priority → cooldown_seconds
        # with a SQL datetime() comparison against last_fetched_at.
        case_branches = " ".join(
            f"WHEN priority >= {threshold} THEN {seconds}"
            for (threshold, seconds) in self._PRIORITY_TIER_COOLDOWN_SECONDS
            if threshold > 0
        )
        # Trailing ELSE matches the priority=0 row (rest tier — hourly).
        rest_seconds = self._PRIORITY_TIER_COOLDOWN_SECONDS[-1][1]
        case_sql = f"CASE {case_branches} ELSE {rest_seconds} END"
        sql = f"""
            SELECT handle, user_id, category, priority, since_id,
                   last_fetched_at
            FROM x_tracked_accounts
            WHERE is_active = 1
              AND (
                last_fetched_at = ''
                OR (CAST((julianday(?) - julianday(last_fetched_at)) * 86400 AS INTEGER))
                   >= ({case_sql})
              )
            ORDER BY priority DESC, last_fetched_at ASC
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._connection(commit=False) as connection:
            rows = connection.execute(sql, (now,)).fetchall()
        return [dict(row) for row in rows]

    def get_x_tracked_account_by_handle(
        self, handle: str,
    ) -> dict[str, object] | None:
        with self._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT * FROM x_tracked_accounts WHERE handle = ?",
                (handle,),
            ).fetchone()
        return dict(row) if row is not None else None

    # ── Keyword-search write path (issue #76 P2) ─────────────────

    def update_x_keyword_since_id(
        self, *, keyword: str, since_id: str, fetched_at: str | None = None,
    ) -> None:
        """Persist the search-cursor + last-fetched stamp.

        Same shape as the tracked-account variant — operators see one
        cooldown semantic across both polling lanes."""
        stamp = fetched_at or utc_now().isoformat()
        with self._connection(commit=True) as connection:
            connection.execute(
                "UPDATE x_keyword_pool "
                "SET since_id = ?, last_fetched_at = ?, updated_at = ? "
                "WHERE keyword = ?",
                (since_id, stamp, stamp, keyword),
            )

    def list_x_keywords_due_for_polling(
        self, *, now_iso: str | None = None, limit: int | None = None,
    ) -> list[dict[str, object]]:
        """Active keywords whose tier cooldown has elapsed.

        Tier mapping is the same one the tracked-account scheduler
        uses — issue #76 keeps the ``priority`` field semantics
        consistent across both polling paths."""
        now = now_iso or utc_now().isoformat()
        case_branches = " ".join(
            f"WHEN priority >= {threshold} THEN {seconds}"
            for (threshold, seconds) in self._PRIORITY_TIER_COOLDOWN_SECONDS
            if threshold > 0
        )
        rest_seconds = self._PRIORITY_TIER_COOLDOWN_SECONDS[-1][1]
        case_sql = f"CASE {case_branches} ELSE {rest_seconds} END"
        sql = f"""
            SELECT keyword, category, priority, since_id, last_fetched_at
            FROM x_keyword_pool
            WHERE is_active = 1
              AND (
                last_fetched_at = ''
                OR (CAST((julianday(?) - julianday(last_fetched_at)) * 86400 AS INTEGER))
                   >= ({case_sql})
              )
            ORDER BY priority DESC, last_fetched_at ASC
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._connection(commit=False) as connection:
            rows = connection.execute(sql, (now,)).fetchall()
        return [dict(row) for row in rows]

    def link_x_post_to_keyword(
        self, *, post_id: str, keyword: str, first_seen_at: str | None = None,
    ) -> None:
        """Append-only fan-out into ``x_post_keywords``.

        First write per (post_id, keyword) wins on the
        ``first_seen_at`` stamp; subsequent calls for the same pair
        are no-ops via INSERT OR IGNORE — the keyword that surfaced
        the post first stays as the discovery context, even if the
        same post turns up under a related keyword later."""
        with self._connection(commit=True) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO x_post_keywords ("
                "  post_id, keyword, first_seen_at"
                ") VALUES (?, ?, ?)",
                (post_id, keyword, first_seen_at or utc_now().isoformat()),
            )
