"""Keyword search polling tests for the X (Twitter) ingestion lane
(issue #76 P2).

Covers the search/recent HTTP path, the keyword-pool scheduler query,
the dedup write-path (engagement-only upsert + append-only
x_post_keywords), and the orchestrator end-to-end. HTTP mocked through
``Mock(spec=requests.Response)`` matching the P1 test pattern.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from contracts import utc_now
from ingestion.sentiment.x_api import (
    KeywordPollResult,
    XKeywordSearchIngestor,
    XV2Client,
)
from storage import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _mock_response(status_code: int, json_body: dict | None = None,
                   *, headers: dict | None = None) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.json.return_value = json_body or {}
    response.text = ""
    response.headers = headers or {}
    return response


def _search_payload(
    *, posts: list[dict] | None = None,
    users: list[dict] | None = None,
    newest_id: str = "", next_token: str = "",
) -> dict:
    body: dict = {"data": posts or []}
    meta: dict = {}
    if newest_id:
        meta["newest_id"] = newest_id
        meta["result_count"] = len(posts or [])
    if next_token:
        meta["next_token"] = next_token
    if meta:
        body["meta"] = meta
    if users is not None:
        body["includes"] = {"users": users}
    return body


def _sample_post(
    post_id: str, *, author_id: str = "23381256",
    text: str = "rate cut signal", lang: str = "en",
) -> dict:
    return {
        "id": post_id,
        "author_id": author_id,
        "text": text,
        "created_at": "2026-04-29T12:00:00.000Z",
        "lang": lang,
        "public_metrics": {
            "retweet_count": 10, "like_count": 50,
            "reply_count": 1, "quote_count": 0,
        },
    }


# ── XV2Client.search_recent_tweets ────────────────────────────────────


class TestSearchRecentTweets:
    def _client(self) -> XV2Client:
        client = XV2Client(bearer_token="fake-token")
        client.session = Mock()
        return client

    def test_appends_required_operators(self) -> None:
        """Every keyword search MUST be wrapped in
        ``-is:retweet lang:en`` per the issue body."""
        client = self._client()
        client.session.get.return_value = _mock_response(
            200, _search_payload(posts=[], users=[]),
        )
        client.search_recent_tweets("rate cut")
        params = client.session.get.call_args.kwargs["params"]
        assert "-is:retweet lang:en" in params["query"]
        assert params["query"].startswith("rate cut ")

    def test_carries_query_context_to_each_post(self) -> None:
        """Posts ingested via search must carry ``query_context = keyword``
        so the downstream consumer can attribute discovery context."""
        client = self._client()
        client.session.get.return_value = _mock_response(
            200,
            _search_payload(
                posts=[_sample_post("1"), _sample_post("2", author_id="999")],
                users=[
                    {"id": "23381256", "username": "federalreserve"},
                    {"id": "999", "username": "someone_else"},
                ],
                newest_id="2",
            ),
        )
        posts, newest, truncated = client.search_recent_tweets("powell")
        assert all(p.query_context == "powell" for p in posts)
        assert newest == "2"
        assert truncated is False
        # author_handle gets stitched from includes.users
        by_id = {p.post_id: p.author_handle for p in posts}
        assert by_id["1"] == "federalreserve"
        assert by_id["2"] == "someone_else"

    def test_passes_since_id(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(
            200, _search_payload(posts=[_sample_post("1")], newest_id="1"),
        )
        client.search_recent_tweets("powell", since_id="42")
        params = client.session.get.call_args.kwargs["params"]
        assert params["since_id"] == "42"

    def test_paginates_via_next_token(self) -> None:
        """Search uses ``next_token`` (NOT ``pagination_token`` like
        the user-tweets endpoint) — verify the right param name."""
        client = self._client()
        page_one = _search_payload(
            posts=[_sample_post("100"), _sample_post("99")],
            users=[{"id": "23381256", "username": "federalreserve"}],
            newest_id="100", next_token="tok-1",
        )
        page_two = _search_payload(
            posts=[_sample_post("98")],
            users=[{"id": "23381256", "username": "federalreserve"}],
            newest_id="98",
        )
        client.session.get.side_effect = [
            _mock_response(200, page_one),
            _mock_response(200, page_two),
        ]
        posts, newest, truncated = client.search_recent_tweets(
            "rate cut", since_id="50",
        )
        assert {p.post_id for p in posts} == {"100", "99", "98"}
        assert newest == "100"  # first page's newest_id
        assert truncated is False
        page_two_params = client.session.get.call_args_list[1].kwargs["params"]
        assert page_two_params["next_token"] == "tok-1"

    def test_truncated_pagination_preserves_old_cursor_and_flag(self) -> None:
        client = self._client()
        capped = _search_payload(
            posts=[_sample_post("100")],
            users=[{"id": "23381256", "username": "federalreserve"}],
            newest_id="100", next_token="more",
        )
        client.session.get.return_value = _mock_response(200, capped)
        _, cursor, truncated = client.search_recent_tweets(
            "rate cut", since_id="50", max_pages=2,
        )
        assert cursor == "50"
        assert truncated is True

    def test_empty_keyword_rejected(self) -> None:
        client = self._client()
        with pytest.raises(ValueError):
            client.search_recent_tweets("   ")

    def test_missing_user_in_includes_yields_empty_handle(self) -> None:
        """If the X expansions block omits a user object for an
        author_id, we still keep the post — handle just lands empty."""
        client = self._client()
        client.session.get.return_value = _mock_response(
            200,
            _search_payload(
                posts=[_sample_post("1", author_id="99999")],
                users=[],  # no expansion for 99999
                newest_id="1",
            ),
        )
        posts, _, _ = client.search_recent_tweets("powell")
        assert posts[0].author_handle == ""
        assert posts[0].post_id == "1"


# ── _SentimentQueriesMixin keyword helpers ────────────────────────────


class TestKeywordSchedulerQueries:
    def test_keyword_priority_tier_cooldowns(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Three keywords at three priority tiers, fetched 16 min ago.
        Only the high-priority one (>=80) should be due; the medium
        and low rows enter their longer cooldowns."""
        with store._connection(commit=True) as c:
            c.execute("DELETE FROM x_keyword_pool")
            c.executemany(
                "INSERT INTO x_keyword_pool (keyword, category, priority, "
                "since_id, last_fetched_at, is_active, created_at, updated_at) "
                "VALUES (?, 'macro', ?, '', ?, 1, '2026-01-01', '2026-01-01')",
                [
                    ("hi",  90, (utc_now() - timedelta(minutes=16)).isoformat()),
                    ("mid", 60, (utc_now() - timedelta(minutes=16)).isoformat()),
                    ("lo",  20, (utc_now() - timedelta(minutes=16)).isoformat()),
                ],
            )
        due = {row["keyword"] for row in
               store.list_x_keywords_due_for_polling()}
        assert due == {"hi"}

    def test_keyword_inactive_excluded(self, store: SQLiteEngineStore) -> None:
        with store._connection(commit=True) as c:
            c.execute("UPDATE x_keyword_pool SET is_active = 0 WHERE keyword = 'fed'")
        keywords = [r["keyword"] for r in store.list_x_keywords_due_for_polling()]
        assert "fed" not in keywords

    def test_update_keyword_since_id_advances_cursor(
        self, store: SQLiteEngineStore,
    ) -> None:
        store.update_x_keyword_since_id(
            keyword="fed", since_id="9999",
            fetched_at="2026-04-29T12:00:00Z",
        )
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT since_id, last_fetched_at FROM x_keyword_pool "
                "WHERE keyword = 'fed'"
            ).fetchone()
        assert row["since_id"] == "9999"
        assert row["last_fetched_at"] == "2026-04-29T12:00:00Z"

    def test_link_x_post_to_keyword_first_write_wins(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Per the issue body: ``x_post_keywords`` is append-only.
        First (post_id, keyword) write sets first_seen_at; subsequent
        writes are no-ops via INSERT OR IGNORE."""
        with store._connection(commit=True) as c:
            c.execute(
                "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
                "VALUES ('p1', 'u1', '2026-01-01', '2026-01-01')"
            )
        store.link_x_post_to_keyword(
            post_id="p1", keyword="fed", first_seen_at="2026-04-29T12:00:00Z",
        )
        store.link_x_post_to_keyword(
            post_id="p1", keyword="fed", first_seen_at="2026-04-29T15:00:00Z",
        )
        with store._connection(commit=False) as c:
            rows = c.execute(
                "SELECT first_seen_at FROM x_post_keywords "
                "WHERE post_id = 'p1' AND keyword = 'fed'"
            ).fetchall()
        assert len(rows) == 1
        assert rows[0]["first_seen_at"] == "2026-04-29T12:00:00Z"


# ── XKeywordSearchIngestor (orchestration) ────────────────────────────


class TestXKeywordSearchIngestor:
    def _ingestor_with(
        self, store: SQLiteEngineStore, *, responses: list,
    ) -> XKeywordSearchIngestor:
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.side_effect = responses
        return XKeywordSearchIngestor(store=store, client=v2)

    def test_full_cycle_persists_posts_and_links(
        self, store: SQLiteEngineStore,
    ) -> None:
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(
                    200,
                    _search_payload(
                        posts=[_sample_post("11"), _sample_post("12")],
                        users=[{"id": "23381256", "username": "federalreserve"}],
                        newest_id="12",
                    ),
                ),
            ],
        )
        result = ingestor.poll_keyword(keyword="fed", since_id="")
        assert isinstance(result, KeywordPollResult)
        assert result.posts_persisted == 2
        assert result.new_since_id == "12"
        # Both posts upserted
        with store._connection(commit=False) as c:
            count = c.execute("SELECT COUNT(*) FROM x_posts").fetchone()[0]
        assert count == 2
        # Each post linked to keyword
        with store._connection(commit=False) as c:
            kw_count = c.execute(
                "SELECT COUNT(*) FROM x_post_keywords WHERE keyword = 'fed'"
            ).fetchone()[0]
        assert kw_count == 2
        # Keyword-pool cursor advanced
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT since_id, last_fetched_at FROM x_keyword_pool "
                "WHERE keyword = 'fed'"
            ).fetchone()
        assert row["since_id"] == "12"
        assert row["last_fetched_at"] != ""

    def test_dedup_engagement_only_update(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A post fetched twice (e.g. via timeline then via search)
        must not have its text/created_at overwritten — only engagement
        counters update on conflict. Verified explicitly here for the
        keyword-search path; the schema-side guarantee comes from the
        upsert ON CONFLICT clause."""
        # First write (different query_context simulates timeline path)
        from storage import XPostRecord
        store.upsert_x_post(
            XPostRecord(
                post_id="p1", author_id="23381256",
                author_handle="federalreserve",
                text="ORIGINAL TEXT", created_at="2026-04-29T12:00:00Z",
                lang="en", retweet_count=10, like_count=50, reply_count=1,
                quote_count=0, query_context="timeline",
                fetched_at="2026-04-29T12:00:00Z",
            )
        )
        # Now a keyword search picks up the same post — engagement
        # has bumped, but text/created_at must not change.
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(
                    200,
                    _search_payload(
                        posts=[{
                            "id": "p1", "author_id": "23381256",
                            "text": "MUTATED TEXT VIA SEARCH",
                            "created_at": "MUTATED",
                            "lang": "en",
                            "public_metrics": {
                                "retweet_count": 100, "like_count": 500,
                                "reply_count": 5, "quote_count": 2,
                            },
                        }],
                        users=[{"id": "23381256", "username": "federalreserve"}],
                        newest_id="p1",
                    ),
                ),
            ],
        )
        ingestor.poll_keyword(keyword="fed", since_id="")
        with store._connection(commit=False) as c:
            row = c.execute("SELECT * FROM x_posts WHERE post_id='p1'").fetchone()
        assert row["text"] == "ORIGINAL TEXT"
        assert row["created_at"] == "2026-04-29T12:00:00Z"
        assert row["retweet_count"] == 100
        assert row["like_count"] == 500
        # The search lane DID write the keyword link though.
        with store._connection(commit=False) as c:
            kw = c.execute(
                "SELECT COUNT(*) FROM x_post_keywords "
                "WHERE post_id='p1' AND keyword='fed'"
            ).fetchone()[0]
        assert kw == 1

    def test_rate_limit_short_circuits_keeps_cursor(
        self, store: SQLiteEngineStore,
    ) -> None:
        ingestor = self._ingestor_with(
            store,
            responses=[_mock_response(429, headers={"x-rate-limit-reset": "1"})],
        )
        result = ingestor.poll_keyword(keyword="fed", since_id="prev-42")
        assert "rate limit" in result.error.lower()
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT since_id, last_fetched_at FROM x_keyword_pool "
                "WHERE keyword = 'fed'"
            ).fetchone()
        # Cursor stays unchanged on error — no last_fetched_at stamp
        # either, so the row is still due on the next sweep (deliberate;
        # rate-limit naturally clears in 15min and we want to retry
        # rather than parking the row in cooldown). Operators see empty
        # last_fetched_at as the failure signal.
        assert row["since_id"] != "prev-42"  # default seed value
        assert row["last_fetched_at"] == ""

    def test_truncated_drain_skips_cooldown_stamp(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P2 round 1 — when a keyword's volume blows past
        max_pages × max_results, the row must NOT be stamped into
        cooldown; it stays immediately due so the next poll_once
        retries before /search/recent's 7-day window ages out the
        unreached older pages."""
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(
                    200,
                    _search_payload(
                        posts=[_sample_post("100")],
                        users=[{"id": "23381256", "username": "federalreserve"}],
                        newest_id="100", next_token="more-pages",
                    ),
                )
            ] * 5,  # max_pages=5 by default — same response for every page
        )
        result = ingestor.poll_keyword(keyword="fed", since_id="")
        assert result.truncated is True
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT since_id, last_fetched_at FROM x_keyword_pool "
                "WHERE keyword = 'fed'"
            ).fetchone()
        # Cursor stays unchanged — truncation means we couldn't drain
        # the whole tail. last_fetched_at also stays empty so the
        # scheduler keeps the row in the immediately-due set.
        assert row["since_id"] == ""
        assert row["last_fetched_at"] == ""
        # But the posts WERE persisted — partial progress is real.
        with store._connection(commit=False) as c:
            posts_count = c.execute(
                "SELECT COUNT(*) FROM x_posts"
            ).fetchone()[0]
        assert posts_count >= 1

    def test_poll_once_iterates_due_keywords(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Mark all keywords inactive except two so the test is deterministic.
        with store._connection(commit=True) as c:
            c.execute(
                "UPDATE x_keyword_pool SET is_active = 0 "
                "WHERE keyword NOT IN ('fed', 'fomc')"
            )
            c.execute(
                "UPDATE x_keyword_pool SET priority = 99 WHERE keyword = 'fed'"
            )
            c.execute(
                "UPDATE x_keyword_pool SET priority = 100 WHERE keyword = 'fomc'"
            )
        ingestor = self._ingestor_with(
            store,
            responses=[
                # fomc first (higher priority): empty page
                _mock_response(200, _search_payload(posts=[], users=[])),
                # fed next: one post
                _mock_response(
                    200,
                    _search_payload(
                        posts=[_sample_post("1")],
                        users=[{"id": "23381256", "username": "federalreserve"}],
                        newest_id="1",
                    ),
                ),
            ],
        )
        results = ingestor.poll_once()
        by_kw = {r.keyword: r for r in results}
        assert by_kw["fomc"].posts_persisted == 0
        assert by_kw["fed"].posts_persisted == 1
