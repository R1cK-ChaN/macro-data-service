"""Timeline polling tests for the X (Twitter) ingestion lane (issue #76 P1).

Covers the HTTP client (XV2Client), the orchestrator (XTimelineIngestor),
and the storage write-path / scheduler queries on _SentimentQueriesMixin.
HTTP is mocked through ``Mock(spec=requests.Response)`` matching the
EODHD client test pattern — no live API calls.
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
    TimelinePollResult,
    XAuthError,
    XNotFoundError,
    XPost,
    XRateLimitError,
    XTimelineIngestor,
    XV2Client,
)
from storage import SQLiteEngineStore, XPostRecord


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _mock_response(status_code: int, json_body: dict | None = None,
                   *, text: str = "", headers: dict | None = None) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.json.return_value = json_body or {}
    response.text = text
    response.headers = headers or {}
    return response


def _user_payload(user_id: str = "23381256",
                  username: str = "federalreserve") -> dict:
    return {"data": {"id": user_id, "username": username}}


def _timeline_payload(
    *, posts: list[dict] | None = None, newest_id: str = "",
) -> dict:
    body: dict = {"data": posts or []}
    if newest_id:
        body["meta"] = {"newest_id": newest_id, "result_count": len(posts or [])}
    return body


def _sample_post(
    post_id: str, *, author_id: str = "23381256",
    text: str = "rate cut signal", lang: str = "en",
    likes: int = 100, retweets: int = 50,
) -> dict:
    return {
        "id": post_id,
        "author_id": author_id,
        "text": text,
        "created_at": "2026-04-29T12:00:00.000Z",
        "lang": lang,
        "public_metrics": {
            "retweet_count": retweets,
            "like_count": likes,
            "reply_count": 5,
            "quote_count": 2,
        },
    }


# ── XV2Client (low-level HTTP) ────────────────────────────────────────


class TestXV2Client:
    def _client(self) -> XV2Client:
        client = XV2Client(bearer_token="fake-token")
        client.session = Mock()
        return client

    def test_resolves_handle_to_user_id(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(200, _user_payload())
        user = client.get_user_by_username("federalreserve")
        assert user.user_id == "23381256"
        assert user.username == "federalreserve"
        called_url = client.session.get.call_args[0][0]
        assert called_url.endswith("/users/by/username/federalreserve")

    def test_handle_resolution_404_raises_not_found(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(404)
        with pytest.raises(XNotFoundError):
            client.get_user_by_username("definitely-not-a-handle")

    def test_auth_error_maps_to_xautherror(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(401)
        with pytest.raises(XAuthError):
            client.get_user_by_username("federalreserve")

    def test_rate_limit_carries_reset_epoch(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(
            429, headers={"x-rate-limit-reset": "1735689600"},
        )
        with pytest.raises(XRateLimitError) as ei:
            client.get_user_by_username("federalreserve")
        assert ei.value.reset_epoch == 1735689600

    def test_get_user_tweets_passes_since_id(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(
            200, _timeline_payload(posts=[_sample_post("1")], newest_id="1"),
        )
        posts, newest = client.get_user_tweets(
            "23381256", since_id="0", author_handle="federalreserve",
        )
        assert len(posts) == 1
        assert newest == "1"
        params = client.session.get.call_args.kwargs["params"]
        assert params["since_id"] == "0"
        assert "tweet.fields" in params

    def test_get_user_tweets_empty_keeps_old_since_id(self) -> None:
        """No new posts — meta block missing — must NOT blank since_id."""
        client = self._client()
        client.session.get.return_value = _mock_response(200, {"meta": {}})
        posts, newest = client.get_user_tweets(
            "23381256", since_id="42", author_handle="federalreserve",
        )
        assert posts == []
        assert newest == "42"

    def test_get_user_tweets_paginates_before_advancing_cursor(self) -> None:
        """When the unseen tail spans multiple pages, the older pages
        live behind ``meta.next_token``. We must drain them before
        advancing — otherwise the first page's newest_id becomes the
        new since_id and the older intermediate tweets fall behind it
        on the next sweep, never to be fetched."""
        client = self._client()
        page_one = {
            "data": [_sample_post("100"), _sample_post("99")],
            "meta": {"newest_id": "100", "oldest_id": "99",
                     "next_token": "tok-1"},
        }
        page_two = {
            "data": [_sample_post("98"), _sample_post("97")],
            "meta": {"newest_id": "98", "oldest_id": "97",
                     "next_token": "tok-2"},
        }
        page_three = {
            "data": [_sample_post("96")],
            "meta": {"newest_id": "96", "oldest_id": "96"},
        }
        client.session.get.side_effect = [
            _mock_response(200, page_one),
            _mock_response(200, page_two),
            _mock_response(200, page_three),
        ]
        posts, newest = client.get_user_tweets(
            "23381256", since_id="50", author_handle="federalreserve",
        )
        # All five posts (across three pages) returned.
        assert {p.post_id for p in posts} == {"100", "99", "98", "97", "96"}
        # Cursor advances to the FIRST page's newest_id, not the last.
        assert newest == "100"
        # Pagination tokens passed correctly.
        page_two_params = client.session.get.call_args_list[1].kwargs["params"]
        assert page_two_params["pagination_token"] == "tok-1"
        page_three_params = client.session.get.call_args_list[2].kwargs["params"]
        assert page_three_params["pagination_token"] == "tok-2"

    def test_pagination_max_pages_caps_runaway_loop(self) -> None:
        """A misbehaving upstream that always ships next_token must
        terminate after ``max_pages`` calls — the cap protects the
        per-account budget."""
        client = self._client()
        looping_page = {
            "data": [_sample_post("1")],
            "meta": {"newest_id": "1", "next_token": "loop"},
        }
        client.session.get.return_value = _mock_response(200, looping_page)
        client.get_user_tweets(
            "23381256", since_id="0", author_handle="federalreserve",
            max_pages=3,
        )
        assert client.session.get.call_count == 3

    def test_truncated_pagination_preserves_old_cursor(self) -> None:
        """When the unseen tail is longer than ``max_pages * max_results``
        we hit the cap with ``next_token`` still set and older pages
        unreachable. Advancing the cursor here would make those pages
        permanently unreachable; preserve the OLD ``since_id`` so the
        next sweep resumes the drain (upserts dedupe duplicates)."""
        client = self._client()
        capped_page = {
            "data": [_sample_post("100")],
            "meta": {"newest_id": "100", "next_token": "more-older-pages"},
        }
        client.session.get.return_value = _mock_response(200, capped_page)
        posts, cursor = client.get_user_tweets(
            "23381256", since_id="50", author_handle="federalreserve",
            max_pages=2,
        )
        # Posts from drained pages were captured.
        assert len(posts) == 2
        # Cursor stays at the OLD since_id — the next sweep re-fetches
        # and drains another chunk; never advances over unseen pages.
        assert cursor == "50"

    def test_missing_bearer_token_raises_auth_error(self) -> None:
        client = XV2Client(bearer_token="")
        with pytest.raises(XAuthError):
            client.get_user_by_username("federalreserve")

    def test_malformed_post_skipped(self) -> None:
        """A row without ``id`` must be dropped, not crash the batch."""
        client = self._client()
        client.session.get.return_value = _mock_response(
            200,
            _timeline_payload(
                posts=[
                    {"id": "1", "author_id": "23381256", "created_at": "2026-04-29T12:00:00Z"},
                    {"text": "no id here"},
                    {"id": "2", "author_id": "23381256", "created_at": "2026-04-29T13:00:00Z"},
                ],
                newest_id="2",
            ),
        )
        posts, _ = client.get_user_tweets(
            "23381256", since_id="0", author_handle="federalreserve",
        )
        assert {p.post_id for p in posts} == {"1", "2"}


# ── _SentimentQueriesMixin ────────────────────────────────────────────


class TestSentimentQueries:
    def test_upsert_x_post_inserts_then_updates_engagement(
        self, store: SQLiteEngineStore,
    ) -> None:
        rec = XPostRecord(
            post_id="p1", author_id="23381256", author_handle="federalreserve",
            text="rate cut signal", created_at="2026-04-29T12:00:00Z",
            lang="en", retweet_count=10, like_count=50, reply_count=2,
            quote_count=1, query_context="timeline",
            fetched_at="2026-04-29T12:05:00Z",
        )
        store.upsert_x_post(rec)
        # Re-poll an hour later — engagement counts went up; original
        # text/created_at must NOT be overwritten.
        store.upsert_x_post(
            XPostRecord(
                post_id="p1", author_id="23381256", author_handle="federalreserve",
                text="OVERWRITE ATTEMPT", created_at="OVERWRITE",
                lang="en", retweet_count=200, like_count=1000,
                reply_count=42, quote_count=10, query_context="timeline",
                fetched_at="2026-04-29T13:05:00Z",
            )
        )
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT * FROM x_posts WHERE post_id = 'p1'"
            ).fetchone()
        assert row["text"] == "rate cut signal"
        assert row["created_at"] == "2026-04-29T12:00:00Z"
        assert row["retweet_count"] == 200
        assert row["like_count"] == 1000
        assert row["fetched_at"] == "2026-04-29T13:05:00Z"

    def test_update_user_id_sets_value(self, store: SQLiteEngineStore) -> None:
        store.update_x_tracked_account_user_id(
            handle="federalreserve", user_id="23381256",
        )
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["user_id"] == "23381256"

    def test_update_since_id_advances_cursor(self, store: SQLiteEngineStore) -> None:
        store.update_x_tracked_account_since_id(
            handle="federalreserve", since_id="9999",
            fetched_at="2026-04-29T12:00:00Z",
        )
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["since_id"] == "9999"
        assert row["last_fetched_at"] == "2026-04-29T12:00:00Z"

    def test_priority_tier_cooldowns(self, store: SQLiteEngineStore) -> None:
        """Three accounts at three priority tiers, each just-fetched at
        different offsets. The 15-min tier is due at 16 min; the 30-min
        tier is not due at 16 min but is at 35 min; the hourly tier needs
        an hour."""
        with store._connection(commit=True) as c:
            c.execute("DELETE FROM x_tracked_accounts")
            c.executemany(
                "INSERT INTO x_tracked_accounts (handle, user_id, category, "
                "priority, last_fetched_at, is_active, created_at, updated_at) "
                "VALUES (?, '', '', ?, ?, 1, '2026-01-01', '2026-01-01')",
                [
                    # high priority, fetched 16 min ago — DUE
                    ("hi",  90, (utc_now() - timedelta(minutes=16)).isoformat()),
                    # medium priority, fetched 16 min ago — NOT DUE (needs 30)
                    ("mid", 60, (utc_now() - timedelta(minutes=16)).isoformat()),
                    # low priority, fetched 16 min ago — NOT DUE (needs 60)
                    ("lo",  20, (utc_now() - timedelta(minutes=16)).isoformat()),
                ],
            )
        due = {row["handle"] for row in
               store.list_x_tracked_accounts_due_for_polling()}
        assert due == {"hi"}

        # Now fast-forward — 35 min after fetch — mid should be due, lo still not.
        future = (utc_now() + timedelta(minutes=19)).isoformat()
        due_later = {row["handle"] for row in
                     store.list_x_tracked_accounts_due_for_polling(now_iso=future)}
        assert due_later == {"hi", "mid"}

    def test_never_fetched_accounts_are_due(self, store: SQLiteEngineStore) -> None:
        """Fresh-seeded rows have last_fetched_at='' — must surface
        unconditionally so the bootstrap pass picks them up."""
        rows = store.list_x_tracked_accounts_due_for_polling()
        assert rows, "fresh-seeded rows should be due"
        assert all(r["last_fetched_at"] == "" for r in rows)

    def test_inactive_accounts_excluded(self, store: SQLiteEngineStore) -> None:
        with store._connection(commit=True) as c:
            c.execute(
                "UPDATE x_tracked_accounts SET is_active = 0 "
                "WHERE handle = 'federalreserve'"
            )
        handles = [r["handle"] for r in
                   store.list_x_tracked_accounts_due_for_polling()]
        assert "federalreserve" not in handles


# ── XTimelineIngestor (orchestration) ─────────────────────────────────


class TestXTimelineIngestor:
    def _ingestor_with(
        self, store: SQLiteEngineStore, *, responses: list,
    ) -> XTimelineIngestor:
        """Build an ingestor whose XV2Client.session.get walks through
        the provided responses (in order) — one per HTTP call."""
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.side_effect = responses
        return XTimelineIngestor(store=store, client=v2)

    def test_resolves_then_polls_then_persists_since_id(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Seed has user_id='' for every handle. Bootstrap must resolve
        # then poll, then persist since_id.
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(200, _user_payload()),
                _mock_response(
                    200,
                    _timeline_payload(
                        posts=[_sample_post("11"), _sample_post("12")],
                        newest_id="12",
                    ),
                ),
            ],
        )
        result = ingestor.poll_account(
            handle="federalreserve", user_id="", since_id="",
        )
        assert isinstance(result, TimelinePollResult)
        assert result.posts_persisted == 2
        assert result.new_since_id == "12"
        assert result.user_id == "23381256"
        assert result.error == ""
        # since_id + user_id persisted
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["user_id"] == "23381256"
        assert row["since_id"] == "12"
        with store._connection(commit=False) as c:
            count = c.execute(
                "SELECT COUNT(*) FROM x_posts WHERE author_handle = 'federalreserve'"
            ).fetchone()[0]
        assert count == 2

    def test_skip_resolution_when_user_id_present(
        self, store: SQLiteEngineStore,
    ) -> None:
        store.update_x_tracked_account_user_id(
            handle="federalreserve", user_id="23381256",
        )
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(
                    200, _timeline_payload(posts=[_sample_post("1")], newest_id="1"),
                ),
            ],
        )
        ingestor.poll_account(
            handle="federalreserve", user_id="23381256", since_id="",
        )
        # Only ONE call should have happened (timeline) — no resolver call.
        assert ingestor.client.session.get.call_count == 1

    def test_handle_not_found_returns_error_no_persist(
        self, store: SQLiteEngineStore,
    ) -> None:
        ingestor = self._ingestor_with(
            store,
            responses=[_mock_response(404)],
        )
        result = ingestor.poll_account(
            handle="federalreserve", user_id="", since_id="",
        )
        assert result.error == "handle_not_found"
        # since_id stays empty
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["since_id"] == ""

    def test_handle_not_found_stamps_cooldown(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A permanent-404 handle must be moved out of the
        immediately-due set — without this, every sweep re-calls the
        paid resolver for the same misspelled handle. Stamping
        ``last_fetched_at`` puts the row into its priority-tier
        cooldown so it only retries on the schedule (15min/30min/1h)."""
        ingestor = self._ingestor_with(
            store,
            responses=[_mock_response(404)],
        )
        ingestor.poll_account(
            handle="federalreserve", user_id="", since_id="",
        )
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["last_fetched_at"] != ""
        # Confirm the row no longer surfaces in the immediately-due
        # query — only the OTHER seeded rows should.
        due_handles = [r["handle"] for r in
                       store.list_x_tracked_accounts_due_for_polling()]
        assert "federalreserve" not in due_handles

    def test_rate_limit_short_circuits(
        self, store: SQLiteEngineStore,
    ) -> None:
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(200, _user_payload()),
                _mock_response(429, headers={"x-rate-limit-reset": "1"}),
            ],
        )
        result = ingestor.poll_account(
            handle="federalreserve", user_id="", since_id="",
        )
        assert "rate limit" in result.error.lower()
        # user_id was resolved and persisted; since_id stays empty.
        row = store.get_x_tracked_account_by_handle("federalreserve")
        assert row is not None
        assert row["user_id"] == "23381256"
        assert row["since_id"] == ""

    def test_one_account_failure_doesnt_block_others(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Limit pollable rows to two by deactivating the rest.
        with store._connection(commit=True) as c:
            c.execute(
                "UPDATE x_tracked_accounts SET is_active = 0 "
                "WHERE handle NOT IN ('federalreserve', 'ecb')"
            )
            # Force ordering: federalreserve priority 100, ecb priority 100;
            # both are seeded that way. Bump federalreserve to 99 so ecb
            # comes first deterministically.
            c.execute(
                "UPDATE x_tracked_accounts SET priority = 99 "
                "WHERE handle = 'federalreserve'"
            )
        ingestor = self._ingestor_with(
            store,
            responses=[
                # ecb first (priority 100): resolution 404 → error
                _mock_response(404),
                # federalreserve next: resolution OK → timeline empty
                _mock_response(200, _user_payload()),
                _mock_response(200, _timeline_payload(posts=[], newest_id="")),
            ],
        )
        results = ingestor.poll_once()
        by_handle = {r.handle: r for r in results}
        assert by_handle["ecb"].error == "handle_not_found"
        assert by_handle["federalreserve"].error == ""

    def test_post_query_context_is_timeline(
        self, store: SQLiteEngineStore,
    ) -> None:
        """All posts ingested through this lane carry
        ``query_context='timeline'`` — keyword search ships its own
        context string in P2."""
        ingestor = self._ingestor_with(
            store,
            responses=[
                _mock_response(200, _user_payload()),
                _mock_response(
                    200, _timeline_payload(posts=[_sample_post("1")], newest_id="1"),
                ),
            ],
        )
        ingestor.poll_account(
            handle="federalreserve", user_id="", since_id="",
        )
        with store._connection(commit=False) as c:
            ctx = c.execute(
                "SELECT query_context FROM x_posts WHERE post_id='1'"
            ).fetchone()["query_context"]
        assert ctx == "timeline"
