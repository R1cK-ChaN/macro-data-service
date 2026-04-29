"""Soft-deletion availability patrol tests (issue #76 P4).

Covers the candidate-selection query (engagement floor + 72h window),
the batch lookup HTTP path, the per-id classification, and the
end-to-end orchestrator behavior (404 → mark unavailable, alive →
stamp checked-at and rotate to back of queue).
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
    AvailabilityPatrolResult,
    XAvailabilityPatrol,
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


def _seed_post(
    store: SQLiteEngineStore,
    *,
    post_id: str,
    likes: int = 0,
    retweets: int = 0,
    fetched_at: str | None = None,
    is_available: int = 1,
) -> None:
    fetched_at = fetched_at or utc_now().isoformat()
    with store._connection(commit=True) as c:
        c.execute(
            """
            INSERT INTO x_posts (
                post_id, author_id, author_handle, text, created_at,
                lang, retweet_count, like_count, reply_count, quote_count,
                query_context, fetched_at, is_available
            ) VALUES (?, 'u1', '', '', ?, 'en', ?, ?, 0, 0, '', ?, ?)
            """,
            (post_id, fetched_at, retweets, likes, fetched_at, is_available),
        )


# ── Candidate selection ──────────────────────────────────────────────


class TestAvailabilityCandidateQuery:
    def test_below_engagement_floor_excluded(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Issue body: engagement threshold is ``> 50`` — a post at
        exactly 50 must NOT be picked up."""
        _seed_post(store, post_id="p_low", likes=30, retweets=20)  # 50 — at floor
        _seed_post(store, post_id="p_high", likes=40, retweets=20)  # 60 — over
        ids = store.list_x_posts_for_availability_check()
        assert "p_low" not in ids
        assert "p_high" in ids

    def test_old_posts_excluded(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Posts fetched > 72h ago drop out of the patrol window —
        the issue body bounds the patrol to recent / popular posts."""
        now = utc_now()
        _seed_post(
            store, post_id="p_recent", likes=100,
            fetched_at=(now - timedelta(hours=1)).isoformat(),
        )
        _seed_post(
            store, post_id="p_old", likes=100,
            fetched_at=(now - timedelta(hours=80)).isoformat(),
        )
        ids = store.list_x_posts_for_availability_check(
            now_iso=now.isoformat(),
        )
        assert "p_recent" in ids
        assert "p_old" not in ids

    def test_already_unavailable_excluded(
        self, store: SQLiteEngineStore,
    ) -> None:
        _seed_post(
            store, post_id="p_dead", likes=200, retweets=200,
            is_available=0,
        )
        ids = store.list_x_posts_for_availability_check()
        assert "p_dead" not in ids

    def test_orders_by_least_recently_checked_first(
        self, store: SQLiteEngineStore,
    ) -> None:
        """The patrol runs every 6h; the queue should rotate in least-
        recently-checked-first order so a single sweep covers the
        whole working set evenly."""
        _seed_post(store, post_id="p_a", likes=100)
        _seed_post(store, post_id="p_b", likes=100)
        _seed_post(store, post_id="p_c", likes=100)
        # Stamp checks at different times.
        with store._connection(commit=True) as c:
            c.execute(
                "UPDATE x_posts SET availability_checked_at = '2026-04-29T10:00:00Z' "
                "WHERE post_id = 'p_a'"
            )
            c.execute(
                "UPDATE x_posts SET availability_checked_at = '2026-04-29T08:00:00Z' "
                "WHERE post_id = 'p_b'"
            )
            # p_c left at default '' — sorts before any non-empty
            # value lexicographically, so it should come first.
        ids = store.list_x_posts_for_availability_check()
        assert ids[0] == "p_c"
        assert ids.index("p_b") < ids.index("p_a")


# ── Batch HTTP lookup ────────────────────────────────────────────────


class TestLookupTweets:
    def _client(self) -> XV2Client:
        client = XV2Client(bearer_token="fake-token")
        client.session = Mock()
        return client

    def test_classifies_found_and_not_found(self) -> None:
        client = self._client()
        client.session.get.return_value = _mock_response(
            200,
            {
                "data": [{"id": "100"}, {"id": "101"}],
                "errors": [
                    {
                        "value": "999",
                        "title": "Not Found Error",
                        "type": "https://api.twitter.com/2/problems/resource-not-found",
                    }
                ],
            },
        )
        found, not_found = client.lookup_tweets(["100", "101", "999"])
        assert found == {"100", "101"}
        assert not_found == {"999"}

    def test_non_not_found_errors_not_counted_as_deletions(self) -> None:
        """Other error types (e.g. transient lookup failures) must NOT
        flip is_available — only ``Not Found`` resource errors."""
        client = self._client()
        client.session.get.return_value = _mock_response(
            200,
            {
                "data": [{"id": "100"}],
                "errors": [
                    {
                        "value": "200",
                        "title": "Unauthorized",
                        "type": "https://api.twitter.com/2/problems/not-authorized-for-resource",
                    },
                ],
            },
        )
        found, not_found = client.lookup_tweets(["100", "200"])
        assert found == {"100"}
        assert not_found == set()

    def test_batch_size_cap_raises(self) -> None:
        client = self._client()
        with pytest.raises(ValueError):
            client.lookup_tweets([str(i) for i in range(101)])

    def test_empty_input_no_http_call(self) -> None:
        client = self._client()
        found, not_found = client.lookup_tweets([])
        assert found == set()
        assert not_found == set()
        assert client.session.get.call_count == 0


# ── Orchestrator ─────────────────────────────────────────────────────


class TestXAvailabilityPatrol:
    def _patrol(
        self, store: SQLiteEngineStore, response_body: dict,
    ) -> XAvailabilityPatrol:
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.return_value = _mock_response(200, response_body)
        return XAvailabilityPatrol(store=store, client=v2)

    def test_404_marks_unavailable(self, store: SQLiteEngineStore) -> None:
        _seed_post(store, post_id="p_dead", likes=100, retweets=100)
        _seed_post(store, post_id="p_alive", likes=100, retweets=100)
        patrol = self._patrol(
            store,
            {
                "data": [{"id": "p_alive"}],
                "errors": [
                    {
                        "value": "p_dead",
                        "title": "Not Found Error",
                        "type": "https://api.twitter.com/2/problems/resource-not-found",
                    }
                ],
            },
        )
        result = patrol.run()
        assert isinstance(result, AvailabilityPatrolResult)
        assert "p_dead" in result.marked_unavailable
        assert "p_alive" not in result.marked_unavailable
        with store._connection(commit=False) as c:
            dead_row = c.execute(
                "SELECT is_available, availability_checked_at FROM x_posts "
                "WHERE post_id = 'p_dead'"
            ).fetchone()
            alive_row = c.execute(
                "SELECT is_available, availability_checked_at FROM x_posts "
                "WHERE post_id = 'p_alive'"
            ).fetchone()
        assert dead_row["is_available"] == 0
        assert dead_row["availability_checked_at"] != ""
        # Alive post stays available, but its checked-at is stamped.
        assert alive_row["is_available"] == 1
        assert alive_row["availability_checked_at"] != ""

    def test_no_candidates_no_http_call(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Only seed below-floor posts — nothing for the patrol to do.
        _seed_post(store, post_id="p_quiet", likes=10)
        patrol = self._patrol(store, {"data": []})
        result = patrol.run()
        assert result.checked == 0
        assert result.marked_unavailable == ()
        assert patrol.client.session.get.call_count == 0

    def test_chunks_into_100_id_batches(
        self, store: SQLiteEngineStore,
    ) -> None:
        # Seed 150 posts — should hit /tweets twice (100 + 50).
        # Use deterministic post_ids that match the patrol's sort order
        # (availability_checked_at ASC, fetched_at DESC) — IDs seeded
        # later have newer fetched_at and come first.
        for i in range(150):
            _seed_post(
                store, post_id=f"p{i}", likes=100, retweets=100,
            )
        # Newest-fetched first → the patrol queues p149, p148, ..., p0.
        # All IDs returned in ``data`` so all are classified (found),
        # giving an exact ``checked == 150`` for the metric.
        ids_first_chunk = [f"p{i}" for i in range(149, 49, -1)]
        ids_second_chunk = [f"p{i}" for i in range(49, -1, -1)]
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.side_effect = [
            _mock_response(200, {"data": [{"id": pid} for pid in ids_first_chunk]}),
            _mock_response(200, {"data": [{"id": pid} for pid in ids_second_chunk]}),
        ]
        patrol = XAvailabilityPatrol(store=store, client=v2)
        result = patrol.run()
        assert v2.session.get.call_count == 2
        assert result.checked == 150

    def test_resurfaced_post_restores_availability(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P4 round 2: a previously-marked-unavailable post that
        resurfaces in a fresh timeline/search fetch must flip
        ``is_available`` back to 1 — otherwise downstream spike SQL
        keeps it suppressed even though the X API just re-confirmed
        it as publicly visible."""
        from storage import XPostRecord
        # Initial fetch + patrol marks unavailable.
        rec = XPostRecord(
            post_id="p1", author_id="23381256",
            author_handle="federalreserve",
            text="rate cut signal",
            created_at="2026-04-29T12:00:00Z",
            lang="en", retweet_count=10, like_count=50,
            reply_count=2, quote_count=1, query_context="timeline",
            fetched_at="2026-04-29T12:00:00Z",
        )
        store.upsert_x_post(rec)
        store.mark_x_post_unavailable(
            post_id="p1", checked_at="2026-04-29T18:00:00Z",
        )
        with store._connection(commit=False) as c:
            assert c.execute(
                "SELECT is_available FROM x_posts WHERE post_id = 'p1'"
            ).fetchone()["is_available"] == 0

        # Post resurfaces — fresh fetch with new engagement.
        store.upsert_x_post(
            XPostRecord(
                post_id="p1", author_id="23381256",
                author_handle="federalreserve",
                text="ignored on conflict",
                created_at="ignored on conflict",
                lang="en", retweet_count=200, like_count=500,
                reply_count=10, quote_count=5, query_context="timeline",
                fetched_at="2026-04-30T08:00:00Z",
            )
        )
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT is_available, availability_checked_at, "
                "       like_count, fetched_at, text "
                "FROM x_posts WHERE post_id = 'p1'"
            ).fetchone()
        # Availability restored, fresh fetched_at carried into
        # availability_checked_at, engagement updated, original text
        # preserved.
        assert row["is_available"] == 1
        assert row["availability_checked_at"] == "2026-04-30T08:00:00Z"
        assert row["like_count"] == 500
        assert row["fetched_at"] == "2026-04-30T08:00:00Z"
        assert row["text"] == "rate cut signal"

    def test_non_404_per_id_errors_not_counted_as_checked(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P4 round 1: when an ID returns a non-404 per-id error
        the patrol leaves it unstamped for the next sweep, so it must
        NOT count toward ``checked`` either — operator coverage
        metrics should reflect actually completed checks."""
        _seed_post(store, post_id="p_alive", likes=100, retweets=100)
        _seed_post(store, post_id="p_unauthorized", likes=100, retweets=100)
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.return_value = _mock_response(
            200,
            {
                "data": [{"id": "p_alive"}],
                "errors": [
                    {
                        "value": "p_unauthorized",
                        "title": "Unauthorized",
                        "type": "https://api.twitter.com/2/problems/not-authorized-for-resource",
                    },
                ],
            },
        )
        patrol = XAvailabilityPatrol(store=store, client=v2)
        result = patrol.run()
        # Only p_alive was classified → checked=1, not 2
        assert result.checked == 1
        # p_unauthorized stays in is_available=1 with empty
        # availability_checked_at so the next sweep retries.
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT is_available, availability_checked_at "
                "FROM x_posts WHERE post_id = 'p_unauthorized'"
            ).fetchone()
        assert row["is_available"] == 1
        assert row["availability_checked_at"] == ""

    def test_rate_limit_short_circuits_after_partial_progress(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A 429 mid-sweep must surface in the result with the
        already-marked-unavailable list intact — operators see how
        much progress was made."""
        # Seed q's first so they have older fetched_at, then p_dead
        # last so it lands in chunk 1 under the
        # ``ORDER BY availability_checked_at ASC, fetched_at DESC``
        # queue.
        for i in range(100):
            _seed_post(store, post_id=f"q{i}", likes=200, retweets=200)
        _seed_post(store, post_id="p_dead", likes=100, retweets=100)
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.side_effect = [
            _mock_response(
                200,
                {
                    "data": [],
                    "errors": [
                        {
                            "value": "p_dead",
                            "title": "Not Found Error",
                            "type": "https://api.twitter.com/2/problems/resource-not-found",
                        }
                    ],
                },
            ),
            _mock_response(429, headers={"x-rate-limit-reset": "1"}),
        ]
        patrol = XAvailabilityPatrol(store=store, client=v2)
        result = patrol.run()
        assert "p_dead" in result.marked_unavailable
        assert "rate limit" in result.error.lower()