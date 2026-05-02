"""Hot-topic discovery on the X (Twitter) sentiment lane.

Issue #76 P3 originally bundled spike detection with a calendar-event
write-back. Issue #113 P2 unwound the write-back; the detector now
returns spike observations and nothing else writes to ``cal_econ_event``
from this lane.

Covers:

* Volume-spike SQL on ``_SentimentQueriesMixin`` — tier mapping,
  baseline floor, future-dated post exclusion.
* ``XSpikeDetector.run`` mapping the SQL rows to ``SpikeObservation``
  records (no calendar side effect).
* Broad-discovery hashtag mining — entities flag hits the API,
  hashtags are extracted, novel ones land in ``x_keyword_pool`` with
  ``category='derived'``, and existing keywords are not double-added.
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
    SpikeObservation,
    XHashtagDiscoveryRunner,
    XSpikeDetector,
    XV2Client,
)
from storage import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _mock_response(status_code: int, json_body: dict | None = None) -> Mock:
    response = Mock()
    response.status_code = status_code
    response.json.return_value = json_body or {}
    response.text = ""
    response.headers = {}
    return response


def _seed_posts_and_keyword(
    store: SQLiteEngineStore,
    *,
    keyword: str,
    posts: list[tuple[str, str]],  # (post_id, created_at_iso)
) -> None:
    """Populate x_posts + x_post_keywords for spike-detector tests."""
    with store._connection(commit=True) as c:
        for post_id, created_at in posts:
            c.execute(
                "INSERT OR IGNORE INTO x_posts ("
                "  post_id, author_id, created_at, fetched_at, is_available"
                ") VALUES (?, 'u1', ?, ?, 1)",
                (post_id, created_at, created_at),
            )
            c.execute(
                "INSERT OR IGNORE INTO x_post_keywords ("
                "  post_id, keyword, first_seen_at"
                ") VALUES (?, ?, ?)",
                (post_id, keyword, created_at),
            )


# ── Volume-spike SQL ──────────────────────────────────────────────────


class TestVolumeSpikeDetection:
    def test_keyword_with_3x_burst_is_detected(
        self, store: SQLiteEngineStore,
    ) -> None:
        """20 posts in the last hour, 24 in total over 24h →
        per-hour avg = 1, recent = 20 → > 3× spike fires."""
        now = utc_now()
        # 20 posts in the trailing hour (last 30 min)
        recent = [
            (f"r{i}", (now - timedelta(minutes=i)).isoformat())
            for i in range(20)
        ]
        # 4 background posts spread across 23 prior hours
        background = [
            (f"b{i}", (now - timedelta(hours=4 + i * 5)).isoformat())
            for i in range(4)
        ]
        _seed_posts_and_keyword(
            store, keyword="rate cut", posts=recent + background,
        )
        spikes = store.detect_x_volume_spikes(now_iso=now.isoformat())
        by_keyword = {row["keyword"]: row for row in spikes}
        assert "rate cut" in by_keyword
        assert int(by_keyword["rate cut"]["count_window"]) == 20
        # 20 (window) + 4 (older) = 24 total in 24h baseline
        assert int(by_keyword["rate cut"]["count_baseline"]) == 24

    def test_below_baseline_floor_not_emitted(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A keyword with 1 post in last 1h and 1 total in 24h
        ratio-passes (1 > 3 * 1/24) but min_baseline_count=12 floors
        it out to keep noise from triggering false spikes."""
        now = utc_now()
        _seed_posts_and_keyword(
            store, keyword="quiet", posts=[("q1", (now - timedelta(minutes=10)).isoformat())],
        )
        spikes = store.detect_x_volume_spikes(now_iso=now.isoformat())
        keywords = {row["keyword"] for row in spikes}
        assert "quiet" not in keywords

    def test_normal_steady_volume_not_detected(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A keyword posting 1/h steadily for 24h has count_window=1,
        count_baseline=24 → 1 * 24 = 24 = 3 * 24/3 — NOT > threshold."""
        now = utc_now()
        steady = [
            (f"s{i}", (now - timedelta(minutes=i * 60)).isoformat())
            for i in range(24)
        ]
        _seed_posts_and_keyword(store, keyword="steady", posts=steady)
        spikes = store.detect_x_volume_spikes(now_iso=now.isoformat())
        keywords = {row["keyword"] for row in spikes}
        assert "steady" not in keywords

    def test_window_length_scales_correctly(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P3 round 1: a 6-hour window must compare per-hour
        rates, not raw totals. 6 posts in last 6h (1/h) and 24 in
        last 24h (1/h) is steady traffic — must NOT fire."""
        now = utc_now()
        steady = [
            (f"s{i}", (now - timedelta(minutes=i * 60)).isoformat())
            for i in range(24)
        ]
        _seed_posts_and_keyword(store, keyword="steady6h", posts=steady)
        spikes = store.detect_x_volume_spikes(
            now_iso=now.isoformat(), window_hours=6,
        )
        keywords = {row["keyword"] for row in spikes}
        assert "steady6h" not in keywords

    def test_unavailable_posts_excluded_from_spike(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Soft-deleted (is_available=0) posts must not count toward
        a spike. With them excluded, the keyword falls below the
        baseline floor."""
        now = utc_now()
        recent = [
            (f"r{i}", (now - timedelta(minutes=i)).isoformat())
            for i in range(20)
        ]
        _seed_posts_and_keyword(store, keyword="rate cut", posts=recent)
        with store._connection(commit=True) as c:
            c.execute("UPDATE x_posts SET is_available = 0")
        spikes = store.detect_x_volume_spikes(now_iso=now.isoformat())
        assert all(r["keyword"] != "rate cut" for r in spikes)

    def test_future_dated_posts_excluded(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P3 round 2: posts with created_at > now_iso (replay,
        clock skew) must not contribute to spike counts."""
        now = utc_now()
        # Mostly past posts, but two with future stamps.
        seeded = [
            (f"r{i}", (now - timedelta(minutes=i)).isoformat())
            for i in range(20)
        ]
        future = [
            ("f1", (now + timedelta(hours=2)).isoformat()),
            ("f2", (now + timedelta(hours=3)).isoformat()),
        ]
        _seed_posts_and_keyword(store, keyword="rate cut",
                                posts=seeded + future)
        spikes = store.detect_x_volume_spikes(now_iso=now.isoformat())
        by_kw = {row["keyword"]: row for row in spikes}
        # count_window should be exactly 20 (all past, in last hour),
        # not 22 (which would include the two future stamps).
        assert int(by_kw["rate cut"]["count_window"]) == 20
        assert int(by_kw["rate cut"]["count_baseline"]) == 20


# ── Spike detector runner (no calendar side-effect after #113 P2) ─────


class TestXSpikeDetectorRunner:
    def test_run_returns_one_observation_per_spike(
        self, store: SQLiteEngineStore,
    ) -> None:
        now = utc_now()
        # Spike-eligible: 20 in last hour, 24 in last 24h
        recent = [
            (f"r{i}", (now - timedelta(minutes=i)).isoformat())
            for i in range(20)
        ]
        background = [
            (f"b{i}", (now - timedelta(hours=2 + i * 5)).isoformat())
            for i in range(4)
        ]
        _seed_posts_and_keyword(store, keyword="rate cut",
                                posts=recent + background)
        detector = XSpikeDetector(store=store)
        observations = detector.run(now_iso=now.isoformat())
        assert len(observations) == 1
        assert isinstance(observations[0], SpikeObservation)
        assert observations[0].keyword == "rate cut"
        assert observations[0].count_window == 20
        assert observations[0].count_baseline == 24
        # No calendar event was synthesised.
        with store._connection(commit=False) as c:
            cal_count = c.execute(
                "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'x_derived'"
            ).fetchone()[0]
            link_count = c.execute(
                "SELECT COUNT(*) FROM x_post_event_links "
                "WHERE cal_provider = 'x_derived'"
            ).fetchone()[0]
        assert cal_count == 0
        assert link_count == 0


# ── Hashtag discovery ──────────────────────────────────────────────────


class TestXHashtagDiscoveryRunner:
    def _ingestor(
        self, store: SQLiteEngineStore, response_body: dict,
    ) -> XHashtagDiscoveryRunner:
        v2 = XV2Client(bearer_token="fake-token")
        v2.session = Mock()
        v2.session.get.return_value = _mock_response(200, response_body)
        return XHashtagDiscoveryRunner(store=store, client=v2)

    def test_extracts_hashtags_and_adds_novel_ones(
        self, store: SQLiteEngineStore,
    ) -> None:
        # 6 posts each with ``#newtopic`` so it crosses the
        # novelty_min_count=5 threshold; 2 with ``#fed`` (already
        # seeded — must not be re-added).
        posts = []
        for i in range(6):
            posts.append({
                "id": f"a{i}", "author_id": "1",
                "text": "x", "created_at": "2026-04-29T12:00:00Z",
                "lang": "en",
                "public_metrics": {
                    "retweet_count": 0, "like_count": 0,
                    "reply_count": 0, "quote_count": 0,
                },
                "entities": {"hashtags": [{"tag": "newtopic"}]},
            })
        for i in range(2):
            posts.append({
                "id": f"b{i}", "author_id": "1",
                "text": "x", "created_at": "2026-04-29T12:00:00Z",
                "lang": "en",
                "public_metrics": {
                    "retweet_count": 0, "like_count": 0,
                    "reply_count": 0, "quote_count": 0,
                },
                "entities": {"hashtags": [{"tag": "FED"}]},
            })
        runner = self._ingestor(
            store,
            {
                "data": posts,
                "includes": {"users": [{"id": "1", "username": "x"}]},
                "meta": {"newest_id": "a5", "result_count": 8},
            },
        )
        result = runner.run()
        assert result.posts_seen == 8
        assert "newtopic" in result.novel_keywords_added
        assert "fed" not in result.novel_keywords_added  # already seeded
        # Confirm the keyword is in the pool with category='derived'
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT category, priority FROM x_keyword_pool "
                "WHERE keyword = 'newtopic'"
            ).fetchone()
        assert row["category"] == "derived"
        # Confirm fed wasn't downgraded
        with store._connection(commit=False) as c:
            fed = c.execute(
                "SELECT category FROM x_keyword_pool WHERE keyword = 'fed'"
            ).fetchone()
        assert fed["category"] == "macro"

    def test_request_includes_entities_and_default_query(
        self, store: SQLiteEngineStore,
    ) -> None:
        runner = self._ingestor(
            store,
            {"data": [], "includes": {"users": []}, "meta": {}},
        )
        runner.run()
        params = runner.client.session.get.call_args.kwargs["params"]
        assert "entities" in params["tweet.fields"]
        # Default discovery query terms appear
        assert "market" in params["query"]
        assert "macro" in params["query"]
        assert "fed" in params["query"]
        assert "inflation" in params["query"]
        # And the standard operators are still appended
        assert "lang:en" in params["query"]
        assert "-is:retweet" in params["query"]

    def test_below_threshold_not_added(
        self, store: SQLiteEngineStore,
    ) -> None:
        """A hashtag seen only 3 times must not pass
        novelty_min_count=5 and must NOT land in the pool."""
        posts = []
        for i in range(3):
            posts.append({
                "id": f"r{i}", "author_id": "1",
                "text": "x", "created_at": "2026-04-29T12:00:00Z",
                "lang": "en",
                "public_metrics": {
                    "retweet_count": 0, "like_count": 0,
                    "reply_count": 0, "quote_count": 0,
                },
                "entities": {"hashtags": [{"tag": "rare"}]},
            })
        runner = self._ingestor(
            store,
            {
                "data": posts,
                "includes": {"users": [{"id": "1", "username": "x"}]},
                "meta": {"newest_id": "r2"},
            },
        )
        result = runner.run()
        assert "rare" not in result.novel_keywords_added
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT keyword FROM x_keyword_pool WHERE keyword = 'rare'"
            ).fetchone()
        assert row is None
