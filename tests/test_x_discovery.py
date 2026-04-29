"""Hot-topic discovery + social_breakout event injection (issue #76 P3).

Covers:

* Volume-spike SQL on ``_SentimentQueriesMixin`` — tier mapping,
  baseline floor, idempotent ``provider_event_id`` synthesis.
* Calendar event injection — ``cal_econ_event`` row is created with
  ``source='x_derived'`` / ``event_type='social_breakout'``, links are
  back-filled into ``x_post_event_links``, and re-running the spike
  detector for the same window doesn't duplicate either.
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
    SocialBreakoutInjection,
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
        it out to keep noise from triggering social_breakout events."""
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
        clock skew) must not contribute to spike counts — otherwise
        backfill data inflates count_window/count_baseline against
        evidence the link-query already filters."""
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


# ── Calendar event injection ──────────────────────────────────────────


class TestSocialBreakoutInjection:
    def test_inject_creates_cal_event_and_links(
        self, store: SQLiteEngineStore,
    ) -> None:
        now_iso = "2026-04-29T12:00:00+00:00"
        with store._connection(commit=True) as c:
            c.executemany(
                "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
                "VALUES (?, 'u1', '2026-04-29T11:55:00Z', '2026-04-29T11:55:00Z')",
                [("p1",), ("p2",)],
            )
        store.inject_social_breakout_event(
            keyword="rate cut",
            event_time_utc=now_iso,
            title="X social_breakout: rate cut",
            triggering_post_ids=["p1", "p2"],
            observed_at_epoch_ms=1000,
        )
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT provider, provider_event_id, source, event_type, title "
                "FROM cal_econ_event WHERE provider = 'x_derived'"
            ).fetchone()
        assert row["source"] == "x_derived"
        assert row["event_type"] == "social_breakout"
        # Time-of-day in the id (Codex P3 round 2) so same-day
        # double-spikes don't collapse onto one row.
        assert row["provider_event_id"] == "breakout-rate-cut-2026-04-29-12-00"
        assert row["title"] == "X social_breakout: rate cut"

        with store._connection(commit=False) as c:
            link_rows = c.execute(
                "SELECT post_id, link_type FROM x_post_event_links "
                "WHERE cal_provider = 'x_derived' "
                "AND cal_provider_event_id = 'breakout-rate-cut-2026-04-29-12-00'"
            ).fetchall()
        assert {r["post_id"] for r in link_rows} == {"p1", "p2"}
        assert all(r["link_type"] == "social_breakout" for r in link_rows)

    def test_inject_idempotent_for_same_window(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Re-running the detector for the same (keyword, date)
        must not duplicate the cal_econ_event row or the links."""
        now_iso = "2026-04-29T12:00:00+00:00"
        with store._connection(commit=True) as c:
            c.executemany(
                "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
                "VALUES (?, 'u1', '2026-04-29T11:55:00Z', '2026-04-29T11:55:00Z')",
                [("p1",), ("p2",)],
            )
        store.inject_social_breakout_event(
            keyword="rate cut", event_time_utc=now_iso,
            title="t1", triggering_post_ids=["p1"],
            observed_at_epoch_ms=1000,
        )
        store.inject_social_breakout_event(
            keyword="rate cut", event_time_utc=now_iso,
            title="t2", triggering_post_ids=["p1", "p2"],  # adds p2
            observed_at_epoch_ms=2000,
        )
        with store._connection(commit=False) as c:
            count = c.execute(
                "SELECT COUNT(*) FROM cal_econ_event WHERE provider='x_derived'"
            ).fetchone()[0]
        assert count == 1
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT title, observed_at_epoch_ms FROM cal_econ_event "
                "WHERE provider='x_derived'"
            ).fetchone()
        # Title + observed_at advance on conflict (latest spike wins)
        assert row["title"] == "t2"
        assert row["observed_at_epoch_ms"] == 2000
        with store._connection(commit=False) as c:
            link_count = c.execute(
                "SELECT COUNT(*) FROM x_post_event_links "
                "WHERE cal_provider = 'x_derived'"
            ).fetchone()[0]
        # Both posts linked, no dupes
        assert link_count == 2

    def test_morning_and_afternoon_spike_create_separate_events(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P3 round 2: a keyword that spikes twice on the
        same date must produce two cal_econ_event rows, not one
        merged row that hides the second spike's timestamp."""
        with store._connection(commit=True) as c:
            c.executemany(
                "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
                "VALUES (?, 'u1', '2026-04-29T11:55:00Z', '2026-04-29T11:55:00Z')",
                [("p1",), ("p2",)],
            )
        store.inject_social_breakout_event(
            keyword="rate cut",
            event_time_utc="2026-04-29T08:00:00+00:00",
            title="morning",
            triggering_post_ids=["p1"], observed_at_epoch_ms=1000,
        )
        store.inject_social_breakout_event(
            keyword="rate cut",
            event_time_utc="2026-04-29T15:00:00+00:00",
            title="afternoon",
            triggering_post_ids=["p2"], observed_at_epoch_ms=2000,
        )
        with store._connection(commit=False) as c:
            count = c.execute(
                "SELECT COUNT(*) FROM cal_econ_event WHERE provider = 'x_derived'"
            ).fetchone()[0]
        assert count == 2
        with store._connection(commit=False) as c:
            ids = sorted(r["provider_event_id"] for r in c.execute(
                "SELECT provider_event_id FROM cal_econ_event "
                "WHERE provider = 'x_derived'"
            ).fetchall())
        assert ids[0].endswith("08-00")
        assert ids[1].endswith("15-00")

    def test_event_surfaces_through_v_calendar_item(
        self, store: SQLiteEngineStore,
    ) -> None:
        """End-to-end: detector → injection → v_calendar_item read.
        Confirms the P0 view extension actually projects the new
        subtype to downstream calendar consumers."""
        with store._connection(commit=True) as c:
            c.execute(
                "INSERT INTO x_posts (post_id, author_id, created_at, fetched_at) "
                "VALUES ('p9', 'u1', '2026-04-29T11:55:00Z', '2026-04-29T11:55:00Z')"
            )
        store.inject_social_breakout_event(
            keyword="cpi shock",
            event_time_utc="2026-04-29T12:00:00+00:00",
            title="X social_breakout: cpi shock",
            triggering_post_ids=["p9"], observed_at_epoch_ms=1000,
        )
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT subtype FROM v_calendar_item "
                "WHERE provider = 'x_derived'"
            ).fetchone()
        assert row["subtype"] == "social_breakout"


class TestXSpikeDetectorRunner:
    def test_run_injects_one_event_per_spike(
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
        injections = detector.run(now_iso=now.isoformat())
        assert len(injections) == 1
        assert isinstance(injections[0], SocialBreakoutInjection)
        assert injections[0].keyword == "rate cut"
        assert injections[0].triggering_post_count == 20
        # cal_econ_event row exists
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT * FROM cal_econ_event WHERE provider = 'x_derived'"
            ).fetchone()
        assert row is not None

    def test_run_excludes_unavailable_posts_from_evidence(
        self, store: SQLiteEngineStore,
    ) -> None:
        """Codex P3 round 1: triggering_post_ids must match the
        post population the detector counts — soft-deleted posts
        excluded from spike count must also be excluded from the
        evidence link backfill."""
        now = utc_now()
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
        # Mark r0 unavailable AFTER seeding — spike still fires
        # (19 / hour vs 24 / 24h baseline) but r0 must not be linked.
        with store._connection(commit=True) as c:
            c.execute(
                "UPDATE x_posts SET is_available = 0 WHERE post_id = 'r0'"
            )
        detector = XSpikeDetector(store=store)
        injections = detector.run(now_iso=now.isoformat())
        assert len(injections) == 1
        with store._connection(commit=False) as c:
            linked = {
                r["post_id"] for r in c.execute(
                    "SELECT post_id FROM x_post_event_links "
                    "WHERE cal_provider = 'x_derived'"
                ).fetchall()
            }
        assert "r0" not in linked


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
