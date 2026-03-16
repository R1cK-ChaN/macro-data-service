from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers.reddit import RedditTrendPost
from ingestion.scrapers.weibo import WeiboTrendItem
from ingestion.sources import (
    RawTrendEntry,
    RedditTrendIngestionClient,
    RedditTrendSourceConfig,
    WeiboTrendIngestionClient,
)
from macro_data.service import LocalMacroDataService
from storage import SQLiteEngineStore, TrendTopicRecord


class _FakeRedditClient:
    def __init__(self, posts_by_subreddit: dict[str, list[RedditTrendPost]]) -> None:
        self._posts_by_subreddit = posts_by_subreddit

    def fetch_hot_posts(self, subreddit: str, *, limit: int = 25) -> list[RedditTrendPost]:
        del limit
        return list(self._posts_by_subreddit.get(subreddit, []))

    def close(self) -> None:
        return None


class _FakeWeiboClient:
    def __init__(self, items: list[WeiboTrendItem]) -> None:
        self._items = items

    def fetch_hot_band(self) -> list[WeiboTrendItem]:
        return list(self._items)

    def close(self) -> None:
        return None


class TrendDataTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.store = SQLiteEngineStore(db_path=Path(self.temp_dir.name) / "engine.db")
        self.service = LocalMacroDataService(store=self.store)

    def test_list_active_trends_filters_expired_and_sorts(self) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        self.store.upsert_trend_topic(
            TrendTopicRecord(
                trend_id="reddit:technology",
                provider="reddit",
                provider_topic_id="abc123",
                title_raw="AI chips surge",
                topic="AI chips surge",
                summary="AI chips surge is drawing heavy discussion in technology conversations.",
                keywords=["chips", "surge"],
                category="technology",
                popularity_score=91.0,
                observed_at=now_ts - 600,
                expires_at=now_ts + 3600,
                normalized_topic_hash="hash-technology",
            )
        )
        self.store.upsert_trend_topic(
            TrendTopicRecord(
                trend_id="reddit:expired",
                provider="reddit",
                provider_topic_id="expired",
                title_raw="Old topic",
                topic="Old topic",
                summary="Old topic is drawing heavy discussion in news conversations.",
                keywords=["topic"],
                category="news",
                popularity_score=99.0,
                observed_at=now_ts - 86_400,
                expires_at=now_ts - 60,
                normalized_topic_hash="hash-expired",
            )
        )
        self.store.upsert_trend_topic(
            TrendTopicRecord(
                trend_id="reddit:business",
                provider="reddit",
                provider_topic_id="biz123",
                title_raw="Treasury auction drives yields lower",
                topic="Treasury auction drives yields lower",
                summary="Treasury auction drives yields lower is drawing heavy discussion in business conversations.",
                keywords=["treasury", "auction", "yields"],
                category="business",
                popularity_score=82.0,
                observed_at=now_ts - 300,
                expires_at=now_ts + 3600,
                normalized_topic_hash="hash-business",
            )
        )

        trends = self.store.list_active_trends(limit=10, hours=48)
        business_only = self.store.list_active_trends(limit=10, hours=48, category="business")

        self.assertEqual([item.trend_id for item in trends], ["reddit:technology", "reddit:business"])
        self.assertEqual([item.trend_id for item in business_only], ["reddit:business"])

    def test_get_trends_hides_provider_fields(self) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        self.store.upsert_trend_topic(
            TrendTopicRecord(
                trend_id="reddit:ai_jobs",
                provider="reddit",
                provider_topic_id="job999",
                title_raw="AI replacing programming jobs",
                topic="AI replacing programming jobs",
                summary="AI replacing programming jobs is drawing heavy discussion in technology conversations.",
                keywords=["replacing", "programming", "jobs"],
                category="technology",
                popularity_score=95.0,
                observed_at=now_ts - 1200,
                expires_at=now_ts + 3600,
                raw_json={"subreddit": "technology", "provider_rank": 1},
                normalized_topic_hash="hash-jobs",
            )
        )

        payload = self.service.invoke("get_trends", {"limit": 5})

        self.assertEqual(payload["total"], 1)
        topic = payload["topics"][0]
        self.assertEqual(topic["topic"], "AI replacing programming jobs")
        self.assertEqual(topic["category"], "technology")
        self.assertIn("programming", topic["keywords"])
        for hidden_key in ("provider", "provider_topic_id", "raw_json", "title_raw", "normalized_topic_hash"):
            self.assertNotIn(hidden_key, topic)

    def test_reddit_trend_fetch_entries_skips_stickied_and_nsfw_posts(self) -> None:
        client = RedditTrendIngestionClient(
            client=_FakeRedditClient(
                {
                    "technology": [
                        RedditTrendPost(
                            subreddit="technology",
                            post_id="good-1",
                            title="AI tooling keeps shipping faster",
                            permalink="https://reddit.com/r/technology/good-1",
                            score=800,
                            num_comments=120,
                            created_utc=1_773_200_000,
                        ),
                        RedditTrendPost(
                            subreddit="technology",
                            post_id="stickied-1",
                            title="Daily thread",
                            permalink="https://reddit.com/r/technology/stickied-1",
                            score=100,
                            num_comments=10,
                            created_utc=1_773_200_100,
                            is_stickied=True,
                        ),
                        RedditTrendPost(
                            subreddit="technology",
                            post_id="nsfw-1",
                            title="Private footage leaks",
                            permalink="https://reddit.com/r/technology/nsfw-1",
                            score=200,
                            num_comments=30,
                            created_utc=1_773_200_200,
                            is_nsfw=True,
                        ),
                    ],
                }
            ),
            source_configs=(RedditTrendSourceConfig(subreddit="technology", category="technology"),),
        )

        entries = client.fetch_entries()

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].provider_topic_id, "good-1")

    def test_reddit_trend_normalization_validation_and_deduplication(self) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        client = RedditTrendIngestionClient(client=_FakeRedditClient({}))
        raw_entries = [
            RawTrendEntry(
                subreddit="technology",
                category="technology",
                region="global",
                provider_topic_id="topic-a",
                title_raw="[Discussion] AI replacing programming jobs???",
                url="https://reddit.com/r/technology/topic-a",
                score=450,
                comment_count=120,
                provider_rank=3,
                observed_at=now_ts - 1800,
                is_stickied=False,
                is_nsfw=False,
            ),
            RawTrendEntry(
                subreddit="artificial",
                category="technology",
                region="global",
                provider_topic_id="topic-b",
                title_raw="AI replacing programming jobs?",
                url="https://reddit.com/r/artificial/topic-b",
                score=900,
                comment_count=240,
                provider_rank=1,
                observed_at=now_ts - 600,
                is_stickied=False,
                is_nsfw=False,
            ),
            RawTrendEntry(
                subreddit="technology",
                category="technology",
                region="global",
                provider_topic_id="topic-c",
                title_raw="[Meta] Too short",
                url="https://reddit.com/r/technology/topic-c",
                score=300,
                comment_count=40,
                provider_rank=2,
                observed_at=now_ts - 900,
                is_stickied=False,
                is_nsfw=False,
            ),
        ]

        normalized = client.normalize_entries(raw_entries)
        valid = client.validate_entries(normalized)
        deduplicated = client.deduplicate_entries(valid)

        self.assertEqual(len(valid), 2)
        self.assertEqual(len(deduplicated), 1)
        topic = deduplicated[0]
        self.assertEqual(topic.topic, "AI replacing programming jobs?")
        self.assertIn("programming", topic.keywords)
        self.assertNotIn("reddit", topic.summary.lower())
        self.assertEqual(topic.provider_topic_id, "topic-b")
        self.assertGreater(topic.popularity_score, 0.0)

    def test_weibo_trend_normalization_preserves_cjk_and_maps_category(self) -> None:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        client = WeiboTrendIngestionClient(
            client=_FakeWeiboClient(
                [
                    WeiboTrendItem(
                        word="中方回应特朗普计划访华",
                        note="中方回应特朗普计划访华",
                        word_scheme="#中方回应特朗普计划访华#",
                        category="国内时政",
                        realpos=2,
                        num=711440,
                        raw_hot=209692,
                        onboard_time=now_ts - 900,
                        label_name="",
                        topic_flag=1,
                    ),
                    WeiboTrendItem(
                        word="中国109项硬核项目来了",
                        note="中国109项硬核项目来了",
                        word_scheme="#中国109项硬核项目来了#",
                        category="互联网",
                        realpos=3,
                        num=582134,
                        raw_hot=56164,
                        onboard_time=now_ts - 300,
                        label_name="新",
                        topic_flag=1,
                    ),
                ]
            )
        )

        normalized = client.normalize_entries(client.fetch_entries())
        valid = client.validate_entries(normalized)
        deduplicated = client.deduplicate_entries(valid)

        self.assertEqual(len(deduplicated), 2)
        first = deduplicated[0]
        self.assertTrue(first.normalized_topic_hash)
        self.assertEqual(first.region, "china")
        self.assertIn(first.category, {"news", "technology"})
        self.assertTrue(any(keyword for keyword in first.keywords))
        self.assertTrue(any("\u4e00" <= char <= "\u9fff" for char in first.topic))

    def test_weibo_fetch_entries_filters_non_topics(self) -> None:
        client = WeiboTrendIngestionClient(
            client=_FakeWeiboClient(
                [
                    WeiboTrendItem(
                        word="有效热搜",
                        note="有效热搜",
                        word_scheme="#有效热搜#",
                        category="民生新闻",
                        realpos=1,
                        num=1000,
                        raw_hot=100,
                        onboard_time=1_773_200_000,
                        topic_flag=1,
                    ),
                    WeiboTrendItem(
                        word="忽略项",
                        note="忽略项",
                        word_scheme="#忽略项#",
                        category="民生新闻",
                        realpos=2,
                        num=900,
                        raw_hot=80,
                        onboard_time=1_773_200_100,
                        topic_flag=0,
                    ),
                ]
            )
        )

        entries = client.fetch_entries()

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].word, "有效热搜")


if __name__ == "__main__":
    unittest.main()
