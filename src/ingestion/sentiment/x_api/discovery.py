"""Hot-topic discovery + social-breakout event injection.

Issue #76 P3. Wraps three flows:

1. **Volume / engagement spike detection.** SQL on
   ``_SentimentQueriesMixin`` — windowed counts per keyword, fired
   when the recent-window rate exceeds a multiple of the rolling
   baseline.

2. **Calendar event injection.** Each spike materialises a synthetic
   ``cal_econ_event`` row with ``source='x_derived'`` and
   ``event_type='social_breakout'``, and backfills
   ``x_post_event_links`` for every post in the trigger window so the
   calendar consumer can pull the supporting evidence at read time.

3. **Broad-discovery hashtag mining.** ``GET /2/tweets/search/recent``
   on the issue body's stock query
   (``(market OR macro OR fed OR inflation) lang:en -is:retweet has:links``)
   with ``entities`` requested. Co-occurring hashtags are counted; any
   that pass the frequency threshold and aren't already in the pool
   land as ``category='derived'`` rows for the next P2 sweep to pick
   up.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass

from contracts import utc_now
from ingestion.sentiment.x_api._types import (
    XAPIError,
    XAuthError,
    XPost,
    XRateLimitError,
)
from ingestion.sentiment.x_api.scrapers import XV2Client
from storage import SQLiteEngineStore

logger = logging.getLogger(__name__)

DEFAULT_DISCOVERY_QUERY = (
    "(market OR macro OR fed OR inflation) has:links"
)


@dataclass(frozen=True)
class SocialBreakoutInjection:
    keyword: str
    cal_provider_event_id: str
    triggering_post_count: int
    count_window: int
    count_baseline: int


@dataclass(frozen=True)
class HashtagDiscoveryResult:
    posts_seen: int
    novel_keywords_added: tuple[str, ...]
    error: str = ""


def _hashtag_co_occurrences(posts: list[XPost]) -> Counter[str]:
    """Tally hashtag frequency across a batch of posts.

    Hashtags are normalized lower-case at parse time
    (``scrapers._parse_post``). The Counter is what the caller
    truncates against the novelty threshold to decide which tags to
    upsert as derived keywords."""
    counter: Counter[str] = Counter()
    for post in posts:
        for tag in post.hashtags:
            if tag:
                counter[tag] += 1
    return counter


class XSpikeDetector:
    """Volume / engagement spike → calendar event injection."""

    def __init__(self, store: SQLiteEngineStore) -> None:
        self.store = store

    def run(
        self,
        *,
        now_iso: str | None = None,
        window_hours: int = 1,
        baseline_hours: int = 24,
        threshold: float = 3.0,
    ) -> list[SocialBreakoutInjection]:
        """Scan once for volume spikes and inject one calendar event
        per triggering keyword.

        Returns one ``SocialBreakoutInjection`` per spike injected so
        the caller can log run statistics. Idempotent — re-running for
        the same spike-window collapses onto the same
        ``provider_event_id`` via the cal_econ_event PK."""
        now = now_iso or utc_now().isoformat()
        spikes = self.store.detect_x_volume_spikes(
            now_iso=now,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            threshold=threshold,
        )
        injections: list[SocialBreakoutInjection] = []
        for spike in spikes:
            keyword = str(spike["keyword"])
            window_start = self._window_start_iso(now, window_hours)
            triggering_post_ids = (
                self.store.list_x_post_ids_for_keyword_window(
                    keyword=keyword,
                    window_start_iso=window_start,
                    window_end_iso=now,
                )
            )
            cal_provider_event_id = self.store.inject_social_breakout_event(
                keyword=keyword,
                event_time_utc=now,
                title=f"X social_breakout: {keyword}",
                triggering_post_ids=triggering_post_ids,
                observed_at_epoch_ms=int(utc_now().timestamp() * 1000),
            )
            injections.append(
                SocialBreakoutInjection(
                    keyword=keyword,
                    cal_provider_event_id=cal_provider_event_id,
                    triggering_post_count=len(triggering_post_ids),
                    count_window=int(spike["count_window"]),
                    count_baseline=int(spike["count_baseline"]),
                )
            )
        return injections

    @staticmethod
    def _window_start_iso(now_iso: str, hours: int) -> str:
        from datetime import datetime, timedelta, timezone
        # The store-side comparison uses datetime() so we don't need
        # microsecond precision here — minute-grain is enough.
        # Parse the ISO stamp; tolerate trailing ``Z``.
        normalized = now_iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - timedelta(hours=hours)).isoformat()


class XHashtagDiscoveryRunner:
    """Broad-query hashtag mining → derived keyword pool."""

    def __init__(
        self,
        store: SQLiteEngineStore,
        client: XV2Client | None = None,
    ) -> None:
        self.store = store
        self.client = client or XV2Client()

    def run(
        self,
        *,
        query: str = DEFAULT_DISCOVERY_QUERY,
        novelty_min_count: int = 5,
        max_new_keywords: int = 20,
        derived_priority: int = 40,
    ) -> HashtagDiscoveryResult:
        """One discovery sweep.

        Runs the broad query with ``include_entities=True``, tallies
        co-occurring hashtags, upserts the top
        ``max_new_keywords`` novel ones (frequency ≥
        ``novelty_min_count``) into ``x_keyword_pool`` with
        ``category='derived'`` and ``priority=derived_priority``.
        ``upsert_x_keyword`` returns False for keywords that already
        exist, so the result list only contains genuinely new
        additions."""
        try:
            posts, _, _ = self.client.search_recent_tweets(
                query, include_entities=True,
            )
        except (XAuthError, XRateLimitError, XAPIError) as exc:
            return HashtagDiscoveryResult(
                posts_seen=0, novel_keywords_added=(), error=str(exc),
            )
        counter = _hashtag_co_occurrences(posts)
        added: list[str] = []
        # Sorted by count desc then keyword asc for deterministic
        # selection when frequencies tie.
        for keyword, count in sorted(
            counter.items(), key=lambda kv: (-kv[1], kv[0]),
        ):
            if count < novelty_min_count:
                break
            if len(added) >= max_new_keywords:
                break
            inserted = self.store.upsert_x_keyword(
                keyword=keyword,
                category="derived",
                priority=derived_priority,
            )
            if inserted:
                added.append(keyword)
        return HashtagDiscoveryResult(
            posts_seen=len(posts),
            novel_keywords_added=tuple(added),
        )
