"""Hot-topic discovery on the X (Twitter) sentiment lane.

Issue #76 P3 originally bundled spike detection with calendar-event
injection. Issue #113 P2 unwound the injection — synthesising
``cal_econ_event`` rows from social signal is downstream territory, not
the data layer's job. The detector still scans for volume spikes and
returns them so a downstream service can decide what to do; nothing
writes back to ``cal_econ_event`` from this module any more.

Two flows remain:

1. **Volume / engagement spike detection.** SQL on
   ``_SentimentQueriesMixin`` — windowed counts per keyword, fired
   when the recent-window rate exceeds a multiple of the rolling
   baseline.

2. **Broad-discovery hashtag mining.** ``GET /2/tweets/search/recent``
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
class SpikeObservation:
    """One detected spike. Returned by ``XSpikeDetector.run``; no
    side-effect on ``cal_econ_event`` after issue #113 P2."""

    keyword: str
    count_window: int
    count_baseline: int


@dataclass(frozen=True)
class HashtagDiscoveryResult:
    posts_seen: int
    novel_keywords_added: tuple[str, ...]
    error: str = ""


@dataclass(frozen=True)
class AvailabilityPatrolResult:
    checked: int
    marked_unavailable: tuple[str, ...]
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
    """Volume / engagement spike scanner.

    Issue #113 P2 unhooked the calendar-event injection. ``run`` now
    just returns the spikes the underlying SQL flagged so a downstream
    consumer can act on them.
    """

    def __init__(self, store: SQLiteEngineStore) -> None:
        self.store = store

    def run(
        self,
        *,
        now_iso: str | None = None,
        window_hours: int = 1,
        baseline_hours: int = 24,
        threshold: float = 3.0,
    ) -> list[SpikeObservation]:
        """Scan once for volume spikes and return one observation per
        triggering keyword."""
        spikes = self.store.detect_x_volume_spikes(
            now_iso=now_iso,
            window_hours=window_hours,
            baseline_hours=baseline_hours,
            threshold=threshold,
        )
        return [
            SpikeObservation(
                keyword=str(spike["keyword"]),
                count_window=int(spike["count_window"]),
                count_baseline=int(spike["count_baseline"]),
            )
            for spike in spikes
        ]


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


# ── Availability patrol (issue #76 P4) ────────────────────────────────


_LOOKUP_BATCH_SIZE = 100


class XAvailabilityPatrol:
    """Re-verify high-engagement posts via ``GET /2/tweets?ids=...``.

    Issue #76 P4. Every 6h the operator runs ``run()``; the orchestrator
    pulls all posts with ``is_available=1`` whose ``like_count +
    retweet_count > min_engagement`` (default 50) and were
    ``fetched_at`` within ``fetched_within_hours`` (default 72h),
    chunks them into 100-id batches, calls the X batch endpoint, and
    flips ``is_available`` to 0 for any id that comes back with a
    ``Not Found`` error. Posts that are still alive get their
    ``availability_checked_at`` stamped so the patrol's queue rotates
    them to the back."""

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
        min_engagement: int = 50,
        fetched_within_hours: int = 72,
        max_posts: int | None = None,
    ) -> AvailabilityPatrolResult:
        post_ids = self.store.list_x_posts_for_availability_check(
            min_engagement=min_engagement,
            fetched_within_hours=fetched_within_hours,
            limit=max_posts,
        )
        if not post_ids:
            return AvailabilityPatrolResult(
                checked=0, marked_unavailable=(),
            )

        unavailable: list[str] = []
        # ``checked`` counts only IDs the patrol actually classified
        # as found OR not-found (Codex P4 round 1). A non-404 per-id
        # error or a mid-sweep XAPIError leaves IDs unstamped for the
        # next sweep — they don't count as completed checks.
        classified = 0
        try:
            for start in range(0, len(post_ids), _LOOKUP_BATCH_SIZE):
                chunk = post_ids[start:start + _LOOKUP_BATCH_SIZE]
                found, not_found = self.client.lookup_tweets(chunk)
                stamp = utc_now().isoformat()
                for post_id in chunk:
                    if post_id in not_found:
                        self.store.mark_x_post_unavailable(
                            post_id=post_id, checked_at=stamp,
                        )
                        unavailable.append(post_id)
                        classified += 1
                    elif post_id in found:
                        self.store.stamp_x_post_availability_checked(
                            post_id=post_id, checked_at=stamp,
                        )
                        classified += 1
                    # ``post_id`` neither in ``found`` nor ``not_found``
                    # means a non-404 error came back for it — leave
                    # ``is_available`` alone and let the next sweep retry.
        except (XAuthError, XRateLimitError, XAPIError) as exc:
            return AvailabilityPatrolResult(
                checked=classified,
                marked_unavailable=tuple(unavailable),
                error=str(exc),
            )
        return AvailabilityPatrolResult(
            checked=classified,
            marked_unavailable=tuple(unavailable),
        )
