"""Storage records — X (Twitter) sentiment lane (issue #76).

Layout matches the per-domain models (``storage.models.news``,
``storage.models.market`` …). Re-exported from ``storage.models.__init__``
so ``from storage import XPostRecord`` works alongside the other domains.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class XPostRecord:
    """Row in ``x_posts`` — engagement counters update in place via the
    upsert helper, while textual columns stay immutable from first
    write. ``query_context`` is ``'timeline'`` for the user-tweets
    endpoint or the keyword string for ``search/recent``."""

    post_id: str
    author_id: str
    author_handle: str
    text: str
    created_at: str
    lang: str
    retweet_count: int
    like_count: int
    reply_count: int
    quote_count: int
    query_context: str
    fetched_at: str
