"""Canonicalization for ``market_price_bars_raw`` content hashing (issue #69 slice 2).

Both EODHD ``/api/eod/{ticker}`` and Tiingo
``/tiingo/daily/{ticker}/prices`` return JSON arrays of bar dicts. The
canonical form sorts bars by ``date`` and serializes with sorted keys —
that way map-insertion order, server-side reordering, or identical-bars
+ new-fetch-time can't masquerade as a revision and INSERT OR IGNORE
correctly dedupes the daily refresh.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonicalize_bars_payload(payload: list[dict[str, Any]]) -> str:
    """Sort by ``date`` and emit sorted-key JSON for hashing.

    Both providers ship volatile envelope fields per bar that are
    request-time noise rather than facts about the bar — sorting by
    date plus ``sort_keys=True`` removes any source of nondeterminism.
    A revised close (corrected adjusted_close, late corp-action
    backfill, etc.) flips the hash and lands as a new audit row.
    """
    if not isinstance(payload, list):
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)
    cleaned = [row for row in payload if isinstance(row, dict)]
    cleaned.sort(key=lambda r: str(r.get("date") or ""))
    return json.dumps(cleaned, sort_keys=True, ensure_ascii=False)


def bars_content_hash(payload: list[dict[str, Any]]) -> str:
    """sha256 of the canonical JSON string."""
    canonical = canonicalize_bars_payload(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
