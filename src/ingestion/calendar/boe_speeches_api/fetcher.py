"""Drive the BoE speeches sitemap sweep through the calendar projection.

``fetch_boe_speeches_calendar`` GETs the sitemap in one request,
parses every current-format speech link (year/month/slug shape), and
writes one calendar event per row through the shared projector. The
sitemap is large (~1MB, ~1500 links across 1997-present) but the
single request stays cheap on the upstream side and idempotent on
ours (slug-anchored ids collapse identical re-fetches).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import requests

from .parser import (
    BOE_SPEECHES_SITEMAP_URL,
    BoeSpeechesEventRecord,
    BoeSpeechesRawRecord,
    BoeSpeechesSitemapParseError,
    parse_speeches_sitemap,
    speech_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


# bankofengland.co.uk sits behind Akamai (myracloud) which 403s
# the default python-requests UA. Use a Safari-shaped UA matching
# the existing ``boe_api`` Bank-Rate workaround.
_BOE_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.1 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_boe_speeches_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    speeches_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BOE_SPEECHES_SITEMAP_URL,
        headers=_BOE_BROWSER_HEADERS,
        timeout=60.0,
    )
    response.raise_for_status()
    return response.text


def fetch_boe_speeches_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the BoE speeches sitemap and project each row."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BOE_SPEECHES"],
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = html_fetcher or _live_fetcher
    try:
        html = fetcher()
    except Exception as exc:
        logger.warning("BoE speeches sitemap fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    try:
        speeches = parse_speeches_sitemap(html)
    except BoeSpeechesSitemapParseError as exc:
        logger.warning("BoE speeches sitemap parse failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BoeSpeechesRawRecord] = []
    event_records: list[BoeSpeechesEventRecord] = []
    for speech in speeches:
        raw_rec, event_rec = speech_to_records(
            speech, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.speeches_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_boe_speeches_calendar"]
