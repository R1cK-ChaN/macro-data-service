"""Drive the BOK Meeting Dates sweep through the calendar projection.

``fetch_bok_calendar`` GETs the BOK Meeting Dates page, parses every
inline year block's MPB meeting cells, and writes one calendar event
per scheduled meeting through the shared projector.

One request per fetch — the page returns every inline year (2011 →
current schedule) in a single HTML response. Future-year dates that
BOK posts only as a ``.hwp``/``.pdf`` attachment on a separate news
article (typical lag between fiscal-year close and the inline update)
are silently absent until BOK adds the new ``<h3>YYYY</h3>`` block.
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
    BOK_MEETING_DATES_URL,
    BOKCalendarEventRecord,
    BOKCalendarRawRecord,
    BOKMeetingScheduleParseError,
    meeting_to_records,
    parse_meeting_schedule,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_BOK_HEADERS: dict[str, str] = {
    # BOK rejects the default Python-requests UA on some request paths.
    # A browser-shaped UA matches the workaround used by the BLS / Fed /
    # NBS / RBI connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.bok_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_bok_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    meetings_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BOK_MEETING_DATES_URL,
        headers=_BOK_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_bok_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the BOK Meeting Dates page and project each MPB meeting."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BOK_RATE"],
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
        logger.warning("BOK Meeting Dates fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    try:
        meetings = parse_meeting_schedule(html)
    except BOKMeetingScheduleParseError as exc:
        logger.warning("BOK Meeting Dates parse failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BOKCalendarRawRecord] = []
    event_records: list[BOKCalendarEventRecord] = []
    for meeting in meetings:
        raw_rec, event_rec = meeting_to_records(
            meeting, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.meetings_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_bok_calendar"]
