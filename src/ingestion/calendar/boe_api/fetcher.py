"""Drive the BoE Bank Rate scrape through the calendar projection.

``fetch_boe_calendar`` GETs ``boeapps/database/Bank-Rate.asp`` (or
accepts a fixture seam for tests), parses every decision row, and
writes one calendar event per row through the shared projector.

One request per fetch — the page is a single HTML document
covering Bank Rate history back to 1975.
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
    BOE_BANK_RATE_URL,
    BoECalendarEventRecord,
    BoECalendarRawRecord,
    BoERatePageParseError,
    decision_to_records,
    parse_bank_rate_html,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


# bankofengland.co.uk passes the Bank-Rate.asp page on a plain UA,
# but the rest of the site (release calendar, monetary-policy
# minutes) sits behind Akamai. Use a browser-shaped UA bundle so
# the same session works across pages once future slices add the
# minutes scrape.
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
    """Outcome of one ``fetch_boe_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BOE_BANK_RATE_URL, headers=_BOE_BROWSER_HEADERS, timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_boe_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the BoE Bank Rate page and project rows into the calendar."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BOE_RATE"],
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
        decisions = parse_bank_rate_html(html)
    except (BoERatePageParseError, requests.exceptions.RequestException) as exc:
        logger.warning("BoE Bank Rate fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BoECalendarRawRecord] = []
    event_records: list[BoECalendarEventRecord] = []
    for decision in decisions:
        raw_rec, event_rec = decision_to_records(
            decision, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.decisions_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_boe_calendar"]
