"""Drive the Bank Indonesia BI-Rate sweep through the calendar projection.

``fetch_bi_calendar`` GETs the BI-Rate history HTML page, parses the
rate table on page 1 (the most recent ~10 BI Board of Governors
decisions), and writes one calendar event per decision through the
shared projector. Page-1 coverage is enough to anchor parity from day
one — the daily sweep catches every new meeting, and the projector's
idempotent upsert keeps recent decisions fresh across reschedules.

Backfill of the older pagination pages (which would require posting
the SharePoint ``__VIEWSTATE`` + ``__EVENTVALIDATION`` tokens through
the ASP.NET ``__doPostBack`` pipeline) is deferred to a P2 follow-up.
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
    BI_RATE_HISTORY_URL,
    BICalendarEventRecord,
    BICalendarRawRecord,
    BIRateHistoryParseError,
    decision_to_records,
    parse_rate_history,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_BI_HEADERS: dict[str, str] = {
    # Bank Indonesia's public site responds to the default Python-
    # requests UA in captured testing, but a browser-shaped UA matches
    # the workaround used by sibling central-bank connectors and
    # makes our traffic identifiable in BI's access logs as the
    # macro-data-service.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.bi_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_bi_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BI_RATE_HISTORY_URL,
        headers=_BI_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_bi_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the BI-Rate history HTML and project each decision."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BI_RATE"],
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
        payload = fetcher()
        decisions = parse_rate_history(payload)
    except (BIRateHistoryParseError, requests.exceptions.RequestException) as exc:
        logger.warning("Bank Indonesia rate-history fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BICalendarRawRecord] = []
    event_records: list[BICalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_bi_calendar"]
