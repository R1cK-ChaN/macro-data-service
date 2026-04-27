"""Drive the RBA cash-rate-target sweep through the calendar projection.

``fetch_rba_calendar`` GETs the RBA cash-rate page, parses every
MPB rate decision in the table, and writes one calendar event per
decision (change OR hold) through the shared projector.

One request per fetch — the page returns the full decision history
(every MPB announcement since 6 January 2008) in a single HTML
response. The projector's idempotent upsert collapses repeated
sweeps to no-ops on rows already at the latest content_hash.
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
    RBA_CASH_RATE_URL,
    RBACalendarEventRecord,
    RBACalendarRawRecord,
    RBACashRateParseError,
    decision_to_records,
    parse_cash_rate_table,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_RBA_HEADERS: dict[str, str] = {
    # RBA sits behind Akamai which 403s on default-Python UA strings.
    # A browser-shaped UA is the documented workaround used by the
    # other Akamai-fronted government scrapers in this repo (DOL).
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.rba_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_rba_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        RBA_CASH_RATE_URL,
        headers=_RBA_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_rba_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the RBA cash-rate page and project each decision into the calendar."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["RBA_RATE"],
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
        decisions = parse_cash_rate_table(html)
    except (RBACashRateParseError, requests.exceptions.RequestException) as exc:
        logger.warning("RBA cash-rate fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[RBACalendarRawRecord] = []
    event_records: list[RBACalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_rba_calendar"]
