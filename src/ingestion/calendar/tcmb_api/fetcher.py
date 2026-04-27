"""Drive the TCMB 1-week repo rate-history sweep through the calendar projection.

``fetch_tcmb_calendar`` GETs the static rate-history HTML, parses
every PPK rate-change decision since 20 May 2010, and writes one
calendar event per decision through the shared projector.

One request per fetch — the page returns the full rate-change history
in a single HTML response. The projector's idempotent upsert
collapses repeated sweeps to no-ops on rows already at the latest
content_hash.
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
    TCMB_RATE_HISTORY_URL,
    TCMBCalendarEventRecord,
    TCMBCalendarRawRecord,
    TCMBRateHistoryParseError,
    decision_to_records,
    parse_rate_history,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_TCMB_HEADERS: dict[str, str] = {
    # TCMB sites have rejected the default Python-requests UA on some
    # request paths. A browser-shaped UA matches the workaround used
    # by the BLS / RBI / KOSTAT / IBGE / BCB connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.tcmb_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_tcmb_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        TCMB_RATE_HISTORY_URL,
        headers=_TCMB_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_tcmb_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the TCMB rate-history HTML and project each PPK decision."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["TCMB_RATE"],
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
    except (TCMBRateHistoryParseError, requests.exceptions.RequestException) as exc:
        logger.warning("TCMB rate-history fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[TCMBCalendarRawRecord] = []
    event_records: list[TCMBCalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_tcmb_calendar"]
