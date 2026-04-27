"""Drive the BCB Copom history sweep through the calendar projection.

``fetch_bcb_calendar`` GETs the BCB historical-rates JSON service,
parses every Copom decision (change OR hold), and writes one calendar
event per decision through the shared projector.

One request per fetch — the service returns the full Copom decision
history (every meeting since 26 June 1996) in a single JSON response.
The projector's idempotent upsert collapses repeated sweeps to no-ops
on rows already at the latest content_hash.
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
    BCB_COPOM_HISTORY_URL,
    BCBCalendarEventRecord,
    BCBCalendarRawRecord,
    BCBCopomParseError,
    decision_to_records,
    parse_copom_history,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_BCB_HEADERS: dict[str, str] = {
    # BCB sites have rejected the default Python-requests UA on some
    # request paths. A browser-shaped UA matches the workaround used
    # by the BLS / RBI / KOSTAT / IBGE connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.bcb_api)"
    ),
    "Accept": "application/json,text/javascript;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_bcb_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BCB_COPOM_HISTORY_URL,
        headers=_BCB_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_bcb_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the Copom history JSON and project each decision."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BCB_RATE"],
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = json_fetcher or _live_fetcher
    try:
        payload = fetcher()
        decisions = parse_copom_history(payload)
    except (BCBCopomParseError, requests.exceptions.RequestException) as exc:
        logger.warning("BCB Copom history fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BCBCalendarRawRecord] = []
    event_records: list[BCBCalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_bcb_calendar"]
