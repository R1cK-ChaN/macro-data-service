"""Drive the SARB repo-rate sweep through the calendar projection.

``fetch_sarb_calendar`` GETs the MRDREPOR JSON timeseries from the
public ``custom.resbank.co.za/SarbWebApi`` indicator service, parses
every rate-change row, and writes one calendar event per decision
through the shared projector.

One request per fetch — the endpoint returns the full repo-rate
change history (~25 rows in the captured fixture) in a single JSON
response. The projector's idempotent upsert collapses repeated sweeps
to no-ops on rows already at the latest content_hash.
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
    SARB_RATE_HISTORY_URL,
    SARBCalendarEventRecord,
    SARBCalendarRawRecord,
    SARBRateHistoryParseError,
    decision_to_records,
    parse_repo_rate_history,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_SARB_HEADERS: dict[str, str] = {
    # SARB's public API responds to the default Python-requests UA in
    # captured testing, but a browser-shaped UA matches the workaround
    # used by the sibling central-bank connectors and is known-good
    # against the same domain (the MPC-statements page is more
    # selective). Pinning a UA also makes traffic identifiable in
    # SARB's access logs as the macro-data-service.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.sarb_api)"
    ),
    "Accept": "application/json,text/javascript;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-ZA,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_sarb_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> list[dict]:
    response = requests.get(
        SARB_RATE_HISTORY_URL,
        headers=_SARB_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


def fetch_sarb_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[], list[dict]] | None = None,
) -> FetchRunSummary:
    """Sweep the SARB MRDREPOR JSON and project each rate-change row."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["SARB_RATE"],
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
        decisions = parse_repo_rate_history(payload)
    except (SARBRateHistoryParseError, requests.exceptions.RequestException) as exc:
        logger.warning("SARB rate-history fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[SARBCalendarRawRecord] = []
    event_records: list[SARBCalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_sarb_calendar"]
