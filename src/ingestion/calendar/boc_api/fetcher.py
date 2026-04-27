"""Drive the BoC Valet observations sweep through the calendar projection.

``fetch_boc_calendar`` GETs the Valet observations endpoint for
``V39079`` (target overnight rate), parses every rate-change
decision in the daily series, and writes one calendar event per
change through the shared projector.

One request per fetch — the Valet endpoint returns the full
observations list in a single JSON payload. The default lookback
covers ~3 years (``start_date`` set on the request) — long enough
to span the BoC's published change history without ballooning
payload size.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import requests

from .parser import (
    BOC_VALET_URL,
    BoCCalendarEventRecord,
    BoCCalendarRawRecord,
    BoCValetParseError,
    decision_to_records,
    parse_overnight_rate_observations,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_BOC_HEADERS: dict[str, str] = {
    "User-Agent": "macro-data-service/0.1 (calendar.boc_api)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}

# Three-year lookback bounds the Valet payload. The change-detection
# loop only needs the prior business day to fire on the first change
# in the window, so a three-year horizon comfortably spans the full
# BoC change cadence (~6-8 changes per year over recent cycles).
_VALET_LOOKBACK_DAYS = 365 * 3


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_boc_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    start = (date.today() - timedelta(days=_VALET_LOOKBACK_DAYS)).isoformat()
    response = requests.get(
        BOC_VALET_URL,
        headers=_BOC_HEADERS,
        params={"start_date": start},
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_boc_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the BoC Valet API and project rate-change rows into the calendar."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BOC_RATE"],
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
        text = fetcher()
        decisions = parse_overnight_rate_observations(text)
    except (BoCValetParseError, requests.exceptions.RequestException) as exc:
        logger.warning("BoC Valet fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BoCCalendarRawRecord] = []
    event_records: list[BoCCalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_boc_calendar"]
