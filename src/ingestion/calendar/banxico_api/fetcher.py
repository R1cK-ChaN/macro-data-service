"""Drive the Banxico Tasa Objetivo sweep through the calendar projection.

``fetch_banxico_calendar`` GETs the ``Anuncios de las decisiones de
política monetaria`` HTML page, parses every Tasa Objetivo decision
(change OR hold), and writes one calendar event per decision through
the shared projector.

One request per fetch — the page returns the full Tasa Objetivo
decision history (every meeting since 21 Jan 2008) in a single HTML
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
    BANXICO_DECISIONS_URL,
    BanxicoCalendarEventRecord,
    BanxicoCalendarRawRecord,
    BanxicoDecisionsParseError,
    decision_to_records,
    parse_decisions_history,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_BANXICO_HEADERS: dict[str, str] = {
    # Banxico's public site has rejected the default Python-requests UA
    # on some request paths. A browser-shaped UA matches the workaround
    # used by the IBGE / BCB / TÜİK / TCMB connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.banxico_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_banxico_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    decisions_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        BANXICO_DECISIONS_URL,
        headers=_BANXICO_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    # Banxico returns the page as ISO-8859-1 — set the encoding
    # explicitly so ``response.text`` decodes correctly. ``apparent_
    # encoding`` would also work but adds a dependency on chardet.
    response.encoding = "iso-8859-1"
    return response.text


def fetch_banxico_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the Banxico decisions HTML and project each Tasa Objetivo decision."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BANXICO_RATE"],
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
        decisions = parse_decisions_history(payload)
    except (BanxicoDecisionsParseError, requests.exceptions.RequestException) as exc:
        logger.warning("Banxico decisions fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BanxicoCalendarRawRecord] = []
    event_records: list[BanxicoCalendarEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_banxico_calendar"]
