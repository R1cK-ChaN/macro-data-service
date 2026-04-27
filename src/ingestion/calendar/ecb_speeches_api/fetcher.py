"""Drive the ECB speeches CSV sweep through the calendar projection.

``fetch_ecb_speeches_calendar`` GETs the official CSV in one
request, parses every data row, and writes one calendar event per
speech through the shared projector. The CSV refreshes monthly per
the ECB downloads page documentation, so frequent re-fetches are
cheap on the upstream side and idempotent on ours (slug-anchored
ids collapse identical re-fetches at the projector).
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
    ECB_SPEECHES_CSV_URL,
    EcbSpeechesCsvParseError,
    EcbSpeechesEventRecord,
    EcbSpeechesRawRecord,
    parse_speeches_csv,
    speech_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_ECB_BROWSER_HEADERS: dict[str, str] = {
    # ECB serves the CSV behind myracloud which sometimes 403s plain
    # python-requests UAs. Browser-shaped UA matches the existing
    # ``ecb_api`` workaround.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.ecb_speeches)"
    ),
    "Accept": "text/csv,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_ecb_speeches_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    speeches_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _live_fetcher() -> str:
    response = requests.get(
        ECB_SPEECHES_CSV_URL,
        headers=_ECB_BROWSER_HEADERS,
        timeout=60.0,
    )
    response.raise_for_status()
    response.encoding = "utf-8"
    return response.text


def fetch_ecb_speeches_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    csv_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the ECB speeches CSV and project each row."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["ECB_SPEECHES"],
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = csv_fetcher or _live_fetcher
    try:
        csv_text = fetcher()
    except Exception as exc:
        logger.warning("ECB speeches CSV fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    try:
        speeches = parse_speeches_csv(csv_text)
    except EcbSpeechesCsvParseError as exc:
        logger.warning("ECB speeches CSV parse failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[EcbSpeechesRawRecord] = []
    event_records: list[EcbSpeechesEventRecord] = []
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


__all__ = ["FetchRunSummary", "fetch_ecb_speeches_calendar"]
