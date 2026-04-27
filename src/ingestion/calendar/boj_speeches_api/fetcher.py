"""Drive the BoJ speeches archive sweep through the calendar projection.

``fetch_boj_speeches_calendar`` GETs every per-year archive page in
the configured ``years`` window (defaults to current + previous
year), parses each row, and writes one calendar event per
rate-setter speech through the shared projector.

One request per year — the page returns the year's full speech list
in a single HTML response. Years that haven't started yet typically
404; the connector logs and continues so a mid-year sweep doesn't
fail outright on a forward-year miss.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

import requests

from .parser import (
    BOJ_SPEECHES_URL_TEMPLATE,
    BojSpeechesArchiveParseError,
    BojSpeechesEventRecord,
    BojSpeechesRawRecord,
    parse_speeches_archive,
    speech_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


# boj.or.jp serves the per-year archive on a plain UA, but match the
# existing ``boj_api`` MPM scraper's browser-shaped UA for consistency
# across BoJ surfaces.
_BOJ_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.boj_speeches)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_boj_speeches_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    years_planned: list[int] = field(default_factory=list)
    dry_run: bool = True
    speeches_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    per_year_errors: dict[int, str] = field(default_factory=dict)
    wall_seconds: float = 0.0


def _default_years(now: datetime | None = None) -> tuple[int, int]:
    reference = now or datetime.now(timezone.utc)
    return (reference.year, reference.year - 1)


def _live_fetcher(year: int) -> str:
    response = requests.get(
        BOJ_SPEECHES_URL_TEMPLATE.format(year=year),
        headers=_BOJ_BROWSER_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_boj_speeches_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[int], str] | None = None,
    years: Iterable[int] | None = None,
) -> FetchRunSummary:
    """Sweep the BoJ speeches archive and project each rate-setter row."""
    started = time.monotonic()
    plan_years = list(years) if years is not None else list(_default_years())
    summary = FetchRunSummary(
        indicators_planned=["BOJ_SPEECHES"],
        years_planned=plan_years,
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = html_fetcher or _live_fetcher

    raw_records: list[BojSpeechesRawRecord] = []
    event_records: list[BojSpeechesEventRecord] = []
    for year in plan_years:
        try:
            html = fetcher(year)
        except Exception as exc:
            logger.warning("BoJ speeches %s fetch failed: %s", year, exc)
            summary.per_year_errors[year] = str(exc)
            continue
        try:
            speeches = parse_speeches_archive(html)
        except BojSpeechesArchiveParseError as exc:
            logger.warning("BoJ speeches %s parse failed: %s", year, exc)
            summary.per_year_errors[year] = str(exc)
            continue
        for speech in speeches:
            raw_rec, event_rec = speech_to_records(
                speech, snapshot_epoch_ms=snapshot,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)

    if not raw_records and summary.per_year_errors:
        first_year = next(iter(summary.per_year_errors))
        summary.fetch_error = (
            f"all years failed; year {first_year}: "
            f"{summary.per_year_errors[first_year]}"
        )

    summary.speeches_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_boj_speeches_calendar"]
