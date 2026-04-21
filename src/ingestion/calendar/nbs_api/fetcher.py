"""Drive the NBS yearly-calendar scrape through the calendar projection.

``fetch_nbs_calendar`` fetches a specific NBS yearly-calendar article
(or accepts a caller-supplied fixture via the ``html_fetcher`` seam
used by tests), parses it into :class:`NBSReleaseEntry` rows through
:func:`scraper.parse_nbs_calendar_html`, turns each entry into a
``(raw, event)`` tuple through :func:`parser.release_entry_to_records`,
and persists via :func:`projector.store_raw` +
:func:`projector.project_events`.

Nothing auto-runs: callers invoke ``fetch_nbs_calendar`` with a
specific article URL. A dry-run path returns the planned indicator
list without issuing any HTTP request.

One article per fetch — the NBS publishes one calendar document per
year. Multi-year backfill is the caller's loop.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY
from .parser import (
    NBSCalendarEventRecord,
    NBSCalendarRawRecord,
    release_entry_to_records,
)
from .projector import project_events, store_raw
from .scraper import (
    NBSCalendarParseError,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_nbs_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    calendar_url: str = ""
    year: int | None = None
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_nbs_calendar(
    connection: sqlite3.Connection,
    *,
    calendar_url: str,
    year: int | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[str], str] | None = None,
) -> FetchRunSummary:
    """Scrape a specific NBS yearly-calendar article and project rows.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    calendar_url:
        Absolute URL of the NBS yearly-calendar article
        (``.../ReleaseCalendar/YYYYMM/tYYYYMMDD_N.html``). Required
        — the scraper does not auto-discover the current year's
        article. Stored on every event's ``source_url``.
    year:
        Optional year override fed to the parser. When omitted, the
        parser reads the year from the article title.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; the returned summary shows the indicator plan only.
    snapshot_epoch_ms:
        Fetch-time anchor on every raw row. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, called with ``calendar_url`` in
        place of :func:`scraper.fetch_nbs_yearly_calendar_html`.
    """
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=list(INDICATOR_REGISTRY.keys()),
        calendar_url=calendar_url,
        year=year,
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or (
        lambda url: fetch_nbs_yearly_calendar_html(url)
    )
    html = fetcher(calendar_url)
    entries = parse_nbs_calendar_html(html, year_override=year)
    if not entries:
        # NBS is the highest-risk upstream (HTTP-only, HTML-fragile,
        # frequent timeouts). A successful-looking 200 with no parsed
        # entries means the document shape drifted or an interstitial
        # came back; surface instead of committing a no-op.
        raise NBSCalendarParseError(
            "NBS calendar fetch parsed zero scheduled releases — upstream "
            "DOM drift or interstitial response"
        )
    summary.entries_parsed = len(entries)
    summary.year = entries[0].year if summary.year is None else summary.year

    raw_records: list[NBSCalendarRawRecord] = []
    event_records: list[NBSCalendarEventRecord] = []
    for entry in entries:
        raw_rec, event_rec = release_entry_to_records(
            entry,
            snapshot_epoch_ms=snapshot,
            calendar_url=calendar_url,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
