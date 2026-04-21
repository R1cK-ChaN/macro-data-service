"""Drive the FOMC calendar scrape through the calendar projection.

``fetch_fed_calendar`` fetches the FOMC calendar HTML (or accepts a
caller-supplied fixture via the ``html_fetcher`` seam used by tests),
parses it into :class:`FomcMeetingEntry` rows through
:func:`scraper.parse_fomc_calendar_html`, turns each entry into a
``(raw, event)`` tuple through :func:`parser.meeting_entry_to_records`,
and persists via :func:`projector.store_raw` +
:func:`projector.project_events`.

Nothing auto-runs: callers invoke ``fetch_fed_calendar``. A dry-run
path returns the planned indicator list without issuing any HTTP
request.

One request per fetch — the FOMC calendar page is a single HTML
document covering 2021-through-two-years-forward, so there's no
per-year fan-out.
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
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    meeting_entry_to_records,
)
from .projector import project_events, store_raw
from .releases import (
    fetch_releasedates_html,
    parse_releasedates_html,
    release_entry_to_records,
)
from .scraper import fetch_fomc_calendar_html, parse_fomc_calendar_html

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_fed_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    meetings_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_fed_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the FOMC calendar and project rows into the calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; the returned summary shows the indicator plan only.
    snapshot_epoch_ms:
        Fetch-time anchor on every raw row. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, called in place of
        :func:`scraper.fetch_fomc_calendar_html`. Tests pass in a
        fixture-reading lambda to avoid real HTTP.
    """
    started = time.monotonic()
    # FOMC scraper owns the FOMC_RATE indicator only. After P4a added
    # BEIGE_BOOK / FED_H41 / FED_H8 to the shared registry, reporting
    # the full keyset here would mislead callers into thinking this
    # op will also write the releasedates-surface indicators — it
    # doesn't; those ship via ``fetch_fed_releasedates``.
    summary = FetchRunSummary(
        indicators_planned=["FOMC_RATE"],
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or fetch_fomc_calendar_html
    html = fetcher()
    entries = parse_fomc_calendar_html(html)
    if not entries:
        # The FOMC calendar page carries ~48 meetings (6 years × 8
        # meetings) in a stable layout. Zero parsed rows means the
        # upstream DOM drifted or the response is a
        # Cloudflare-style 200 interstitial; either way, an empty
        # projection is the wrong outcome and must surface instead
        # of committing a no-op.
        from .scraper import FomcCalendarParseError
        raise FomcCalendarParseError(
            "FOMC calendar fetch returned zero meetings — upstream "
            "DOM drift or access-denied interstitial"
        )
    summary.meetings_parsed = len(entries)

    raw_records: list[FedCalendarRawRecord] = []
    event_records: list[FedCalendarEventRecord] = []
    for entry in entries:
        raw_rec, event_rec = meeting_entry_to_records(
            entry, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ReleasedatesRunSummary:
    """Outcome of a single :func:`fetch_fed_releasedates` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    entries_parsed: int = 0
    entries_by_indicator: dict[str, int] = field(default_factory=dict)
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    row_issues: list[str] = field(default_factory=list)
    wall_seconds: float = 0.0
    fetch_error: str | None = None


def fetch_fed_releasedates(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> ReleasedatesRunSummary:
    """Scrape ``federalreserve.gov/newsevents/releasedates.htm`` and
    project Beige Book / H.4.1 / H.8 rows into the calendar.

    Parameters mirror :func:`fetch_fed_calendar`. SEP rows are
    explicitly excluded — see :mod:`ingestion.calendar.fed_api.releases`
    for the rationale. Scheduled speeches / testimony are out of
    P4a scope.

    The Fed releasedates page is a single HTML document covering
    ~60 days forward of release dates; weekly H.4.1 / H.8 and ~8/year
    Beige Book entries dominate the whitelist output.
    """
    started = time.monotonic()
    planned = ["BEIGE_BOOK", "FED_H41", "FED_H8"]
    summary = ReleasedatesRunSummary(
        indicators_planned=planned,
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or fetch_releasedates_html
    try:
        html = fetcher()
        entries = parse_releasedates_html(html, row_issues=summary.row_issues)
    except Exception as exc:
        logger.warning("Fed releasedates fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[FedCalendarRawRecord] = []
    event_records: list[FedCalendarEventRecord] = []
    for entry in entries:
        raw_rec, event_rec = release_entry_to_records(
            entry, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        summary.entries_by_indicator[entry.series_id] = (
            summary.entries_by_indicator.get(entry.series_id, 0) + 1
        )

    summary.entries_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
