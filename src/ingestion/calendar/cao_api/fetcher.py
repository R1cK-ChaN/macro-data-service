"""Drive the CAO schedule + Consumer Confidence scrapes through projection.

``fetch_cao_calendar`` fetches the ESRI release-schedule page, parses
the Consumer Confidence column into ``(raw, event)`` tuples via
:func:`scraper.schedule_entry_to_records`, and persists through
:func:`projector.store_raw` + :func:`projector.project_schedule_events`.

``fetch_cao_consumer_confidence_values`` fetches the
``shouhi-e.html`` landing page once, parses the single visible
release (reference month / release day / CCI seasonally adjusted),
looks up the stored schedule-side event-time for that reference
month if a row already exists, and upserts through
:func:`projector.project_events`. The landing page overwrites on
each release so there is no per-reference iteration — the sweep
only ever writes the release currently on-screen.

Nothing auto-runs: callers invoke the entry points. Dry-run paths
return the planned indicator list without issuing any HTTP request.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY
from .parser import (
    CaoCalendarEventRecord,
    CaoCalendarRawRecord,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    CaoCalendarParseError,
    fetch_cao_schedule_html,
    parse_cao_schedule_html,
    schedule_entry_to_records,
)
from .surveys import (
    CaoConsumerConfidenceParseError,
    consumer_confidence_to_records,
    fetch_consumer_confidence_summary_html,
    parse_consumer_confidence_summary,
)

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_cao_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_cao_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the ESRI release-schedule page and project schedule rows."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or fetch_cao_schedule_html
    html = fetcher()
    entries = parse_cao_schedule_html(html)
    if not entries:
        # The schedule table publishes at least three forward-looking
        # Consumer Confidence rows at all times. Zero parsed rows
        # means DOM drift or an outage page — fail loud so the
        # scheduler's circuit breaker counts the trip.
        raise CaoCalendarParseError(
            "ESRI schedule fetch returned zero Consumer Confidence "
            "rows — upstream DOM drift or access-denied interstitial"
        )
    summary.releases_parsed = len(entries)

    raw_records: list[CaoCalendarRawRecord] = []
    event_records: list[CaoCalendarEventRecord] = []
    for entry in entries:
        for raw, event in schedule_entry_to_records(
            entry, snapshot_epoch_ms=snapshot,
        ):
            raw_records.append(raw)
            event_records.append(event)

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ConsumerConfidenceValuesRunSummary:
    """Outcome of a single :func:`fetch_cao_consumer_confidence_values` pass."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_planned: int = 0
    releases_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    # List of ``(reference_date, event_time_utc)`` tuples for rows
    # whose scheduled release time is already past but whose
    # ``actual`` is still NULL *after* this sweep runs. Non-empty
    # means the landing page is lagging the next due release and the
    # operator has a gap to chase. Without this signal, a stale
    # ``shouhi-e.html`` could leave an overdue April row unfilled
    # while the sweep reports ``releases_fetched=1`` for an already-
    # captured March.
    overdue_references: list[tuple[str, str]] = field(default_factory=list)
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


def _lookup_stored_event_time(
    connection: sqlite3.Connection,
    reference: date,
) -> str:
    """Return the schedule-side ``event_time_utc`` for a reference month.

    Returns empty string when no schedule row exists yet (in which
    case :func:`consumer_confidence_to_records` synthesises the
    value from ``summary.release_date`` + 14:00 JST — the same logic
    the schedule-side write would use if it had seeded the row).
    """
    row = connection.execute(
        """
        SELECT event_time_utc
        FROM cal_econ_event
        WHERE provider = 'cao'
          AND title = 'Consumer Confidence'
          AND reference_date = ?
        LIMIT 1
        """,
        (reference.isoformat(),),
    ).fetchone()
    return row[0] if row and row[0] else ""


def fetch_cao_consumer_confidence_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> ConsumerConfidenceValuesRunSummary:
    """Scrape the Consumer Confidence landing page and fill ``actual``.

    One GET per sweep — the landing page carries at most one release
    at a time. Per-URL fetch / parse failures are collected so a
    stale-cache hit doesn't abort the rest of the value-side pass.
    """
    started = time.monotonic()
    summary = ConsumerConfidenceValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )
    # ``releases_planned`` is always 1 — the shouhi-e.html surface
    # serves at most one release per sweep. Parallels the MoF /
    # Tankan auto-discovery shape, which reports per-reference
    # planning; here the GET is the unit.
    summary.releases_planned = 1

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or fetch_consumer_confidence_summary_html
    try:
        html = fetcher()
    except Exception as exc:
        logger.warning(
            "CAO Consumer Confidence landing-page fetch failed: %s", exc,
        )
        summary.fetch_failures.append(("shouhi-e.html", str(exc)))
        summary.wall_seconds = time.monotonic() - started
        return summary

    try:
        cc_summary = parse_consumer_confidence_summary(html)
    except CaoConsumerConfidenceParseError as exc:
        logger.warning(
            "CAO Consumer Confidence landing-page parse failed: %s", exc,
        )
        summary.parse_failures.append(("shouhi-e.html", str(exc)))
        summary.wall_seconds = time.monotonic() - started
        return summary

    event_time_override = _lookup_stored_event_time(
        connection, cc_summary.reference_date,
    ) or None
    raw, event = consumer_confidence_to_records(
        cc_summary,
        snapshot_epoch_ms=snapshot,
        event_time_utc=event_time_override,
    )
    summary.rows_raw_inserted = store_raw(connection, [raw])
    summary.events_upserted = project_events(connection, [event])
    summary.releases_fetched = 1

    # Surface any still-unfilled schedule rows whose release time is
    # already past. If the landing page is lagging (shouhi-e.html
    # still showing an older month while the next due release has
    # already crossed 14:00 JST), the upsert above would write a
    # no-op for the stale row and the newer expected row would stay
    # ``actual IS NULL`` silently. Recording the overdue list lets
    # the scheduler + operator trace the gap rather than accept a
    # misleading ``releases_fetched=1`` as success for the due
    # release.
    now_iso = datetime.fromtimestamp(
        snapshot / 1000, tz=timezone.utc,
    ).isoformat()
    pending_rows = connection.execute(
        """
        SELECT reference_date, event_time_utc
        FROM cal_econ_event
        WHERE provider = 'cao'
          AND title = 'Consumer Confidence'
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY reference_date DESC
        """,
        (now_iso,),
    ).fetchall()
    summary.overdue_references = [
        (ref_iso or "", evt_iso or "")
        for ref_iso, evt_iso in pending_rows
    ]

    summary.wall_seconds = time.monotonic() - started
    return summary
