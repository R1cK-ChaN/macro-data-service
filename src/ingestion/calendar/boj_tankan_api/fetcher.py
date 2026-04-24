"""Drive the Tankan schedule and outline scrapes through projection.

``fetch_boj_tankan_calendar`` fetches the yoshi-index page (or
accepts a caller-supplied fixture via ``html_fetcher``), parses the
release rows through :func:`scraper.parse_tankan_schedule_html`,
turns each entry into two ``(raw, event)`` tuples (Large Mfg + Large
Non-Mfg) via :func:`scraper.schedule_entry_to_records`, and persists
through :func:`projector.store_raw` +
:func:`projector.project_schedule_events`.

``fetch_boj_tankan_outlines`` auto-discovers past Tankan rows still
carrying ``actual IS NULL``, fetches each outline page, parses the
Large-Enterprises DI block, and upserts the DI values through the
full :func:`projector.project_events` upsert. The stored
``event_time_utc`` from the schedule write is passed through
verbatim so the upsert preserves the canonical publish timestamp
without re-deriving it from the outline page (the outline page
carries no release-time block).

Nothing auto-runs: callers invoke the entry points. Dry-run paths
return the planned indicator list without issuing any HTTP request.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY
from .outlines import (
    TankanOutlineParseError,
    fetch_outline_html,
    outline_value_to_records,
    parse_outline_html,
)
from .parser import (
    TankanCalendarEventRecord,
    TankanCalendarRawRecord,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    TankanScheduleParseError,
    fetch_tankan_yoshi_index_html,
    parse_tankan_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_boj_tankan_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_boj_tankan_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the Tankan yoshi-index page and project schedule rows.

    Shape mirrors :func:`ingestion.calendar.boj_api.fetch_boj_calendar`:
    dry-run returns the indicator plan only; execute mode performs
    one HTTP request, parses every row, emits two ``(raw, event)``
    tuples per release (Large Mfg + Large Non-Mfg), and upserts via
    :func:`project_schedule_events` so a later value-side sweep's
    ``actual`` isn't clobbered.
    """
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

    fetcher = html_fetcher or fetch_tankan_yoshi_index_html
    html = fetcher()
    entries = parse_tankan_schedule_html(html)
    if not entries:
        # The yoshi-index page carries the last ~12 quarterly releases
        # in a stable layout. Zero parsed rows means DOM drift or an
        # access-denied interstitial — fail loud.
        raise TankanScheduleParseError(
            "Tankan yoshi-index fetch returned zero releases — upstream "
            "DOM drift or access-denied interstitial"
        )
    summary.releases_parsed = len(entries)

    raw_records: list[TankanCalendarRawRecord] = []
    event_records: list[TankanCalendarEventRecord] = []
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
class OutlineValuesRunSummary:
    """Outcome of a single :func:`fetch_boj_tankan_outlines` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_planned: int = 0
    releases_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


@dataclass(frozen=True)
class _PendingOutline:
    """One Tankan release pending a value-side fill."""

    reference_date: date
    event_time_utc: str


def _lookup_stored_event_time(
    connection: sqlite3.Connection,
    reference: date,
) -> _PendingOutline:
    """Resolve ``event_time_utc`` for a caller-supplied ``reference_date``.

    A manual replay against a known reference (via the service op's
    ``reference_dates`` argument) must keep the schedule-side publish
    clock intact. We pull the stored ``event_time_utc`` off either
    indicator's row — both indicators for a given reference share a
    release date, so the lookup is order-agnostic. Missing rows fall
    through with an empty override, which lets the writer synthesize
    a plausible release day (first-time writes only).
    """
    row = connection.execute(
        """
        SELECT event_time_utc
        FROM cal_econ_event
        WHERE provider = 'boj'
          AND title LIKE 'Tankan Large %'
          AND reference_date = ?
        ORDER BY event_time_utc DESC
        LIMIT 1
        """,
        (reference.isoformat(),),
    ).fetchone()
    event_time_utc = row[0] if row and row[0] else ""
    return _PendingOutline(
        reference_date=reference,
        event_time_utc=event_time_utc,
    )


def _discover_pending_references(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[_PendingOutline]:
    """Find past Tankan schedule rows with no ``actual`` yet.

    Filters on the Tankan titles and ``actual IS NULL``. A **one-hour
    buffer** past the scheduled 08:50 JST event time (00:50 UTC on
    the prior day under JST) is applied to absorb any race where the
    frequent cron sweep fires between yoshi-index refresh and outline
    page availability — in practice BoJ publishes both surfaces
    together, but the buffer matches the circuit-breaker pattern
    used by :mod:`ingestion.calendar.boj_api.fetcher` for MPM.

    Aggregates at the ``(reference_date, event_time_utc)`` level so
    the outline fetch runs once per release even though each release
    writes two rows (one per indicator).
    """
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    rows = connection.execute(
        """
        SELECT DISTINCT reference_date, event_time_utc
        FROM cal_econ_event
        WHERE provider = 'boj'
          AND title LIKE 'Tankan Large %'
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY event_time_utc DESC
        """,
        (threshold_iso,),
    ).fetchall()
    out: list[_PendingOutline] = []
    for reference_iso, event_time_utc in rows:
        if not reference_iso or not event_time_utc:
            continue
        try:
            reference_date = date.fromisoformat(reference_iso)
        except ValueError:
            continue
        out.append(_PendingOutline(
            reference_date=reference_date,
            event_time_utc=event_time_utc,
        ))
    return out


def fetch_boj_tankan_outlines(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    reference_dates: list[date] | None = None,
    html_fetcher: Callable[[date], str] | None = None,
) -> OutlineValuesRunSummary:
    """Scrape Tankan outline pages and fill ``actual`` on existing rows.

    Mirrors :func:`ingestion.calendar.boj_api.fetch_boj_statement_values`.
    When ``reference_dates`` is None, the op auto-discovers past
    Tankan rows with ``actual IS NULL`` from ``cal_econ_event``.
    Per-page fetch / parse failures are collected so one missing
    outline URL doesn't abort the rest of the run.
    """
    started = time.monotonic()
    summary = OutlineValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    as_of_iso = datetime.fromtimestamp(
        snapshot / 1000, tz=timezone.utc,
    ).isoformat()

    if reference_dates is None:
        planned = _discover_pending_references(connection, as_of_utc_iso=as_of_iso)
    else:
        # Manual replay path. We still need to carry the canonical
        # schedule-side ``event_time_utc`` through so the value-side
        # upsert doesn't shift the stored row — Dec 2025 was
        # released on Dec 15, but ``_default_release_for_reference``
        # (which the writer would otherwise fall back to) resolves
        # to Jan 1 2026 and would stamp the wrong UTC date onto an
        # existing schedule row.
        planned = [
            _lookup_stored_event_time(connection, d) for d in reference_dates
        ]
    summary.releases_planned = len(planned)

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    fetcher = html_fetcher or fetch_outline_html
    raw_records: list[TankanCalendarRawRecord] = []
    event_records: list[TankanCalendarEventRecord] = []
    for pending in planned:
        ref = pending.reference_date
        try:
            html = fetcher(ref)
        except Exception as exc:
            logger.warning(
                "Tankan outline fetch failed for %s: %s", ref.isoformat(), exc,
            )
            summary.fetch_failures.append((ref.isoformat(), str(exc)))
            continue
        try:
            value = parse_outline_html(html, reference_date=ref)
        except TankanOutlineParseError as exc:
            logger.warning(
                "Tankan outline parse failed for %s: %s", ref.isoformat(), exc,
            )
            summary.parse_failures.append((ref.isoformat(), str(exc)))
            continue
        override = pending.event_time_utc or None
        for raw, event in outline_value_to_records(
            value,
            snapshot_epoch_ms=snapshot,
            event_time_utc=override,
        ):
            raw_records.append(raw)
            event_records.append(event)
        summary.releases_fetched += 1

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
