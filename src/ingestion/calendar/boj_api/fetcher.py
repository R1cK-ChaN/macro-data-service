"""Drive the BoJ MPM calendar and statement scrapes through projection.

``fetch_boj_calendar`` fetches the MPM schedule page (or accepts a
caller-supplied fixture via the ``html_fetcher`` seam used by tests),
parses it into :class:`BojMpmEntry` rows through
:func:`scraper.parse_boj_mpm_calendar_html`, turns each entry into a
``(raw, event)`` tuple through :func:`parser.mpm_entry_to_records`,
and persists via :func:`projector.store_raw` +
:func:`projector.project_schedule_events`.

``fetch_boj_statement_values`` walks past MPM rows that still lack an
``actual``, scrapes the statement page for each, parses the policy-
rate target, and upserts onto the existing schedule row through the
full :func:`projector.project_events` upsert.

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
from .parser import (
    BojCalendarEventRecord,
    BojCalendarRawRecord,
    mpm_entry_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    fetch_boj_mpm_calendar_html,
    parse_boj_mpm_calendar_html,
)
from .statements import (
    BojStatementParseError,
    BojStatementUrlNotFoundError,
    StatementValue,
    fetch_statement,
    statement_value_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_boj_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    meetings_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_boj_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the BoJ MPM calendar and project rows into the calendar.

    Parameters mirror :func:`ingestion.calendar.fed_api.fetch_fed_calendar`.
    """
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=["BOJ_RATE"],
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    fetcher = html_fetcher or fetch_boj_mpm_calendar_html
    html = fetcher()
    entries = parse_boj_mpm_calendar_html(html)
    if not entries:
        # The MPM schedule page carries 8 current-year + 8 prior-year
        # rows in a stable layout. Zero parsed rows means DOM drift or
        # an access-denied interstitial — must fail loud.
        from .scraper import BojMpmCalendarParseError
        raise BojMpmCalendarParseError(
            "BoJ MPM calendar fetch returned zero meetings — upstream "
            "DOM drift or access-denied interstitial"
        )
    summary.meetings_parsed = len(entries)

    raw_records: list[BojCalendarRawRecord] = []
    event_records: list[BojCalendarEventRecord] = []
    for entry in entries:
        raw_rec, event_rec = mpm_entry_to_records(
            entry, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class StatementValuesRunSummary:
    """Outcome of a single :func:`fetch_boj_statement_values` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    meetings_planned: int = 0
    meetings_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


def _discover_pending_closings(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[date]:
    """Find past BoJ meetings with no ``actual`` yet.

    Driven by schedule rows already in ``cal_econ_event`` (written by
    :func:`fetch_boj_calendar`). Filters on the MPM title and a
    missing ``actual`` so only meetings whose statement hasn't been
    scraped qualify.

    The filter applies a **one-hour buffer** past the scheduled
    12:00 JST event time (so polling starts at 13:00 JST of the
    closing day). Publication is observed between 11:25 and 12:56
    JST across the captured fixtures, so a 13:00 JST floor finds
    every meeting live on first poll. Without the buffer, a frequent
    cron that fires between 12:00 and publication would 404 three
    times in a row, and the circuit breaker would cool
    ``boj-values`` for 15 minutes. A rare late-closing meeting that
    publishes past 13:00 JST trips the cool-down once, then the
    retry ~15 minutes later finds the page live — the worst-case
    lag is minutes, not hours.
    """
    # Subtract the buffer from the caller's "now" so the SQL still
    # reads ``event_time_utc < threshold``: a row whose event_time
    # is ``now - 1h`` qualifies, a row whose event_time is ``now - 0``
    # does not.
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    rows = connection.execute(
        """
        SELECT reference_date
        FROM cal_econ_event
        WHERE provider = 'boj'
          AND title LIKE 'BoJ Interest Rate Decision%'
          AND actual IS NULL
          AND event_time_utc < ?
        ORDER BY event_time_utc DESC
        """,
        (threshold_iso,),
    ).fetchall()
    out: list[date] = []
    for (reference_date_iso,) in rows:
        if reference_date_iso is None:
            continue
        out.append(date.fromisoformat(reference_date_iso))
    return out


def fetch_boj_statement_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    closing_dates: list[date] | None = None,
    statement_fetcher: Callable[..., StatementValue] | None = None,
) -> StatementValuesRunSummary:
    """Scrape BoJ statement pages and fill ``actual`` on existing rows.

    Mirrors :func:`ingestion.calendar.fed_api.fetch_fed_statement_values`.
    Per-page fetch / parse failures are collected rather than raising
    so one missing statement URL doesn't abort the rest of the run.
    """
    started = time.monotonic()
    summary = StatementValuesRunSummary(
        indicators_planned=["BOJ_RATE"],
        dry_run=dry_run,
    )

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    as_of_iso = datetime.fromtimestamp(
        snapshot / 1000, tz=timezone.utc,
    ).isoformat()

    if closing_dates is None:
        planned = _discover_pending_closings(connection, as_of_utc_iso=as_of_iso)
    else:
        planned = list(closing_dates)
    summary.meetings_planned = len(planned)

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    # Per-year statement-index lookups are cached across the burst loop
    # so a 30-attempt sweep on one connector doesn't re-fetch the index
    # for every closing date. The cache is wired only into the default
    # `fetch_statement` so user-supplied fetchers (manual replays,
    # execute-mode tests) keep their simple `(closing_date) -> StatementValue`
    # shape and aren't broken by an extra keyword.
    if statement_fetcher is None:
        index_cache: dict[int, dict[str, str]] = {}
        def fetcher(closing: date) -> StatementValue:
            return fetch_statement(closing, index_cache=index_cache)
    else:
        fetcher = statement_fetcher
    raw_records: list[BojCalendarRawRecord] = []
    event_records: list[BojCalendarEventRecord] = []
    for closing in planned:
        try:
            value = fetcher(closing)
        except (BojStatementUrlNotFoundError, BojStatementParseError) as exc:
            logger.warning(
                "BoJ statement parse failed for %s: %s",
                closing.isoformat(), exc,
            )
            summary.parse_failures.append((closing.isoformat(), str(exc)))
            continue
        except Exception as exc:
            logger.warning(
                "BoJ statement fetch failed for %s: %s",
                closing.isoformat(), exc,
            )
            summary.fetch_failures.append((closing.isoformat(), str(exc)))
            continue
        raw_rec, event_rec = statement_value_to_records(
            value, snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        summary.meetings_fetched += 1

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
