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
from datetime import date, datetime, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY
from .parser import (
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    meeting_entry_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .releases import (
    fetch_fed_calendar_json,
    parse_fed_calendar_json,
    release_entry_to_records,
)
from .scraper import fetch_fomc_calendar_html, parse_fomc_calendar_html
from .statements import (
    FomcStatementParseError,
    build_statement_url,
    fetch_statement_html,
    parse_statement_html,
    statement_value_to_records,
)

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
    # Schedule-side write: use ``project_schedule_events`` so the
    # value columns (``actual`` / ``previous`` / …) and the
    # ``observed_at_epoch_ms`` freshness guard stay untouched. The
    # P4b-values statement scrape is the value-side writer; without
    # this split, a later schedule re-scrape with ``actual=None``
    # would clobber a published rate decision.
    summary.events_upserted = project_schedule_events(connection, event_records)
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
    """Consume ``federalreserve.gov/json/calendar.json`` and project
    Beige Book / H.4.1 / H.8 rows into the calendar.

    Parameters mirror :func:`fetch_fed_calendar`. SEP rows are
    explicitly excluded — see :mod:`ingestion.calendar.fed_api.releases`
    for the rationale. Scheduled speeches / testimony are out of
    P4a scope.

    The JSON feed is a single document carrying the Fed's full rolling
    calendar — speeches, testimony, FOMC meetings, and statistical
    releases. Weekly H.4.1 / H.8 (one feed entry per month, days
    listed as a comma-separated string) and ~8/year Beige Book entries
    dominate the whitelisted output.

    The ``html_fetcher`` parameter name is kept for backwards
    compatibility with earlier test seams — callers pass a lambda
    returning the JSON text.
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

    fetcher = html_fetcher or fetch_fed_calendar_json
    try:
        text = fetcher()
        entries = parse_fed_calendar_json(text, row_issues=summary.row_issues)
    except Exception as exc:
        logger.warning("Fed calendar JSON fetch failed: %s", exc)
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


@dataclass
class StatementValuesRunSummary:
    """Outcome of a single :func:`fetch_fed_statement_values` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    meetings_planned: int = 0
    meetings_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


def _title_carries_sep(title: str | None) -> bool:
    """``"FOMC Rate Decision + SEP"`` → True; anything else → False.

    SEP status rides as a boolean suffix on the title column. The
    value-side write needs to read it so a later ``project_events``
    upsert doesn't drop the marker from quarterly projection-
    materials meetings.
    """
    if title is None:
        return False
    return title.rstrip().endswith("+ SEP")


def _discover_pending_closings(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[tuple[date, bool]]:
    """Find past FOMC meetings with no ``actual`` yet.

    Driven by the schedule rows already present in ``cal_econ_event``
    (written by :func:`fetch_fed_calendar`). Filters on title because
    FOMC rows don't populate ``indicator_id``; the title string
    ``"FOMC Rate Decision"`` ± ``" + SEP"`` is stable across every
    meeting the P4 scraper emits. Returns ``(closing, has_sep)`` so
    the value-side projector can preserve the SEP marker.
    """
    rows = connection.execute(
        """
        SELECT reference_date, title
        FROM cal_econ_event
        WHERE provider = 'federal-reserve'
          AND title LIKE 'FOMC Rate Decision%'
          AND actual IS NULL
          AND event_time_utc < ?
        ORDER BY event_time_utc DESC
        """,
        (as_of_utc_iso,),
    ).fetchall()
    out: list[tuple[date, bool]] = []
    for reference_date_iso, title in rows:
        if reference_date_iso is None:
            continue
        out.append((
            date.fromisoformat(reference_date_iso),
            _title_carries_sep(title),
        ))
    return out


def _load_sep_flags(
    connection: sqlite3.Connection,
    closings: list[date],
) -> dict[date, bool]:
    """Look up the stored SEP flag for each explicit closing.

    Explicit callers (operator targeting a single meeting) bypass the
    auto-discovery query, so this reads back the schedule row's title
    for the given dates. Closings with no existing schedule row fall
    back to ``False`` — the value lands, just without a SEP marker.
    """
    if not closings:
        return {}
    placeholders = ",".join("?" for _ in closings)
    iso_keys = [c.isoformat() for c in closings]
    rows = connection.execute(
        f"""
        SELECT reference_date, title
        FROM cal_econ_event
        WHERE provider = 'federal-reserve'
          AND title LIKE 'FOMC Rate Decision%'
          AND reference_date IN ({placeholders})
        """,
        iso_keys,
    ).fetchall()
    flags: dict[date, bool] = {c: False for c in closings}
    for reference_date_iso, title in rows:
        if reference_date_iso is None:
            continue
        flags[date.fromisoformat(reference_date_iso)] = _title_carries_sep(title)
    return flags


def fetch_fed_statement_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    closing_dates: list[date] | None = None,
    html_fetcher: Callable[[date], str] | None = None,
) -> StatementValuesRunSummary:
    """Scrape FOMC statement pages and fill ``actual`` on existing rows.

    Dry-run returns the plan only — the list of closing dates the
    caller would fetch. Execute mode walks each closing date, hits the
    statement page via ``html_fetcher`` (or the default
    :func:`fetch_statement_html` for live runs), parses the target
    range, and upserts through the shared full-row projector. The
    ``provider_event_id`` matches the schedule-side write exactly so
    the ``actual`` value lands on the existing row.

    When ``closing_dates`` is not supplied, the op auto-discovers
    closings from ``cal_econ_event`` — any FOMC meeting already
    scheduled but still missing ``actual`` and already in the past.

    Per-page fetch / parse failures are collected into the summary
    rather than aborting the run — a single statement page 404 (e.g.,
    during an upstream layout change for one meeting) shouldn't block
    progress on the others.
    """
    started = time.monotonic()
    summary = StatementValuesRunSummary(
        indicators_planned=["FOMC_RATE"],
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
        sep_flags = _load_sep_flags(connection, list(closing_dates))
        planned = [(c, sep_flags.get(c, False)) for c in closing_dates]
    summary.meetings_planned = len(planned)

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    fetcher = html_fetcher or fetch_statement_html
    raw_records: list[FedCalendarRawRecord] = []
    event_records: list[FedCalendarEventRecord] = []
    for closing, has_sep in planned:
        try:
            html = fetcher(closing)
        except Exception as exc:
            logger.warning(
                "Fed statement fetch failed for %s: %s", closing.isoformat(), exc,
            )
            summary.fetch_failures.append((closing.isoformat(), str(exc)))
            continue
        try:
            value = parse_statement_html(html, closing_date=closing)
        except FomcStatementParseError as exc:
            logger.warning(
                "Fed statement parse failed for %s: %s", closing.isoformat(), exc,
            )
            summary.parse_failures.append((closing.isoformat(), str(exc)))
            continue
        raw_rec, event_rec = statement_value_to_records(
            value, snapshot_epoch_ms=snapshot, has_sep=has_sep,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        summary.meetings_fetched += 1

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
