"""Drive the MoF calendar + trade-report scrapes through projection.

``fetch_mof_calendar`` fetches the release-calendar page (or accepts
a caller-supplied fixture via ``html_fetcher``), parses each
Monthly Data release row into a ``(raw, event)`` tuple via
:func:`scraper.schedule_entry_to_records`, and persists through
:func:`projector.store_raw` +
:func:`projector.project_schedule_events`.

``fetch_mof_trade_values`` auto-discovers past Balance-of-Trade
rows still carrying ``actual IS NULL``, fetches each report XML,
extracts the headline balance, and upserts through
:func:`projector.project_events`. The stored ``event_time_utc``
from the schedule write is passed through verbatim so the upsert
preserves the canonical publish timestamp.

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
    MofCalendarEventRecord,
    MofCalendarRawRecord,
)
from .projector import project_events, project_schedule_events, store_raw
from .reports import (
    MofTradeReportParseError,
    fetch_trade_report_xml,
    parse_trade_report_xml,
    trade_report_to_records,
)
from .scraper import (
    MofCalendarParseError,
    fetch_mof_calendar_html,
    parse_mof_calendar_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_mof_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_mof_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape the MoF release calendar and project schedule rows."""
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

    fetcher = html_fetcher or fetch_mof_calendar_html
    html = fetcher()
    entries = parse_mof_calendar_html(html)
    if not entries:
        # The calendar page carries 12+ monthly rows per published
        # year in a stable layout. Zero parsed rows means DOM drift
        # or an access-denied interstitial — fail loud.
        raise MofCalendarParseError(
            "MoF calendar fetch returned zero releases — upstream "
            "DOM drift or access-denied interstitial"
        )
    summary.releases_parsed = len(entries)

    raw_records: list[MofCalendarRawRecord] = []
    event_records: list[MofCalendarEventRecord] = []
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
class TradeValuesRunSummary:
    """Outcome of a single :func:`fetch_mof_trade_values` invocation."""

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
class _PendingTrade:
    """One Balance-of-Trade release pending a value-side fill."""

    reference_date: date
    event_time_utc: str


def _lookup_stored_event_time(
    connection: sqlite3.Connection,
    reference: date,
) -> _PendingTrade:
    """Resolve ``event_time_utc`` for a caller-supplied ``reference_date``.

    Parallels the Tankan connector — a manual replay must keep the
    schedule-side publish clock intact so the value-side upsert
    doesn't shift the stored row to the fallback release day.
    """
    row = connection.execute(
        """
        SELECT event_time_utc
        FROM cal_econ_event
        WHERE provider = 'mof-jp'
          AND title = 'Balance of Trade'
          AND reference_date = ?
        LIMIT 1
        """,
        (reference.isoformat(),),
    ).fetchone()
    event_time_utc = row[0] if row and row[0] else ""
    return _PendingTrade(reference_date=reference, event_time_utc=event_time_utc)


def _discover_pending_references(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[_PendingTrade]:
    """Find past BoT schedule rows with no ``actual`` yet.

    Applies a **1-hour release-time buffer** past the scheduled
    08:50 JST event time so a frequent cron that fires between the
    calendar refresh and the XML feed going live doesn't 404 three
    times and trip the 15-minute breaker cool-down. Matches the BoJ
    MPM + Tankan pattern.
    """
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    rows = connection.execute(
        """
        SELECT reference_date, event_time_utc
        FROM cal_econ_event
        WHERE provider = 'mof-jp'
          AND title = 'Balance of Trade'
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY event_time_utc DESC
        """,
        (threshold_iso,),
    ).fetchall()
    out: list[_PendingTrade] = []
    for reference_iso, event_time_utc in rows:
        if not reference_iso or not event_time_utc:
            continue
        try:
            reference_date = date.fromisoformat(reference_iso)
        except ValueError:
            continue
        out.append(_PendingTrade(
            reference_date=reference_date,
            event_time_utc=event_time_utc,
        ))
    return out


def fetch_mof_trade_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    reference_dates: list[date] | None = None,
    xml_fetcher: Callable[[date], str] | None = None,
) -> TradeValuesRunSummary:
    """Scrape MoF trade XMLs and fill ``actual`` on existing rows.

    Mirrors :func:`ingestion.calendar.boj_tankan_api.fetch_boj_tankan_outlines`.
    Per-URL fetch / parse failures are collected so one missing
    XML doesn't abort the rest of the run.
    """
    started = time.monotonic()
    summary = TradeValuesRunSummary(
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
        planned = [
            _lookup_stored_event_time(connection, d) for d in reference_dates
        ]
    summary.releases_planned = len(planned)

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    fetcher = xml_fetcher or fetch_trade_report_xml
    raw_records: list[MofCalendarRawRecord] = []
    event_records: list[MofCalendarEventRecord] = []
    for pending in planned:
        ref = pending.reference_date
        try:
            xml_text = fetcher(ref)
        except Exception as exc:
            logger.warning(
                "MoF trade XML fetch failed for %s: %s", ref.isoformat(), exc,
            )
            summary.fetch_failures.append((ref.isoformat(), str(exc)))
            continue
        try:
            value = parse_trade_report_xml(xml_text, reference_date=ref)
        except MofTradeReportParseError as exc:
            logger.warning(
                "MoF trade XML parse failed for %s: %s", ref.isoformat(), exc,
            )
            summary.parse_failures.append((ref.isoformat(), str(exc)))
            continue
        override = pending.event_time_utc or None
        raw, event = trade_report_to_records(
            value,
            snapshot_epoch_ms=snapshot,
            event_time_utc=override,
        )
        raw_records.append(raw)
        event_records.append(event)
        summary.releases_fetched += 1

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
