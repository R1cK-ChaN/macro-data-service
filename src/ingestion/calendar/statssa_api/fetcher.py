"""Drive Stats SA calendar ingestion through the calendar projection.

Per pass: one POST per month inside a rolling current + N month-ahead
window, against
``statssa.gov.za/wp-content/themes/umkhanyakude-v2.1/ajax_server.php?req=recently_scheduled_eddie_t``.
The response is the full schedule for that month — typically 5-15
rows; the parser projects every row that matches an indicator in the
allowlist.

Window: current month + the next 14 months by default. Stats SA
publishes the next year's full Publication Schedule from October
onward, so a 14-month forward window comfortably covers the
publication horizon while keeping the per-pass request budget small
(15 POSTs / pass at the default).

The projector's ``(provider, provider_event_id)`` upsert collapses
repeated sweeps to no-ops on rows already at the latest content_hash.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, StatsSAIndicatorSpec
from .parser import (
    STATSSA_SCHEDULE_API_URL,
    StatsSACalendarEventRecord,
    StatsSACalendarParseError,
    StatsSACalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_publication_schedule,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_STATSSA_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.statssa_api)"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-ZA,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
}


# Rolling window default: the current month + 14 lookahead months
# (15 POSTs per pass). Stats SA publishes the following year's full
# Publication Schedule by mid-October, so a 14-month forward window
# comfortably reaches the next year's December horizon by then.
_DEFAULT_LOOKAHEAD_MONTHS = 14

_EN_MONTH_NAMES: tuple[str, ...] = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_statssa_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    months_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    months_fetched: int = 0
    announcements_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _resolve_indicators(
    indicators: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    if indicators is None:
        return list(INDICATOR_REGISTRY.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for ind in indicators:
        if ind in INDICATOR_REGISTRY:
            known.append(ind)
        else:
            unknown.append(ind)
    return known, unknown


def _month_tokens(today: date | None = None) -> list[str]:
    base = today or datetime.now(timezone.utc).date()
    tokens: list[str] = []
    year = base.year
    month = base.month
    # Current month + ``_DEFAULT_LOOKAHEAD_MONTHS`` future months. The
    # ``+ 1`` makes the loop bound match the documented "current + 14
    # months" horizon — without it the December horizon row drops out
    # until next month rolls around, shortening the visibility window.
    for _ in range(_DEFAULT_LOOKAHEAD_MONTHS + 1):
        tokens.append(f"{_EN_MONTH_NAMES[month - 1]} {year}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return tokens


def _live_fetcher(month_token: str) -> str:
    body = {
        "sel_publication": month_token,
        "selec":           "200",
        "start":           "0",
        "page_no":         "1",
    }
    response = requests.post(
        STATSSA_SCHEDULE_API_URL,
        headers=_STATSSA_HEADERS,
        data=body,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_statssa_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    months: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[str], str] | None = None,
) -> FetchRunSummary:
    """Sweep Stats SA's Publication Schedule and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    months:
        Optional sequence of ``"<MonthName> <YYYY>"`` tokens. Defaults
        to the current month + the next 14 months. Useful for
        backfill replay or for trimming a daily sweep to the next
        couple of months only.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, replaces the per-month POST.
        Receives ``month_token``; returns the HTML response body.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    if months is None:
        months_planned = _month_tokens()
    else:
        months_planned = [m for m in months if m]

    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        months_planned=list(months_planned),
        dry_run=dry_run,
    )
    if dry_run or not known or not months_planned:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = html_fetcher or _live_fetcher

    indicators_ok: set[str] = set()
    indicators_empty: set[str] = set(known)
    raw_records: list[StatsSACalendarRawRecord] = []
    event_records: list[StatsSACalendarEventRecord] = []
    known_specs: list[tuple[str, StatsSAIndicatorSpec]] = [
        (ind, INDICATOR_REGISTRY[ind]) for ind in known
    ]

    for month_token in months_planned:
        try:
            payload = fetcher(month_token)
        except Exception as exc:  # pragma: no cover — exception passthrough
            logger.warning(
                "Stats SA schedule fetch failed for month=%s: %s",
                month_token, exc,
            )
            summary.fetch_error = str(exc)
            continue
        try:
            announcements = parse_publication_schedule(
                payload, schedule_month=month_token,
            )
        except StatsSACalendarParseError as exc:
            logger.warning(
                "Stats SA schedule parse failed for month=%s: %s",
                month_token, exc,
            )
            summary.fetch_error = str(exc)
            continue
        summary.months_fetched += 1

        for announcement in announcements:
            for ind, spec in known_specs:
                if not announcement_matches_spec(announcement, spec):
                    continue
                try:
                    raw_rec, event_rec = announcement_to_records(
                        announcement, spec=spec, snapshot_epoch_ms=snapshot,
                    )
                except (StatsSACalendarParseError, ValueError, KeyError) as exc:
                    logger.warning(
                        "Stats SA projection failed for %s on ppn=%s "
                        "month=%s: %s",
                        ind, announcement.ppn, month_token, exc,
                    )
                    continue
                raw_records.append(raw_rec)
                event_records.append(event_rec)
                indicators_ok.add(ind)
                indicators_empty.discard(ind)
                # First matched indicator wins for a given row — Stats
                # SA's PPNs are unique per indicator in the allowlist
                # so the inner loop can short-circuit.
                break

    summary.indicators_ok = sorted(indicators_ok)
    summary.indicators_empty = sorted(indicators_empty)
    summary.announcements_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_statssa_calendar"]
