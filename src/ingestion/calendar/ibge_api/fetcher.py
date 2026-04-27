"""Drive IBGE monthly-calendar ingestion through the calendar projection.

For each ``(year, month)`` in the rolling window, fetch the
release-calendar HTML and project every matching row into
``cal_econ_event``. The projector's idempotency key
(``provider``, ``provider_event_id``) collapses repeated sweeps to
no-ops on rows already at the latest content_hash.

One GET per requested month — the full per-month event list is
embedded in a single HTML response. The fetcher's default window is
the current calendar month plus the next three (4 GETs per pass), so
the rolling forward look-ahead lands every IPCA / PIM-PF / PNAD /
PIB / IPCA-15 release that IBGE has published into its public
calendar at the time of the sweep.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, IBGEIndicatorSpec
from .parser import (
    IBGE_CALENDAR_URL_TEMPLATE,
    IBGECalendarEventRecord,
    IBGECalendarParseError,
    IBGECalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_IBGE_HEADERS: dict[str, str] = {
    # IBGE's public site rejects the default Python-requests UA on
    # some request paths. A browser-shaped UA matches the workaround
    # used by the BLS / RBI / MoSPI / KOSTAT connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.ibge_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}


# Rolling window: current month + next three months. Past months are
# unchanging once a release lands; the connector's job is to anchor
# upcoming releases. Four months strikes the balance between coverage
# (IBGE typically publishes the next quarter's schedule a month
# ahead) and request count.
_DEFAULT_LOOKAHEAD_MONTHS = 4


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_ibge_calendar`` invocation."""

    months_planned: list[tuple[int, int]] = field(default_factory=list)
    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
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


def _default_window(today: date | None = None) -> list[tuple[int, int]]:
    """Return the default ``(year, month)`` window — current + next 3."""
    base = today or datetime.now(timezone.utc).date()
    months: list[tuple[int, int]] = []
    year = base.year
    month = base.month
    for _ in range(_DEFAULT_LOOKAHEAD_MONTHS):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def _live_fetcher(year: int, month: int) -> str:
    response = requests.get(
        IBGE_CALENDAR_URL_TEMPLATE.format(month=month, year=year),
        headers=_IBGE_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_ibge_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    months: Iterable[tuple[int, int]] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[int, int], str] | None = None,
) -> FetchRunSummary:
    """Sweep the IBGE monthly calendar pages and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    months:
        Optional iterable of ``(year, month)`` pairs. Defaults to the
        current calendar month plus the next three.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, replaces the per-month HTTP GET.
        Receives ``(year, month)``; returns the response body string.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    planned_months = list(months) if months is not None else _default_window()
    summary = FetchRunSummary(
        months_planned=list(planned_months),
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
    )
    if dry_run or not known or not planned_months:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = html_fetcher or _live_fetcher

    indicators_ok: set[str] = set()
    indicators_empty: set[str] = set(known)
    raw_records: list[IBGECalendarRawRecord] = []
    event_records: list[IBGECalendarEventRecord] = []

    # Matching is exclusive — the registry is iteration-ordered with
    # longer-prefix indicators first, so the inner walk stops at the
    # first hit. This prevents IPCA-15's row from also being attributed
    # to IPCA (whose title substring is a prefix of IPCA-15's).
    known_specs: list[tuple[str, IBGEIndicatorSpec]] = [
        (ind, INDICATOR_REGISTRY[ind]) for ind in known
    ]

    for year, month in planned_months:
        try:
            html = fetcher(year, month)
        except Exception as exc:
            logger.warning(
                "IBGE calendar fetch failed for %04d-%02d: %s", year, month, exc,
            )
            summary.fetch_error = str(exc)
            continue
        try:
            announcements = parse_release_calendar(
                html, schedule_year=year, schedule_month=month,
            )
        except IBGECalendarParseError as exc:
            logger.warning(
                "IBGE calendar parse failed for %04d-%02d: %s", year, month, exc,
            )
            summary.fetch_error = str(exc)
            continue
        summary.months_fetched += 1

        for announcement in announcements:
            indicator: str | None = None
            spec: IBGEIndicatorSpec | None = None
            for ind, candidate in known_specs:
                if announcement_matches_spec(announcement, candidate):
                    indicator = ind
                    spec = candidate
                    break
            if indicator is None or spec is None:
                continue
            try:
                raw_rec, event_rec = announcement_to_records(
                    announcement, spec=spec, snapshot_epoch_ms=snapshot,
                )
            except (IBGECalendarParseError, ValueError, KeyError) as exc:
                logger.warning(
                    "IBGE projection failed for %s on %04d-%02d: %s",
                    indicator, year, month, exc,
                )
                summary.series_failed.append((indicator, str(exc)))
                continue
            raw_records.append(raw_rec)
            event_records.append(event_rec)
            indicators_ok.add(indicator)
            indicators_empty.discard(indicator)

    summary.indicators_ok = sorted(indicators_ok)
    summary.indicators_empty = sorted(indicators_empty)
    summary.announcements_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_ibge_calendar"]
