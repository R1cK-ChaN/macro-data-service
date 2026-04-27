"""Drive TÜİK national-calendar ingestion through the calendar projection.

For each year in the rolling window, fetch the unified national
release-calendar JSON, filter to TÜİK-owned rows, and project every
matching row into ``cal_econ_event``. The projector's idempotency key
(``provider``, ``provider_event_id``) collapses repeated sweeps to
no-ops on rows already at the latest content_hash.

One GET per requested year — the full per-year event list is embedded
in a single JSON response (~1.4 MB / 4000 rows for 2026, of which
~390 are TÜİK-owned). The fetcher's default window is the current
year plus next year (2 GETs per pass) — TÜİK typically publishes the
following year's headline schedule from December onward, so the
two-year forward look-ahead lands every CPI / PPI / IP / GDP /
Unemployment / Trade release that's been published into the public
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

from .indicators import INDICATOR_REGISTRY, TUIKIndicatorSpec
from .parser import (
    TUIK_CALENDAR_URL_TEMPLATE,
    TUIKCalendarEventRecord,
    TUIKCalendarParseError,
    TUIKCalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_TUIK_HEADERS: dict[str, str] = {
    # TÜİK's public site rejects the default Python-requests UA on
    # some request paths. A browser-shaped UA matches the workaround
    # used by the BLS / RBI / MoSPI / KOSTAT / IBGE / BCB connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.tuik_api)"
    ),
    "Accept": "application/json,text/javascript;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}


# Rolling window: current year + next year. TÜİK publishes the
# following year's calendar from December (Yıllık Veri Yayım Takvimi),
# so a two-year forward look-ahead reliably anchors every upcoming
# headline release without burning a request on a year that returns
# an empty payload.
_DEFAULT_LOOKAHEAD_YEARS = 2


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_tuik_calendar`` invocation."""

    years_planned: list[int] = field(default_factory=list)
    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    years_fetched: int = 0
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


def _default_window(today: date | None = None) -> list[int]:
    base = today or datetime.now(timezone.utc).date()
    return [base.year + offset for offset in range(_DEFAULT_LOOKAHEAD_YEARS)]


def _live_fetcher(year: int) -> str:
    response = requests.get(
        TUIK_CALENDAR_URL_TEMPLATE.format(year=year),
        headers=_TUIK_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_tuik_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    years: Iterable[int] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[int], str] | None = None,
) -> FetchRunSummary:
    """Sweep TÜİK national-calendar pages and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    years:
        Optional iterable of years. Defaults to the current year +
        next year.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    json_fetcher:
        Test seam — when supplied, replaces the per-year HTTP GET.
        Receives ``year``; returns the response body string.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    planned_years = list(years) if years is not None else _default_window()
    summary = FetchRunSummary(
        years_planned=list(planned_years),
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
    )
    if dry_run or not known or not planned_years:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = json_fetcher or _live_fetcher

    indicators_ok: set[str] = set()
    indicators_empty: set[str] = set(known)
    raw_records: list[TUIKCalendarRawRecord] = []
    event_records: list[TUIKCalendarEventRecord] = []

    known_specs: list[tuple[str, TUIKIndicatorSpec]] = [
        (ind, INDICATOR_REGISTRY[ind]) for ind in known
    ]

    for year in planned_years:
        try:
            payload_text = fetcher(year)
        except Exception as exc:
            logger.warning(
                "TÜİK calendar fetch failed for %d: %s", year, exc,
            )
            summary.fetch_error = str(exc)
            continue
        try:
            announcements = parse_release_calendar(
                payload_text, schedule_year=year,
            )
        except TUIKCalendarParseError as exc:
            logger.warning(
                "TÜİK calendar parse failed for %d: %s", year, exc,
            )
            summary.fetch_error = str(exc)
            continue
        summary.years_fetched += 1

        for announcement in announcements:
            indicator: str | None = None
            spec: TUIKIndicatorSpec | None = None
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
            except (TUIKCalendarParseError, ValueError, KeyError) as exc:
                logger.warning(
                    "TÜİK projection failed for %s on %d: %s",
                    indicator, year, exc,
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


__all__ = ["FetchRunSummary", "fetch_tuik_calendar"]
