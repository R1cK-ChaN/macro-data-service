"""Drive MoSPI release-calendar ingestion through the calendar projection.

For each indicator in :data:`INDICATOR_REGISTRY`, fetch the JSON
release calendar for the requested year (defaults to the current
calendar year) and project one schedule-only event per matching row
into ``cal_econ_event``.

One POST per ``year`` per fetch — the API returns the year's full
release calendar in a single response (~30-100 rows). The
projector's idempotency key (``provider``, ``provider_event_id``)
collapses repeated sweeps to no-ops on rows already at the latest
content_hash.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, MoSPIIndicatorSpec
from .parser import (
    MOSPI_RELEASE_CALENDAR_URL,
    MoSPICalendarEventRecord,
    MoSPICalendarParseError,
    MoSPICalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_MOSPI_HEADERS: dict[str, str] = {
    # The MoSPI release-calendar SPA hosts an open JSON API but the
    # WAF rejects default-Python UA strings on cross-origin requests.
    # A browser-shaped UA matches the same workaround used by the
    # Akamai-fronted government scrapers in this repo.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.mospi_api)"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/json",
    "Origin": "https://www.mospi.gov.in",
    "Referer": "https://www.mospi.gov.in/release-calendar",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_mospi_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    years_planned: list[int] = field(default_factory=list)
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
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


def _resolve_years(years: Iterable[int] | None) -> list[int]:
    if years is not None:
        return [int(y) for y in years]
    # Default window is "current year only" — the daily scheduler runs
    # year-round, so a single-year fetch keeps the request count down.
    # Operators backfilling history pass an explicit list.
    return [datetime.now(timezone.utc).year]


def _live_fetcher(year: int) -> str:
    body = {"lang": "en", "year": year, "page": 1, "limit": 200}
    response = requests.post(
        MOSPI_RELEASE_CALENDAR_URL,
        headers=_MOSPI_HEADERS,
        data=json.dumps(body),
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_mospi_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    years: Iterable[int] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[int], str] | None = None,
) -> FetchRunSummary:
    """Sweep the MoSPI release calendar and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    years:
        Optional list of years to fetch; defaults to the current UTC year.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    json_fetcher:
        Test seam — when supplied, replaces the HTTP POST. Receives the
        target ``year`` and returns the response body string.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    years_list = _resolve_years(years)
    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        years_planned=list(years_list),
        dry_run=dry_run,
    )
    if dry_run or not known or not years_list:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = json_fetcher or _live_fetcher

    all_announcements = []
    for year in years_list:
        try:
            response_text = fetcher(year)
        except Exception as exc:
            logger.warning(
                "MoSPI release-calendar fetch failed for year %d: %s", year, exc,
            )
            summary.fetch_error = str(exc)
            for indicator in known:
                summary.series_failed.append((indicator, str(exc)))
                if indicator not in summary.indicators_empty:
                    summary.indicators_empty.append(indicator)
            summary.wall_seconds = time.monotonic() - started
            return summary
        try:
            announcements = parse_release_calendar(response_text)
        except MoSPICalendarParseError as exc:
            logger.warning(
                "MoSPI release-calendar parse failed for year %d: %s", year, exc,
            )
            summary.fetch_error = str(exc)
            for indicator in known:
                summary.series_failed.append((indicator, str(exc)))
                if indicator not in summary.indicators_empty:
                    summary.indicators_empty.append(indicator)
            summary.wall_seconds = time.monotonic() - started
            return summary
        all_announcements.extend(announcements)

    raw_records: list[MoSPICalendarRawRecord] = []
    event_records: list[MoSPICalendarEventRecord] = []
    for indicator in known:
        spec: MoSPIIndicatorSpec = INDICATOR_REGISTRY[indicator]
        matched = [
            a for a in all_announcements
            if announcement_matches_spec(a, spec)
        ]
        if not matched:
            summary.indicators_empty.append(indicator)
            continue
        try:
            for announcement in matched:
                raw_rec, event_rec = announcement_to_records(
                    announcement, spec=spec, snapshot_epoch_ms=snapshot,
                )
                raw_records.append(raw_rec)
                event_records.append(event_rec)
        except (MoSPICalendarParseError, ValueError, KeyError) as exc:
            logger.warning(
                "MoSPI projection failed for %s: %s", indicator, exc,
            )
            summary.series_failed.append((indicator, str(exc)))
            continue
        summary.indicators_ok.append(indicator)

    summary.announcements_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_mospi_calendar"]
