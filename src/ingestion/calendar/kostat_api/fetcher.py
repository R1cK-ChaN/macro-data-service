"""Drive KOSTAT release-schedule ingestion through the calendar projection.

For each indicator in :data:`INDICATOR_REGISTRY`, fetch the schedule
HTML once per pass and project one schedule-only event per matching
row into ``cal_econ_event``.

One GET per fetch — the schedule page returns the full year's release
calendar in a single response (~80KB). The projector's idempotency
key (``provider``, ``provider_event_id``) collapses repeated sweeps to
no-ops on rows already at the latest content_hash.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, KOSTATIndicatorSpec
from .parser import (
    KOSTAT_RELEASE_SCHEDULE_URL,
    KOSTATCalendarEventRecord,
    KOSTATCalendarParseError,
    KOSTATCalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_schedule,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_KOSTAT_HEADERS: dict[str, str] = {
    # The KOSTAT / mods.go.kr stack rejects the default Python-requests
    # UA on cross-origin requests. A browser-shaped UA matches the
    # workaround used by the BLS / RBI / MoSPI connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.kostat_api)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ko;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_kostat_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
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


def _live_fetcher() -> str:
    response = requests.get(
        KOSTAT_RELEASE_SCHEDULE_URL,
        headers=_KOSTAT_HEADERS,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_kostat_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Sweep the KOSTAT release schedule and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, replaces the HTTP GET. Returns the
        response body string.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
    )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = html_fetcher or _live_fetcher
    try:
        html = fetcher()
    except Exception as exc:
        logger.warning("KOSTAT schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        for indicator in known:
            summary.series_failed.append((indicator, str(exc)))
            if indicator not in summary.indicators_empty:
                summary.indicators_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary
    try:
        announcements = parse_release_schedule(html)
    except KOSTATCalendarParseError as exc:
        logger.warning("KOSTAT schedule parse failed: %s", exc)
        summary.fetch_error = str(exc)
        for indicator in known:
            summary.series_failed.append((indicator, str(exc)))
            if indicator not in summary.indicators_empty:
                summary.indicators_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[KOSTATCalendarRawRecord] = []
    event_records: list[KOSTATCalendarEventRecord] = []
    for indicator in known:
        spec: KOSTATIndicatorSpec = INDICATOR_REGISTRY[indicator]
        matched = [
            a for a in announcements
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
        except (KOSTATCalendarParseError, ValueError, KeyError) as exc:
            logger.warning(
                "KOSTAT projection failed for %s: %s", indicator, exc,
            )
            summary.series_failed.append((indicator, str(exc)))
            continue
        summary.indicators_ok.append(indicator)

    summary.announcements_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_kostat_calendar"]
