"""Drive the BLS Public Data API through the calendar projection.

Given a year range and a list of series ids (or the default whitelist),
``fetch_bls_calendar`` pulls observations via the shared
:class:`ingestion.timeseries.scrapers.bls.BLSClient`, turns each one
into a ``(raw, event)`` tuple through :func:`parser.parse_observation`,
and persists via :func:`projector.store_raw` + :func:`project_events`.

Nothing auto-runs: callers construct a :class:`BLSClient`, pick their
year window, and invoke ``fetch_bls_calendar``. A dry-run path returns
the planned series × year chunks without issuing any HTTP request.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from ingestion.timeseries.scrapers.bls import BLSClient

from .indicators import INDICATOR_REGISTRY
from .parser import (
    BLSCalendarEventRecord,
    BLSCalendarRawRecord,
    parse_observation,
)
from .projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from .schedule import (
    SCHEDULE_URL_SLUG,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_bls_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    start_year: int = 0
    end_year: int = 0
    dry_run: bool = True
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    wall_seconds: float = 0.0


def _resolve_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split caller-supplied ids into known + unknown against the registry."""
    if series_ids is None:
        return list(INDICATOR_REGISTRY.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for sid in series_ids:
        if sid in INDICATOR_REGISTRY:
            known.append(sid)
        else:
            unknown.append(sid)
    return known, unknown


def fetch_bls_calendar(
    connection: sqlite3.Connection,
    client: BLSClient,
    *,
    start_year: int,
    end_year: int,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch BLS observations for ``series_ids`` and project to calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    client:
        An authenticated :class:`BLSClient`. Tests inject a fake with
        a compatible ``get_series`` signature.
    start_year, end_year:
        Inclusive year window. Passed through to ``BLSClient.get_series``.
    series_ids:
        Iterable of series ids to fetch. ``None`` means the full
        :data:`INDICATOR_REGISTRY`. Unknown ids are collected in
        ``summary.series_unknown`` and skipped — never silently coerced.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; the returned summary shows the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor on every raw row. Defaults to "now UTC".
    """
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = FetchRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        start_year=start_year,
        end_year=end_year,
        dry_run=dry_run,
    )
    if unknown:
        logger.warning(
            "BLS calendar fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    series_to_obs = client.get_series(
        known, start_year=start_year, end_year=end_year,
    )

    raw_records: list[BLSCalendarRawRecord] = []
    event_records: list[BLSCalendarEventRecord] = []
    for sid in known:
        observations = series_to_obs.get(sid, []) or []
        if not observations:
            summary.series_empty.append(sid)
            continue
        summary.series_ok.append(sid)
        spec = INDICATOR_REGISTRY[sid]
        for obs in observations:
            raw_rec, event_rec = parse_observation(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ScheduleRunSummary:
    """Outcome of a single :func:`schedule_bls_calendar` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def _resolve_schedule_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split into series that have a schedule URL registered + unknown."""
    if series_ids is None:
        return list(SCHEDULE_URL_SLUG.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for sid in series_ids:
        if sid in SCHEDULE_URL_SLUG:
            known.append(sid)
        else:
            unknown.append(sid)
    return known, unknown


def schedule_bls_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: "requests.Session | None" = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape BLS release schedules and project to ``cal_econ_event``.

    Schedule rows land with ``actual=NULL`` and
    ``event_time_precision='datetime'``. When the matching API-side
    values arrive (via :func:`fetch_bls_calendar`), the shared
    ``provider_event_id`` makes the projector merge the two sides on
    the same row: the schedule side keeps owning the scheduled
    datetime; the API side owns ``actual``.

    ``html_fetcher`` is the seam tests inject to feed fixture HTML.
    """
    started = time.monotonic()
    known, unknown = _resolve_schedule_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning(
            "BLS schedule fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    raw_records: list[BLSCalendarRawRecord] = []
    event_records: list[BLSCalendarEventRecord] = []
    for sid in known:
        try:
            html = html_fetcher(sid, session=session)
            entries = parse_schedule_html(html, series_id=sid)
        except Exception as exc:
            logger.warning("BLS schedule fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        if not entries:
            summary.series_failed.append((sid, "no entries parsed"))
            continue
        summary.series_ok.append(sid)
        for entry in entries:
            raw_rec, event_rec = schedule_entry_to_records(
                entry, snapshot_epoch_ms=snapshot,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)

    summary.entries_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(
        connection, event_records,
    )
    summary.wall_seconds = time.monotonic() - started
    return summary
