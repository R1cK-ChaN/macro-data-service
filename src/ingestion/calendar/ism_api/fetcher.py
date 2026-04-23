"""Drive ISM Manufacturing PMI schedule and value ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from .indicators import INDICATOR_REGISTRY
from .parser import (
    ISMCalendarEventRecord,
    ISMCalendarRawRecord,
    parse_report_html,
    report_value_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    discover_current_report_url,
    fetch_report_html,
    fetch_reports_landing_html,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_ism_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    dry_run: bool = True
    report_url: str = ""
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of a single ``schedule_ism_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    row_issues: list[str] = field(default_factory=list)
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _resolve_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split caller-supplied ids into known and unknown registry ids."""
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


def schedule_ism_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape the ISM release calendar for whitelisted indicators."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("ISM schedule fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    try:
        html = html_fetcher(session=session)
        entries = parse_schedule_html(
            html,
            series_ids=set(known),
            row_issues=summary.row_issues,
        )
    except Exception as exc:
        logger.warning("ISM schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    if not entries:
        summary.series_empty.extend(known)
        summary.fetch_error = "no ISM schedule entries parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[ISMCalendarRawRecord] = []
    event_records: list[ISMCalendarEventRecord] = []
    for entry in entries:
        spec = INDICATOR_REGISTRY[entry.series_id]
        raw_rec, event_rec = schedule_entry_to_records(
            entry, snapshot_epoch_ms=snapshot, spec=spec,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        hits[entry.series_id] += 1

    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        else:
            summary.series_empty.append(sid)
    summary.entries_parsed = len(entries)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


def fetch_ism_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    landing_html_fetcher=fetch_reports_landing_html,
    report_html_fetcher=fetch_report_html,
) -> FetchRunSummary:
    """Fetch the current ISM Manufacturing PMI report value."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    value_known = [
        sid for sid in known if INDICATOR_REGISTRY[sid].value_fetch
    ]
    summary = FetchRunSummary(
        series_planned=list(value_known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("ISM value fetch: unknown series skipped: %s", unknown)
    if dry_run or not value_known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[ISMCalendarRawRecord] = []
    event_records: list[ISMCalendarEventRecord] = []
    try:
        landing_html = landing_html_fetcher(session=session)
        report_url = discover_current_report_url(landing_html)
        report_html = report_html_fetcher(report_url, session=session)
        summary.report_url = report_url
        value = parse_report_html(report_html, source_url=report_url)
        if value.series_id in value_known:
            spec = INDICATOR_REGISTRY[value.series_id]
            raw_rec, event_rec = report_value_to_records(
                value, snapshot_epoch_ms=snapshot, spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)
    except Exception as exc:
        logger.warning("ISM value fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    if not event_records:
        summary.series_empty.extend(value_known)
        summary.fetch_error = "no ISM report values parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    summary.series_ok.append(value.series_id)
    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
