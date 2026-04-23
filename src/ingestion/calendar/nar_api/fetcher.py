"""Drive NAR schedule and value ingestion."""

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
    NARCalendarEventRecord,
    NARCalendarRawRecord,
    current_value_to_records,
    parse_current_value_html,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    fetch_current_html,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_nar_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of a single ``schedule_nar_calendar`` invocation."""

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


def schedule_nar_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape NAR statistical release dates for whitelisted series."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("NAR schedule fetch: unknown series skipped: %s", unknown)
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
        logger.warning("NAR schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    if not entries:
        summary.series_empty.extend(known)
        summary.fetch_error = "no NAR schedule entries parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[NARCalendarRawRecord] = []
    event_records: list[NARCalendarEventRecord] = []
    for entry in entries:
        spec = INDICATOR_REGISTRY[entry.series_id]
        raw_rec, event_rec = schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=snapshot,
            spec=spec,
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


def fetch_nar_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    current_html_fetcher=fetch_current_html,
) -> FetchRunSummary:
    """Fetch current NAR housing indicator values."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    value_known = [sid for sid in known if INDICATOR_REGISTRY[sid].value_fetch]
    summary = FetchRunSummary(
        series_planned=list(value_known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("NAR value fetch: unknown series skipped: %s", unknown)
    if dry_run or not value_known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[NARCalendarRawRecord] = []
    event_records: list[NARCalendarEventRecord] = []
    for sid in value_known:
        spec = INDICATOR_REGISTRY[sid]
        try:
            html = current_html_fetcher(spec.source_url, session=session)
            value = parse_current_value_html(
                html,
                source_url=spec.source_url,
                series_id=sid,
            )
            raw_rec, event_rec = current_value_to_records(
                value,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
        except Exception as exc:
            logger.warning("NAR value fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        summary.series_ok.append(sid)

    for sid in value_known:
        if sid not in summary.series_ok and all(
            sid != fail[0] for fail in summary.series_failed
        ):
            summary.series_empty.append(sid)
    if not event_records:
        summary.fetch_error = "no NAR current values parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
