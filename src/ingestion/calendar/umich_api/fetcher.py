"""Drive U Michigan Consumer Sentiment schedule and value ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from .indicators import INDICATOR_REGISTRY, UMICH_SURVEY_INFO_URL
from .parser import (
    UMichCalendarEventRecord,
    UMichCalendarRawRecord,
    current_value_to_records,
    parse_current_results_html,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    UMichScheduleDocument,
    fetch_current_results_html,
    fetch_release_dates_document,
    parse_release_dates_text,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_umich_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    dry_run: bool = True
    release_stage: str = ""
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of a single ``schedule_umich_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    dry_run: bool = True
    year: int | None = None
    source_url: str = ""
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
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


def _coerce_schedule_document(doc: object) -> tuple[str, str]:
    if isinstance(doc, UMichScheduleDocument):
        return doc.text, doc.source_url
    return str(doc), ""


def schedule_umich_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    year: int | None = None,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    document_fetcher=fetch_release_dates_document,
) -> ScheduleRunSummary:
    """Scrape U Michigan release dates for whitelisted indicators."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
        year=year,
    )
    if unknown:
        logger.warning("U Michigan schedule fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    try:
        doc = document_fetcher(year=year, session=session)
        text, source_url = _coerce_schedule_document(doc)
        entries = parse_release_dates_text(
            text,
            source_url=source_url or UMICH_SURVEY_INFO_URL,
            series_ids=set(known),
        )
        summary.source_url = source_url
    except Exception as exc:
        logger.warning("U Michigan schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    if not entries:
        summary.series_empty.extend(known)
        summary.fetch_error = "no U Michigan schedule entries parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[UMichCalendarRawRecord] = []
    event_records: list[UMichCalendarEventRecord] = []
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


def fetch_umich_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    current_html_fetcher=fetch_current_results_html,
) -> FetchRunSummary:
    """Fetch the current U Michigan Consumer Sentiment value."""
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
        logger.warning("U Michigan value fetch: unknown series skipped: %s", unknown)
    if dry_run or not value_known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    try:
        html = current_html_fetcher(session=session)
        value = parse_current_results_html(html)
    except Exception as exc:
        logger.warning("U Michigan value fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[UMichCalendarRawRecord] = []
    event_records: list[UMichCalendarEventRecord] = []
    if value.series_id in value_known:
        spec = INDICATOR_REGISTRY[value.series_id]
        raw_rec, event_rec = current_value_to_records(
            value, snapshot_epoch_ms=snapshot, spec=spec,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    if not event_records:
        summary.series_empty.extend(value_known)
        summary.fetch_error = "no U Michigan current values parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    summary.release_stage = value.release_stage
    summary.series_ok.append(value.series_id)
    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
