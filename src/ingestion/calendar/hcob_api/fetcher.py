"""Drive HCOB Germany PMI schedule ingestion (schedule-only for P5)."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import HCOB_RELEASE_DATES_URL, INDICATOR_REGISTRY
from .parser import (
    HCOBCalendarEventRecord,
    HCOBCalendarRawRecord,
)
from .projector import project_schedule_events, store_raw
from .schedule import (
    default_schedule_window,
    fetch_release_dates_html,
    parse_release_dates_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class ScheduleRunSummary:
    """Outcome of one ``schedule_hcob_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    start_date: str = ""
    end_date: str = ""
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
    """Split caller-supplied ids into known + unknown registry ids."""
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


def _coerce_date(raw: str | date | None) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, date):
        return raw
    return date.fromisoformat(str(raw)[:10])


def schedule_hcob_calendar(
    connection: sqlite3.Connection,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str | bytes] | None = None,
    today: date | None = None,
) -> ScheduleRunSummary:
    """Fetch HCOB Germany PMI release-date rows for whitelisted indicators."""
    started = time.monotonic()
    default_start, default_end = default_schedule_window()
    resolved_start = _coerce_date(start_date) or default_start
    resolved_end = _coerce_date(end_date) or default_end
    if resolved_end < resolved_start:
        resolved_start, resolved_end = resolved_end, resolved_start

    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        start_date=resolved_start.isoformat(),
        end_date=resolved_end.isoformat(),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("HCOB schedule fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    entries = []
    try:
        payload = (
            html_fetcher()
            if html_fetcher is not None
            else fetch_release_dates_html(session=session)
        )
        entries = parse_release_dates_html(
            payload,
            series_ids=set(known),
            source_url=HCOB_RELEASE_DATES_URL,
            today=today,
            row_issues=summary.row_issues,
        )
    except Exception as exc:
        logger.warning("HCOB release-date fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    entries = [
        entry for entry in entries
        if resolved_start <= entry.release_date <= resolved_end
    ]
    summary.entries_parsed = len(entries)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[HCOBCalendarRawRecord] = []
    event_records: list[HCOBCalendarEventRecord] = []
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
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
