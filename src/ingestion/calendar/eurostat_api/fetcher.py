"""Drive Eurostat value and release-schedule ingestion."""

from __future__ import annotations

import calendar
import logging
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Iterable

import requests

from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient

from .indicators import INDICATOR_REGISTRY
from .parser import (
    EurostatCalendarEventRecord,
    EurostatCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    default_schedule_window,
    fetch_release_calendar_json,
    parse_release_calendar_json,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)

_QUARTER_SLASH_RE = re.compile(r"^\s*Q([1-4])\s*/\s*(\d{4})\s*$", re.I)
_QUARTER_YEAR_RE = re.compile(r"^\s*(\d{4})[- ]?Q([1-4])\s*$", re.I)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_eurostat_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    start_period: str = ""
    end_period: str = ""
    dry_run: bool = True
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of a single ``schedule_eurostat_calendar`` invocation."""

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


def _quarter_parts(text: str) -> tuple[int, int] | None:
    match = _QUARTER_SLASH_RE.match(text)
    if match:
        quarter, year = match.groups()
        return int(year), int(quarter)
    match = _QUARTER_YEAR_RE.match(text)
    if match:
        year, quarter = match.groups()
        return int(year), int(quarter)
    return None


def _period_bound_start(raw: str | None) -> date | None:
    if not raw:
        return None
    text = str(raw).strip()
    quarter = _quarter_parts(text)
    if quarter is not None:
        year, quarter_num = quarter
        return date(year, (quarter_num - 1) * 3 + 1, 1)
    if len(text) == 4 and text.isdigit():
        return date(int(text), 1, 1)
    if len(text) == 7 and text[4] == "-":
        return date.fromisoformat(f"{text}-01")
    return date.fromisoformat(text[:10])


def _period_bound_end(raw: str | None) -> date | None:
    if not raw:
        return None
    text = str(raw).strip()
    quarter = _quarter_parts(text)
    if quarter is not None:
        year, quarter_num = quarter
        month = (quarter_num - 1) * 3 + 3
        return date(year, month, calendar.monthrange(year, month)[1])
    if len(text) == 4 and text.isdigit():
        return date(int(text), 12, 31)
    if len(text) == 7 and text[4] == "-":
        start = date.fromisoformat(f"{text}-01")
        if start.month == 12:
            return date(start.year, 12, 31)
        next_month = date(start.year, start.month + 1, 1)
        return date.fromordinal(next_month.toordinal() - 1)
    return date.fromisoformat(text[:10])


def _in_window(obs_date: str, start: date | None, end: date | None) -> bool:
    current = date.fromisoformat(obs_date[:10])
    if start is not None and current < start:
        return False
    if end is not None and current > end:
        return False
    return True


def fetch_eurostat_calendar(
    connection: sqlite3.Connection,
    client: EurostatClient,
    *,
    start_period: str | None = None,
    end_period: str | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    limit: int = 0,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch Eurostat JSON-stat observations and project calendar rows."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = FetchRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        start_period=start_period or "",
        end_period=end_period or "",
        dry_run=dry_run,
    )
    if unknown:
        logger.warning(
            "Eurostat calendar fetch: %d unknown series skipped: %s",
            len(unknown),
            unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    start_bound = _period_bound_start(start_period)
    end_bound = _period_bound_end(end_period)
    client_limit = limit if limit > 0 else 10_000
    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[EurostatCalendarRawRecord] = []
    event_records: list[EurostatCalendarEventRecord] = []
    hits: dict[str, int] = {sid: 0 for sid in known}

    for sid in known:
        spec = INDICATOR_REGISTRY[sid]
        try:
            observations = client.get_dataset(
                spec.dataset,
                params=dict(spec.params),
                series_id=spec.series_id,
                limit=client_limit,
            )
        except Exception as exc:
            logger.warning("Eurostat value fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        for obs in observations:
            if not _in_window(obs.date, start_bound, end_bound):
                continue
            raw_rec, event_rec = parse_observation(
                obs, snapshot_epoch_ms=snapshot, spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)
            hits[sid] += 1

    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        elif all(failed_sid != sid for failed_sid, _ in summary.series_failed):
            summary.series_empty.append(sid)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


def _coerce_date(raw: str | date | None) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, date):
        return raw
    return date.fromisoformat(str(raw)[:10])


def schedule_eurostat_calendar(
    connection: sqlite3.Connection,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    json_fetcher=fetch_release_calendar_json,
) -> ScheduleRunSummary:
    """Fetch Eurostat release-calendar rows for whitelisted indicators."""
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
        logger.warning(
            "Eurostat schedule fetch: %d unknown series skipped: %s",
            len(unknown),
            unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    try:
        payload = json_fetcher(
            resolved_start,
            resolved_end,
            session=session,
        )
        entries = parse_release_calendar_json(
            payload,
            series_ids=set(known),
            row_issues=summary.row_issues,
        )
    except Exception as exc:
        logger.warning("Eurostat schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    if not entries:
        summary.series_empty.extend(known)
        summary.fetch_error = "no Eurostat schedule entries parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[EurostatCalendarRawRecord] = []
    event_records: list[EurostatCalendarEventRecord] = []
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
