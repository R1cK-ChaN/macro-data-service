"""Drive Destatis value and release-table ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Callable, Iterable

import requests

from .client import DestatisGenesisClient
from .indicators import INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    DestatisCalendarEventRecord,
    DestatisCalendarRawRecord,
    parse_genesis_csv_table,
    parse_observation,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    default_schedule_window,
    fetch_release_table_html,
    parse_release_table_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_destatis_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    start_year: int | None = None
    end_year: int | None = None
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
    """Outcome of one ``schedule_destatis_calendar`` invocation."""

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


def _apply_stored_schedule_times(
    connection: sqlite3.Connection,
    records: list[DestatisCalendarEventRecord],
) -> list[DestatisCalendarEventRecord]:
    ids = [r.provider_event_id for r in records]
    if not ids:
        return records
    placeholders = ",".join("?" for _ in ids)
    rows = connection.execute(
        f"""
        SELECT provider_event_id, event_time_utc, event_time_precision
        FROM cal_econ_event
        WHERE provider = ?
          AND provider_event_id IN ({placeholders})
        """,
        (PROVIDER, *ids),
    ).fetchall()
    by_id = {
        row["provider_event_id"] if hasattr(row, "keys") else row[0]: (
            row["event_time_utc"] if hasattr(row, "keys") else row[1],
            row["event_time_precision"] if hasattr(row, "keys") else row[2],
        )
        for row in rows
    }
    out: list[DestatisCalendarEventRecord] = []
    for record in records:
        stored = by_id.get(record.provider_event_id)
        if stored is None:
            out.append(record)
            continue
        event_time_utc, precision = stored
        out.append(replace(
            record,
            event_time_utc=event_time_utc,
            event_time_precision=precision,
        ))
    return out


def fetch_destatis_calendar(
    connection: sqlite3.Connection,
    client: DestatisGenesisClient,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch GENESIS observations and project calendar rows."""
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
            "Destatis calendar fetch: %d unknown series skipped: %s",
            len(unknown),
            unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[DestatisCalendarRawRecord] = []
    event_records: list[DestatisCalendarEventRecord] = []
    hits: dict[str, int] = {sid: 0 for sid in known}

    for sid in known:
        spec = INDICATOR_REGISTRY[sid]
        try:
            payload = client.tablefile(
                spec.table_name,
                start_year=start_year,
                end_year=end_year,
                extra_params=spec.table_params,
            )
            observations = parse_genesis_csv_table(payload, spec=spec)
        except Exception as exc:
            logger.warning("Destatis value fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        for obs in observations:
            raw_rec, event_rec = parse_observation(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)
            hits[sid] += 1

    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        elif all(failed_sid != sid for failed_sid, _ in summary.series_failed):
            summary.series_empty.append(sid)

    event_records = _apply_stored_schedule_times(connection, event_records)
    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


def schedule_destatis_calendar(
    connection: sqlite3.Connection,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[], str] | None = None,
) -> ScheduleRunSummary:
    """Fetch Destatis release-table rows for whitelisted indicators."""
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
            "Destatis schedule fetch: %d unknown series skipped: %s",
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
        payload = (
            html_fetcher()
            if html_fetcher is not None
            else fetch_release_table_html(session=session)
        )
        entries = parse_release_table_html(
            payload,
            series_ids=set(known),
            row_issues=summary.row_issues,
        )
    except Exception as exc:
        logger.warning("Destatis release-table fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    entries = [
        entry for entry in entries
        if resolved_start <= entry.release_date <= resolved_end
    ]
    summary.entries_parsed = len(entries)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[DestatisCalendarRawRecord] = []
    event_records: list[DestatisCalendarEventRecord] = []
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
