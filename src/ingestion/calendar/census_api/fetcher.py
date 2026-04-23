"""Drive Census EITS value and release-schedule ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from .client import CensusEITSClient, CensusEITSObservation
from .indicators import INDICATOR_REGISTRY, CensusIndicatorSpec
from .parser import (
    CensusCalendarEventRecord,
    CensusCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_census_calendar`` invocation."""

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
    requests_made: int = 0
    wall_seconds: float = 0.0


def _resolve_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split caller-supplied ids into known + unknown against registry."""
    if series_ids is None:
        return (
            [
                sid for sid, spec in INDICATOR_REGISTRY.items()
                if spec.api_fetch
            ],
            [],
        )
    known: list[str] = []
    unknown: list[str] = []
    for sid in series_ids:
        if sid in INDICATOR_REGISTRY:
            known.append(sid)
        else:
            unknown.append(sid)
    return known, unknown


def _group_by_dataset(series_ids: Iterable[str]) -> dict[str, list[CensusIndicatorSpec]]:
    grouped: dict[str, list[CensusIndicatorSpec]] = {}
    for sid in series_ids:
        spec = INDICATOR_REGISTRY[sid]
        grouped.setdefault(spec.dataset, []).append(spec)
    return grouped


def _row_to_observation(
    row: dict[str, str],
    *,
    spec: CensusIndicatorSpec,
) -> CensusEITSObservation:
    return CensusEITSObservation(
        series_id=spec.series_id,
        dataset=spec.dataset,
        time=row.get("time", ""),
        data_type_code=row.get("data_type_code", ""),
        category_code=row.get("category_code", ""),
        seasonally_adj=row.get("seasonally_adj", ""),
        time_slot_id=row.get("time_slot_id", ""),
        time_slot_name=row.get("time_slot_name", ""),
        cell_value=row.get("cell_value", ""),
        error_data=row.get("error_data", ""),
        raw=dict(row),
    )


def _match_key(spec: CensusIndicatorSpec) -> tuple[str, str, str, str]:
    return (
        spec.data_type_code,
        spec.seasonally_adj,
        spec.category_code,
        spec.time_slot_id,
    )


def _row_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        row.get("data_type_code", ""),
        row.get("seasonally_adj", ""),
        row.get("category_code", ""),
        row.get("time_slot_id", ""),
    )


def fetch_census_calendar(
    connection: sqlite3.Connection,
    client: CensusEITSClient,
    *,
    start_year: int,
    end_year: int,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch Census EITS observations and project to calendar rows."""
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
            "Census calendar fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    requests_before = getattr(client, "requests_made", 0)
    raw_records: list[CensusCalendarRawRecord] = []
    event_records: list[CensusCalendarEventRecord] = []
    hits: dict[str, int] = {sid: 0 for sid in known}

    for dataset, specs in _group_by_dataset(known).items():
        specs_by_key = {_match_key(spec): spec for spec in specs}
        for year in range(start_year, end_year + 1):
            rows = client.get_dataset_year(dataset, year)
            for row in rows:
                spec = specs_by_key.get(_row_key(row))
                if spec is None:
                    continue
                obs = _row_to_observation(row, spec=spec)
                raw_rec, event_rec = parse_observation(
                    obs, snapshot_epoch_ms=snapshot, spec=spec,
                )
                raw_records.append(raw_rec)
                event_records.append(event_rec)
                hits[spec.series_id] += 1

    requests_after = getattr(client, "requests_made", 0)
    summary.requests_made = max(0, requests_after - requests_before)
    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        else:
            summary.series_empty.append(sid)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ScheduleRunSummary:
    """Outcome of a single ``schedule_census_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    row_issues: list[str] = field(default_factory=list)
    wall_seconds: float = 0.0
    fetch_error: str | None = None


def schedule_census_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: "requests.Session | None" = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape the Census list-view release calendar."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning(
            "Census schedule fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
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
        logger.warning("Census schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary
    if not entries:
        summary.series_empty.extend(known)
        summary.fetch_error = "no Census schedule entries parsed"
        summary.wall_seconds = time.monotonic() - started
        return summary

    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[CensusCalendarRawRecord] = []
    event_records: list[CensusCalendarEventRecord] = []
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

    summary.entries_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
