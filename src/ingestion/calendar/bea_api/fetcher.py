"""Drive the BEA REST API through the calendar projection.

Given a year range (or explicit year comma-list) and a list of series
ids (or the default whitelist), ``fetch_bea_calendar`` pulls data via
the shared :class:`ingestion.timeseries.scrapers.bea.BEAClient`, turns
each observation into a ``(raw, event)`` tuple through
:func:`parser.parse_observation`, and persists via :func:`projector.store_raw`
+ :func:`project_events`.

Nothing auto-runs: callers construct a :class:`BEAClient`, pick their
year window, and invoke ``fetch_bea_calendar``. A dry-run path returns
the planned series × year chunks without issuing any HTTP request.

One request per ``(dataset, table)`` coordinate — the BEA API returns
every line for the table in a single call, so series sharing a table
(e.g. multiple lines of NIPA ``T20600``) piggyback on one fetch.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from ingestion.timeseries.scrapers.bea import BEAClient, BEAObservation

from .indicators import INDICATOR_REGISTRY, BEAIndicatorSpec
from .parser import (
    BEACalendarEventRecord,
    BEACalendarRawRecord,
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
    """Outcome of a single ``fetch_bea_calendar`` invocation."""

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
    """Split caller-supplied ids into known + unknown against the registry.

    When ``series_ids is None`` (default-full-registry path), entries
    flagged ``api_fetch=False`` are excluded — currently GDP, whose
    staged schedule (advance + second + third per quarter) has no
    clean anchor alignment with the bare-date API observation. Still
    callable with explicit ``series_ids=["BEA_NIPA_T10101_1"]``."""
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


def _year_param(start_year: int, end_year: int) -> str:
    """Render the BEA ``Year`` parameter as a comma-separated list.

    BEA's ``Year`` parameter accepts either ``"ALL"``, a single year,
    or an explicit comma-separated list. The list form keeps payload
    size predictable and matches what :meth:`BEAClient.get_nipa_table`
    does by default.
    """
    return ",".join(str(y) for y in range(start_year, end_year + 1))


def _group_by_table(
    series_ids: Iterable[str],
) -> dict[tuple[str, str, str], list[BEAIndicatorSpec]]:
    """Bucket specs by ``(dataset, table, frequency)``.

    All three must match for a single BEA ``GetData`` call to satisfy
    them — different frequencies require separate calls even on the
    same table.
    """
    grouped: dict[tuple[str, str, str], list[BEAIndicatorSpec]] = {}
    for sid in series_ids:
        spec = INDICATOR_REGISTRY[sid]
        key = (spec.dataset, spec.table, spec.frequency)
        grouped.setdefault(key, []).append(spec)
    return grouped


def fetch_bea_calendar(
    connection: sqlite3.Connection,
    client: BEAClient,
    *,
    start_year: int,
    end_year: int,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch BEA observations for ``series_ids`` and project to calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    client:
        An authenticated :class:`BEAClient`. Tests inject a fake with
        a compatible ``get_data`` signature.
    start_year, end_year:
        Inclusive year window. Translated to the BEA ``Year`` query
        parameter as a comma-separated list.
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
            "BEA calendar fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    year_param = _year_param(start_year, end_year)

    raw_records: list[BEACalendarRawRecord] = []
    event_records: list[BEACalendarEventRecord] = []

    for (dataset, table, frequency), specs in _group_by_table(known).items():
        # One HTTP call returns every line on the table; filter to the
        # whitelisted lines after the response lands.
        observations = client.get_data(
            dataset,
            TableName=table,
            Frequency=frequency,
            Year=year_param,
        )
        wanted_lines = {spec.line_number: spec for spec in specs}
        line_hits: dict[str, int] = {spec.line_number: 0 for spec in specs}

        for obs in observations:
            spec = wanted_lines.get(obs.line_number)
            if spec is None:
                continue
            line_hits[spec.line_number] += 1
            raw_rec, event_rec = parse_observation(
                obs, snapshot_epoch_ms=snapshot, spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)

        for spec in specs:
            if line_hits.get(spec.line_number, 0) == 0:
                summary.series_empty.append(spec.series_id)
            else:
                summary.series_ok.append(spec.series_id)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ScheduleRunSummary:
    """Outcome of a single :func:`schedule_bea_calendar` invocation."""

    dry_run: bool = True
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    # Row-level parse issues surfaced after a whitelisted title was
    # matched but its reference or release-date cell couldn't be
    # parsed. Populated by :func:`parse_schedule_html`. Empty in the
    # happy path — a non-empty list means BEA's schedule-page shape
    # drifted for at least one release we actively track.
    row_issues: list[str] = field(default_factory=list)
    wall_seconds: float = 0.0
    fetch_error: str | None = None


def schedule_bea_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    session: "requests.Session | None" = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape BEA's release calendar and project to ``cal_econ_event``.

    Schedule rows land with ``actual=NULL`` and
    ``event_time_precision='datetime'``. When the matching API-side
    values arrive via :func:`fetch_bea_calendar`, the shared
    ``provider_event_id`` makes the projector merge the two sides on
    the same row for unstaged indicators (Personal Income). Staged
    GDP rows land schedule-only — their ``release_stage`` folds into
    the id, so API-side bare-date observations don't merge in.

    ``html_fetcher`` is the seam tests inject to feed fixture HTML
    without hitting bea.gov.
    """
    started = time.monotonic()
    summary = ScheduleRunSummary(dry_run=dry_run)
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    try:
        html = html_fetcher(session=session)
        entries = parse_schedule_html(html, row_issues=summary.row_issues)
    except Exception as exc:
        logger.warning("BEA schedule fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[BEACalendarRawRecord] = []
    # Partition by spec.api_fetch:
    # - True (Personal Income): project_schedule_events preserves the
    #   API-side value + observed_at guard so a later API write isn't
    #   clobbered.
    # - False (GDP, schedule-only under P2a): schedule is the sole
    #   writer, so project_events is the right upsert — otherwise
    #   later rescrapes wouldn't update content_hash /
    #   observed_at_epoch_ms and the row's audit trail goes stale.
    schedule_merge_records: list[BEACalendarEventRecord] = []
    schedule_only_records: list[BEACalendarEventRecord] = []
    hits_by_series: dict[str, int] = {}
    for entry in entries:
        spec = INDICATOR_REGISTRY[entry.series_id]
        raw_rec, event_rec = schedule_entry_to_records(
            entry, snapshot_epoch_ms=snapshot, spec=spec,
        )
        raw_records.append(raw_rec)
        if spec.api_fetch:
            schedule_merge_records.append(event_rec)
        else:
            schedule_only_records.append(event_rec)
        hits_by_series[entry.series_id] = hits_by_series.get(entry.series_id, 0) + 1

    for series_id in INDICATOR_REGISTRY:
        (summary.series_ok if hits_by_series.get(series_id, 0) > 0
         else summary.series_empty).append(series_id)

    summary.entries_parsed = (
        len(schedule_merge_records) + len(schedule_only_records)
    )
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = (
        project_schedule_events(connection, schedule_merge_records)
        + project_events(connection, schedule_only_records)
    )
    summary.wall_seconds = time.monotonic() - started
    return summary
