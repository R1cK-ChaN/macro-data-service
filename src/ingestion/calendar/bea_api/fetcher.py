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

from ingestion.timeseries.scrapers.bea import BEAClient, BEAObservation

from .indicators import INDICATOR_REGISTRY, BEAIndicatorSpec
from .parser import (
    BEACalendarEventRecord,
    BEACalendarRawRecord,
    parse_observation,
)
from .projector import project_events, store_raw

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
