"""Drive the ECB Data Portal SDMX API through the calendar projection.

Given a date window and a list of series ids (or the default
whitelist), ``fetch_ecb_calendar`` pulls observations via the shared
:class:`ingestion.timeseries.sdmx.providers.ecb.ECBClient`, turns each
observation into a ``(raw, event)`` tuple through
:func:`parser.parse_observation`, and persists via
:func:`projector.store_raw` + :func:`projector.project_events`.

Nothing auto-runs: callers construct an :class:`ECBClient`, pick their
date range, and invoke ``fetch_ecb_calendar``. A dry-run path returns
the planned series list without issuing any HTTP request.

One request per series — ECB Data Portal returns the full history (or
window) for a specific SDMX key in a single call. Unlike BEA, there's
no table-wide fan-out to dedupe at the client.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from ingestion.timeseries.sdmx._types import SDMXObservation
from ingestion.timeseries.sdmx.providers.ecb import ECBClient

from .indicators import INDICATOR_REGISTRY
from .parser import (
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, store_raw
from .schedule import (
    fetch_bulletin_html,
    fetch_gc_meetings_html,
    parse_bulletin_html,
    parse_gc_meetings_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_ecb_calendar`` invocation."""

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


def _collapse_to_rate_changes(
    observations: list[SDMXObservation],
    *,
    prior_value: float | None,
) -> list[SDMXObservation]:
    """Keep only observations whose value differs from the preceding level.

    The ECB FM dataflow publishes policy-rate levels at business-daily
    (``B``) frequency — the same level repeats every business day until
    the Governing Council moves it. A flat six-week period between two
    policy meetings produces ~30 identical observations. Projecting
    each one into ``cal_econ_event`` would pollute the calendar with
    spurious "rate unchanged" entries.

    Parameters
    ----------
    observations:
        Raw observations from a single series.
    prior_value:
        The most recent rate level from before the first observation
        in ``observations``. Sources:

        - On an incremental fetch, the caller looks this up from
          ``cal_econ_event`` (the most recent row already projected
          for this series with ``reference_date`` strictly before the
          window's start). This is the correct classification for
          every boundary case Codex flagged — the first in-window
          observation is projected iff its value genuinely differs
          from the prior level.
        - On a first-ever fetch, the DB has no prior state; pass
          ``None`` so the first observation is projected as the
          historical baseline (ECB's inception rate).
    """
    if not observations:
        return []
    ordered = sorted(observations, key=lambda o: o.date)
    kept: list[SDMXObservation] = []
    previous_value = prior_value
    for obs in ordered:
        if previous_value is None or obs.value != previous_value:
            kept.append(obs)
            previous_value = obs.value
    return kept


def _latest_stored_rate(
    connection: sqlite3.Connection,
    *,
    source_url: str,
    before_date: str | None,
) -> float | None:
    """Return the most recent ``cal_econ_event.actual`` for this series.

    Each spec in :data:`INDICATOR_REGISTRY` resolves to a single
    ``source_url`` (series-unique, stable across snapshots), so a
    WHERE match on ``(provider='ecb', source_url=?)`` picks out
    every row for that series. Order by ``reference_date DESC`` with
    an optional ``< before_date`` bound and take the top row — the
    rate level immediately preceding the fetch window.

    Returns ``None`` when no prior event exists — the caller
    interprets this as "first-ever fetch, treat the first returned
    observation as historical baseline". Callers derive
    ``before_date`` from the earliest returned observation (not from
    ``start_period``) so bounded fetches, ``limit``-only refreshes,
    and unbounded backfills all classify the leading observation the
    same way.
    """
    if not source_url or not before_date:
        return None
    row = connection.execute(
        """
        SELECT actual FROM cal_econ_event
        WHERE provider = 'ecb'
          AND source_url = ?
          AND reference_date < ?
        ORDER BY reference_date DESC LIMIT 1
        """,
        (source_url, before_date),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def fetch_ecb_calendar(
    connection: sqlite3.Connection,
    client: ECBClient,
    *,
    start_period: str | None = None,
    end_period: str | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    limit: int = 0,
) -> FetchRunSummary:
    """Fetch ECB observations for ``series_ids`` and project to calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    client:
        An authenticated :class:`ECBClient`. Tests inject a fake with
        a compatible ``get_data`` signature.
    start_period, end_period:
        Optional ISO date strings (``"YYYY-MM-DD"``) bounding the
        ECB observation range. ECB accepts partial precision too
        (``"2024"``, ``"2024-Q1"``) — passed through verbatim.
    series_ids:
        Iterable of full SDMX series ids (e.g.
        ``"FM.B.U2.EUR.4F.KR.DFR.LEV"``). ``None`` means the full
        :data:`INDICATOR_REGISTRY`. Unknown ids are collected in
        ``summary.series_unknown`` and skipped — never silently coerced.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; the returned summary shows the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor on every raw row. Defaults to "now UTC".
    limit:
        ECB ``lastNObservations`` cap. ``0`` (default) means no cap
        — the full series in the window is returned.
    """
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
            "ECB calendar fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    raw_records: list[ECBCalendarRawRecord] = []
    event_records: list[ECBCalendarEventRecord] = []

    for sid in known:
        spec = INDICATOR_REGISTRY[sid]
        observations: list[SDMXObservation] = client.get_data(
            spec.dataflow_id,
            spec.series_key,
            series_id=sid,
            start_period=start_period,
            end_period=end_period,
            limit=limit,
        )
        # ECB's SDMX client populates ``series_id`` from the caller's
        # ``series_id=`` kwarg; the resulting SDMXObservation therefore
        # matches the registry key one-to-one.
        if not observations:
            summary.series_empty.append(sid)
            continue
        summary.series_ok.append(sid)
        # Classify the first in-window observation using the latest
        # rate stored in the calendar's own state. Bounded fetches,
        # ``limit``-only refreshes, and full backfills all share one
        # path: an observation becomes a calendar event iff it differs
        # from the prior stored rate. Known scaffold limitation — on a
        # first-ever bounded fetch into a flat-rate regime (empty DB,
        # no prior rate to compare against) the earliest in-window obs
        # is projected as a historical baseline, producing one
        # synthetic row at the window boundary. P3a's live probe will
        # reveal whether real usage hits this case often enough to
        # justify an extra HTTP round-trip for a lookback.
        earliest_date = min(o.date for o in observations)
        prior_rate = _latest_stored_rate(
            connection,
            source_url=(
                "https://data-api.ecb.europa.eu/service/data/"
                f"{spec.dataflow_id}/{spec.series_key}"
            ),
            before_date=earliest_date,
        )
        rate_changes = _collapse_to_rate_changes(
            observations, prior_value=prior_rate,
        )
        for obs in rate_changes:
            raw_rec, event_rec = parse_observation(
                obs, snapshot_epoch_ms=snapshot, spec=spec,
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
    """Outcome of a single :func:`schedule_ecb_calendar` invocation."""

    dry_run: bool = True
    mp_decision_entries: int = 0
    bulletin_entries: int = 0
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    # Per-row failures after page identification — e.g. a GC meeting
    # row with a garbled time. Empty in happy path.
    row_issues: list[str] = field(default_factory=list)
    wall_seconds: float = 0.0
    fetch_errors: dict[str, str] = field(default_factory=dict)


def schedule_ecb_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    session: "requests.Session | None" = None,
    snapshot_epoch_ms: int | None = None,
    gc_meetings_fetcher=fetch_gc_meetings_html,
    bulletin_fetcher=fetch_bulletin_html,
) -> ScheduleRunSummary:
    """Scrape ECB's press-calendar pages and project to ``cal_econ_event``.

    Emits schedule-only events under distinct indicators
    (``ECB_MP_DECISION`` / ``ECB_BULLETIN``) — not merged with the
    SDMX rate-level rows, which anchor on a different date (effective
    vs decision). Schedule rows flow through
    :func:`project_events` (full writer) because the schedule is the
    sole writer for these indicators — same pattern as BEA P2a's GDP.

    Fetch failures on either page are collected in ``fetch_errors``
    and don't stop the run; the other page still lands.
    """
    started = time.monotonic()
    summary = ScheduleRunSummary(dry_run=dry_run)
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    page_jobs = (
        ("mp_decision", gc_meetings_fetcher, parse_gc_meetings_html),
        ("bulletin",    bulletin_fetcher,    parse_bulletin_html),
    )

    raw_records: list[ECBCalendarRawRecord] = []
    event_records: list[ECBCalendarEventRecord] = []
    for kind, fetcher, parser in page_jobs:
        try:
            html = fetcher(session=session)
            entries = parser(html, row_issues=summary.row_issues)
        except Exception as exc:
            logger.warning("ECB schedule fetch failed (%s): %s", kind, exc)
            summary.fetch_errors[kind] = str(exc)
            continue
        for entry in entries:
            raw_rec, event_rec = schedule_entry_to_records(
                entry, snapshot_epoch_ms=snapshot,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)
            if kind == "mp_decision":
                summary.mp_decision_entries += 1
            else:
                summary.bulletin_entries += 1

    summary.entries_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
