"""Drive the BLS Public Data API through the calendar projection.

Given a year range and a list of series ids (or the default whitelist),
``fetch_bls_calendar`` pulls observations via the shared
:class:`ingestion.timeseries.scrapers.bls.BLSClient`, turns each one
into a ``(raw, event)`` tuple through :func:`parser.parse_observation`,
and persists via :func:`projector.store_raw` + :func:`project_events`.

Nothing auto-runs: callers construct a :class:`BLSClient`, pick their
year window, and invoke ``fetch_bls_calendar``. A dry-run path returns
the planned series × year chunks without issuing any HTTP request.
"""

from __future__ import annotations

import dataclasses
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

import requests

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)
from ingestion.timeseries.scrapers.bls import BLSClient

from .indicators import BLSIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    BLSCalendarEventRecord,
    BLSCalendarRawRecord,
    parse_observation,
)
from .projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from .schedule import (
    SCHEDULE_URL_SLUG,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)

# Release-stage qualifiers that may ride on a BLS schedule row's
# ``provider_event_id`` (see ``schedule._QUARTER_QUALIFIER_RE``). When
# the fetcher rebases a staged observation it enumerates these to
# build candidate ids without a LIKE match on the hash bytes.
_STAGED_ANCHORS: tuple[str, ...] = (
    "preliminary", "prelim", "revised", "advance", "final",
)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_bls_calendar`` invocation."""

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
    # Count of staged-schedule observations skipped because no
    # eligible schedule row existed at snapshot time. Writing them
    # under a bare-date anchor would leave an orphan row that a
    # later schedule scrape can't merge with — the scrape would
    # create stage-qualified rows under different ids.
    staged_skipped: int = 0
    # BLS API calls this run consumed (delta of ``client.daily_query_count``);
    # feeds the scheduler's per-day budget tracker.
    requests_made: int = 0
    wall_seconds: float = 0.0


def _resolve_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split caller-supplied ids into known + unknown against the registry.

    When ``series_ids is None`` (default-full-registry path), entries
    flagged ``api_fetch=False`` are excluded — currently no registry
    entry, but the switch stays available as a quarantine lever if a
    future indicator needs it. Still callable with explicit
    ``series_ids=[...]``."""
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


def _resolve_staged_event_id(
    connection: sqlite3.Connection,
    *,
    spec: BLSIndicatorSpec,
    reference_date: str,
    snapshot_epoch_ms: int,
) -> tuple[str, str] | None:
    """Find the schedule-row anchor a staged API observation should merge onto.

    For indicators with ``staged_schedule=True`` (Productivity today)
    the BLS Public Data API returns one bare-date observation per
    ``(year, period)`` that represents whichever stage is currently
    published — preliminary up to the revised release date, revised
    thereafter. The schedule-side scrape has already written one row
    per stage under a ``|<stage>``-qualified id with a label like
    ``"3rd Quarter 2025 (Preliminary)"``. We pick the schedule row
    whose ``event_time_utc`` is the latest among those at or before
    ``snapshot_epoch_ms`` so the API value lands on the stage it
    actually represents.

    Returns ``(provider_event_id, reference_label)`` from that
    schedule row, or ``None`` when no eligible row exists (cold
    start, or the observation is for a future quarter whose releases
    haven't happened yet). Callers skip the observation rather than
    writing a bare-date row that a later schedule scrape would
    duplicate under a stage-qualified id.
    """
    indicator_canonical = canonicalize_indicator(spec.indicator)
    candidate_ids = [
        synthesize_event_id(
            PROVIDER,
            spec.country_code,
            indicator_canonical,
            f"{reference_date}|{stage}",
        )
        for stage in _STAGED_ANCHORS
    ]
    placeholders = ",".join(["?"] * len(candidate_ids))
    cursor = connection.execute(
        f"""
        SELECT provider_event_id, event_time_utc, reference_label
        FROM cal_econ_event
        WHERE provider = ?
          AND event_time_precision = 'datetime'
          AND provider_event_id IN ({placeholders})
        """,
        (PROVIDER, *candidate_ids),
    )
    best_id: str | None = None
    best_label: str = ""
    best_ms: int = -1
    for pid, event_time_utc, reference_label in cursor.fetchall():
        try:
            released_ms = int(
                datetime.fromisoformat(event_time_utc).timestamp() * 1000
            )
        except (TypeError, ValueError):
            # A non-ISO ``event_time_utc`` shouldn't happen — schedule
            # rows always write aware UTC. Skip the row rather than
            # failing the whole fetch if an upstream DOM change ever
            # drifts the column.
            continue
        if released_ms <= snapshot_epoch_ms and released_ms > best_ms:
            best_ms = released_ms
            best_id = pid
            best_label = reference_label or ""
    if best_id is None:
        return None
    return best_id, best_label


def _rebase_records_onto(
    raw: BLSCalendarRawRecord,
    event: BLSCalendarEventRecord,
    new_event_id: str,
    new_reference_label: str,
) -> tuple[BLSCalendarRawRecord, BLSCalendarEventRecord]:
    """Return (raw, event) with ``provider_event_id`` + ``reference_label``
    replaced by the schedule row's values.

    Carrying the schedule row's label through keeps the stage marker
    (``"(Preliminary)"`` / ``"(Revised)"``) visible after the API
    merge — the projector's upsert rule always writes
    ``excluded.reference_label``, so without this the schedule-side
    label is clobbered by the parser's bare ``"Q03"`` string and the
    two staged Productivity events collapse to identical labels in
    the event table.
    """
    return (
        dataclasses.replace(raw, provider_event_id=new_event_id),
        dataclasses.replace(
            event,
            provider_event_id=new_event_id,
            reference_label=new_reference_label,
        ),
    )


def fetch_bls_calendar(
    connection: sqlite3.Connection,
    client: BLSClient,
    *,
    start_year: int,
    end_year: int,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
) -> FetchRunSummary:
    """Fetch BLS observations for ``series_ids`` and project to calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    client:
        An authenticated :class:`BLSClient`. Tests inject a fake with
        a compatible ``get_series`` signature.
    start_year, end_year:
        Inclusive year window. Passed through to ``BLSClient.get_series``.
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
            "BLS calendar fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    requests_before = getattr(client, "daily_query_count", 0)
    series_to_obs = client.get_series(
        known, start_year=start_year, end_year=end_year,
    )
    requests_after = getattr(client, "daily_query_count", 0)
    summary.requests_made = max(0, requests_after - requests_before)

    raw_records: list[BLSCalendarRawRecord] = []
    event_records: list[BLSCalendarEventRecord] = []
    for sid in known:
        observations = series_to_obs.get(sid, []) or []
        if not observations:
            summary.series_empty.append(sid)
            continue
        summary.series_ok.append(sid)
        spec = INDICATOR_REGISTRY[sid]
        for obs in observations:
            raw_rec, event_rec = parse_observation(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
            if spec.staged_schedule:
                resolved = _resolve_staged_event_id(
                    connection,
                    spec=spec,
                    reference_date=obs.date,
                    snapshot_epoch_ms=snapshot,
                )
                if resolved is None:
                    # Skip rather than fall back to the bare-date
                    # anchor — a later schedule scrape would land a
                    # stage-qualified row under a different id,
                    # orphaning the bare-date row we'd have written.
                    logger.warning(
                        "BLS staged observation for %s ref=%s has no "
                        "eligible schedule row; skipping",
                        sid, obs.date,
                    )
                    summary.staged_skipped += 1
                    continue
                new_event_id, new_reference_label = resolved
                raw_rec, event_rec = _rebase_records_onto(
                    raw_rec, event_rec,
                    new_event_id, new_reference_label,
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
    """Outcome of a single :func:`schedule_bls_calendar` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def _resolve_schedule_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split into series that have a schedule URL registered + unknown."""
    if series_ids is None:
        return list(SCHEDULE_URL_SLUG.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for sid in series_ids:
        if sid in SCHEDULE_URL_SLUG:
            known.append(sid)
        else:
            unknown.append(sid)
    return known, unknown


def schedule_bls_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: "requests.Session | None" = None,
    snapshot_epoch_ms: int | None = None,
    html_fetcher=fetch_schedule_html,
) -> ScheduleRunSummary:
    """Scrape BLS release schedules and project to ``cal_econ_event``.

    Schedule rows land with ``actual=NULL`` and
    ``event_time_precision='datetime'``. When the matching API-side
    values arrive (via :func:`fetch_bls_calendar`), the shared
    ``provider_event_id`` makes the projector merge the two sides on
    the same row: the schedule side keeps owning the scheduled
    datetime; the API side owns ``actual``.

    ``html_fetcher`` is the seam tests inject to feed fixture HTML.
    """
    started = time.monotonic()
    known, unknown = _resolve_schedule_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning(
            "BLS schedule fetch: %d unknown series skipped: %s",
            len(unknown), unknown,
        )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    raw_records: list[BLSCalendarRawRecord] = []
    event_records: list[BLSCalendarEventRecord] = []
    # Per-run slug → HTML cache. Four CES/CPS series share empsit.htm
    # under P1c; without the cache each run re-downloads the same page
    # four times. Politeness to bls.gov and a small bandwidth win.
    slug_html_cache: dict[str, str] = {}
    for sid in known:
        slug = SCHEDULE_URL_SLUG[sid]
        try:
            html = slug_html_cache.get(slug)
            if html is None:
                html = html_fetcher(sid, session=session)
                slug_html_cache[slug] = html
            entries = parse_schedule_html(html, series_id=sid)
        except Exception as exc:
            logger.warning("BLS schedule fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        if not entries:
            summary.series_failed.append((sid, "no entries parsed"))
            continue
        summary.series_ok.append(sid)
        spec = INDICATOR_REGISTRY[sid]
        for entry in entries:
            raw_rec, event_rec = schedule_entry_to_records(
                entry, snapshot_epoch_ms=snapshot, spec=spec,
            )
            raw_records.append(raw_rec)
            event_records.append(event_rec)

    summary.entries_parsed = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    # ``project_schedule_events`` preserves ``content_hash`` and
    # ``observed_at_epoch_ms`` on conflict so an API-merged row isn't
    # clobbered by a later schedule re-scrape. Every BLS indicator
    # now has an API side (Productivity routes through the staged
    # rebase in :func:`fetch_bls_calendar`), so no schedule-only
    # branch remains.
    summary.events_upserted = project_schedule_events(
        connection, event_records,
    )
    summary.wall_seconds = time.monotonic() - started
    return summary
