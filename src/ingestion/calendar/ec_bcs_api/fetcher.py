"""Drive EC BCS schedule and value ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, reference_label_en
from .parser import (
    PROVIDER,
    EcBcsCalendarEventRecord,
    EcBcsCalendarRawRecord,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    EcBcsScheduleDocument,
    EcBcsScheduleParseError,
    default_schedule_window,
    document_bytes_to_text,
    fetch_press_release_pdf,
    fetch_press_releases_listing_html,
    fetch_release_dates_document,
    parse_release_dates_text,
    resolve_press_release_link,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_ec_bcs_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    observations_seen: int = 0
    pending_releases: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of one ``schedule_ec_bcs_calendar`` invocation."""

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


def schedule_ec_bcs_calendar(
    connection: sqlite3.Connection,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    document_fetcher: Callable[[int], EcBcsScheduleDocument | None] | None = None,
) -> ScheduleRunSummary:
    """Fetch EC BCS release-date rows for whitelisted indicators.

    EC publishes one calendar PDF per year, so the fetcher iterates
    every calendar year touched by ``[start_date, end_date]``. A year
    whose PDF isn't yet linked from the survey landing page is logged
    to ``row_issues`` and skipped — late-December refreshes can hit a
    next-year window before the next-year PDF is published. The run
    fails outright only when every requested year fails.
    """
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
        logger.warning("EC BCS schedule fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    # The December full-survey ESI release publishes on the first
    # business day of the *next* year and lives only in the year-N
    # PDF, so a January-only window must also pull (year-1) to pick up
    # that crossover row. The lookback is bounded to one prior year —
    # earlier December rows would already be in the cron-frequented
    # rolling window.
    year_set = set(range(resolved_start.year, resolved_end.year + 1))
    if resolved_start.month == 1:
        year_set.add(resolved_start.year - 1)
    years = sorted(year_set)
    entries: list = []
    seen: set[tuple[str, str]] = set()
    fetch_errors: list[str] = []
    missing_years: list[int] = []
    documents_seen = 0
    for year in years:
        try:
            document = (
                document_fetcher(year)
                if document_fetcher is not None
                else fetch_release_dates_document(year=year, session=session)
            )
        except Exception as exc:
            fetch_errors.append(f"{year}: {type(exc).__name__}: {exc}")
            continue
        if document is None:
            missing_years.append(year)
            continue
        documents_seen += 1
        try:
            year_entries = parse_release_dates_text(
                document.text,
                series_ids=set(known),
                source_url=document.source_url,
                row_issues=summary.row_issues,
            )
        except EcBcsScheduleParseError as exc:
            fetch_errors.append(f"{year}: {exc}")
            continue
        for entry in year_entries:
            key = (entry.series_id, entry.release_date.isoformat())
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)

    # Decide what counts as a connector-level outage vs. a benign skip.
    # A future-year PDF that hasn't been published yet is benign so
    # long as at least one other year delivered rows. Treat as an
    # outage if no entries were obtained AND either every requested
    # year went missing (link-pattern drift on the survey landing
    # page) or every year that did fetch failed parsing.
    if not entries:
        if fetch_errors:
            summary.fetch_error = "; ".join(fetch_errors)
        elif missing_years:
            summary.fetch_error = (
                "EC BCS publication-dates PDF link not found for "
                f"years {missing_years}"
            )
        summary.wall_seconds = time.monotonic() - started
        return summary

    if fetch_errors:
        summary.row_issues.extend(fetch_errors)
    for year in missing_years:
        summary.row_issues.append(
            f"{year}: publication-dates PDF not yet linked"
        )

    entries = [
        entry for entry in entries
        if resolved_start <= entry.release_date <= resolved_end
    ]
    summary.entries_parsed = len(entries)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[EcBcsCalendarRawRecord] = []
    event_records: list[EcBcsCalendarEventRecord] = []
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


def _pending_rows(
    connection: sqlite3.Connection,
    series_ids: set[str],
    *,
    now_utc: datetime,
) -> list[sqlite3.Row]:
    titles = [INDICATOR_REGISTRY[sid].title for sid in series_ids]
    if not titles:
        return []
    placeholders = ",".join("?" for _ in titles)
    return connection.execute(
        f"""
        SELECT provider_event_id, reference_date, reference_label,
               event_time_utc, event_time_precision, source_url, title
        FROM cal_econ_event
        WHERE provider = ?
          AND actual IS NULL
          AND reference_date IS NOT NULL
          AND event_time_utc <= ?
          AND title IN ({placeholders})
        ORDER BY event_time_utc ASC
        """,
        (PROVIDER, now_utc.isoformat(), *titles),
    ).fetchall()


def _series_id_for_title(title: str) -> str | None:
    for sid, spec in INDICATOR_REGISTRY.items():
        if spec.title == title:
            return sid
    return None


def fetch_ec_bcs_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    listing_fetcher: Callable[[], str | bytes] | None = None,
    pdf_fetcher: Callable[[str], bytes] | None = None,
    now_utc: datetime | None = None,
) -> FetchRunSummary:
    """Fetch due EC BCS press releases and project value rows."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = FetchRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("EC BCS value fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    now = now_utc or datetime.now(timezone.utc)
    pending = _pending_rows(connection, set(known), now_utc=now)
    summary.pending_releases = len(pending)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[EcBcsCalendarRawRecord] = []
    event_records: list[EcBcsCalendarEventRecord] = []

    listing_payload: str | bytes | None = None

    for row in pending:
        sid = _series_id_for_title(row["title"])
        if sid is None or sid not in known:
            continue
        spec = INDICATOR_REGISTRY[sid]
        reference_date = str(row["reference_date"])
        reference = date.fromisoformat(reference_date)
        release_date = date.fromisoformat(str(row["event_time_utc"])[:10])
        reference_label = str(
            row["reference_label"] or reference_label_en(reference)
        )
        try:
            if listing_payload is None:
                listing_payload = (
                    listing_fetcher()
                    if listing_fetcher is not None
                    else fetch_press_releases_listing_html(session=session)
                )
            resolved = resolve_press_release_link(
                listing_payload,
                series_id=sid,
                release_date=release_date,
            )
            payload = (
                pdf_fetcher(resolved.source_url)
                if pdf_fetcher is not None
                else fetch_press_release_pdf(resolved.source_url, session=session)
            )
            text = (
                document_bytes_to_text(payload)
                if isinstance(payload, (bytes, bytearray))
                else str(payload)
            )
            obs = parse_press_release_value(
                text,
                spec=spec,
                reference_date=reference_date,
                reference_label=reference_label,
                event_time_utc=str(row["event_time_utc"]),
                event_time_precision=str(row["event_time_precision"] or "datetime"),
                source_url=resolved.source_url,
            )
            raw_rec, event_rec = parse_observation(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
        except Exception as exc:
            logger.warning("EC BCS value fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
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
