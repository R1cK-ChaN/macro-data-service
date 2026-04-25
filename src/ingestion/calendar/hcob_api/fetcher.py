"""Drive HCOB Germany PMI schedule + value ingestion."""

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
    parse_observation,
    parse_press_release_pdf,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    default_schedule_window,
    fetch_press_release_pdf_text,
    fetch_press_releases_listing_html,
    fetch_release_dates_html,
    parse_release_dates_html,
    resolve_press_release_link,
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


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_hcob_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    pending_releases: int = 0
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
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
        ("hcob", now_utc.isoformat(), *titles),
    ).fetchall()


def _series_id_for_title(title: str) -> str | None:
    for sid, spec in INDICATOR_REGISTRY.items():
        if spec.title == title:
            return sid
    return None


def fetch_hcob_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    listing_fetcher: Callable[[], str | bytes] | None = None,
    pdf_text_fetcher: Callable[[str], str] | None = None,
    now_utc: datetime | None = None,
) -> FetchRunSummary:
    """Fetch due HCOB / S&P Global press-release values and project them.

    Auto-discovers ``actual IS NULL`` rows already staged by
    :func:`schedule_hcob_calendar`, resolves each release's PDF URL by
    matching ``(release_date, expected_listing_title)`` on the public
    press-release listing, downloads the PDF, extracts headline indices,
    and upserts via the shared ``provider_event_id``.

    The flash trio's three series resolve to the *same* PDF — the
    listing fetch happens once per sweep, and the PDF text is cached
    per URL so the value parser only re-runs the per-series regex.
    """
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = FetchRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("HCOB value fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    now = now_utc or datetime.now(timezone.utc)
    pending = _pending_rows(connection, set(known), now_utc=now)
    summary.pending_releases = len(pending)

    if not pending:
        for sid in known:
            summary.series_empty.append(sid)
        summary.wall_seconds = time.monotonic() - started
        return summary

    listing_html: str | bytes | None = None
    pdf_text_cache: dict[str, str] = {}
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[HCOBCalendarRawRecord] = []
    event_records: list[HCOBCalendarEventRecord] = []

    for row in pending:
        sid = _series_id_for_title(row["title"])
        if sid is None or sid not in known:
            continue
        spec = INDICATOR_REGISTRY[sid]
        reference_date = str(row["reference_date"])
        release_date = date.fromisoformat(str(row["event_time_utc"])[:10])
        reference_label = str(row["reference_label"] or "")

        try:
            if listing_html is None:
                listing_html = (
                    listing_fetcher()
                    if listing_fetcher is not None
                    else fetch_press_releases_listing_html(session=session)
                )
            resolved = resolve_press_release_link(
                listing_html,
                release_date=release_date,
                expected_listing_match=spec.press_listing_match,
            )
            if resolved.source_url not in pdf_text_cache:
                pdf_text_cache[resolved.source_url] = (
                    pdf_text_fetcher(resolved.source_url)
                    if pdf_text_fetcher is not None
                    else fetch_press_release_pdf_text(
                        resolved.source_url, session=session,
                    )
                )
            obs = parse_press_release_pdf(
                pdf_text_cache[resolved.source_url],
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
            logger.warning("HCOB value fetch failed for %s: %s", sid, exc)
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
