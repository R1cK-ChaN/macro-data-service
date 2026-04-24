"""Drive CAO GDP schedule and value scrapes through projection."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY, FIRST_PRELIMINARY, SECOND_PRELIMINARY
from .parser import build_report_url
from .projector import project_events, project_schedule_events, store_raw
from .reports import (
    CaoGdpReportParseError,
    fetch_gdp_csv_text,
    fetch_gdp_report_menu_html,
    gdp_value_to_records,
    parse_gdp_growth_csv,
    parse_gdp_report_menu_html,
)
from .scraper import (
    CaoGdpArchiveParseError,
    archive_year_url,
    fetch_gdp_archive_index_html,
    fetch_gdp_archive_year_html,
    parse_gdp_archive_html,
    parse_gdp_archive_index_html,
    schedule_entry_to_records,
    select_archive_years,
)
from .parser import CaoGdpCalendarEventRecord, CaoGdpCalendarRawRecord

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


def _normalize_requested_archive_years(years: list[int]) -> list[int]:
    """Return explicit archive years once each, newest first."""
    return sorted(set(years), reverse=True)


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_cao_gdp_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    archive_years_planned: list[int] = field(default_factory=list)
    archive_pages_fetched: int = 0
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


def fetch_cao_gdp_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    archive_years: list[int] | None = None,
    index_html_fetcher: Callable[[], str] | None = None,
    archive_html_fetcher: Callable[[int], str] | None = None,
) -> FetchRunSummary:
    """Scrape ESRI GDP archive pages and project schedule rows."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )
    if archive_years is not None:
        summary.archive_years_planned = _normalize_requested_archive_years(
            archive_years
        )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    if archive_years is None:
        index_fetcher = index_html_fetcher or fetch_gdp_archive_index_html
        index_html = index_fetcher()
        discovered_years = parse_gdp_archive_index_html(index_html)
        planned_years = select_archive_years(discovered_years)
    else:
        planned_years = _normalize_requested_archive_years(archive_years)
    if not planned_years:
        raise CaoGdpArchiveParseError("CAO GDP archive index exposed zero years")
    summary.archive_years_planned = planned_years

    html_fetcher = archive_html_fetcher or fetch_gdp_archive_year_html
    raw_records: list[CaoGdpCalendarRawRecord] = []
    event_records: list[CaoGdpCalendarEventRecord] = []
    for year in planned_years:
        html = html_fetcher(year)
        summary.archive_pages_fetched += 1
        entries = parse_gdp_archive_html(
            html,
            base_url=archive_year_url(year),
        )
        if not entries:
            raise CaoGdpArchiveParseError(
                f"CAO GDP archive year {year} returned zero releases"
            )
        summary.releases_parsed += len(entries)
        for entry in entries:
            raw, event = schedule_entry_to_records(
                entry,
                snapshot_epoch_ms=snapshot,
            )
            raw_records.append(raw)
            event_records.append(event)

    if summary.releases_parsed == 0:
        raise CaoGdpArchiveParseError(
            "CAO GDP archive scrape returned zero releases"
        )
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class GdpValuesRunSummary:
    """Outcome of one ``fetch_cao_gdp_values`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_planned: int = 0
    releases_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


@dataclass(frozen=True)
class _PendingGdpRelease:
    """One GDP release pending a value-side fill."""

    provider_event_id: str
    reference_date: date
    release_stage: str
    event_time_utc: str
    report_url: str


def _stage_from_title(title: str) -> str | None:
    if title == "GDP Growth Rate QoQ Prel":
        return FIRST_PRELIMINARY
    if title == "GDP Growth Rate QoQ Final":
        return SECOND_PRELIMINARY
    return None


def _discover_pending_releases(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[_PendingGdpRelease]:
    """Find past GDP schedule rows with ``actual IS NULL``."""
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    rows = connection.execute(
        """
        SELECT provider_event_id, reference_date, title, event_time_utc, source_url
        FROM cal_econ_event
        WHERE provider = 'cao'
          AND title IN ('GDP Growth Rate QoQ Prel', 'GDP Growth Rate QoQ Final')
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY event_time_utc DESC
        """,
        (threshold_iso,),
    ).fetchall()
    out: list[_PendingGdpRelease] = []
    for provider_event_id, ref_iso, title, event_time_utc, source_url in rows:
        if not ref_iso or not event_time_utc:
            continue
        try:
            reference = date.fromisoformat(ref_iso)
        except ValueError:
            continue
        stage = _stage_from_title(title or "")
        if stage is None:
            continue
        out.append(
            _PendingGdpRelease(
                provider_event_id=provider_event_id,
                reference_date=reference,
                release_stage=stage,
                event_time_utc=event_time_utc,
                report_url=source_url or build_report_url(reference, stage),
            )
        )
    return out


def _lookup_stored_pending(
    connection: sqlite3.Connection,
    references: list[date],
) -> list[_PendingGdpRelease]:
    """Resolve stored staged GDP rows for manual replay."""
    out: list[_PendingGdpRelease] = []
    for reference in references:
        rows = connection.execute(
            """
            SELECT provider_event_id, title, event_time_utc, source_url
            FROM cal_econ_event
            WHERE provider = 'cao'
              AND title IN ('GDP Growth Rate QoQ Prel', 'GDP Growth Rate QoQ Final')
              AND reference_date = ?
            ORDER BY event_time_utc
            """,
            (reference.isoformat(),),
        ).fetchall()
        for provider_event_id, title, event_time_utc, source_url in rows:
            stage = _stage_from_title(title or "")
            if stage is None:
                continue
            out.append(
                _PendingGdpRelease(
                    provider_event_id=provider_event_id,
                    reference_date=reference,
                    release_stage=stage,
                    event_time_utc=event_time_utc or "",
                    report_url=source_url or build_report_url(reference, stage),
                )
            )
    return out


def fetch_cao_gdp_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    reference_dates: list[date] | None = None,
    menu_html_fetcher: Callable[[str], str] | None = None,
    csv_fetcher: Callable[[str], str] | None = None,
) -> GdpValuesRunSummary:
    """Scrape CAO GDP CSVs and fill ``actual`` on staged GDP rows."""
    started = time.monotonic()
    summary = GdpValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )
    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    as_of_iso = datetime.fromtimestamp(
        snapshot / 1000, tz=timezone.utc,
    ).isoformat()

    if reference_dates is None:
        planned = _discover_pending_releases(connection, as_of_utc_iso=as_of_iso)
    else:
        planned = _lookup_stored_pending(connection, reference_dates)
    summary.releases_planned = len(planned)

    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    menu_fetcher = menu_html_fetcher or fetch_gdp_report_menu_html
    value_fetcher = csv_fetcher or fetch_gdp_csv_text
    raw_records: list[CaoGdpCalendarRawRecord] = []
    event_records: list[CaoGdpCalendarEventRecord] = []
    for pending in planned:
        key = f"{pending.reference_date.isoformat()}|{pending.release_stage}"
        try:
            menu_html = menu_fetcher(pending.report_url)
        except Exception as exc:
            logger.warning("CAO GDP menu fetch failed for %s: %s", key, exc)
            summary.fetch_failures.append((key, str(exc)))
            continue
        try:
            csv_url = parse_gdp_report_menu_html(
                menu_html,
                report_url=pending.report_url,
            )
        except CaoGdpReportParseError as exc:
            logger.warning("CAO GDP menu parse failed for %s: %s", key, exc)
            summary.parse_failures.append((key, str(exc)))
            continue
        try:
            csv_text = value_fetcher(csv_url)
        except Exception as exc:
            logger.warning("CAO GDP CSV fetch failed for %s: %s", key, exc)
            summary.fetch_failures.append((key, str(exc)))
            continue
        try:
            value = parse_gdp_growth_csv(
                csv_text,
                reference_date=pending.reference_date,
                csv_url=csv_url,
            )
            raw, event = gdp_value_to_records(
                value,
                release_stage=pending.release_stage,
                snapshot_epoch_ms=snapshot,
                report_url=pending.report_url,
                event_time_utc=pending.event_time_utc or None,
            )
        except CaoGdpReportParseError as exc:
            logger.warning("CAO GDP parse failed for %s: %s", key, exc)
            summary.parse_failures.append((key, str(exc)))
            continue
        raw_records.append(raw)
        event_records.append(event)
        summary.releases_fetched += 1

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
