"""Drive Statistics Bureau schedule and value scrapes through projection."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from .estat import (
    StatBureauValueParseError,
    estat_value_to_records,
    fetch_estat_value_json,
    parse_estat_value_json,
)
from .indicators import INDICATOR_REGISTRY, StatBureauIndicatorSpec
from .parser import (
    PROVIDER,
    StatBureauCalendarEventRecord,
    StatBureauCalendarRawRecord,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    StatBureauCalendarParseError,
    fetch_cpi_release_schedule_html,
    fetch_lfs_release_schedule_html,
    parse_cpi_release_schedule_html,
    parse_lfs_release_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_stat_bureau_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


def fetch_stat_bureau_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    cpi_html_fetcher: Callable[[], str] | None = None,
    lfs_html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape Statistics Bureau schedule surfaces and project rows."""
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[StatBureauCalendarRawRecord] = []
    event_records: list[StatBureauCalendarEventRecord] = []

    for name, fetcher, parser in (
        (
            "cpi",
            cpi_html_fetcher or fetch_cpi_release_schedule_html,
            parse_cpi_release_schedule_html,
        ),
        (
            "lfs",
            lfs_html_fetcher or fetch_lfs_release_schedule_html,
            parse_lfs_release_schedule_html,
        ),
    ):
        try:
            html = fetcher()
            entries = parser(html)
            if not entries:
                raise StatBureauCalendarParseError(
                    f"Statistics Bureau {name} schedule exposed zero rows"
                )
            for entry in entries:
                raw, event = schedule_entry_to_records(
                    entry,
                    snapshot_epoch_ms=snapshot,
                )
                raw_records.append(raw)
                event_records.append(event)
            summary.releases_parsed += len(entries)
        except Exception as exc:
            marker = (name, str(exc))
            if isinstance(exc, StatBureauCalendarParseError):
                summary.parse_failures.append(marker)
            else:
                summary.fetch_failures.append(marker)

    if not event_records:
        details = summary.fetch_failures + summary.parse_failures
        raise StatBureauCalendarParseError(
            f"Statistics Bureau schedule scrape returned zero releases: {details}"
        )

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ValuesRunSummary:
    """Outcome of one ``fetch_stat_bureau_values`` invocation."""

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
class _PendingStatBureauRelease:
    """One Statistics Bureau value release pending an ``actual`` fill."""

    indicator: str
    reference_date: date
    event_time_utc: str
    source_url: str


_TITLE_TO_INDICATOR = {
    spec.title: indicator
    for indicator, spec in INDICATOR_REGISTRY.items()
}


def _discover_pending_releases(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[_PendingStatBureauRelease]:
    """Find past Statistics Bureau rows whose value is still pending."""
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    titles = tuple(_TITLE_TO_INDICATOR.keys())
    placeholders = ",".join("?" for _ in titles)
    rows = connection.execute(
        f"""
        SELECT title, reference_date, event_time_utc, source_url
        FROM cal_econ_event
        WHERE provider = ?
          AND title IN ({placeholders})
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY event_time_utc DESC
        """,
        (PROVIDER, *titles, threshold_iso),
    ).fetchall()
    out: list[_PendingStatBureauRelease] = []
    for title, reference_iso, event_time_utc, source_url in rows:
        indicator = _TITLE_TO_INDICATOR.get(title or "")
        if indicator is None or not reference_iso:
            continue
        try:
            reference = date.fromisoformat(reference_iso)
        except ValueError:
            continue
        out.append(_PendingStatBureauRelease(
            indicator=indicator,
            reference_date=reference,
            event_time_utc=event_time_utc or "",
            source_url=source_url or "",
        ))
    return out


def _lookup_stored_pending(
    connection: sqlite3.Connection,
    references: list[date],
) -> list[_PendingStatBureauRelease]:
    """Build Statistics Bureau replay rows for caller-supplied dates."""
    out: list[_PendingStatBureauRelease] = []
    titles = tuple(_TITLE_TO_INDICATOR.keys())
    placeholders = ",".join("?" for _ in titles)
    for reference in references:
        rows = connection.execute(
            f"""
            SELECT title, event_time_utc, source_url
            FROM cal_econ_event
            WHERE provider = ?
              AND title IN ({placeholders})
              AND reference_date = ?
            ORDER BY title
            """,
            (PROVIDER, *titles, reference.isoformat()),
        ).fetchall()
        stored: dict[str, _PendingStatBureauRelease] = {}
        for title, event_time_utc, source_url in rows:
            indicator = _TITLE_TO_INDICATOR.get(title or "")
            if indicator is None:
                continue
            stored[indicator] = _PendingStatBureauRelease(
                indicator=indicator,
                reference_date=reference,
                event_time_utc=event_time_utc or "",
                source_url=source_url or "",
            )
        for indicator in ALL_INDICATORS:
            pending = stored.get(indicator)
            if pending is not None:
                out.append(pending)
                continue
            spec = INDICATOR_REGISTRY[indicator]
            out.append(_PendingStatBureauRelease(
                indicator=indicator,
                reference_date=reference,
                event_time_utc="",
                source_url=spec.source_url,
            ))
    return out


def fetch_stat_bureau_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    reference_dates: list[date] | None = None,
    app_id: str | None = None,
    json_fetcher: Callable[[StatBureauIndicatorSpec, date], dict[str, Any]] | None = None,
) -> ValuesRunSummary:
    """Fetch e-Stat scalar values and fill ``actual`` on pending rows."""
    started = time.monotonic()
    summary = ValuesRunSummary(
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

    raw_records: list[StatBureauCalendarRawRecord] = []
    event_records: list[StatBureauCalendarEventRecord] = []

    for pending in planned:
        spec = INDICATOR_REGISTRY[pending.indicator]
        marker = f"{pending.indicator}:{pending.reference_date.isoformat()}"
        try:
            data = (
                json_fetcher(spec, pending.reference_date)
                if json_fetcher is not None
                else fetch_estat_value_json(
                    spec,
                    pending.reference_date,
                    app_id=app_id,
                )
            )
        except Exception as exc:
            summary.fetch_failures.append((marker, str(exc)))
            continue
        try:
            value = parse_estat_value_json(
                data,
                indicator=pending.indicator,
                reference=pending.reference_date,
            )
            raw, event = estat_value_to_records(
                value,
                snapshot_epoch_ms=snapshot,
                event_time_utc=pending.event_time_utc,
            )
            raw_records.append(raw)
            event_records.append(event)
            summary.releases_fetched += 1
        except (StatBureauValueParseError, KeyError, ValueError) as exc:
            summary.parse_failures.append((marker, str(exc)))

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
