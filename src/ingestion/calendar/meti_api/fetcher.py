"""Drive METI schedule and value scrapes through projection."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY
from .parser import (
    PROVIDER,
    MetiCalendarEventRecord,
    MetiCalendarRawRecord,
    build_iip_report_url,
)
from .projector import project_events, project_schedule_events, store_raw
from .reports import (
    MetiReportParseError,
    extract_pdf_text,
    fetch_iip_report_html,
    fetch_retail_current_page_html,
    fetch_retail_outline_pdf_bytes,
    iip_value_to_records,
    parse_iip_report_html,
    parse_retail_current_page_html,
    parse_retail_outline_text,
    retail_value_to_records,
)
from .scraper import (
    MetiCalendarParseError,
    fetch_iip_release_calendar_xml,
    fetch_retail_schedule_html,
    parse_iip_release_calendar_xml,
    parse_retail_schedule_html,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)


ALL_INDICATORS: list[str] = sorted(INDICATOR_REGISTRY.keys())


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_meti_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


def fetch_meti_calendar(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    iip_xml_fetcher: Callable[[], str] | None = None,
    retail_html_fetcher: Callable[[], str] | None = None,
) -> FetchRunSummary:
    """Scrape METI schedule surfaces and project schedule rows."""
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
    raw_records: list[MetiCalendarRawRecord] = []
    event_records: list[MetiCalendarEventRecord] = []

    try:
        xml_text = (iip_xml_fetcher or fetch_iip_release_calendar_xml)()
        iip_entries = parse_iip_release_calendar_xml(xml_text)
        if not iip_entries:
            raise MetiCalendarParseError(
                "METI IIP release calendar exposed zero preliminary rows"
            )
        for entry in iip_entries:
            raw, event = schedule_entry_to_records(
                entry,
                snapshot_epoch_ms=snapshot,
            )
            raw_records.append(raw)
            event_records.append(event)
        summary.releases_parsed += len(iip_entries)
    except Exception as exc:
        marker = ("iip", str(exc))
        if isinstance(exc, MetiCalendarParseError):
            summary.parse_failures.append(marker)
        else:
            summary.fetch_failures.append(marker)

    try:
        retail_html = (retail_html_fetcher or fetch_retail_schedule_html)()
        retail_entry = parse_retail_schedule_html(retail_html)
        raw, event = schedule_entry_to_records(
            retail_entry,
            snapshot_epoch_ms=snapshot,
        )
        raw_records.append(raw)
        event_records.append(event)
        summary.releases_parsed += 1
    except Exception as exc:
        marker = ("retail", str(exc))
        if isinstance(exc, MetiCalendarParseError):
            summary.parse_failures.append(marker)
        else:
            summary.fetch_failures.append(marker)

    if not event_records:
        details = summary.fetch_failures + summary.parse_failures
        raise MetiCalendarParseError(
            f"METI calendar scrape returned zero releases: {details}"
        )

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


@dataclass
class ValuesRunSummary:
    """Outcome of one ``fetch_meti_values`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    releases_planned: int = 0
    releases_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_failures: list[tuple[str, str]] = field(default_factory=list)
    parse_failures: list[tuple[str, str]] = field(default_factory=list)
    stale_references: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


@dataclass(frozen=True)
class _PendingMetiRelease:
    """One METI value release pending an ``actual`` fill."""

    indicator: str
    reference_date: date
    event_time_utc: str
    source_url: str


_TITLE_TO_INDICATOR = {
    "Industrial Production MoM Prel": "INDUSTRIAL_PRODUCTION",
    "Retail Sales YoY": "RETAIL_SALES",
}


def _discover_pending_releases(
    connection: sqlite3.Connection,
    *,
    as_of_utc_iso: str,
) -> list[_PendingMetiRelease]:
    """Find past METI schedule rows whose value is still pending."""
    as_of = datetime.fromisoformat(as_of_utc_iso)
    threshold_iso = (as_of - timedelta(hours=1)).isoformat()
    rows = connection.execute(
        """
        SELECT title, reference_date, event_time_utc, source_url
        FROM cal_econ_event
        WHERE provider = ?
          AND title IN ('Industrial Production MoM Prel', 'Retail Sales YoY')
          AND actual IS NULL
          AND event_time_utc < ?
          AND reference_date IS NOT NULL
        ORDER BY event_time_utc DESC
        """,
        (PROVIDER, threshold_iso),
    ).fetchall()
    out: list[_PendingMetiRelease] = []
    for title, reference_iso, event_time_utc, source_url in rows:
        indicator = _TITLE_TO_INDICATOR.get(title or "")
        if indicator is None or not reference_iso:
            continue
        try:
            reference = date.fromisoformat(reference_iso)
        except ValueError:
            continue
        out.append(_PendingMetiRelease(
            indicator=indicator,
            reference_date=reference,
            event_time_utc=event_time_utc or "",
            source_url=source_url or "",
        ))
    return out


def _lookup_stored_pending(
    connection: sqlite3.Connection,
    references: list[date],
) -> list[_PendingMetiRelease]:
    """Resolve stored METI rows for manual replay."""
    out: list[_PendingMetiRelease] = []
    for reference in references:
        rows = connection.execute(
            """
            SELECT title, event_time_utc, source_url
            FROM cal_econ_event
            WHERE provider = ?
              AND title IN ('Industrial Production MoM Prel', 'Retail Sales YoY')
              AND reference_date = ?
            ORDER BY title
            """,
            (PROVIDER, reference.isoformat()),
        ).fetchall()
        for title, event_time_utc, source_url in rows:
            indicator = _TITLE_TO_INDICATOR.get(title or "")
            if indicator is None:
                continue
            out.append(_PendingMetiRelease(
                indicator=indicator,
                reference_date=reference,
                event_time_utc=event_time_utc or "",
                source_url=source_url or "",
            ))
    return out


def _stored_event_state(
    connection: sqlite3.Connection,
    *,
    indicator: str,
    reference: date,
) -> tuple[str, bool]:
    title = INDICATOR_REGISTRY[indicator].title
    row = connection.execute(
        """
        SELECT event_time_utc, actual
        FROM cal_econ_event
        WHERE provider = ?
          AND title = ?
          AND reference_date = ?
        LIMIT 1
        """,
        (PROVIDER, title, reference.isoformat()),
    ).fetchone()
    if row is None:
        return "", False
    return (row[0] or ""), row[1] not in (None, "")


def fetch_meti_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    reference_dates: list[date] | None = None,
    iip_html_fetcher: Callable[[date], str] | None = None,
    retail_page_fetcher: Callable[[], str] | None = None,
    retail_pdf_text_fetcher: Callable[[str], str] | None = None,
) -> ValuesRunSummary:
    """Scrape METI value reports and fill ``actual``."""
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

    raw_records: list[MetiCalendarRawRecord] = []
    event_records: list[MetiCalendarEventRecord] = []
    iip_fetcher = iip_html_fetcher or fetch_iip_report_html

    for pending in [p for p in planned if p.indicator == "INDUSTRIAL_PRODUCTION"]:
        try:
            html = iip_fetcher(pending.reference_date)
        except Exception as exc:
            summary.fetch_failures.append((
                f"iip:{pending.reference_date.isoformat()}",
                str(exc),
            ))
            continue
        try:
            value = parse_iip_report_html(
                html,
                source_url=build_iip_report_url(pending.reference_date),
            )
            if value.reference_date != pending.reference_date:
                raise MetiReportParseError(
                    "METI IIP reference mismatch: "
                    f"pending={pending.reference_date.isoformat()} "
                    f"report={value.reference_date.isoformat()}"
                )
            raw, event = iip_value_to_records(
                value,
                snapshot_epoch_ms=snapshot,
                event_time_utc=pending.event_time_utc,
            )
            raw_records.append(raw)
            event_records.append(event)
            summary.releases_fetched += 1
        except Exception as exc:
            summary.parse_failures.append((
                f"iip:{pending.reference_date.isoformat()}",
                str(exc),
            ))

    retail_pendings = [p for p in planned if p.indicator == "RETAIL_SALES"]
    should_fetch_retail = reference_dates is None or bool(retail_pendings)
    if should_fetch_retail:
        try:
            page_html = (retail_page_fetcher or fetch_retail_current_page_html)()
            page = parse_retail_current_page_html(page_html)
            matches = [
                p for p in retail_pendings
                if p.reference_date == page.reference_date
            ]
            if not matches and reference_dates is None:
                stored_event_time, stored_has_actual = _stored_event_state(
                    connection,
                    indicator="RETAIL_SALES",
                    reference=page.reference_date,
                )
                if not stored_has_actual:
                    matches = [_PendingMetiRelease(
                        indicator="RETAIL_SALES",
                        reference_date=page.reference_date,
                        event_time_utc=stored_event_time,
                        source_url=page.outline_pdf_url,
                    )]
                    summary.releases_planned += 1
            for pending in retail_pendings:
                if pending.reference_date != page.reference_date:
                    summary.stale_references.append((
                        pending.reference_date.isoformat(),
                        page.reference_date.isoformat(),
                    ))
            if matches:
                if retail_pdf_text_fetcher is None:
                    pdf_bytes = fetch_retail_outline_pdf_bytes(page.outline_pdf_url)
                    pdf_text = extract_pdf_text(pdf_bytes)
                else:
                    pdf_text = retail_pdf_text_fetcher(page.outline_pdf_url)
                value = parse_retail_outline_text(pdf_text, page=page)
                for pending in matches:
                    raw, event = retail_value_to_records(
                        value,
                        snapshot_epoch_ms=snapshot,
                        event_time_utc=pending.event_time_utc,
                    )
                    raw_records.append(raw)
                    event_records.append(event)
                    summary.releases_fetched += 1
        except Exception as exc:
            summary.fetch_failures.append(("retail", str(exc)))

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
