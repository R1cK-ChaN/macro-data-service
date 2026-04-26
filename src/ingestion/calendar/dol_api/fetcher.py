"""Drive DOL UI Weekly Claims ingestion through the calendar projection.

The DOL doesn't publish a forward calendar — UI Claims releases on
a fixed Thursday 8:30 AM ET cadence. This fetcher walks the ETA
newsroom listing for recent ``Unemployment Insurance Weekly Claims
Report`` rows, downloads each release's PDF, parses the headline
Initial / Continuing Claims values, and writes both schedule and
value into ``cal_econ_event`` in one pass.

Two indicator rows land per release date — Initial Claims (week
ending five days back from the Thursday) and Continuing Claims
(week ending twelve days back; the SA continuing figure lags by
one week vs. Initial). Both share the same release datetime but
different ``reference_date`` anchors.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, DOLIndicatorSpec
from .listing import (
    DOLListingParseError,
    DOLReleaseEntry,
    fetch_listing_html,
    fetch_release_pdf_bytes,
    parse_listing_html,
    session_for_dol,
)
from .parser import (
    DOLCalendarEventRecord,
    DOLCalendarRawRecord,
    DOLPressReleaseParseError,
    parse_press_release_pdf,
    value_observation_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


# Default lookback window. ETA's listing carries ~25 entries (~6
# months) on the first page — 60 days covers the schedule-aware
# burst plus daily catch-up for any release missed by an outage.
_DEFAULT_LOOKBACK_DAYS: int = 60


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_dol_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    dry_run: bool = True
    listing_entries: int = 0
    releases_fetched: int = 0
    releases_failed: list[tuple[str, str]] = field(default_factory=list)
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _resolve_indicators(
    indicators: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    if indicators is None:
        return list(INDICATOR_REGISTRY.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for ind in indicators:
        if ind in INDICATOR_REGISTRY:
            known.append(ind)
        else:
            unknown.append(ind)
    return known, unknown


def fetch_dol_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    session: requests.Session | None = None,
    listing_fetcher: Callable[[], str | bytes] | None = None,
    pdf_text_fetcher: Callable[[str], str] | None = None,
    today: date | None = None,
) -> FetchRunSummary:
    """Sweep DOL UI Weekly Claims releases into ``cal_econ_event``.

    Test seams:

    - ``listing_fetcher`` — replaces the ETA listing GET. Receives no
      arguments; returns the listing HTML.
    - ``pdf_text_fetcher`` — replaces the per-release PDF GET +
      pypdf extraction. Receives the release URL; returns the PDF
      body text already extracted (production path uses ``pypdf``).
    - ``today`` — pin "today" so the lookback window is reproducible.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
    )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    anchor_today = today or date.today()
    earliest = anchor_today - timedelta(days=lookback_days)

    # When the caller didn't pre-seed a session, build one and reuse
    # it across the listing GET + every PDF GET. DOL's Akamai stack
    # sets a ``sec_cpt`` challenge cookie on the listing response;
    # the per-release PDF fetch must replay it or land a 403.
    owned_session = session is None and listing_fetcher is None
    runtime_session = session
    if owned_session:
        runtime_session = session_for_dol()

    try:
        listing_html = (
            listing_fetcher() if listing_fetcher is not None
            else fetch_listing_html(session=runtime_session)
        )
        entries = parse_listing_html(listing_html)
    except (DOLListingParseError, requests.exceptions.RequestException) as exc:
        logger.warning("DOL listing fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        if owned_session and runtime_session is not None:
            runtime_session.close()
        return summary

    entries_in_window: list[DOLReleaseEntry] = [
        e for e in entries if earliest <= e.release_date <= anchor_today
    ]
    summary.listing_entries = len(entries_in_window)

    raw_records: list[DOLCalendarRawRecord] = []
    event_records: list[DOLCalendarEventRecord] = []
    hits: dict[str, int] = {ind: 0 for ind in known}

    for entry in entries_in_window:
        try:
            text = (
                pdf_text_fetcher(entry.detail_url)
                if pdf_text_fetcher is not None
                else _fetch_pdf_text(entry.detail_url, session=runtime_session)
            )
        except Exception as exc:
            logger.warning(
                "DOL PDF fetch failed for %s: %s",
                entry.release_date.isoformat(), exc,
            )
            summary.releases_failed.append(
                (entry.release_date.isoformat(), str(exc)),
            )
            continue
        summary.releases_fetched += 1
        for indicator in known:
            spec = INDICATOR_REGISTRY[indicator]
            try:
                obs = parse_press_release_pdf(
                    text, spec=spec,
                    release_date=entry.release_date,
                    source_url=entry.detail_url,
                )
                raw_rec, event_rec = value_observation_to_records(
                    obs, snapshot_epoch_ms=snapshot, spec=spec,
                )
            except (DOLPressReleaseParseError, ValueError, KeyError) as exc:
                logger.warning(
                    "DOL value parse failed for %s/%s: %s",
                    indicator, entry.release_date.isoformat(), exc,
                )
                summary.releases_failed.append(
                    (f"{indicator}@{entry.release_date.isoformat()}", str(exc)),
                )
                continue
            raw_records.append(raw_rec)
            event_records.append(event_rec)
            hits[indicator] += 1

    for indicator in known:
        if hits.get(indicator, 0) > 0:
            summary.indicators_ok.append(indicator)
        else:
            summary.indicators_empty.append(indicator)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    if owned_session and runtime_session is not None:
        runtime_session.close()
    return summary


def _fetch_pdf_text(
    url: str, *, session: requests.Session | None = None,
) -> str:
    """Production PDF-text extraction via ``pypdf``.

    DOL serves the press-release PDF directly off the ``/newsroom/...``
    URL when the request carries the right browser-shaped headers.
    """
    from io import BytesIO
    from pypdf import PdfReader

    pdf_bytes = fetch_release_pdf_bytes(url, session=session)
    reader = PdfReader(BytesIO(pdf_bytes))
    parts: list[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            # Skip individual page errors — pypdf raises various
            # decoding exceptions on encrypted / scanned pages.
            continue
    return "\n".join(parts)


__all__ = ["FetchRunSummary", "fetch_dol_calendar"]
