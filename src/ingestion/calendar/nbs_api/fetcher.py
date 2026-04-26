"""Drive the NBS yearly-calendar scrape through the calendar projection.

``fetch_nbs_calendar`` fetches a specific NBS yearly-calendar article
(or accepts a caller-supplied fixture via the ``html_fetcher`` seam
used by tests), parses it into :class:`NBSReleaseEntry` rows through
:func:`scraper.parse_nbs_calendar_html`, turns each entry into a
``(raw, event)`` tuple through :func:`parser.release_entry_to_records`,
and persists via :func:`projector.store_raw` +
:func:`projector.project_events`.

Nothing auto-runs: callers invoke ``fetch_nbs_calendar`` with a
specific article URL. A dry-run path returns the planned indicator
list without issuing any HTTP request.

One article per fetch — the NBS publishes one calendar document per
year. Multi-year backfill is the caller's loop.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .indicators import INDICATOR_REGISTRY, NBSIndicatorSpec
from .parser import (
    NBSCalendarEventRecord,
    NBSCalendarRawRecord,
    release_entry_to_records,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    NBSCalendarParseError,
    discover_nbs_calendar_url,
    fetch_nbs_calendar_index_html,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
)
from .value_listing import (
    NBSPressListingEntry,
    fetch_press_listing_html,
    fetch_press_release_html,
    parse_press_listing_html,
    resolve_release_url,
)
from .value_parser import (
    NBSValueParseError,
    parse_press_release_html,
    value_observation_to_records,
)

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of a single ``fetch_nbs_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    calendar_url: str = ""
    year: int | None = None
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    url_auto_discovered: bool = False
    wall_seconds: float = 0.0


def fetch_nbs_calendar(
    connection: sqlite3.Connection,
    *,
    calendar_url: str | None = None,
    year: int | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    html_fetcher: Callable[[str], str] | None = None,
    index_fetcher: Callable[..., str] | None = None,
) -> FetchRunSummary:
    """Scrape an NBS yearly-calendar article and project rows.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    calendar_url:
        Absolute URL of the NBS yearly-calendar article
        (``.../ReleaseCalendar/YYYYMM/tYYYYMMDD_N.html``). When
        ``None`` (the P5a default path), the fetcher hits the
        release-calendar index page at
        :data:`NBS_CALENDAR_INDEX_URL`, finds the link for ``year``
        (or the current calendar year), and uses that. Still
        overridable for ad-hoc historical scrapes.
    year:
        Year used for index-page resolution when ``calendar_url`` is
        ``None``. Also fed to the parser as ``year_override``; when
        ``None`` the parser reads the year from the article title.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; the returned summary shows the indicator plan only.
    snapshot_epoch_ms:
        Fetch-time anchor on every raw row. Defaults to "now UTC".
    html_fetcher:
        Test seam — when supplied, called with ``calendar_url`` in
        place of :func:`scraper.fetch_nbs_yearly_calendar_html`.
    index_fetcher:
        Test seam for the index-page fetch (used by the
        auto-discovery path). When supplied, called in place of
        :func:`scraper.fetch_nbs_calendar_index_html`.
    """
    started = time.monotonic()
    summary = FetchRunSummary(
        indicators_planned=list(INDICATOR_REGISTRY.keys()),
        calendar_url=calendar_url or "",
        year=year,
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )

    # Auto-discover URL when caller didn't supply one. The index-page
    # lookup needs a concrete year; fall back to the current UTC year
    # when the caller didn't set ``year`` explicitly. Callers who
    # already know the article URL (ad-hoc historical fetch) skip the
    # index entirely and spend only the one article request.
    if not calendar_url:
        resolved_index_fetcher = index_fetcher or fetch_nbs_calendar_index_html
        discovery_year = year or datetime.now(timezone.utc).year
        calendar_url = discover_nbs_calendar_url(
            discovery_year,
            index_fetcher=resolved_index_fetcher,
        )
        summary.calendar_url = calendar_url
        summary.url_auto_discovered = True

    fetcher = html_fetcher or (
        lambda url: fetch_nbs_yearly_calendar_html(url)
    )
    html = fetcher(calendar_url)
    entries = parse_nbs_calendar_html(html, year_override=year)
    if not entries:
        # NBS is the highest-risk upstream (HTTP-only, HTML-fragile,
        # frequent timeouts). A successful-looking 200 with no parsed
        # entries means the document shape drifted or an interstitial
        # came back; surface instead of committing a no-op.
        raise NBSCalendarParseError(
            "NBS calendar fetch parsed zero scheduled releases — upstream "
            "DOM drift or interstitial response"
        )
    summary.entries_parsed = len(entries)
    summary.year = entries[0].year if summary.year is None else summary.year

    raw_records: list[NBSCalendarRawRecord] = []
    event_records: list[NBSCalendarEventRecord] = []
    for entry in entries:
        raw_rec, event_rec = release_entry_to_records(
            entry,
            snapshot_epoch_ms=snapshot,
            calendar_url=calendar_url,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)

    summary.rows_raw_inserted = store_raw(connection, raw_records)
    # Schedule-only path. Issue #49 added a value-side fetcher; the
    # schedule writer must use ``project_schedule_events`` so a daily
    # schedule refresh after a value sweep doesn't blank the
    # ``actual`` column the value side just filled. The schedule
    # path doesn't own value fields by construction (no ``actual``
    # in ``release_entry_to_records``).
    summary.events_upserted = project_schedule_events(
        connection, event_records,
    )
    summary.wall_seconds = time.monotonic() - started
    return summary


# ──────────────────────────────────────────────────────────────────────────
# Issue #49 — value-side fetcher
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class FetchValuesRunSummary:
    """Outcome of one ``fetch_nbs_values`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    dry_run: bool = True
    pending_releases: int = 0
    listing_misses: int = 0
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    wall_seconds: float = 0.0


# Indicators with a value-side fetcher (``listing_title_fragment``
# set on the spec). PMI / GDP / Manufacturing PMI / Non-Manufacturing
# PMI stay schedule-only — see issue #49 scope notes.
def _value_side_indicators() -> list[str]:
    return [
        indicator
        for indicator, spec in INDICATOR_REGISTRY.items()
        if spec.listing_title_fragment is not None
    ]


def _spec_for_title(title: str) -> NBSIndicatorSpec | None:
    """Reverse-lookup an indicator spec by its ``cal_econ_event.title``."""
    for spec in INDICATOR_REGISTRY.values():
        if spec.title == title:
            return spec
    return None


# Past-due lookback for the value-side discovery query. Pagination on
# the press-release listing is intentionally out of scope (issue #49
# scope freeze) — older rows that fall off the first listing page
# would otherwise stay in every sweep's pending set forever, inflating
# ``listing_misses`` and burning a listing fetch per sweep on rows the
# auto-discovery path can't fulfil. 30 days covers the full monthly
# release cadence (NBS publishes ~13th-17th of each month) plus
# operator triage tolerance — values older than that fall to the
# manual-backfill path.
_VALUE_DISCOVERY_LOOKBACK_DAYS: int = 30


def _pending_value_rows(
    connection: sqlite3.Connection,
    *,
    titles: list[str],
    now_iso: str,
    earliest_iso: str,
) -> list[sqlite3.Row]:
    """Schedule rows whose value side hasn't landed yet.

    Selects ``actual IS NULL`` rows whose ``event_time_utc`` falls
    inside ``[earliest_iso, now_iso]``. The lower bound caps how far
    back the auto-discovery path looks — older rows roll off the
    listing's first page and would never be filled by this fetcher
    anyway. The driver-level burst window
    (``_VALUE_SIDE_DUE_ROW_FILTERS`` in :mod:`scheduler`) clamps the
    upper bound; this helper just answers "what's due right now?".
    """
    if not titles:
        return []
    placeholders = ",".join("?" for _ in titles)
    return connection.execute(
        f"""
        SELECT provider_event_id, reference_date, reference_label,
               event_time_utc, event_time_precision, source_url, title
        FROM cal_econ_event
        WHERE provider = 'nbs'
          AND actual IS NULL
          AND reference_date IS NOT NULL
          AND event_time_utc <= ?
          AND event_time_utc >= ?
          AND title IN ({placeholders})
        ORDER BY event_time_utc ASC
        """,
        (now_iso, earliest_iso, *titles),
    ).fetchall()


def fetch_nbs_values(
    connection: sqlite3.Connection,
    *,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    listing_fetcher: Callable[[], str] | None = None,
    article_fetcher: Callable[[str], str] | None = None,
    now_utc: datetime | None = None,
) -> FetchValuesRunSummary:
    """Fill ``actual`` on past-due NBS schedule rows from press-release pages.

    Auto-discovers ``actual IS NULL`` rows whose ``event_time_utc`` has
    already passed for the indicators with a registered
    ``listing_title_fragment`` (CPI / PPI / Industrial Production /
    Fixed Asset Investment / Retail Sales). For each, resolves the
    article URL on the public press-release listing, downloads the
    article, parses the headline value, and upserts via the shared
    ``provider_event_id``.

    Pagination is intentionally out of scope (issue #49 scope freeze):
    the listing's first page carries ~25–30 entries, comfortably
    covering the burst window plus the daily catch-up sweep. Older
    backfill is a manual op that would need a paginated walk.

    Test seams:

    - ``listing_fetcher`` — replaces the network listing GET. Receives
      no arguments; returns the listing HTML string.
    - ``article_fetcher`` — replaces the per-release article GET.
      Receives the article URL; returns the article HTML string.
    - ``now_utc`` — pin "now" so the pending-rows query is reproducible.
    """
    started = time.monotonic()
    summary = FetchValuesRunSummary(
        indicators_planned=_value_side_indicators(),
        dry_run=dry_run,
    )
    if dry_run:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    now = now_utc or datetime.now(timezone.utc)

    titles = [
        INDICATOR_REGISTRY[indicator].title
        for indicator in summary.indicators_planned
    ]
    earliest = now - timedelta(days=_VALUE_DISCOVERY_LOOKBACK_DAYS)
    pending = _pending_value_rows(
        connection,
        titles=titles,
        now_iso=now.isoformat(),
        earliest_iso=earliest.isoformat(),
    )
    summary.pending_releases = len(pending)
    if not pending:
        # Nothing due — every value-side indicator's actuals are
        # already filled (or no schedule row covers them yet).
        for indicator in summary.indicators_planned:
            summary.series_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary

    # Listing fetch is once per sweep — the press-release index page
    # carries every recent release. The fetcher seam runs through the
    # parser even on injected fixtures so the parse error surface
    # exercises in test.
    listing_html = (
        listing_fetcher() if listing_fetcher is not None
        else fetch_press_listing_html()
    )
    listing_entries: list[NBSPressListingEntry] = parse_press_listing_html(
        listing_html,
    )

    # Per-URL HTML cache — Industrial Production + Fixed Asset
    # Investment + Retail Sales sometimes co-publish under the same
    # "National Economy …" article; cache so the article fetch runs
    # once even if multiple indicators resolve to the same URL.
    article_cache: dict[str, str] = {}
    raw_records: list[NBSCalendarRawRecord] = []
    event_records: list[NBSCalendarEventRecord] = []
    hits: dict[str, int] = {ind: 0 for ind in summary.indicators_planned}

    for row in pending:
        spec = _spec_for_title(row["title"])
        if spec is None or spec.listing_title_fragment is None:
            # The schedule side may carry titles we don't yet have a
            # value parser for (PMI / GDP). Skip silently — the
            # ``series_empty`` reporting still surfaces the gap.
            continue
        try:
            release_date = date.fromisoformat(str(row["event_time_utc"])[:10])
        except ValueError as exc:
            summary.series_failed.append(
                (spec.indicator, f"unparseable event_time_utc: {exc}"),
            )
            continue

        listing_match = resolve_release_url(
            listing_entries,
            release_date=release_date,
            listing_title_fragment=spec.listing_title_fragment,
        )
        if listing_match is None:
            # Not yet on the listing — typical for a release whose
            # press-release page hasn't gone up despite the schedule
            # row crossing its scheduled time. Surface in summary so
            # operator notices, but don't fail the connector.
            summary.listing_misses += 1
            continue

        url = listing_match.url
        try:
            html = article_cache.get(url)
            if html is None:
                html = (
                    article_fetcher(url) if article_fetcher is not None
                    else fetch_press_release_html(url)
                )
                article_cache[url] = html
            obs = parse_press_release_html(
                html,
                spec=spec,
                reference_date=str(row["reference_date"]),
                reference_label=str(row["reference_label"] or ""),
                event_time_utc=str(row["event_time_utc"]),
                event_time_precision=str(
                    row["event_time_precision"] or "datetime"
                ),
                source_url=url,
            )
            raw_rec, event_rec = value_observation_to_records(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
                schedule_release_date=release_date.isoformat(),
            )
        except Exception as exc:
            # Catch broadly — request timeouts, 404s, parse errors
            # all isolate to this row. Without the wide net the
            # scheduler's connection.commit/rollback wrapper would
            # roll back values already parsed earlier in the same
            # sweep. Records the indicator + error string in the
            # summary so the operator sees which release failed.
            logger.warning(
                "NBS value fetch failed for %s on %s: %s",
                spec.indicator, release_date.isoformat(), exc,
            )
            summary.series_failed.append((spec.indicator, str(exc)))
            continue
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        hits[spec.indicator] += 1

    for indicator in summary.indicators_planned:
        if hits.get(indicator, 0) > 0:
            summary.series_ok.append(indicator)
        elif all(
            failed_indicator != indicator
            for failed_indicator, _ in summary.series_failed
        ):
            summary.series_empty.append(indicator)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary
