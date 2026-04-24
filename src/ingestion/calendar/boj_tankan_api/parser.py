"""Shared dataclasses and constants for the Tankan calendar connector.

No HTTP here — the schedule scraper (:mod:`scraper`) and the outline
value scraper (:mod:`outlines`) both import from this module. Kept
separate from the scrapers so other BoJ connectors can reuse the
record shape if they ever share a projector path.

Event-time shape:

Tankan quarterly summary+outline pages are released at **08:50 JST
on the published release day** — same release-time convention as
every other BoJ statistic carried on the "BOJ Time-Series Data
Search" surface (see the notes in the annual XLSX release schedule
at ``boj.or.jp/en/statistics/outline/tkohyos.xlsx``). The schedule
index page itself (``yoshi/index.htm``) carries the release date but
not the clock, so we anchor at 08:50 JST unconditionally.

``reference_date`` follows the calendar-lane convention of using the
first day of the survey reference month. The March 2026 survey
projects to ``2026-03-01`` even though the release drops on
``2026-04-01`` — analysts reading the event want to pivot on the
quarter the survey covers, not the publication day.

``provider_event_id`` anchors on ``(indicator, reference_date)`` so
every indicator on the release gets its own id and the schedule →
value upgrade lifecycle lands on the same row.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


PROVIDER = "boj"
TANKAN_RELEASE_TZ = "Asia/Tokyo"
TANKAN_RELEASE_TIME_LOCAL = "08:50"
TANKAN_YOSHI_INDEX_URL = (
    "https://www.boj.or.jp/en/statistics/tk/yoshi/index.htm"
)
TANKAN_OUTLINE_URL_TEMPLATE = (
    "https://www.boj.or.jp/en/statistics/tk/yoshi/tk{yymm}.htm"
)


@dataclass(frozen=True)
class TankanCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class TankanCalendarEventRecord:
    """One row destined for ``cal_econ_event`` (PIT projection)."""

    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str | None
    currency: str
    unit: str
    actual: str | None
    previous: str | None
    revised: str | None
    forecast: str | None
    consensus_forecast: str | None
    ticker: str
    source: str
    source_url: str
    content_hash: str
    last_update_epoch_ms: int | None
    observed_at_epoch_ms: int


def reference_date_from_yymm(yymm: str) -> date:
    """Resolve a ``"2603"`` YYMM code to ``date(2026, 3, 1)``.

    The yoshi index's result-page URL (``tk2603.htm``) encodes the
    survey's reference quarter — the first day of that month is the
    canonical ``reference_date`` we store.
    """
    if len(yymm) != 4 or not yymm.isdigit():
        raise ValueError(f"malformed Tankan YYMM code: {yymm!r}")
    year = 2000 + int(yymm[:2])
    month = int(yymm[2:])
    if month not in (3, 6, 9, 12):
        raise ValueError(
            f"Tankan YYMM code month must be 3/6/9/12, got {month} ({yymm!r})"
        )
    return date(year, month, 1)


def yymm_from_reference_date(reference: date) -> str:
    """Inverse of :func:`reference_date_from_yymm`."""
    return f"{reference.year % 100:02d}{reference.month:02d}"


def build_outline_url(reference_date: date) -> str:
    """Construct the outline-page URL for a given survey reference date."""
    return TANKAN_OUTLINE_URL_TEMPLATE.format(
        yymm=yymm_from_reference_date(reference_date),
    )
