"""Bank of Japan Tankan calendar connector (issue #14 P1a).

Projects the quarterly Tankan release calendar (``yoshi/index.htm``)
and each release's Large-Enterprises Business-Conditions Diffusion
Index into ``cal_econ_raw`` / ``cal_econ_event``.

Sources:

- ``boj.or.jp/en/statistics/tk/yoshi/index.htm`` — schedule-side:
  each quarterly release has a dated row pointing at the result page.
  Two calendar events ship per release (Large Manufacturers + Large
  Non-Manufacturers), anchored on 08:50 JST of the release day.
- ``boj.or.jp/en/statistics/tk/yoshi/tk<YYMM>.htm`` — value-side:
  the outline page's Business-Conditions > Large-Enterprises table
  carries the current-quarter DI, previous-quarter DI, and the
  prior survey's forecast for this quarter.
  :func:`fetch_boj_tankan_outlines` fills ``actual`` / ``previous``
  / ``forecast`` on existing schedule rows.

Scope for P1a is the two Large-Enterprises DI anchors. Medium- and
small-enterprise DIs, sector-specific sub-indices, fixed-investment
projections, and price-outlook forecasts are published on the same
page but fall below TE's "high importance" bar and ship in a later
slice if downstream demand surfaces.

``provider_event_id`` anchors on ``(indicator, reference_date)`` so
the schedule → value upgrade lifecycle lands on the same row even
though each release produces two indicator events. Schedule-side
writes route through :func:`project_schedule_events` so the
value-side scrape's ``actual`` isn't nulled out on a later
schedule re-scrape; the value-side writer uses the full
:func:`project_events` upsert.
"""

from __future__ import annotations

from .fetcher import (
    ALL_INDICATORS,
    FetchRunSummary,
    OutlineValuesRunSummary,
    fetch_boj_tankan_calendar,
    fetch_boj_tankan_outlines,
)
from .indicators import BojTankanIndicatorSpec, INDICATOR_REGISTRY
from .outlines import (
    OutlineValue,
    SectorDI,
    TankanOutlineParseError,
    fetch_outline_html,
    outline_value_to_records,
    parse_outline_html,
)
from .parser import (
    PROVIDER,
    TANKAN_OUTLINE_URL_TEMPLATE,
    TANKAN_RELEASE_TIME_LOCAL,
    TANKAN_RELEASE_TZ,
    TANKAN_YOSHI_INDEX_URL,
    TankanCalendarEventRecord,
    TankanCalendarRawRecord,
    build_outline_url,
    reference_date_from_yymm,
    yymm_from_reference_date,
)
from .projector import project_events, project_schedule_events, store_raw
from .scraper import (
    TankanScheduleEntry,
    TankanScheduleParseError,
    fetch_tankan_yoshi_index_html,
    parse_tankan_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "ALL_INDICATORS",
    "BojTankanIndicatorSpec",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "OutlineValue",
    "OutlineValuesRunSummary",
    "PROVIDER",
    "SectorDI",
    "TANKAN_OUTLINE_URL_TEMPLATE",
    "TANKAN_RELEASE_TIME_LOCAL",
    "TANKAN_RELEASE_TZ",
    "TANKAN_YOSHI_INDEX_URL",
    "TankanCalendarEventRecord",
    "TankanCalendarRawRecord",
    "TankanOutlineParseError",
    "TankanScheduleEntry",
    "TankanScheduleParseError",
    "build_outline_url",
    "fetch_boj_tankan_calendar",
    "fetch_boj_tankan_outlines",
    "fetch_outline_html",
    "fetch_tankan_yoshi_index_html",
    "outline_value_to_records",
    "parse_outline_html",
    "parse_tankan_schedule_html",
    "project_events",
    "project_schedule_events",
    "reference_date_from_yymm",
    "schedule_entry_to_records",
    "store_raw",
    "yymm_from_reference_date",
]
