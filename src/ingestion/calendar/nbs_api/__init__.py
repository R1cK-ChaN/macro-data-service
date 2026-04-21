"""NBS (China National Bureau of Statistics) calendar connector —
issue #9 P5 scaffold.

Projects scheduled releases from NBS yearly calendar articles
(``stats.gov.cn/english/PressRelease/ReleaseCalendar/…``) into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Scope for P5:

- ``CPI`` — China Consumer Price Index, 12 scheduled monthly
  releases per year at 09:30 Asia/Shanghai.

PPI, GDP, Industrial Production, Fixed Asset Investment, Retail
Sales, Manufacturing PMI, and Non-manufacturing PMI are reachable
via the same table shape and land in later slices once the scraper
shape is validated against a live probe.

The NBS publishes no authenticated calendar API; this connector is
HTML-scrape-only. Issue #9's plan review flags this as the
highest-risk upstream: HTTP-only (``http://``) by default, HTML-
fragile, and frequently timing out from non-CN IPs. The live probe
in a future P5a slice will confirm the shape is stable enough for a
recurring schedule.

Public surface:

- :data:`INDICATOR_REGISTRY` — canonical indicator → metadata map.
- :func:`parse_nbs_calendar_html` — fixture HTML → entry list.
- :func:`release_entry_to_records` — entry → (raw, event) tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_nbs_calendar` — orchestrates scrape → parse → project.
  Nothing auto-runs; tests feed fixture HTML via the
  ``html_fetcher`` seam.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_nbs_calendar
from .indicators import INDICATOR_REGISTRY, NBSIndicatorSpec
from .parser import (
    NBS_CALENDAR_URL_BASE,
    NBSCalendarEventRecord,
    NBSCalendarRawRecord,
    NBSReleaseEntry,
    release_entry_to_records,
)
from .projector import project_events, store_raw
from .scraper import (
    NBS_CALENDAR_INDEX_URL,
    NBSCalendarParseError,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
)

__all__ = [
    "INDICATOR_REGISTRY",
    "NBS_CALENDAR_INDEX_URL",
    "NBS_CALENDAR_URL_BASE",
    "NBSCalendarEventRecord",
    "NBSCalendarParseError",
    "NBSCalendarRawRecord",
    "NBSIndicatorSpec",
    "NBSReleaseEntry",
    "FetchRunSummary",
    "fetch_nbs_calendar",
    "fetch_nbs_yearly_calendar_html",
    "parse_nbs_calendar_html",
    "project_events",
    "release_entry_to_records",
    "store_raw",
]
