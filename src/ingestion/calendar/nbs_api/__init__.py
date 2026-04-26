"""NBS (China National Bureau of Statistics) calendar connector —
issue #9 P5 + P5a.

Projects scheduled releases from NBS yearly calendar articles
(``stats.gov.cn/english/PressRelease/ReleaseCalendar/…``) into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Whitelist:

- ``CPI`` — China Consumer Price Index, 12 scheduled monthly
  releases per year at 09:30 Asia/Shanghai.

PPI, GDP, Industrial Production, Fixed Asset Investment, Retail
Sales, Manufacturing PMI, and Non-manufacturing PMI are reachable
via the same table shape and land in later slices once the live
probe validates the catalog coverage (P5c).

P5a added URL auto-discovery: ``fetch_nbs_calendar`` no longer
requires a caller-supplied article URL. When ``calendar_url`` is
omitted, the fetcher resolves the article for ``year`` (or the
current UTC year) by scraping the release-calendar index page.
Callers with a known article URL (ad-hoc historical backfill) can
still pass it directly to skip the index round-trip.

The NBS servers are the highest-risk upstream on this issue —
HTTP-only by default, HTML-fragile, frequent timeouts from non-CN
IPs. The index fetcher carries a retry budget (default 2 extra
attempts with 2-second backoff) to absorb transient flakiness.

Public surface:

- :data:`INDICATOR_REGISTRY` — canonical indicator → metadata map.
- :func:`parse_nbs_calendar_html` — fixture HTML → entry list.
- :func:`parse_nbs_calendar_index` — index HTML + year → article URL.
- :func:`discover_nbs_calendar_url` — orchestrate index fetch + match.
- :func:`release_entry_to_records` — entry → (raw, event) tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_nbs_calendar` — orchestrates (optional) discovery +
  scrape + parse + project. Nothing auto-runs; tests feed fixture
  HTML via the ``html_fetcher`` / ``index_fetcher`` seams.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    FetchValuesRunSummary,
    fetch_nbs_calendar,
    fetch_nbs_values,
)
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
    discover_nbs_calendar_url,
    fetch_nbs_calendar_index_html,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
    parse_nbs_calendar_index,
)
from .value_listing import (
    NBS_PRESS_RELEASE_INDEX_URL,
    NBSPressListingEntry,
    NBSPressListingParseError,
    fetch_press_listing_html,
    fetch_press_release_html,
    parse_press_listing_html,
    resolve_release_url,
)
from .value_parser import (
    NBSValueObservation,
    NBSValueParseError,
    extract_press_release_value,
    parse_press_release_html,
    value_observation_to_records,
)

__all__ = [
    "INDICATOR_REGISTRY",
    "NBS_CALENDAR_INDEX_URL",
    "NBS_CALENDAR_URL_BASE",
    "NBS_PRESS_RELEASE_INDEX_URL",
    "NBSCalendarEventRecord",
    "NBSCalendarParseError",
    "NBSCalendarRawRecord",
    "NBSIndicatorSpec",
    "NBSPressListingEntry",
    "NBSPressListingParseError",
    "NBSReleaseEntry",
    "NBSValueObservation",
    "NBSValueParseError",
    "FetchRunSummary",
    "FetchValuesRunSummary",
    "discover_nbs_calendar_url",
    "extract_press_release_value",
    "fetch_nbs_calendar",
    "fetch_nbs_calendar_index_html",
    "fetch_nbs_values",
    "fetch_nbs_yearly_calendar_html",
    "fetch_press_listing_html",
    "fetch_press_release_html",
    "parse_nbs_calendar_html",
    "parse_nbs_calendar_index",
    "parse_press_listing_html",
    "parse_press_release_html",
    "project_events",
    "release_entry_to_records",
    "resolve_release_url",
    "store_raw",
    "value_observation_to_records",
]
