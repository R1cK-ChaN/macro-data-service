"""Federal Reserve calendar connector (issue #9 P4 scaffold).

Projects the FOMC meeting calendar (scraped from
``federalreserve.gov/monetarypolicy/fomccalendars.htm``) into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Scope for P4:

- ``FOMC_RATE`` — the single anchor. Every FOMC meeting publishes
  one rate-decision event; the scraper extracts the closing day and
  this package projects it at 14:00 ET (DST-aware).

Beige Book, Summary of Economic Projections (SEP), H.4.1 weekly
balance sheet, H.8, and scheduled Fed speeches are deferred to P4a —
they live on a different upstream page
(``federalreserve.gov/newsevents/releasedates.htm``) with a
different DOM, so splitting keeps review surface tight.

The Fed publishes no authenticated calendar API; this connector is
HTML-scrape-only. :func:`fetch_fomc_calendar_html` drives the live
request with a browser-UA header bundle (the default
``python-requests`` UA receives a 403 from ``federalreserve.gov``).

Public surface:

- :data:`INDICATOR_REGISTRY` — canonical indicator → metadata map.
- :func:`parse_fomc_calendar_html` — fixture HTML → entry list.
- :func:`meeting_entry_to_records` — entry → (raw, event) tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_fed_calendar` — orchestrates scrape → parse → project.
  Nothing auto-runs; tests feed fixture HTML via the ``html_fetcher``
  seam.
"""

from __future__ import annotations

from .fetcher import FetchRunSummary, fetch_fed_calendar
from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    FomcMeetingEntry,
    meeting_entry_to_records,
)
from .projector import project_events, store_raw
from .scraper import (
    FOMC_CALENDAR_URL,
    FomcCalendarParseError,
    fetch_fomc_calendar_html,
    parse_fomc_calendar_html,
)

__all__ = [
    "FOMC_CALENDAR_URL",
    "FedCalendarEventRecord",
    "FedCalendarRawRecord",
    "FedIndicatorSpec",
    "FetchRunSummary",
    "FomcCalendarParseError",
    "FomcMeetingEntry",
    "INDICATOR_REGISTRY",
    "fetch_fed_calendar",
    "fetch_fomc_calendar_html",
    "meeting_entry_to_records",
    "parse_fomc_calendar_html",
    "project_events",
    "store_raw",
]
