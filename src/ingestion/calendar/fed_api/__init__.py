"""Federal Reserve calendar connector (issue #9 P4 + P4a).

Projects the FOMC meeting calendar (scraped from
``federalreserve.gov/monetarypolicy/fomccalendars.htm``) and the
Fed news-release schedule (``releasedates.htm``) into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Whitelist:

- ``FOMC_RATE`` — every FOMC meeting publishes one rate-decision
  event at 14:00 ET on the meeting's closing day (DST-aware). SEP
  rides as a boolean on the FOMC event (``has_sep=True`` when the
  meeting asterisk is present), not as a separate row.
- ``BEIGE_BOOK`` — P4a, ~8 per year, 14:00 ET two weeks before each
  FOMC meeting.
- ``FED_H41`` — P4a, weekly Thursday 16:30 ET (Factors Affecting
  Reserve Balances).
- ``FED_H8`` — P4a, weekly Friday 16:15 ET (Assets and Liabilities
  of Commercial Banks).

Summary of Economic Projections (SEP) rows on ``releasedates.htm``
are explicitly excluded to avoid duplicating the FOMC row. Scheduled
Chair / Vice-Chair speeches and testimony belong in the news pipeline
and are out of scope for this connector.

The Fed publishes no authenticated calendar API; this connector is
HTML-scrape-only. Both fetchers reuse the same browser-UA header
bundle (the default ``python-requests`` UA receives 403s from
``federalreserve.gov``).

Public surface:

- :data:`INDICATOR_REGISTRY` — canonical indicator → metadata map.
- :func:`parse_fomc_calendar_html` / :func:`parse_releasedates_html`
  — fixture HTML → entry list.
- :func:`meeting_entry_to_records` / :func:`release_entry_to_records`
  — entry → (raw, event) tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_fed_calendar` — orchestrates the FOMC calendar scrape.
- :func:`fetch_fed_releasedates` — orchestrates the release-dates
  scrape. Nothing auto-runs; tests feed fixture HTML via the
  ``html_fetcher`` seam on both.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ReleasedatesRunSummary,
    fetch_fed_calendar,
    fetch_fed_releasedates,
)
from .indicators import FedIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    FedCalendarEventRecord,
    FedCalendarRawRecord,
    FomcMeetingEntry,
    meeting_entry_to_records,
)
from .projector import project_events, store_raw
from .releases import (
    FED_RELEASEDATES_URL,
    FedReleaseEntry,
    FedReleasedatesParseError,
    fetch_releasedates_html,
    parse_releasedates_html,
    release_entry_to_records,
)
from .scraper import (
    FOMC_CALENDAR_URL,
    FomcCalendarParseError,
    fetch_fomc_calendar_html,
    parse_fomc_calendar_html,
)

__all__ = [
    "FED_RELEASEDATES_URL",
    "FOMC_CALENDAR_URL",
    "FedCalendarEventRecord",
    "FedCalendarRawRecord",
    "FedIndicatorSpec",
    "FedReleaseEntry",
    "FedReleasedatesParseError",
    "FetchRunSummary",
    "FomcCalendarParseError",
    "FomcMeetingEntry",
    "INDICATOR_REGISTRY",
    "ReleasedatesRunSummary",
    "fetch_fed_calendar",
    "fetch_fed_releasedates",
    "fetch_fomc_calendar_html",
    "fetch_releasedates_html",
    "meeting_entry_to_records",
    "parse_fomc_calendar_html",
    "parse_releasedates_html",
    "project_events",
    "release_entry_to_records",
    "store_raw",
]
