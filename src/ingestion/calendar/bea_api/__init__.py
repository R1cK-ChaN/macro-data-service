"""BEA calendar connector (issue #9 P2 + P2a).

Projects BEA REST API observations and release-calendar entries into
the ``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Whitelist:

- ``GDP`` — NIPA ``T10101`` line 1 (Real Gross Domestic Product,
  Percent Change From Preceding Period, Quarterly SAAR). Staged
  releases (Advance / Second / Third) — schedule-side only via the
  P2a scraper; ``api_fetch=False`` until staged schedule/API merge
  semantics is designed.
- ``PERSONAL_INCOME`` — NIPA ``T20600`` line 1 (Personal Income,
  Monthly — the headline aggregate on BEA's Personal Income and
  Outlays release). Unstaged; schedule + API lanes both active,
  merge verified.

PCE ships on the same release as Personal Income but at a
not-yet-verified ``(table, line)`` coordinate — deferred to P2b so
the live BEA probe can confirm the exact mapping before the connector
labels rows as PCE. Trade Balance (ITA dataset) and Corporate Profits
(NIPA) land as later slices — ITA uses a different parameter surface
than NIPA, so the fetcher needs per-dataset shape awareness before it
widens.

Public surface:

- :data:`INDICATOR_REGISTRY` — BEA series-id → metadata map.
- :func:`parse_observation` — :class:`BEAObservation` → record tuple.
- :func:`store_raw` / :func:`project_events` / :func:`project_schedule_events`
  — idempotent writers.
- :func:`fetch_bea_calendar` — whitelist iteration + BEA HTTP call
  + record projection. Nothing auto-runs; callers pass a client.
- :func:`schedule_bea_calendar` — scrape ``bea.gov/news/schedule``
  and project schedule rows. Stage-qualified ids for GDP,
  bare-date ids for Personal Income.

The HTTP transport layer is the existing
:class:`ingestion.timeseries.scrapers.bea.BEAClient` — reused verbatim
so auth, rate limiting (~100 req/min), and error handling are already
production-tested. This package adds only the calendar-shaped
projection on top.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_bea_calendar,
    schedule_bea_calendar,
)
from .indicators import BEAIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    BEACalendarEventRecord,
    BEACalendarRawRecord,
    parse_observation,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    BEA_SCHEDULE_URL,
    BEAScheduleEntry,
    BEAScheduleParseError,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "BEA_SCHEDULE_URL",
    "BEACalendarEventRecord",
    "BEACalendarRawRecord",
    "BEAIndicatorSpec",
    "BEAScheduleEntry",
    "BEAScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_bea_calendar",
    "fetch_schedule_html",
    "parse_observation",
    "parse_schedule_html",
    "project_events",
    "project_schedule_events",
    "schedule_bea_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
