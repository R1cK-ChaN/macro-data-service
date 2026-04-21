"""BLS calendar connector (issue #9 P1 / P1a / P1b / P1c).

Projects BLS Public Data API observations into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

P1c whitelist — 11 indicators covering the BLS headline release set:

- Inflation: ``CPI`` / ``Core CPI`` / ``PPI`` / ``Core PPI``.
- Employment: ``NFP`` / ``Unemployment Rate`` / ``Average Hourly
  Earnings`` / ``Average Weekly Hours`` / ``JOLTS`` / ``Employment
  Cost Index``.
- Productivity: ``Productivity`` (Nonfarm Business sector) —
  schedule-side only; ``api_fetch=False`` until staged
  preliminary/revised alignment lands in a follow-up slice.

ECI and Productivity are quarterly; the rest are monthly. Weekly
Jobless Claims is intentionally excluded — published by DOL/ETA, not
BLS; see the issue #9 Non-goals section.

Public surface:

- :data:`INDICATOR_REGISTRY` — series-id → metadata map.
- :func:`parse_observation` — :class:`BLSObservation` → record tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_bls_calendar` — whitelist iteration + BLS HTTP call
  + record projection. Nothing auto-runs; callers pass a client.

The HTTP transport layer is the existing
:class:`ingestion.scrapers.bls.BLSClient` — reused verbatim so auth,
batching, rate limiting, daily-budget tracking, and 429 backoff are
already production-tested. This package adds only the calendar-shaped
projection on top.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_bls_calendar,
    schedule_bls_calendar,
)
from .indicators import (
    INDICATOR_REGISTRY,
    BLSIndicatorSpec,
)
from .parser import (
    BLSCalendarEventRecord,
    BLSCalendarRawRecord,
    parse_observation,
)
from .projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from .schedule import (
    BLSScheduleEntry,
    BLSScheduleParseError,
    SCHEDULE_URL_SLUG,
    fetch_schedule_html,
    parse_schedule_html,
    schedule_entry_to_records,
)

__all__ = [
    "BLSCalendarEventRecord",
    "BLSCalendarRawRecord",
    "BLSIndicatorSpec",
    "BLSScheduleEntry",
    "BLSScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "SCHEDULE_URL_SLUG",
    "ScheduleRunSummary",
    "fetch_bls_calendar",
    "fetch_schedule_html",
    "parse_observation",
    "parse_schedule_html",
    "project_events",
    "project_schedule_events",
    "schedule_bls_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
