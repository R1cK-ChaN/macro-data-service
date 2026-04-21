"""BLS calendar connector (issue #9 P1 scaffold).

Projects BLS Public Data API observations into the
``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar schema.

Scope for P1:

- ``CPI`` — ``CUUR0000SA0`` (Consumer Price Index for All Urban
  Consumers, All Items, Not Seasonally Adjusted).
- ``NFP`` — ``CES0000000001`` (Total nonfarm employment, Seasonally
  Adjusted, thousands).

Both indicators are monthly. Additional BLS series (Core CPI, PPI /
Core PPI, Unemployment Rate, JOLTS, ECI, Productivity, Jobless Claims)
land as P1a slices behind codex-review.

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
