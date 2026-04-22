"""ECB calendar connector (issue #9 P3 + P3a).

Projects ECB Data Portal SDMX observations and press-calendar events
into the ``cal_econ_raw`` / ``cal_econ_event`` two-lane calendar
schema.

Whitelist:

- ``ECB_MRO`` — Main Refinancing Operations fixed rate
  (``FM.B.U2.EUR.4F.KR.MRR_FR.LEV``) — SDMX lane, anchors on
  effective date.
- ``ECB_DFR`` — Deposit Facility Rate
  (``FM.B.U2.EUR.4F.KR.DFR.LEV``) — SDMX lane.
- ``ECB_MLF`` — Marginal Lending Facility Rate
  (``FM.B.U2.EUR.4F.KR.MLFR.LEV``) — SDMX lane.
- ``ECB Monetary Policy Decision`` — schedule-only, decision-date
  anchored, scraped from ``ecb.europa.eu/press/calendars/mgcgc/``.
- ``ECB Economic Bulletin`` — schedule-only, publication-date
  anchored, scraped from
  ``ecb.europa.eu/press/calendars/economic-bulletin/``.

Deviation from the issue's P3a plan: the SDMX lane uses the rate's
**effective** date (next main refinancing settlement day after the
decision) while the press-calendar carries **decision** dates. P3a
ships them as *distinct* schedule-only indicators rather than
merging onto the SDMX rows' provider_event_id — the effective-vs-
decision anchor reconciliation is left to a follow-up slice once a
clean merge semantics is designed.

Public surface:

- :data:`INDICATOR_REGISTRY` — ECB SDMX series id → metadata map.
- :func:`parse_observation` — :class:`SDMXObservation` → record tuple.
- :func:`store_raw` / :func:`project_events` — idempotent writers.
- :func:`fetch_ecb_calendar` — SDMX whitelist iteration + record
  projection. Nothing auto-runs; callers pass a client.
- :func:`schedule_ecb_calendar` — press-calendar HTML scrape for
  monetary-policy decisions + Economic Bulletin releases.

The HTTP transport layer is the existing
:class:`ingestion.timeseries.sdmx.providers.ecb.ECBClient` (points at
``data-api.ecb.europa.eu/service``; no auth required). This package
adds only the calendar-shaped projection on top.
"""

from __future__ import annotations

from .fetcher import (
    FetchRunSummary,
    ScheduleRunSummary,
    fetch_ecb_calendar,
    schedule_ecb_calendar,
)
from .indicators import ECBIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    parse_observation,
)
from .projector import project_events, store_raw
from .schedule import (
    ECB_BULLETIN_SPEC,
    ECB_BULLETIN_URL,
    ECB_GC_MEETINGS_URL,
    ECB_MP_DECISION_SPEC,
    ECBScheduleEntry,
    ECBScheduleIndicatorSpec,
    ECBScheduleParseError,
    fetch_bulletin_html,
    fetch_gc_meetings_html,
    parse_bulletin_html,
    parse_gc_meetings_html,
    schedule_entry_to_records,
)

__all__ = [
    "ECB_BULLETIN_SPEC",
    "ECB_BULLETIN_URL",
    "ECB_GC_MEETINGS_URL",
    "ECB_MP_DECISION_SPEC",
    "ECBCalendarEventRecord",
    "ECBCalendarRawRecord",
    "ECBIndicatorSpec",
    "ECBScheduleEntry",
    "ECBScheduleIndicatorSpec",
    "ECBScheduleParseError",
    "FetchRunSummary",
    "INDICATOR_REGISTRY",
    "ScheduleRunSummary",
    "fetch_bulletin_html",
    "fetch_ecb_calendar",
    "fetch_gc_meetings_html",
    "parse_bulletin_html",
    "parse_gc_meetings_html",
    "parse_observation",
    "project_events",
    "schedule_ecb_calendar",
    "schedule_entry_to_records",
    "store_raw",
]
