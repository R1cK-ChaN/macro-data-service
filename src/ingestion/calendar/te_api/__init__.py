"""TradingEconomics Calendar API scaffold (issue #8 slice 2).

Public surface:

- :class:`TEAPIClient` — HTTP wrapper (auth, rate limit, retry, URL guard).
- :func:`parse_calendar_row` — 22-field TE JSON row → (raw, event) records.
- :class:`BackfillRunner` — adaptive-window backfill orchestrator.
- :class:`UpdatesReconciler` — ``/calendar/updates`` + ``/calendarid`` loop.

Scaffold only: nothing auto-runs. Callers must pass ``dry_run=False``
(and typically a ``max_requests`` cap) to actually hit TE.
"""

from __future__ import annotations

from .backfill import BackfillRunner, RunSummary, Window, plan_windows
from .client import TEAPIClient, TEAuthMissing, TEThrottled
from .daily import DailyPullSummary, pull_daily
from .parser import (
    CalendarEventRecord,
    CalendarRawRecord,
    MUTABLE_FIELDS,
    parse_calendar_row,
    row_content_hash,
)
from .projector import project_events, record_drops, store_raw
from .updates import UpdatesReconciler, UpdatesRunSummary

__all__ = [
    "BackfillRunner",
    "CalendarEventRecord",
    "CalendarRawRecord",
    "DailyPullSummary",
    "MUTABLE_FIELDS",
    "RunSummary",
    "TEAPIClient",
    "TEAuthMissing",
    "TEThrottled",
    "UpdatesReconciler",
    "UpdatesRunSummary",
    "Window",
    "parse_calendar_row",
    "plan_windows",
    "project_events",
    "pull_daily",
    "record_drops",
    "row_content_hash",
    "store_raw",
]
