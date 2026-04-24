"""Persist Tankan records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge CASE so the schedule-side
stored datetime survives a value-side re-scrape that passes through
the same ``event_time_utc``.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
