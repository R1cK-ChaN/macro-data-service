"""Persist BEA records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge CASE (incoming ``datetime``
overwrites stored ``datetime``; stored value is preserved only
when the incoming precision is less granular). The P2a schedule
scraper calls :func:`project_schedule_events` for its pre-release
writes so BEA value-side upserts do not clobber the scheduled
datetime.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
