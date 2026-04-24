"""Persist BoJ records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge CASE — a schedule re-scrape
with a revised MPM closing time overwrites the stored datetime
rather than being silently swallowed while
``observed_at_epoch_ms`` advances.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
