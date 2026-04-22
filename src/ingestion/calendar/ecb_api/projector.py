"""Persist ECB records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector adopts NBS's corrected merge CASE — a schedule re-scrape
with a revised release time overwrites the stored value rather
than being swallowed. Value-side / schedule-side separation for
ECB value-bearing writes still uses the shared
``project_events`` path; P3a's meeting-calendar scraper will
switch to :func:`project_schedule_events` when it lands.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    store_raw,
)

__all__ = ["project_events", "store_raw"]
