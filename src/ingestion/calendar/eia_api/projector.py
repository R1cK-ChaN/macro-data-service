"""Persist EIA records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    store_raw,
)

__all__ = ["project_events", "store_raw"]
