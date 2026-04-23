"""Persist NAR records via the shared official-source projector."""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
