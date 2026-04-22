"""Persist NBS records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge rule first landed with NBS
P5 review: an incoming ``datetime`` row overwrites a stored
``datetime`` row (revisions land); the preservation path only
triggers when the incoming precision is less granular. The
``observed_at_epoch_ms`` WHERE guard still blocks stale snapshots.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    store_raw,
)

__all__ = ["project_events", "store_raw"]
