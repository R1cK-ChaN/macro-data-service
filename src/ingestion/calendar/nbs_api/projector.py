"""Persist NBS records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge rule first landed with NBS
P5 review: an incoming ``datetime`` row overwrites a stored
``datetime`` row (revisions land); the preservation path only
triggers when the incoming precision is less granular. The
``observed_at_epoch_ms`` WHERE guard still blocks stale snapshots.

Issue #49 added a value-side path. The schedule writer now uses
:func:`project_schedule_events` (metadata-only upsert) so a daily
schedule refresh after a value sweep doesn't blank the ``actual``
the value side just filled. The full :func:`project_events` is
still re-exported for the value-side path that owns the value
columns + ``observed_at_epoch_ms`` freshness guard.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
