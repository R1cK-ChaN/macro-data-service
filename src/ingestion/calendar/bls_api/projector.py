"""Persist BLS records into ``cal_econ_raw`` / ``cal_econ_event``.

Thin re-export over
:mod:`ingestion.calendar._official_shared.projector`. The shared
projector carries the corrected merge CASE — an incoming
``datetime`` overwrites a stored ``datetime`` (a legitimate
revision lands); the preservation path fires only when the
incoming row is less precise, which is exactly the BLS API →
schedule collision the dual-write pattern was designed for.

:func:`project_schedule_events` is the P1a schedule-side upsert;
it writes timing + metadata only and leaves ``actual`` /
``previous`` / ``content_hash`` / ``observed_at_epoch_ms`` to
:func:`project_events`.
"""

from __future__ import annotations

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events,
    store_raw,
)

__all__ = ["project_events", "project_schedule_events", "store_raw"]
