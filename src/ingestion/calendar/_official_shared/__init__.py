"""Shared utilities for official-source economic calendar connectors.

Used by BLS / BEA / Census / Federal-Reserve / ECB / NBS ingestion.
Nothing upstream-specific lives here — per-source auth, rate limiting,
and payload shapes live in the source's own ``<source>_api/`` package.
The four utilities this module exposes are the ones that reappear across
every official-source connector:

- :func:`canonicalize_indicator` — collapse upstream release titles
  (``"Consumer Price Index (CPI)"``, ``"CPI — All Urban Consumers"``,
  …) into a short canonical token (``"CPI"``). Two sources publishing
  the same indicator under different labels hash to the same token so
  ``provider_event_id`` matches across connectors.
- :func:`parse_scheduled_release_time` — turn a local-TZ release-time
  string (``"08:30 AM ET"``, ``"10:00 CET"``, ``"09:30"``) into an
  aware UTC datetime. DST-correct for zones that observe it.
- :func:`synthesize_event_id` — stable ``provider_event_id`` for
  connectors whose upstream doesn't expose one. Hash is determined by
  ``(provider, country, indicator_canonical, event_time_utc)`` so the
  same scheduled release produces the same id across snapshots — which
  is what the idempotency logic in
  :mod:`ingestion.calendar.te_api.projector` relies on.
"""

from __future__ import annotations

from .canonicalize import canonicalize_indicator
from .event_id import synthesize_event_id
from .projector import (
    project_events,
    project_schedule_events,
    store_raw,
)
from .release_time import (
    TIMEZONE_ALIASES,
    ScheduledReleaseTime,
    parse_scheduled_release_time,
)

__all__ = [
    "ScheduledReleaseTime",
    "TIMEZONE_ALIASES",
    "canonicalize_indicator",
    "parse_scheduled_release_time",
    "project_events",
    "project_schedule_events",
    "store_raw",
    "synthesize_event_id",
]
