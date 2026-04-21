"""Shared merge-rule projector for official-source calendar connectors.

BLS / BEA / ECB / Fed / NBS each originally copied the same
``store_raw`` + ``project_events`` SQL into their own ``projector``
module. With five concrete callers the discipline-rule threshold
(3× concrete case) is well past; this module lifts the SQL once and
per-connector projectors become thin re-exports.

Corrected merge rule (adopted from NBS P5 review):

- An incoming ``datetime``-precision ``event_time_utc`` **overwrites**
  an existing ``datetime`` value. A schedule re-scrape with a revised
  release time (``9:30`` → ``10:00``) is a legitimate revision and
  must land, not be swallowed.
- An incoming ``approximate`` row with a stored ``datetime`` row
  **preserves** the stored timestamp — the API-side BLS / BEA
  value-bearing write must not clobber a schedule-side datetime.
- The WHERE clause on ``observed_at_epoch_ms`` still requires the
  incoming snapshot to be newer-or-equal before anything updates, so
  a late-arriving older snapshot cannot step on a fresher row.

Two shapes:

- :func:`project_events` — full-row upsert. Used by value-bearing
  projections and by schedule-only connectors (Fed, NBS) whose
  single writer is the schedule scrape.
- :func:`project_schedule_events` — schedule-side upsert that
  touches timing + metadata only and **leaves the value columns
  and the ``observed_at`` freshness guard alone**. Used by BLS's
  P1a schedule scraper so a later BLS-API write can bring in the
  ``actual`` / ``previous`` fields without being clobbered.

Records are duck-typed — any dataclass with the 24 declared fields
works; the five connectors' record dataclasses all satisfy this.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Iterable


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_raw(
    connection: sqlite3.Connection,
    records: Iterable[Any],
) -> int:
    """Insert raw rows with ``INSERT OR IGNORE`` on the natural PK.

    The raw table's uniqueness key is
    ``(provider, provider_event_id, content_hash)`` so identical
    re-fetches are silently dropped. Returns the count of rows
    actually written.
    """
    rows = [
        (
            r.provider,
            r.provider_event_id,
            r.snapshot_epoch_ms,
            r.content_hash,
            r.payload_json,
            r.fetched_at,
        )
        for r in records
    ]
    if not rows:
        return 0
    before = connection.total_changes
    connection.executemany(
        """
        INSERT OR IGNORE INTO cal_econ_raw (
            provider, provider_event_id, snapshot_epoch_ms,
            content_hash, payload_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return connection.total_changes - before


def project_events(
    connection: sqlite3.Connection,
    records: Iterable[Any],
) -> int:
    """Upsert event records into ``cal_econ_event``.

    Conflict key: ``(provider, provider_event_id)``. The update
    branch obeys the corrected merge rule described in this module's
    docstring — a stored ``datetime`` survives only when the incoming
    precision is not ``datetime``. Returns the count of inserted or
    updated rows.
    """
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider,
            r.provider_event_id,
            r.event_time_utc,
            r.event_time_precision,
            r.reference_date,
            r.reference_label,
            r.country_code,
            r.indicator_id,
            r.category,
            r.title,
            r.importance,
            r.currency,
            r.unit,
            r.actual,
            r.previous,
            r.revised,
            r.forecast,
            r.consensus_forecast,
            r.ticker,
            r.source,
            r.source_url,
            r.content_hash,
            r.last_update_epoch_ms,
            r.observed_at_epoch_ms,
            now,
            now,
        )
        cursor = connection.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc, event_time_precision,
                reference_date, reference_label, country_code, indicator_id,
                category, title, importance, currency, unit,
                actual, previous, revised, forecast, consensus_forecast,
                ticker, source, source_url, content_hash,
                last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, provider_event_id) DO UPDATE SET
                event_time_utc       = CASE WHEN cal_econ_event.event_time_precision = 'datetime'
                                              AND excluded.event_time_precision != 'datetime'
                                             THEN cal_econ_event.event_time_utc
                                             ELSE excluded.event_time_utc END,
                event_time_precision = CASE WHEN cal_econ_event.event_time_precision = 'datetime'
                                              AND excluded.event_time_precision != 'datetime'
                                             THEN cal_econ_event.event_time_precision
                                             ELSE excluded.event_time_precision END,
                reference_date       = excluded.reference_date,
                reference_label      = excluded.reference_label,
                country_code         = excluded.country_code,
                indicator_id         = COALESCE(excluded.indicator_id, cal_econ_event.indicator_id),
                category             = excluded.category,
                title                = excluded.title,
                importance           = excluded.importance,
                currency             = excluded.currency,
                unit                 = excluded.unit,
                actual               = excluded.actual,
                previous             = excluded.previous,
                revised              = excluded.revised,
                forecast             = excluded.forecast,
                consensus_forecast   = excluded.consensus_forecast,
                ticker               = excluded.ticker,
                source               = excluded.source,
                source_url           = excluded.source_url,
                content_hash         = excluded.content_hash,
                last_update_epoch_ms = excluded.last_update_epoch_ms,
                observed_at_epoch_ms = excluded.observed_at_epoch_ms,
                updated_at           = excluded.updated_at
            WHERE excluded.observed_at_epoch_ms >= cal_econ_event.observed_at_epoch_ms
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed


def project_schedule_events(
    connection: sqlite3.Connection,
    records: Iterable[Any],
) -> int:
    """Schedule-side upsert — never touches value columns or observed_at.

    Writes event-time + metadata fields only. The value columns
    (``actual`` / ``previous`` / ``revised`` / ``forecast`` /
    ``consensus_forecast`` / ``content_hash``) and the API-side
    freshness guard ``observed_at_epoch_ms`` are left to
    :func:`project_events`. Use when the caller is ingesting
    schedule rows that carry ``actual=None`` by construction (pre-
    release scraping).

    ``reference_label`` on conflict only updates when the stored
    value is empty, and ``reference_date`` fills with ``COALESCE`` —
    the API-side reference shape is authoritative when present.
    """
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider,
            r.provider_event_id,
            r.event_time_utc,
            r.event_time_precision,
            r.reference_date,
            r.reference_label,
            r.country_code,
            r.indicator_id,
            r.category,
            r.title,
            r.importance,
            r.currency,
            r.unit,
            r.actual,
            r.previous,
            r.revised,
            r.forecast,
            r.consensus_forecast,
            r.ticker,
            r.source,
            r.source_url,
            r.content_hash,
            r.last_update_epoch_ms,
            r.observed_at_epoch_ms,
            now,
            now,
        )
        cursor = connection.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc, event_time_precision,
                reference_date, reference_label, country_code, indicator_id,
                category, title, importance, currency, unit,
                actual, previous, revised, forecast, consensus_forecast,
                ticker, source, source_url, content_hash,
                last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, provider_event_id) DO UPDATE SET
                event_time_utc       = excluded.event_time_utc,
                event_time_precision = excluded.event_time_precision,
                reference_date       = COALESCE(cal_econ_event.reference_date, excluded.reference_date),
                reference_label      = CASE WHEN cal_econ_event.reference_label = ''
                                             THEN excluded.reference_label
                                             ELSE cal_econ_event.reference_label END,
                country_code         = excluded.country_code,
                category             = excluded.category,
                title                = excluded.title,
                importance           = excluded.importance,
                unit                 = excluded.unit,
                source               = excluded.source,
                source_url           = excluded.source_url,
                updated_at           = excluded.updated_at
                -- observed_at_epoch_ms deliberately not updated on
                -- conflict: it's the value-side freshness guard used
                -- by project_events' WHERE clause. Bumping it from
                -- the schedule side could let stale API values
                -- overwrite fresher ones on replay.
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed
