"""Persist :mod:`parser` records into ``cal_econ_raw`` + ``cal_econ_event``.

Both operations are idempotent. ``store_raw`` uses ``INSERT OR IGNORE`` on
the ``(provider, provider_event_id, content_hash)`` PK; ``project_events``
does an upsert keyed on ``(provider, provider_event_id)`` and only updates
rows when the incoming ``snapshot_epoch_ms`` is newer than the stored
``observed_at_epoch_ms``. Running either twice is a no-op.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .parser import CalendarEventRecord, CalendarRawRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_raw(
    connection: sqlite3.Connection,
    records: Iterable[CalendarRawRecord],
) -> int:
    """Insert raw rows. Returns the number of new rows actually written.

    Duplicates (same provider + provider_event_id + content_hash) are
    silently ignored — that's the intended idempotency.
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
    records: Iterable[CalendarEventRecord],
) -> int:
    """Upsert ``cal_econ_event`` rows.

    An event is refreshed only when the incoming snapshot is at least as
    recent as the currently-stored ``observed_at_epoch_ms``. This means a
    late-arriving older snapshot cannot overwrite a newer one. First
    insert always wins; subsequent writes go through the ON CONFLICT
    branch.

    Returns the count of inserts + updates.
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


def record_drops(
    connection: sqlite3.Connection,
    provider: str,
    missing_ids: Iterable[str],
    *,
    reason: str = "",
) -> int:
    """Audit upstream-retired / silently-dropped IDs.

    Called by the updates-loop reconciler when a CalendarId submitted to
    ``/calendar/calendarid`` is absent from the response.
    """
    now = _now_iso()
    rows = [(provider, str(cid), now, now, reason) for cid in missing_ids]
    if not rows:
        return 0
    before = connection.total_changes
    connection.executemany(
        """
        INSERT INTO cal_econ_drops (
            provider, provider_event_id,
            first_dropped_at, last_seen_at, reason
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (provider, provider_event_id) DO UPDATE SET
            last_seen_at = excluded.last_seen_at,
            reason       = COALESCE(NULLIF(excluded.reason, ''), cal_econ_drops.reason)
        """,
        rows,
    )
    return connection.total_changes - before
