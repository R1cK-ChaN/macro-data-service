"""Persist BEA records into ``cal_econ_raw`` / ``cal_econ_event``.

Both operations are idempotent. ``store_raw`` uses ``INSERT OR IGNORE``
on the ``(provider, provider_event_id, content_hash)`` PK; ``project_events``
upserts on ``(provider, provider_event_id)`` and only updates rows when
the incoming ``observed_at_epoch_ms`` is newer than the stored value.

The SQL shape deliberately mirrors
:mod:`ingestion.calendar.bls_api.projector`. P2 keeps them as parallel
copies by design (two concrete callers = copy per the discipline rule;
the TE projector is semantically distinct so the "third concrete case"
threshold for promotion to ``_official_shared`` is not yet reached).
When the BEA schedule scraper (P2a) lands with a
``project_schedule_events`` side, both BLS and BEA will have the same
two-function surface — that is the natural promotion trigger.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .parser import BEACalendarEventRecord, BEACalendarRawRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_raw(
    connection: sqlite3.Connection,
    records: Iterable[BEACalendarRawRecord],
) -> int:
    """Insert raw rows; returns the count of new rows written."""
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
    records: Iterable[BEACalendarEventRecord],
) -> int:
    """API-side upsert to ``cal_econ_event``; returns inserts + updates count.

    Cross-source merge rule (same as BLS): if the existing row already
    carries a ``datetime``-precision ``event_time_utc`` (written by a
    future BEA schedule scraper), the API-side write **does not**
    clobber it with its ``approximate`` placeholder. Value columns
    obey the ``observed_at`` monotonicity guard so a late-arriving
    older snapshot cannot overwrite a newer one.

    The preservation clause is a no-op today (BEA has no schedule
    scraper yet) but aligns the SQL so the P2a schedule slice doesn't
    need to retrofit it.
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
                                             THEN cal_econ_event.event_time_utc
                                             ELSE excluded.event_time_utc END,
                event_time_precision = CASE WHEN cal_econ_event.event_time_precision = 'datetime'
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
