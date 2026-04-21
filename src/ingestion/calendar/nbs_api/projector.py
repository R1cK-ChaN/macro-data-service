"""Persist NBS records into ``cal_econ_raw`` / ``cal_econ_event``.

Both operations are idempotent. ``store_raw`` uses ``INSERT OR IGNORE``
on the ``(provider, provider_event_id, content_hash)`` PK;
``project_events`` upserts on ``(provider, provider_event_id)`` and only
updates rows when the incoming ``observed_at_epoch_ms`` is newer than
the stored value.

The SQL shape mirrors :mod:`ingestion.calendar.bls_api.projector`,
:mod:`ingestion.calendar.bea_api.projector`,
:mod:`ingestion.calendar.ecb_api.projector`, and
:mod:`ingestion.calendar.fed_api.projector`. NBS is the fifth
concrete caller of the merge-rule variant; promotion into
``_official_shared`` is the follow-up subtraction commit planned
once P5 lands.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .parser import NBSCalendarEventRecord, NBSCalendarRawRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_raw(
    connection: sqlite3.Connection,
    records: Iterable[NBSCalendarRawRecord],
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
    records: Iterable[NBSCalendarEventRecord],
) -> int:
    """API-side upsert to ``cal_econ_event``; returns inserts + updates count.

    Cross-source merge rule: a ``datetime``-precision
    ``event_time_utc`` in the row is only kept when the **incoming**
    write is less precise (``approximate``). Schedule re-scrapes
    arriving with ``datetime`` precision — the NBS republishing the
    yearly calendar with a revised release time, for example —
    overwrite the stored value so the correction lands. The WHERE
    guard on ``observed_at_epoch_ms`` still requires the incoming
    snapshot to be newer-or-equal, so a late-arriving older
    snapshot cannot step on a fresher row.

    BLS / BEA / ECB / Fed ship an older-shaped CASE that preserves
    any stored ``datetime`` unconditionally — correct for their
    schedule-vs-API dual-write pattern but wrong for a
    schedule-only revision flow. The shared ``_official_shared``
    projector promotion (now five callers overdue) will adopt the
    NBS shape so all five connectors converge on the corrected
    rule.
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
