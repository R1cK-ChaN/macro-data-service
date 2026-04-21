"""Persist :mod:`parser` records into ``cal_corp_raw`` + ``cal_corp_event``.

Both operations are idempotent. ``store_corp_raw`` uses ``INSERT OR IGNORE``
on the ``(provider, provider_event_id, content_hash)`` PK;
``project_corp_events`` upserts on ``(provider, provider_event_id)`` and
only updates rows when the incoming ``observed_at_epoch_ms`` is at least
as recent as the stored value — so a late-arriving older snapshot cannot
overwrite a newer projection.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .parser import CalendarCorpEventRecord, CalendarCorpRawRecord


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_corp_raw(
    connection: sqlite3.Connection,
    records: Iterable[CalendarCorpRawRecord],
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
        INSERT OR IGNORE INTO cal_corp_raw (
            provider, provider_event_id, snapshot_epoch_ms,
            content_hash, payload_json, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return connection.total_changes - before


def project_corp_events(
    connection: sqlite3.Connection,
    records: Iterable[CalendarCorpEventRecord],
) -> int:
    """Upsert ``cal_corp_event`` rows. Returns number of inserts + updates.

    A late-arriving older snapshot cannot overwrite a newer projection —
    the ON CONFLICT branch guards on ``observed_at_epoch_ms``. Unlike
    economic events, corporate events have no upstream ``last_update``
    field, so ``observed_at`` is the only revision ordering we have.
    """
    now = _now_iso()
    changed = 0
    for r in records:
        params = (
            r.provider,
            r.provider_event_id,
            r.event_subtype,
            r.event_time_utc,
            r.event_time_precision,
            r.ticker,
            r.exchange,
            r.currency,
            r.currency_reporting,
            r.title,
            r.reference_date,
            r.source_url,
            r.content_hash,
            r.payload_json,
            r.observed_at_epoch_ms,
            now,
            now,
        )
        cursor = connection.execute(
            """
            INSERT INTO cal_corp_event (
                provider, provider_event_id, event_subtype,
                event_time_utc, event_time_precision,
                ticker, exchange, currency, currency_reporting,
                title, reference_date, source_url,
                content_hash, payload_json,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (provider, provider_event_id) DO UPDATE SET
                event_subtype        = excluded.event_subtype,
                event_time_utc       = excluded.event_time_utc,
                event_time_precision = excluded.event_time_precision,
                ticker               = excluded.ticker,
                exchange             = excluded.exchange,
                currency             = excluded.currency,
                currency_reporting   = excluded.currency_reporting,
                title                = excluded.title,
                reference_date       = excluded.reference_date,
                source_url           = excluded.source_url,
                content_hash         = excluded.content_hash,
                payload_json         = excluded.payload_json,
                observed_at_epoch_ms = excluded.observed_at_epoch_ms,
                updated_at           = excluded.updated_at
            WHERE excluded.observed_at_epoch_ms >= cal_corp_event.observed_at_epoch_ms
            """,
            params,
        )
        if cursor.rowcount > 0:
            changed += 1
    return changed
