"""Persist :mod:`parser` records into ``cal_corp_raw`` + ``cal_corp_event``.

Both operations are idempotent. ``store_corp_raw`` uses ``INSERT OR IGNORE``
on the ``(provider, provider_event_id, content_hash)`` PK;
``project_corp_events`` upserts on ``(provider, provider_event_id)`` and
only updates rows when the incoming ``observed_at_epoch_ms`` is at least
as recent as the stored value — so a late-arriving older snapshot cannot
overwrite a newer projection.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .parser import CalendarCorpEventRecord, CalendarCorpRawRecord

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def store_corp_raw(
    connection: sqlite3.Connection,
    records: Iterable[CalendarCorpRawRecord],
) -> int:
    """Insert raw rows. Returns the number of new rows actually written.

    Duplicates (same provider + provider_event_id + content_hash) are
    silently ignored — that's the intended idempotency.

    When the insert pushes any ``(provider, provider_event_id)`` from
    one distinct ``content_hash`` to two-or-more, one
    ``corp-event revised`` log line per affected event is emitted at
    INFO. Detection runs two aggregate ``COUNT(DISTINCT content_hash)``
    queries — one before the insert, one after — scoped to the keys
    in the input batch (#66).
    """
    rows: list[tuple[str, str, int, str, str, str]] = []
    by_provider: dict[str, list[str]] = {}
    for r in records:
        rows.append(
            (
                r.provider,
                r.provider_event_id,
                r.snapshot_epoch_ms,
                r.content_hash,
                r.payload_json,
                r.fetched_at,
            )
        )
        by_provider.setdefault(r.provider, []).append(r.provider_event_id)
    if not rows:
        return 0

    prior_counts = _versions_per_event(connection, by_provider)

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
    inserted = connection.total_changes - before

    if inserted > 0:
        new_counts = _versions_per_event(connection, by_provider)
        subtypes = _event_subtypes(connection, by_provider)
        for key, new in new_counts.items():
            prior = prior_counts.get(key, 0)
            if new >= 2 and new > prior:
                provider, peid = key
                logger.info(
                    "corp-event revised provider=%s id=%s "
                    "subtype=%s versions=%d",
                    provider, peid, subtypes.get(key, ""), new,
                )

    return inserted


def _versions_per_event(
    connection: sqlite3.Connection,
    by_provider: dict[str, list[str]],
) -> dict[tuple[str, str], int]:
    """Return ``COUNT(DISTINCT content_hash)`` per ``(provider, peid)``
    for keys in ``by_provider``. Sliced into 500-id chunks to stay
    under SQLite's variable-binding limit.
    """
    out: dict[tuple[str, str], int] = {}
    for provider, peids in by_provider.items():
        unique_peids = list({p for p in peids})
        for chunk_start in range(0, len(unique_peids), 500):
            chunk = unique_peids[chunk_start:chunk_start + 500]
            placeholders = ",".join("?" * len(chunk))
            for row in connection.execute(
                f"""
                SELECT provider_event_id,
                       COUNT(DISTINCT content_hash) AS versions
                FROM cal_corp_raw
                WHERE provider = ?
                  AND provider_event_id IN ({placeholders})
                GROUP BY provider_event_id
                """,
                (provider, *chunk),
            ).fetchall():
                out[(provider, row["provider_event_id"])] = int(row["versions"])
    return out


def _event_subtypes(
    connection: sqlite3.Connection,
    by_provider: dict[str, list[str]],
) -> dict[tuple[str, str], str]:
    """``cal_corp_event.event_subtype`` per ``(provider, peid)`` —
    used to enrich the revision log line. Missing rows return an
    empty string."""
    out: dict[tuple[str, str], str] = {}
    for provider, peids in by_provider.items():
        unique_peids = list({p for p in peids})
        for chunk_start in range(0, len(unique_peids), 500):
            chunk = unique_peids[chunk_start:chunk_start + 500]
            placeholders = ",".join("?" * len(chunk))
            for row in connection.execute(
                f"""
                SELECT provider_event_id, event_subtype
                FROM cal_corp_event
                WHERE provider = ?
                  AND provider_event_id IN ({placeholders})
                """,
                (provider, *chunk),
            ).fetchall():
                out[(provider, row["provider_event_id"])] = (
                    row["event_subtype"] or ""
                )
    return out


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
