"""EC BCS calendar projection helpers."""

from __future__ import annotations

import sqlite3
from typing import Any, Iterable

from ingestion.calendar._official_shared.projector import (
    project_events,
    project_schedule_events as _shared_project_schedule_events,
    store_raw,
)


def project_schedule_events(
    connection: sqlite3.Connection,
    records: Iterable[Any],
) -> int:
    """Project schedule rows while keeping resolved release-page URLs."""
    rows = list(records)
    preserved_urls: dict[tuple[str, str], str] = {}
    for record in rows:
        existing = connection.execute(
            """
            SELECT source_url
            FROM cal_econ_event
            WHERE provider = ?
              AND provider_event_id = ?
              AND actual IS NOT NULL
              AND source_url LIKE '%/document/download/%'
            """,
            (record.provider, record.provider_event_id),
        ).fetchone()
        if existing and existing[0]:
            preserved_urls[(record.provider, record.provider_event_id)] = str(existing[0])

    changed = _shared_project_schedule_events(connection, rows)
    if preserved_urls:
        connection.executemany(
            """
            UPDATE cal_econ_event
            SET source_url = ?
            WHERE provider = ?
              AND provider_event_id = ?
              AND actual IS NOT NULL
            """,
            [
                (url, provider, provider_event_id)
                for (provider, provider_event_id), url in preserved_urls.items()
            ],
        )
    return changed


__all__ = ["project_events", "project_schedule_events", "store_raw"]
