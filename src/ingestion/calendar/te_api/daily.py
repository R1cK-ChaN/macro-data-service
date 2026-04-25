"""Single-day TE Calendar pull (issue #22 P1).

One HTTP call against ``/calendar/country/All/{date}/{date}`` per run; the
returned rows go through the existing :func:`parse_calendar_row` plus
:func:`store_raw` / :func:`project_events` path so the same idempotency,
content-hash dedup, and vintage append logic the bulk backfill relies on
applies to the daily tripwire too.

The recurring parity job calls this once per day for ``yesterday``. Result
is summarised so the caller can log the outcome and decide whether to
proceed to comparison; this module owns no comparator logic.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable

from .client import TEAPIClient
from .parser import parse_calendar_row
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)

DAILY_PATH_TEMPLATE = "/calendar/country/All/{start}/{end}"


@dataclass
class DailyPullSummary:
    target_date: str
    rows_returned: int
    rows_raw_inserted: int
    events_upserted: int
    requests_spent: int
    truncated: bool


def pull_daily(
    *,
    connection: sqlite3.Connection,
    client: TEAPIClient,
    target_date: date,
    now_utc: Callable[[], datetime] | None = None,
) -> DailyPullSummary:
    """Fetch one day of TE calendar rows and persist them to the engine DB.

    Idempotent. Re-running the same date is a cheap no-op when nothing
    revised — the raw table dedups on
    ``(provider, provider_event_id, content_hash)`` and the event upsert is
    gated on ``observed_at_epoch_ms``.
    """
    clock = now_utc or (lambda: datetime.now(timezone.utc))
    iso = target_date.isoformat()
    path = DAILY_PATH_TEMPLATE.format(start=iso, end=iso)

    requests_before = client.requests_made
    result = client.get(path)
    requests_spent = client.requests_made - requests_before

    snapshot_ms = int(clock().timestamp() * 1000)
    raws = []
    events = []
    for raw in result.rows:
        try:
            raw_record, event_record = parse_calendar_row(
                raw, snapshot_epoch_ms=snapshot_ms,
            )
        except ValueError:
            continue
        raws.append(raw_record)
        events.append(event_record)

    inserted = store_raw(connection, raws)
    upserted = project_events(connection, events)
    connection.commit()

    if result.truncated:
        # Single-day windows hitting 1000 rows is virtually impossible on a
        # free TE plan — log so the operator sees it before silent gaps
        # accumulate.
        logger.warning(
            "TE daily pull for %s truncated at 1000 rows; tail dropped", iso,
        )

    return DailyPullSummary(
        target_date=iso,
        rows_returned=result.row_count,
        rows_raw_inserted=inserted,
        events_upserted=upserted,
        requests_spent=requests_spent,
        truncated=result.truncated,
    )
