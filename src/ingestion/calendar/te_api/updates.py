"""Incremental revision loop: ``/calendar/updates`` → ``/calendar/calendarid/{ids}``.

The updates endpoint returns a reduced 4-field pointer shape (CalendarId,
Country, Event, LastUpdate) — no Date or values. We identify ids with a
newer ``LastUpdate`` than what we've stored, batch-rehydrate them through
``/calendar/calendarid``, and reconcile sent-vs-returned to catch
silent upstream retirements (which land in ``cal_econ_drops``).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .client import TEAPIClient, URL_MAX_LEN
from .parser import parse_calendar_row
from .projector import project_events, record_drops, store_raw

logger = logging.getLogger(__name__)

PROVIDER = "tradingeconomics"
UPDATES_PATH = "/calendar/updates"
CALENDARID_PATH_TEMPLATE = "/calendar/calendarid/{ids}"

# URL budget for the calendarid batch — max_len minus base+auth overhead.
# Conservative: assumes 13-digit CalendarIds separated by commas.
BATCH_RESERVED_OVERHEAD = 100
DEFAULT_BATCH_ID_COUNT = 30


@dataclass
class UpdatesRunSummary:
    dry_run: bool
    updates_fetched: int = 0
    pointers_seen: int = 0
    ids_to_rehydrate: int = 0
    batches_planned: int = 0
    batches_fetched: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    drops_recorded: int = 0
    requests_spent: int = 0
    stopped_reason: str = ""
    batch_preview: list[list[str]] = field(default_factory=list)


class UpdatesReconciler:
    """One :meth:`sync` call per tick.

    Default is ``dry_run=True`` — returns a plan (batch shape, ids that
    would be rehydrated) without issuing the rehydrate calls.
    """

    def __init__(
        self,
        *,
        connection: sqlite3.Connection,
        client: TEAPIClient,
        provider: str = PROVIDER,
        batch_id_count: int = DEFAULT_BATCH_ID_COUNT,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn = connection
        self._client = client
        self._provider = provider
        self._batch_id_count = max(1, batch_id_count)
        self._now_utc = now_utc or (lambda: datetime.now(timezone.utc))

    def _stored_max_last_update(self, ids: list[str]) -> dict[str, int | None]:
        """Look up the max stored ``last_update_epoch_ms`` for each id.

        Missing ids map to None so they're treated as new.
        """
        if not ids:
            return {}
        placeholders = ",".join("?" * len(ids))
        rows = self._conn.execute(
            f"""
            SELECT provider_event_id, last_update_epoch_ms
            FROM cal_econ_event
            WHERE provider = ? AND provider_event_id IN ({placeholders})
            """,
            (self._provider, *ids),
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    def _plan_batches(self, ids: list[str]) -> list[list[str]]:
        """Chunk ids into batches that fit inside :data:`URL_MAX_LEN`.

        Uses the configured batch size as a ceiling and drops below it if
        any single batch would overflow.
        """
        if not ids:
            return []
        max_path_chars = URL_MAX_LEN - BATCH_RESERVED_OVERHEAD
        out: list[list[str]] = []
        current: list[str] = []
        current_chars = 0
        for cid in ids:
            add_chars = len(cid) + (1 if current else 0)
            if (
                len(current) >= self._batch_id_count
                or (current_chars + add_chars) > max_path_chars
            ):
                if current:
                    out.append(current)
                current = [cid]
                current_chars = len(cid)
            else:
                current.append(cid)
                current_chars += add_chars
        if current:
            out.append(current)
        return out

    def sync(self, *, dry_run: bool = True) -> UpdatesRunSummary:
        summary = UpdatesRunSummary(dry_run=dry_run)

        # Step 1: /calendar/updates (always costs 1 request when not dry).
        if dry_run:
            summary.stopped_reason = "dry_run"
            return summary

        result = self._client.get(UPDATES_PATH)
        summary.requests_spent += 1
        summary.updates_fetched = result.row_count
        summary.pointers_seen = result.row_count

        # Step 2: determine which ids need rehydration.
        to_rehydrate: list[str] = []
        pointers_by_id: dict[str, dict[str, Any]] = {}
        for row in result.rows:
            cid = row.get("CalendarId")
            if cid is None:
                continue
            cid_str = str(cid)
            pointers_by_id[cid_str] = row

        if pointers_by_id:
            ids_to_check = list(pointers_by_id.keys())
            stored = self._stored_max_last_update(ids_to_check)
            for cid_str, pointer in pointers_by_id.items():
                upstream_ms = _iso_to_epoch_ms(pointer.get("LastUpdate"))
                stored_ms = stored.get(cid_str)
                if stored_ms is None or (upstream_ms is not None and upstream_ms > stored_ms):
                    to_rehydrate.append(cid_str)

        summary.ids_to_rehydrate = len(to_rehydrate)
        batches = self._plan_batches(to_rehydrate)
        summary.batches_planned = len(batches)
        summary.batch_preview = batches[:3]

        # Step 3: rehydrate and reconcile.
        snapshot_ms = int(self._now_utc().timestamp() * 1000)
        for batch in batches:
            path = CALENDARID_PATH_TEMPLATE.format(ids=",".join(batch))
            try:
                fetched = self._client.get(path)
            except Exception as exc:  # pragma: no cover — let orchestrator decide
                summary.stopped_reason = f"rehydrate_failed:{exc}"
                break
            summary.requests_spent += 1
            summary.batches_fetched += 1

            raw_records = []
            event_records = []
            returned_ids: set[str] = set()
            for row in fetched.rows:
                try:
                    raw, event = parse_calendar_row(row, snapshot_epoch_ms=snapshot_ms)
                except ValueError:
                    continue
                raw_records.append(raw)
                event_records.append(event)
                returned_ids.add(raw.provider_event_id)

            inserted = store_raw(self._conn, raw_records)
            upserted = project_events(self._conn, event_records)
            summary.rows_raw_inserted += inserted
            summary.events_upserted += upserted

            missing = [cid for cid in batch if cid not in returned_ids]
            if missing:
                added = record_drops(
                    self._conn,
                    self._provider,
                    missing,
                    reason="silent_drop_from_calendarid_batch",
                )
                summary.drops_recorded += added

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary


def _iso_to_epoch_ms(value: Any) -> int | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)
