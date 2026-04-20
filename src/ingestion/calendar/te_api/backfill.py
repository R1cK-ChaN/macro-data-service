"""Adaptive-window backfill planner + driver for the TE Calendar API.

Era brackets, shrink/grow rules, and phase ordering come from
``analyst/te_endpoint/TE_CALENDAR_BACKFILL_PLAN.md``. Nothing in this module
auto-runs — callers must set ``dry_run=False`` and pass a request budget.

Two public entry points:

- :func:`plan_windows` — cheap, network-free. Returns the window list.
- :class:`BackfillRunner.run` — executes a bounded sweep, persisting a
  cursor so a budget breach / 409 storm can resume without replay.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable

from .client import TEAPIClient, TEThrottled
from .parser import parse_calendar_row
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)

PROVIDER = "tradingeconomics"
HISTORY_FLOOR = date(2013, 1, 2)  # earliest non-empty calendar row per survey
MONTHLY_BUDGET = 1000
BUDGET_HALT_THRESHOLD = 950

# (year_lo, year_hi, target_window_days)
ERA_BRACKETS: tuple[tuple[int, int, int], ...] = (
    (2013, 2013, 180),
    (2014, 2015, 30),
    (2016, 2019, 20),
    (2020, 2022, 12),
    (2023, 2099, 10),
)

# Phase spans (inclusive). P1 runs first — recent data has the densest rows
# and the highest analyst value.
PHASES: tuple[tuple[str, date, date], ...] = (
    ("p1_recent", date(2023, 1, 1), date(9999, 12, 31)),  # clamp to today at plan time
    ("p2_mid",    date(2016, 1, 1), date(2022, 12, 31)),
    ("p3_early",  HISTORY_FLOOR,    date(2015, 12, 31)),
)


def _era_window_days(d: date) -> int:
    for lo, hi, days in ERA_BRACKETS:
        if lo <= d.year <= hi:
            return days
    return 10


@dataclass(frozen=True)
class Window:
    phase: str
    start: date
    end: date  # inclusive


@dataclass
class RunSummary:
    dry_run: bool
    phases_planned: list[str]
    windows_planned: int
    requests_spent: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    windows_fetched: int = 0
    truncated_windows: int = 0
    budget_halt: bool = False
    stopped_reason: str = ""
    cursor_state: dict[str, dict] = field(default_factory=dict)


def plan_windows(
    *,
    start: date,
    end: date,
    phases: Iterable[str] | None = None,
) -> list[Window]:
    """Expand a date range into adaptive-width windows across the era
    brackets. Returns in chronological order; phase membership is a pure
    function of ``start``.

    Network-free. Always safe to call.
    """
    if start > end:
        return []
    selected = set(phases) if phases else {p[0] for p in PHASES}
    out: list[Window] = []
    for phase_name, phase_start, phase_end in PHASES:
        if phase_name not in selected:
            continue
        p_start = max(start, phase_start)
        p_end = min(end, phase_end)
        if p_start > p_end:
            continue
        cursor = p_start
        while cursor <= p_end:
            window_days = _era_window_days(cursor)
            window_end = min(cursor + timedelta(days=window_days - 1), p_end)
            out.append(Window(phase=phase_name, start=cursor, end=window_end))
            cursor = window_end + timedelta(days=1)
    # Chronological order across phases makes cursor logic simple to read
    # when debugging; phase order is only a scheduling hint.
    out.sort(key=lambda w: w.start)
    return out


def _phase_end(phase: str) -> date | None:
    for name, _start, end in PHASES:
        if name == phase:
            return end
    return None


def _cumulative_requests(connection: sqlite3.Connection, provider: str) -> int:
    """Sum ``requests_spent`` across all cursor rows for ``provider``.

    This is a lifetime counter — the cursor table has no month column.
    For the ~354-request backfill it is a safe upper bound on the
    monthly quota; once the backfill is complete the incremental loop
    (``/calendar/updates``) dominates and a real month-windowed counter
    becomes worth the complexity.
    """
    row = connection.execute(
        "SELECT COALESCE(SUM(requests_spent), 0) FROM cal_backfill_cursor WHERE provider = ?",
        (provider,),
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


class BackfillRunner:
    """Drives :func:`plan_windows` against a TE client, persisting a cursor.

    Callers typically construct one per request. The runner is single-use:
    one :meth:`run` call advances the cursor and records spend, then the
    instance is discarded.
    """

    CALENDAR_ALL_PATH = "/calendar/country/All/{start}/{end}"

    def __init__(
        self,
        *,
        connection: sqlite3.Connection,
        client: TEAPIClient,
        max_requests: int = 50,
        provider: str = PROVIDER,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn = connection
        self._client = client
        self._max_requests = max_requests
        self._provider = provider
        self._now_utc = now_utc or (lambda: datetime.now(timezone.utc))

    def _load_cursor(self, phase: str) -> date | None:
        row = self._conn.execute(
            "SELECT cursor_date FROM cal_backfill_cursor WHERE provider = ? AND phase = ?",
            (self._provider, phase),
        ).fetchone()
        if not row:
            return None
        try:
            return date.fromisoformat(row[0])
        except (TypeError, ValueError):
            return None

    def _save_cursor(
        self,
        *,
        phase: str,
        cursor_date: date,
        window_end_date: date,
        rows_ingested: int,
        requests_spent: int,
        is_complete: bool,
    ) -> None:
        now_iso = self._now_utc().isoformat()
        self._conn.execute(
            """
            INSERT INTO cal_backfill_cursor (
                provider, phase, cursor_date, window_end_date,
                rows_ingested, requests_spent, last_run_at, is_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (provider, phase) DO UPDATE SET
                cursor_date      = excluded.cursor_date,
                window_end_date  = excluded.window_end_date,
                rows_ingested    = cal_backfill_cursor.rows_ingested + excluded.rows_ingested,
                requests_spent   = cal_backfill_cursor.requests_spent + excluded.requests_spent,
                last_run_at      = excluded.last_run_at,
                is_complete      = excluded.is_complete
            """,
            (
                self._provider,
                phase,
                cursor_date.isoformat(),
                window_end_date.isoformat(),
                rows_ingested,
                requests_spent,
                now_iso,
                1 if is_complete else 0,
            ),
        )

    def _cursor_snapshot(self) -> dict[str, dict]:
        rows = self._conn.execute(
            """
            SELECT phase, cursor_date, window_end_date, rows_ingested,
                   requests_spent, last_run_at, is_complete
            FROM cal_backfill_cursor
            WHERE provider = ?
            """,
            (self._provider,),
        ).fetchall()
        out: dict[str, dict] = {}
        for row in rows:
            out[row[0]] = {
                "cursor_date":     row[1],
                "window_end_date": row[2],
                "rows_ingested":   row[3],
                "requests_spent":  row[4],
                "last_run_at":     row[5],
                "is_complete":     bool(row[6]),
            }
        return out

    def run(
        self,
        *,
        start: date,
        end: date,
        phases: Iterable[str] | None = None,
        dry_run: bool = True,
    ) -> RunSummary:
        planned = plan_windows(start=start, end=end, phases=phases)
        phase_set = sorted({w.phase for w in planned})
        summary = RunSummary(
            dry_run=dry_run,
            phases_planned=phase_set,
            windows_planned=len(planned),
            cursor_state=self._cursor_snapshot(),
        )

        if dry_run:
            summary.stopped_reason = "dry_run"
            return summary

        # Honor existing cursor — skip windows we've already completed.
        active = []
        for window in planned:
            cursor = self._load_cursor(window.phase)
            if cursor is not None and cursor > window.end:
                continue
            if cursor is not None and cursor > window.start:
                window = Window(phase=window.phase, start=cursor, end=window.end)
            active.append(window)

        spent_this_call = 0
        for window in active:
            if spent_this_call >= self._max_requests:
                summary.stopped_reason = "max_requests_reached"
                break
            if _cumulative_requests(self._conn, self._provider) + spent_this_call >= BUDGET_HALT_THRESHOLD:
                summary.budget_halt = True
                summary.stopped_reason = "cumulative_budget_halt"
                break

            path = self.CALENDAR_ALL_PATH.format(
                start=window.start.isoformat(), end=window.end.isoformat()
            )
            try:
                result = self._client.get(path)
            except TEThrottled as exc:
                summary.stopped_reason = f"throttled:{exc}"
                break
            spent_this_call += 1
            summary.windows_fetched += 1
            if result.truncated:
                summary.truncated_windows += 1

            # Parse + persist.
            snapshot_ms = int(self._now_utc().timestamp() * 1000)
            raw_records = []
            event_records = []
            for row in result.rows:
                try:
                    raw, event = parse_calendar_row(row, snapshot_epoch_ms=snapshot_ms)
                except ValueError:
                    continue
                raw_records.append(raw)
                event_records.append(event)
            inserted = store_raw(self._conn, raw_records)
            upserted = project_events(self._conn, event_records)
            summary.rows_raw_inserted += inserted
            summary.events_upserted += upserted

            # Advance cursor. Truncation → next window starts at the last
            # returned Date (inclusive); overlap row de-dups on content
            # hash. When the response fits, advance past window.end.
            if result.truncated and event_records:
                last_date_str = event_records[-1].event_time_utc[:10]
                try:
                    next_cursor = date.fromisoformat(last_date_str)
                except ValueError:
                    next_cursor = window.end + timedelta(days=1)
            else:
                next_cursor = window.end + timedelta(days=1)

            phase_end = _phase_end(window.phase)
            is_complete = phase_end is not None and next_cursor > phase_end
            self._save_cursor(
                phase=window.phase,
                cursor_date=next_cursor,
                window_end_date=window.end,
                rows_ingested=inserted,
                requests_spent=1,
                is_complete=is_complete,
            )

        summary.requests_spent = spent_this_call
        summary.cursor_state = self._cursor_snapshot()
        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary
