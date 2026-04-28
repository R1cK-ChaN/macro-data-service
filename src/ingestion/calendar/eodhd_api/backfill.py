"""Resumable historical backfill driver for the EODHD corp calendar.

Wraps :class:`CorpCalendarFetcher` with three additions the single-shot
fetcher lacks: phase-bounded planning, persistent cursor on
``cal_corp_backfill_cursor``, and idempotent resume — a budget breach or
429 storm advances the cursor only past the windows that completed, so the
next invocation picks up where the last left off.

Two public entry points:

- :func:`plan_corp_windows` — cheap, network-free. Returns the window list.
- :class:`CorpBackfillRunner.run` — executes a bounded sweep, persisting a
  cursor row keyed by ``(provider, subtype, phase)``.

``earnings_trend`` is symbol-scoped only and has no historical floor; the
runner rejects it. Forward maintenance handles trends.

``dividend`` is two-stage: the discovery sweep runs first (date-bounded),
then per-ticker detail enrichment via :func:`fetch_dividend_details` for
the unique symbols the discovery feed surfaced in this invocation.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable

from .client import EODHDAPIClient, EODHDThrottled
from .fetcher import CorpCalendarFetcher, CorpRunSummary, fetch_dividend_details

logger = logging.getLogger(__name__)

PROVIDER = "eodhd"

# Corporate-lane phase boundaries. Earliest cap matches the documented
# EODHD floor for splits / IPOs (2015-01); the fetcher will gracefully
# return empty windows for any subtype whose true floor is later, and
# the cursor advances regardless so dry holes don't block resumption.
PHASES: tuple[tuple[str, date, date], ...] = (
    ("recent", date(2024, 1, 1), date(9999, 12, 31)),  # clamped to today at plan time
    ("mid",    date(2018, 1, 1), date(2023, 12, 31)),
    ("early",  date(2015, 1, 1), date(2017, 12, 31)),
)

# `earnings_trend` is forward-looking and symbol-mandatory; no historical
# floor exists. Forward maintenance covers it. Rejecting here keeps the
# cursor table semantically clean (no rows that never advance).
SUBTYPES_BACKFILLABLE: tuple[str, ...] = ("earnings", "ipo", "split", "dividend")


@dataclass(frozen=True)
class CorpWindow:
    phase: str
    start: date
    end: date  # inclusive


@dataclass
class CorpBackfillSummary:
    subtype: str
    dry_run: bool
    phases_planned: list[str]
    windows_planned: int
    requests_spent: int = 0
    rows_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    parse_errors: int = 0
    windows_fetched: int = 0
    dividend_detail_symbols: int = 0
    stopped_reason: str = ""
    cursor_state: dict[str, dict] = field(default_factory=dict)


def plan_corp_windows(
    *,
    subtype: str,
    start: date,
    end: date,
    window_days: int,
    phases: Iterable[str] | None = None,
    today: date | None = None,
) -> list[CorpWindow]:
    """Expand a date range into ``window_days``-wide slices across phases.

    Network-free. Always safe to call. Slice width is uniform per
    invocation — unlike the econ lane's era-bracketed widths, the corp
    feeds are sparse enough that one width fits all phases.
    """
    del subtype  # accepted for symmetry with future per-subtype tuning
    if start > end:
        return []
    if today is None:
        today = datetime.now(timezone.utc).date()
    selected = set(phases) if phases else {p[0] for p in PHASES}
    out: list[CorpWindow] = []
    width = max(1, window_days)
    for phase_name, phase_start, phase_end in PHASES:
        if phase_name not in selected:
            continue
        # Clamp `recent`'s sentinel upper bound to today so windows don't
        # spill into the future on dry-run plans.
        clamped_phase_end = min(phase_end, today)
        p_start = max(start, phase_start)
        p_end = min(end, clamped_phase_end)
        if p_start > p_end:
            continue
        cursor = p_start
        while cursor <= p_end:
            window_end = min(cursor + timedelta(days=width - 1), p_end)
            out.append(CorpWindow(phase=phase_name, start=cursor, end=window_end))
            cursor = window_end + timedelta(days=1)
    out.sort(key=lambda w: w.start)
    return out


def _phase_end(phase: str, today: date) -> date | None:
    for name, _start, end in PHASES:
        if name == phase:
            return min(end, today) if name == "recent" else end
    return None


class CorpBackfillRunner:
    """Drives :func:`plan_corp_windows` against an EODHD client per subtype.

    Single-use: construct one per invocation. Subsequent invocations read
    the persisted cursor and resume — including across process restarts.
    """

    def __init__(
        self,
        *,
        connection: sqlite3.Connection,
        client: EODHDAPIClient,
        max_requests: int = 50,
        window_days: int = 7,
        provider: str = PROVIDER,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn = connection
        self._client = client
        self._max_requests = max(1, max_requests)
        self._window_days = max(1, window_days)
        self._provider = provider
        self._now_utc = now_utc or (lambda: datetime.now(timezone.utc))

    def _load_cursor(self, *, subtype: str, phase: str) -> date | None:
        row = self._conn.execute(
            "SELECT cursor_date FROM cal_corp_backfill_cursor "
            "WHERE provider = ? AND subtype = ? AND phase = ?",
            (self._provider, subtype, phase),
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
        subtype: str,
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
            INSERT INTO cal_corp_backfill_cursor (
                provider, subtype, phase, cursor_date, window_end_date,
                rows_ingested, requests_spent, last_run_at, is_complete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (provider, subtype, phase) DO UPDATE SET
                cursor_date      = excluded.cursor_date,
                window_end_date  = excluded.window_end_date,
                rows_ingested    = cal_corp_backfill_cursor.rows_ingested + excluded.rows_ingested,
                requests_spent   = cal_corp_backfill_cursor.requests_spent + excluded.requests_spent,
                last_run_at      = excluded.last_run_at,
                is_complete      = excluded.is_complete
            """,
            (
                self._provider,
                subtype,
                phase,
                cursor_date.isoformat(),
                window_end_date.isoformat(),
                rows_ingested,
                requests_spent,
                now_iso,
                1 if is_complete else 0,
            ),
        )

    def _cursor_snapshot(self, subtype: str) -> dict[str, dict]:
        rows = self._conn.execute(
            """
            SELECT phase, cursor_date, window_end_date, rows_ingested,
                   requests_spent, last_run_at, is_complete
            FROM cal_corp_backfill_cursor
            WHERE provider = ? AND subtype = ?
            """,
            (self._provider, subtype),
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
        subtype: str,
        start: date | None = None,
        end: date | None = None,
        phases: Iterable[str] | None = None,
        symbols: Iterable[str] | None = None,
        dry_run: bool = True,
        enrich_dividend_details: bool = True,
    ) -> CorpBackfillSummary:
        if subtype not in SUBTYPES_BACKFILLABLE:
            raise ValueError(
                f"subtype {subtype!r} is not backfillable; expected one of "
                f"{list(SUBTYPES_BACKFILLABLE)} (earnings_trend has no "
                f"historical floor and is forward-only)"
            )

        # A run is "scoped" when the operator narrows the sweep — by
        # ticker filter, custom date range, or both. Scoped runs MUST
        # NOT touch the shared `(provider, subtype, phase)` cursor: a
        # later default backfill would otherwise treat the skipped
        # symbols / earlier dates as already covered and leave gaps.
        scoped = bool(list(symbols or [])) or start is not None or end is not None

        today = self._now_utc().date()
        if start is None:
            start = PHASES[-1][1]  # earliest phase floor
        if end is None:
            end = today

        planned = plan_corp_windows(
            subtype=subtype, start=start, end=end,
            window_days=self._window_days, phases=phases, today=today,
        )
        phase_set = sorted({w.phase for w in planned})
        summary = CorpBackfillSummary(
            subtype=subtype,
            dry_run=dry_run,
            phases_planned=phase_set,
            windows_planned=len(planned),
            cursor_state=self._cursor_snapshot(subtype),
        )

        if dry_run:
            summary.stopped_reason = "dry_run"
            return summary

        symbols_list = [s for s in (symbols or []) if s]

        # Honor existing cursor for unscoped runs only — skip windows
        # we've already completed and clip the resume window to the
        # saved cursor. Scoped runs deliberately ignore the cursor so a
        # one-off `--symbols X --from D` does not race the bulk sweep.
        active: list[CorpWindow] = []
        if scoped:
            active = list(planned)
        else:
            for window in planned:
                cursor = self._load_cursor(subtype=subtype, phase=window.phase)
                if cursor is not None and cursor > window.end:
                    continue
                if cursor is not None and cursor > window.start:
                    window = CorpWindow(
                        phase=window.phase, start=cursor, end=window.end,
                    )
                active.append(window)

        spent_this_call = 0
        for window in active:
            if spent_this_call >= self._max_requests:
                summary.stopped_reason = "max_requests_reached"
                break

            remaining = self._max_requests - spent_this_call
            window_summary = self._fetch_window(
                subtype=subtype,
                window=window,
                symbols=symbols_list,
                budget=remaining,
            )

            spent_this_call += window_summary.requests_spent
            summary.requests_spent += window_summary.requests_spent
            summary.rows_parsed += window_summary.rows_parsed
            summary.rows_raw_inserted += window_summary.rows_raw_inserted
            summary.events_upserted += window_summary.events_upserted
            summary.parse_errors += window_summary.parse_errors

            discovery_completed = window_summary.stopped_reason == "completed"

            # Dividend two-stage: enrich the per-ticker detail feed
            # *inline* for tickers this window surfaced, against the
            # window's own date range (not the original `start`/`end`,
            # which can span 11 years when `--phase recent` runs with
            # no `--from`). Inline enrichment binds the cursor advance
            # to "discovery + detail both finished" — a detail-pass
            # budget exhaustion mid-window leaves the cursor where it
            # was, so the next invocation re-runs discovery (idempotent
            # via `INSERT OR IGNORE`) and resumes detail enrichment.
            detail_completed = True
            detail_stopped_reason = ""
            if (
                subtype == "dividend"
                and enrich_dividend_details
                and discovery_completed
            ):
                window_symbols = self._collect_window_symbols(
                    window=window,
                    requested_symbols=symbols_list,
                )
                detail_budget = self._max_requests - spent_this_call
                if window_symbols:
                    if detail_budget <= 0:
                        # Discovery surfaced tickers but the budget is
                        # exhausted before we could enrich any of them.
                        # Park the cursor on this window so the next
                        # invocation re-runs discovery (idempotent) and
                        # picks up detail enrichment from scratch.
                        detail_completed = False
                        detail_stopped_reason = (
                            "dividend_detail:max_requests_reached"
                        )
                    else:
                        try:
                            detail = fetch_dividend_details(
                                connection=self._conn,
                                client=self._client,
                                symbols=sorted(window_symbols),
                                start=window.start,
                                end=window.end,
                                max_requests=detail_budget,
                                dry_run=False,
                                now_utc=self._now_utc,
                            )
                        except EODHDThrottled as exc:
                            detail_completed = False
                            detail_stopped_reason = f"throttled:{exc}"
                        else:
                            spent_this_call += detail.requests_spent
                            summary.requests_spent += detail.requests_spent
                            summary.rows_parsed += detail.rows_parsed
                            summary.rows_raw_inserted += detail.rows_raw_inserted
                            summary.events_upserted += detail.events_upserted
                            summary.parse_errors += detail.parse_errors
                            summary.dividend_detail_symbols += detail.requests_spent
                            if detail.stopped_reason != "completed":
                                detail_completed = False
                                detail_stopped_reason = (
                                    f"dividend_detail:{detail.stopped_reason}"
                                )

            window_completed = discovery_completed and detail_completed
            if window_completed:
                summary.windows_fetched += 1

            # Scoped runs never write the shared cursor (see comment at
            # the top of run()).
            if scoped:
                if not window_completed:
                    summary.stopped_reason = (
                        detail_stopped_reason or window_summary.stopped_reason
                    )
                    break
                continue

            # Cursor advances only when the window actually finished.
            # A throttle / budget-cap mid-window leaves the cursor on
            # the *current* window so the next invocation re-fetches
            # it (raw `INSERT OR IGNORE` makes the overlap a no-op).
            if window_completed:
                next_cursor = window.end + timedelta(days=1)
                phase_end = _phase_end(window.phase, today)
                is_complete = phase_end is not None and next_cursor > phase_end
                self._save_cursor(
                    subtype=subtype,
                    phase=window.phase,
                    cursor_date=next_cursor,
                    window_end_date=window.end,
                    rows_ingested=window_summary.rows_raw_inserted,
                    requests_spent=window_summary.requests_spent,
                    is_complete=is_complete,
                )
            else:
                # Persist spend even on incomplete windows so the budget
                # accounting in the cursor row stays honest. Cursor stays
                # at window.start so the next invocation re-fetches the
                # whole window — raw dedup absorbs the overlap.
                self._save_cursor(
                    subtype=subtype,
                    phase=window.phase,
                    cursor_date=window.start,
                    window_end_date=window.end,
                    rows_ingested=window_summary.rows_raw_inserted,
                    requests_spent=window_summary.requests_spent,
                    is_complete=False,
                )
                summary.stopped_reason = (
                    detail_stopped_reason or window_summary.stopped_reason
                )
                break

        summary.cursor_state = self._cursor_snapshot(subtype)
        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary

    def _fetch_window(
        self,
        *,
        subtype: str,
        window: CorpWindow,
        symbols: list[str],
        budget: int,
    ) -> CorpRunSummary:
        """Run a single-window fetch through CorpCalendarFetcher.

        Reuses the existing per-window logic (parser, paginator,
        persistence) and surfaces its summary so the runner can read
        `stopped_reason` to decide whether to advance the cursor.
        """
        fetcher = CorpCalendarFetcher(
            connection=self._conn,
            client=self._client,
            max_requests=max(1, budget),
            window_days=self._window_days,
            now_utc=self._now_utc,
        )
        return fetcher.fetch(
            subtype=subtype,
            start=window.start,
            end=window.end,
            symbols=symbols or None,
            dry_run=False,
        )

    def _collect_window_symbols(
        self,
        *,
        window: CorpWindow,
        requested_symbols: list[str] | None = None,
    ) -> set[str]:
        """Read the discovered tickers from cal_corp_event for a window.

        Filters by ``event_subtype='dividend'`` and the date prefix of
        ``event_time_utc`` (which the parser sets to the ex_date for
        dividends). ``reference_date`` is intentionally None on the
        discovery feed — only the per-ticker detail enrichment fills it.

        When the run is symbol-scoped, the result is intersected with
        the requested set so the detail pass cannot spend budget on
        unrelated tickers that happen to live in the table from prior
        unrelated runs.
        """
        rows = self._conn.execute(
            """
            SELECT DISTINCT ticker || CASE WHEN exchange != ''
                                           THEN '.' || exchange
                                           ELSE '' END AS code
            FROM cal_corp_event
            WHERE provider = ?
              AND event_subtype = 'dividend'
              AND substr(event_time_utc, 1, 10) >= ?
              AND substr(event_time_utc, 1, 10) <= ?
              AND ticker != ''
            """,
            (self._provider, window.start.isoformat(), window.end.isoformat()),
        ).fetchall()
        codes = {row[0] for row in rows if row and row[0]}
        if requested_symbols:
            requested = {s.strip() for s in requested_symbols if s and s.strip()}
            codes &= requested
        return codes
