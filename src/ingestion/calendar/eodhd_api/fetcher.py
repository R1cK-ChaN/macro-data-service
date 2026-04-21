"""Window fetcher for EODHD corporate calendar endpoints.

Drives :mod:`client` for one subtype at a time and persists via
:mod:`projector`. Default ``dry_run=True`` — nothing touches HTTP until
callers opt in. A bounded ``max_requests`` cap keeps a single invocation
from walking the entire API consumption budget.

EODHD imposes no server-side date ceiling on the calendar endpoints
(beyond the ``2015-01`` historical floor on ipos/splits), but we still
chunk callers into ``window_days``-wide slices so a wide sweep degrades
gracefully under rate limits and lets the runner advance window-by-window
rather than all-or-nothing.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from .client import EODHDAPIClient, EODHDCallResult, EODHDThrottled
from .parser import (
    CalendarCorpEventRecord,
    CalendarCorpRawRecord,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
)
from .projector import project_corp_events, store_corp_raw

logger = logging.getLogger(__name__)

PROVIDER = "eodhd"

# /calendar/dividends is the only endpoint using JSON:API pagination.
# The server caps each page at 1000 rows and exposes ``links.next`` to
# signal more pages exist. Without active pagination a broad symbol or
# date-range query silently truncates at 1000 — same failure mode as
# the TE /calendar/updates 1000-row truncation surfaced in P3.
_DIVIDEND_PAGE_LIMIT = 1000


@dataclass(frozen=True)
class _SubtypeSpec:
    """Static per-subtype routing.

    ``date_scoped`` is False for trends because ``/calendar/trends``
    only accepts ``symbols``; there's no ``from/to`` parameter pair.
    """

    path: str
    rows_key: str
    parser: Callable[..., tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]]
    date_scoped: bool
    accepts_symbols: bool
    date_params: tuple[str, str] | None = None  # (from_param, to_param)
    # Most endpoints take ``symbols=A,B,C`` in one request; dividends use
    # the JSON:API-style ``filter[symbol]=A`` which is singular and
    # forces one request per symbol.
    symbols_param: str = "symbols"
    one_symbol_per_request: bool = False
    # JSON:API pagination driven by ``links.next``. Only /calendar/dividends
    # needs this today; every other calendar endpoint returns the whole
    # set in one response.
    paginated: bool = False


_EARNINGS = _SubtypeSpec(
    path="/api/calendar/earnings",
    rows_key="earnings",
    parser=parse_earnings_row,
    date_scoped=True,
    accepts_symbols=True,
    date_params=("from", "to"),
)
_IPO = _SubtypeSpec(
    path="/api/calendar/ipos",
    rows_key="ipos",
    parser=parse_ipo_row,
    date_scoped=True,
    accepts_symbols=False,
    date_params=("from", "to"),
)
_SPLIT = _SubtypeSpec(
    path="/api/calendar/splits",
    rows_key="splits",
    parser=parse_split_row,
    date_scoped=True,
    accepts_symbols=True,
    date_params=("from", "to"),
)
_DIVIDEND = _SubtypeSpec(
    path="/api/calendar/dividends",
    rows_key="data",
    parser=parse_dividend_row,
    date_scoped=True,
    accepts_symbols=True,
    date_params=("filter[date_from]", "filter[date_to]"),
    symbols_param="filter[symbol]",
    one_symbol_per_request=True,
    paginated=True,
)
_TREND = _SubtypeSpec(
    path="/api/calendar/trends",
    rows_key="trends",
    parser=parse_trend_row,
    date_scoped=False,
    accepts_symbols=True,
)

_SUBTYPE_SPECS: dict[str, _SubtypeSpec] = {
    "earnings": _EARNINGS,
    "ipo": _IPO,
    "split": _SPLIT,
    "dividend": _DIVIDEND,
    "earnings_trend": _TREND,
}


@dataclass
class CorpRunSummary:
    subtype: str
    dry_run: bool
    windows_planned: int = 0
    requests_spent: int = 0
    rows_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    parse_errors: int = 0
    stopped_reason: str = ""
    windows: list[dict[str, str]] = field(default_factory=list)


class CorpCalendarFetcher:
    """Dispatches to the five EODHD calendar endpoints by subtype.

    Single-use: construct one per invocation. ``fetch`` plans the
    windows, then executes only when ``dry_run=False``.
    """

    def __init__(
        self,
        *,
        connection: sqlite3.Connection,
        client: EODHDAPIClient,
        max_requests: int = 20,
        window_days: int = 7,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._conn = connection
        self._client = client
        self._max_requests = max(1, max_requests)
        self._window_days = max(1, window_days)
        self._now_utc = now_utc or (lambda: datetime.now(timezone.utc))

    def fetch(
        self,
        *,
        subtype: str,
        start: date | None = None,
        end: date | None = None,
        symbols: Iterable[str] | None = None,
        dry_run: bool = True,
    ) -> CorpRunSummary:
        if subtype not in _SUBTYPE_SPECS:
            raise ValueError(
                f"unknown corp subtype: {subtype!r}. "
                f"expected one of {sorted(_SUBTYPE_SPECS)}"
            )
        spec = _SUBTYPE_SPECS[subtype]
        summary = CorpRunSummary(subtype=subtype, dry_run=dry_run)

        symbols_list = [s for s in (symbols or []) if s]

        # Planning -----------------------------------------------------------
        if spec.date_scoped:
            if start is None or end is None:
                today = self._now_utc().date()
                start = start or today
                end = end or today + timedelta(days=7)
            windows = _split_windows(start, end, self._window_days)
            summary.windows_planned = len(windows)
            summary.windows = [
                {"start": w[0].isoformat(), "end": w[1].isoformat()} for w in windows
            ]
        else:
            # Trends is not date-scoped — a single request per symbols batch.
            windows = [(None, None)]
            summary.windows_planned = 1

        if dry_run:
            summary.stopped_reason = "dry_run"
            return summary

        # Fail fast: trends is symbol-mandatory upstream, no point iterating
        # windows when we'd just error on every request.
        if subtype == "earnings_trend" and not symbols_list:
            summary.stopped_reason = "symbols_required_for_earnings_trend"
            return summary

        # /calendar/dividends accepts `filter[symbol]=X` which is singular —
        # multiple symbols require multiple requests. Every other endpoint
        # batches into a single comma-separated `symbols=A,B,C` param.
        if spec.accepts_symbols and symbols_list:
            if spec.one_symbol_per_request:
                symbol_groups: list[list[str]] = [[s] for s in symbols_list]
            else:
                symbol_groups = [symbols_list]
        else:
            symbol_groups = [[]]  # one pass with no symbol filter

        # Execution ----------------------------------------------------------
        snapshot_ms = int(self._now_utc().timestamp() * 1000)
        for window_start, window_end in windows:
            if summary.stopped_reason:
                break
            for symbols_subset in symbol_groups:
                params: dict[str, Any] = {}
                if spec.date_scoped and window_start is not None and window_end is not None:
                    assert spec.date_params is not None
                    from_param, to_param = spec.date_params
                    params[from_param] = window_start.isoformat()
                    params[to_param] = window_end.isoformat()
                if symbols_subset:
                    params[spec.symbols_param] = ",".join(symbols_subset)

                if not self._execute_request(
                    spec, params, snapshot_ms=snapshot_ms, summary=summary,
                ):
                    break
            if summary.stopped_reason:
                break

        if not summary.stopped_reason:
            summary.stopped_reason = "completed"
        return summary

    def _execute_request(
        self,
        spec: _SubtypeSpec,
        params: dict[str, Any],
        *,
        snapshot_ms: int,
        summary: CorpRunSummary,
    ) -> bool:
        """Fetch and persist one (window, symbol_group) slot.

        For ``spec.paginated`` subtypes this loops JSON:API pages driven
        by ``links.next`` in the response envelope, incrementing
        ``page[offset]`` by ``page[limit]`` each round. Returns ``False``
        when the outer loop should stop (budget exhausted or throttled);
        ``True`` otherwise.
        """
        if spec.paginated:
            params = dict(params)
            params.setdefault("page[limit]", _DIVIDEND_PAGE_LIMIT)
            params.setdefault("page[offset]", 0)

        while True:
            if summary.requests_spent >= self._max_requests:
                summary.stopped_reason = "max_requests_reached"
                return False
            try:
                result = self._client.get_rows(
                    spec.path, params=params, rows_key=spec.rows_key,
                )
            except EODHDThrottled as exc:
                summary.stopped_reason = f"throttled:{exc}"
                return False
            summary.requests_spent += 1
            self._persist(spec, result, snapshot_ms=snapshot_ms, summary=summary)

            if not spec.paginated:
                return True
            # ``links.next`` is the authoritative "more pages" signal for
            # the JSON:API dividends feed. Trusting it (rather than
            # ``len(rows) < page_limit``) avoids false positives on the
            # edge case where the final page happens to be exactly full.
            next_link = None
            if isinstance(result.payload, dict):
                next_link = (result.payload.get("links") or {}).get("next")
            if not next_link or not result.rows:
                return True
            params["page[offset]"] = (
                int(params["page[offset]"]) + int(params["page[limit]"])
            )

    def _persist(
        self,
        spec: _SubtypeSpec,
        result: EODHDCallResult,
        *,
        snapshot_ms: int,
        summary: CorpRunSummary,
    ) -> None:
        raw_records: list[CalendarCorpRawRecord] = []
        event_records: list[CalendarCorpEventRecord] = []

        for row in result.rows:
            # Trends rows may arrive wrapped: each outer list element is
            # itself a list of per-period dicts. Flatten one level below.
            if isinstance(row, list):
                for inner in row:
                    if isinstance(inner, dict):
                        self._parse_one(spec, inner, snapshot_ms, raw_records, event_records, summary)
            else:
                self._parse_one(spec, row, snapshot_ms, raw_records, event_records, summary)

        inserted = store_corp_raw(self._conn, raw_records)
        upserted = project_corp_events(self._conn, event_records)
        summary.rows_raw_inserted += inserted
        summary.events_upserted += upserted

    def _parse_one(
        self,
        spec: _SubtypeSpec,
        row: dict[str, Any],
        snapshot_ms: int,
        raw_records: list[CalendarCorpRawRecord],
        event_records: list[CalendarCorpEventRecord],
        summary: CorpRunSummary,
    ) -> None:
        try:
            raw, event = spec.parser(row, snapshot_epoch_ms=snapshot_ms)
        except ValueError:
            summary.parse_errors += 1
            return
        raw_records.append(raw)
        event_records.append(event)
        summary.rows_parsed += 1


def fetch_dividend_details(
    *,
    connection: sqlite3.Connection,
    client: EODHDAPIClient,
    symbols: Iterable[str],
    start: date | None = None,
    end: date | None = None,
    max_requests: int = 20,
    dry_run: bool = True,
    now_utc: Callable[[], datetime] | None = None,
) -> CorpRunSummary:
    """Fetch per-ticker dividend details from ``/api/div/{TICKER}.{EXCHANGE}``.

    Enrichment pass over the discovery-only ``/calendar/dividends`` feed.
    One request per symbol — the endpoint is per-ticker and returns a
    top-level JSON array (no envelope key to extract). Rows land on the
    same ``provider_event_id`` the discovery parser synthesised, so this
    upserts the existing ``cal_corp_event`` rows in place with
    ``value`` / ``currency`` / declaration-record-payment dates.

    ``start`` / ``end`` default to an unconstrained query (no ``from`` /
    ``to`` params). Callers typically pass a bounded window matching the
    discovery sweep so the response size stays small.
    """
    now = now_utc or (lambda: datetime.now(timezone.utc))
    summary = CorpRunSummary(subtype="dividend_detail", dry_run=dry_run)

    symbols_list = [s.strip() for s in symbols if str(s).strip()]
    summary.windows_planned = len(symbols_list)
    summary.windows = [{"symbol": s} for s in symbols_list]

    if dry_run:
        summary.stopped_reason = "dry_run"
        return summary
    if not symbols_list:
        summary.stopped_reason = "no_symbols"
        return summary

    params_base: dict[str, Any] = {}
    if start is not None:
        params_base["from"] = start.isoformat()
    if end is not None:
        params_base["to"] = end.isoformat()

    max_req = max(1, max_requests)
    snapshot_ms = int(now().timestamp() * 1000)

    for code in symbols_list:
        if summary.requests_spent >= max_req:
            summary.stopped_reason = "max_requests_reached"
            break
        path = f"/api/div/{code}"
        try:
            result = client.get(path, params=dict(params_base))
        except EODHDThrottled as exc:
            summary.stopped_reason = f"throttled:{exc}"
            break
        summary.requests_spent += 1

        rows = result.payload if isinstance(result.payload, list) else []
        raw_records: list[CalendarCorpRawRecord] = []
        event_records: list[CalendarCorpEventRecord] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                raw, event = parse_dividend_detail_row(
                    row, code=code, snapshot_epoch_ms=snapshot_ms,
                )
            except ValueError:
                summary.parse_errors += 1
                continue
            raw_records.append(raw)
            event_records.append(event)
            summary.rows_parsed += 1

        summary.rows_raw_inserted += store_corp_raw(connection, raw_records)
        summary.events_upserted += project_corp_events(connection, event_records)

    if not summary.stopped_reason:
        summary.stopped_reason = "completed"
    return summary


def _split_windows(start: date, end: date, window_days: int) -> list[tuple[date, date]]:
    """Chunk ``[start, end]`` (inclusive) into slices of ``window_days`` days.

    Returns ``[]`` on inverted ranges; returns a single one-day slice
    when ``start == end``.
    """
    if start > end:
        return []
    out: list[tuple[date, date]] = []
    cursor = start
    step = timedelta(days=window_days - 1)
    while cursor <= end:
        slice_end = min(cursor + step, end)
        out.append((cursor, slice_end))
        cursor = slice_end + timedelta(days=1)
    return out
