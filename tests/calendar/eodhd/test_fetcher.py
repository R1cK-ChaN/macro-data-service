"""EODHD scaffold tests: CorpCalendarFetcher dry-run / execution / dividend pagination + dividend_detail fetcher.

Split out of the original tests/test_eodhd_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
import httpx
import pytest
import respx
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.eodhd_api import (
    CorpCalendarFetcher,
    EODHDAPIClient,
    fetch_dividend_details,
    parse_dividend_detail_row,
    parse_dividend_row,
    project_corp_events,
    store_corp_raw,
)


def _earnings_row(**overrides):
    base = {
        "code": "AAPL.US",
        "report_date": "2026-05-01",
        "date": "2026-04-30",
        "before_after_market": "AfterMarket",
        "currency": "USD",
        "actual": 1.53,
        "estimate": 1.50,
        "difference": 0.03,
        "percent": 2.0,
    }
    base.update(overrides)
    return base


def _trend_row(**overrides):
    base = {
        "code": "AAPL.US",
        "date": "2026-05-01",
        "period": "0q",
        "growth": "0.05",
        "earningsEstimateAvg": "1.50",
        "earningsEstimateLow": "1.40",
        "earningsEstimateHigh": "1.60",
        "earningsEstimateYearAgoEps": "1.40",
        "earningsEstimateNumberOfAnalysts": "30",
        "earningsEstimateGrowth": "0.07",
        "revenueEstimateAvg": "90000000000",
        "revenueEstimateLow": "88000000000",
        "revenueEstimateHigh": "92000000000",
        "revenueEstimateNumberOfAnalysts": "28",
        "epsTrendCurrent": "1.50",
        "epsTrend7daysAgo": "1.49",
        "epsTrend30daysAgo": "1.48",
        "epsTrend60daysAgo": "1.47",
        "epsTrend90daysAgo": "1.45",
        "epsRevisionsUpLast7days": "3",
        "epsRevisionsUpLast30days": "5",
    }
    base.update(overrides)
    return base


def _dividend_row(**overrides):
    # /calendar/dividends is discovery-only — rows are just (symbol, date).
    # Validated against live EODHD on 2026-04-21 for AAPL.US / filter[date_eq]:
    # no value, period, currency, or declaration/record/payment fields
    # arrived even for major US tickers. EODHD's blog on the "extended"
    # dividend fields refers to the per-ticker /api/div/{TICKER} endpoint,
    # not to this calendar feed.
    base = {
        "symbol": "MSFT.US",
        "date": "2026-05-15",
    }
    base.update(overrides)
    return base


def _dividend_detail_row(**overrides):
    # /api/div/{TICKER}.{EXCHANGE} extended shape. Major US/EU tickers
    # return this rich form; smaller symbols may return just
    # {date, value}. Tests cover both.
    base = {
        "date": "2026-02-09",
        "value": 0.24,
        "unadjustedValue": 0.24,
        "currency": "USD",
        "declarationDate": "2026-01-30",
        "recordDate": "2026-02-10",
        "paymentDate": "2026-02-13",
        "period": "Quarterly",
    }
    base.update(overrides)
    return base


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


@pytest.fixture()
def connection(store: SQLiteEngineStore):
    conn = store.get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def test_fetcher_rejects_unknown_subtype(store: SQLiteEngineStore) -> None:
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(connection=connection, client=client)
        with pytest.raises(ValueError):
            fetcher.fetch(subtype="bogus")
    finally:
        connection.close()


def test_fetcher_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=3,
        )
        with respx.mock(assert_all_called=False) as router:
            router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
            summary = fetcher.fetch(
                subtype="earnings",
                start=date(2026, 5, 1),
                end=date(2026, 5, 10),
                dry_run=True,
            )
            assert router.calls.call_count == 0
    finally:
        connection.close()
    assert summary.dry_run is True
    assert summary.windows_planned >= 3
    assert summary.stopped_reason == "dry_run"


@respx.mock
def test_fetcher_earnings_persists_and_advances(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        return_value=httpx.Response(
            200,
            json={"earnings": [
                _earnings_row(code="AAPL.US"),
                _earnings_row(code="MSFT.US", report_date="2026-05-02"),
            ]},
        )
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fixed_now = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=7,
            now_utc=lambda: fixed_now,
        )
        summary = fetcher.fetch(
            subtype="earnings",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()

    assert summary.requests_spent == 1
    assert summary.rows_parsed == 2
    assert summary.rows_raw_inserted == 2
    assert summary.events_upserted == 2
    assert summary.stopped_reason == "completed"


@respx.mock
def test_fetcher_dividends_handles_jsonapi_envelope(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        return_value=httpx.Response(
            200,
            json={
                "meta": {"total": 1, "offset": 0, "limit": 1000},
                "data": [_dividend_row()],
                "links": {"next": None},
            },
        )
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=7,
        )
        summary = fetcher.fetch(
            subtype="dividend",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.rows_parsed == 1
    assert summary.rows_raw_inserted == 1
    assert summary.events_upserted == 1


@respx.mock
def test_fetcher_trend_flattens_nested_payload(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """/calendar/trends returns ``[[row, row, ...], [row, ...]]`` when
    asked about multiple symbols. The client must preserve the inner
    lists (not filter them out as non-dict), and the fetcher must
    flatten them before parsing."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/trends").mock(
        return_value=httpx.Response(
            200,
            json={"trends": [
                [_trend_row(code="AAPL.US", period="0q"),
                 _trend_row(code="AAPL.US", period="+1q")],
                [_trend_row(code="MSFT.US", period="0q")],
            ]},
        )
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(connection=connection, client=client)
        summary = fetcher.fetch(
            subtype="earnings_trend",
            symbols=["AAPL.US", "MSFT.US"],
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.rows_parsed == 3
    assert summary.rows_raw_inserted == 3
    assert summary.events_upserted == 3
    assert summary.stopped_reason == "completed"


@respx.mock
def test_fetcher_dividends_paginates_until_links_next_null(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """EODHD's /calendar/dividends caps each response at 1000 rows and
    paginates via JSON:API ``links.next``. The fetcher must drain the
    cursor end-to-end instead of taking the first page and silently
    dropping the tail — same failure mode as the TE /calendar/updates
    truncation surfaced in P3."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")

    captured_offsets: list[str] = []

    def _respond(request):
        offset = dict(request.url.params).get("page[offset]", "0")
        captured_offsets.append(offset)
        if offset == "0":
            body = {
                "meta": {"total": 2100, "offset": 0, "limit": 1000},
                "data": [_dividend_row(symbol=f"A{i}.US") for i in range(1000)],
                "links": {"next": "https://eodhd.com/api/calendar/dividends?page[offset]=1000"},
            }
        elif offset == "1000":
            body = {
                "meta": {"total": 2100, "offset": 1000, "limit": 1000},
                "data": [_dividend_row(symbol=f"B{i}.US") for i in range(1000)],
                "links": {"next": "https://eodhd.com/api/calendar/dividends?page[offset]=2000"},
            }
        else:  # offset == "2000"
            body = {
                "meta": {"total": 2100, "offset": 2000, "limit": 1000},
                "data": [_dividend_row(symbol=f"C{i}.US") for i in range(100)],
                "links": {"next": None},
            }
        return httpx.Response(200, json=body)

    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        side_effect=_respond
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=7, max_requests=10,
        )
        summary = fetcher.fetch(
            subtype="dividend",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.requests_spent == 3
    assert summary.rows_parsed == 2100
    assert summary.stopped_reason == "completed"
    assert captured_offsets == ["0", "1000", "2000"]


@respx.mock
def test_fetcher_dividends_pagination_respects_max_requests(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """When ``max_requests`` runs out mid-cursor, the fetcher must halt
    with ``stopped_reason=max_requests_reached`` so the caller can retry
    with more budget — not silently declare completion on a partial
    drain."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")

    def _respond(request):
        # Always claim more pages — budget is what should stop us.
        return httpx.Response(
            200,
            json={
                "meta": {"total": 10_000, "offset": 0, "limit": 1000},
                "data": [_dividend_row(symbol=f"X{i}.US") for i in range(1000)],
                "links": {"next": "https://eodhd.com/api/calendar/dividends?page[offset]=next"},
            },
        )

    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        side_effect=_respond
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=7, max_requests=2,
        )
        summary = fetcher.fetch(
            subtype="dividend",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.requests_spent == 2
    assert summary.stopped_reason == "max_requests_reached"


@respx.mock
def test_fetcher_dividends_stops_on_null_next_even_if_page_is_full(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """``links.next`` is the authoritative terminator. A page that
    happens to land exactly on the 1000-row boundary with
    ``links.next=null`` is the last page, not a mid-cursor stop — the
    fetcher must not request another page just because the row count
    equals the limit."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        return_value=httpx.Response(
            200,
            json={
                "meta": {"total": 1000, "offset": 0, "limit": 1000},
                "data": [_dividend_row(symbol=f"E{i}.US") for i in range(1000)],
                "links": {"next": None},
            },
        )
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=7,
        )
        summary = fetcher.fetch(
            subtype="dividend",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.requests_spent == 1
    assert summary.rows_parsed == 1000
    assert summary.stopped_reason == "completed"


@respx.mock
def test_fetcher_dividend_symbols_route_through_filter_param(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """EODHD's /calendar/dividends uses ``filter[symbol]=X`` (singular) —
    not the generic ``symbols=A,B`` param. The fetcher must issue one
    request per symbol for dividends; otherwise the symbol filter is
    silently ignored and the caller gets the whole date range."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    captured_params: list[dict[str, str]] = []

    def _record(request):
        captured_params.append(dict(request.url.params))
        return httpx.Response(
            200,
            json={"meta": {"total": 1, "offset": 0, "limit": 1000},
                  "data": [_dividend_row()],
                  "links": {"next": None}},
        )

    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        side_effect=_record
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(
            connection=connection, client=client, window_days=30,
        )
        summary = fetcher.fetch(
            subtype="dividend",
            start=date(2026, 5, 1),
            end=date(2026, 5, 3),
            symbols=["AAPL.US", "MSFT.US"],
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()

    # Two symbols → two requests, each carrying filter[symbol]=<one>.
    assert summary.requests_spent == 2
    assert len(captured_params) == 2
    seen_symbols = {p.get("filter[symbol]") for p in captured_params}
    assert seen_symbols == {"AAPL.US", "MSFT.US"}
    # The generic `symbols` param must NOT appear — that's the bug this
    # test locks against.
    for p in captured_params:
        assert "symbols" not in p


def test_dividend_detail_upserts_existing_discovery_event(
    connection: sqlite3.Connection,
) -> None:
    """Discovery writes the thin event first; detail upserts in place so
    we end up with one cal_corp_event row carrying the richer fields
    plus two cal_corp_raw snapshots."""
    code = "AAPL.US"
    ex_date = "2026-02-09"
    # Step 1: discovery lands at t=1.
    raw_d, ev_d = parse_dividend_row(
        _dividend_row(symbol=code, date=ex_date), snapshot_epoch_ms=1_000,
    )
    store_corp_raw(connection, [raw_d])
    project_corp_events(connection, [ev_d])
    # Step 2: detail lands later at t=2 with richer fields.
    raw_x, ev_x = parse_dividend_detail_row(
        _dividend_detail_row(date=ex_date),
        code=code,
        snapshot_epoch_ms=2_000,
    )
    store_corp_raw(connection, [raw_x])
    project_corp_events(connection, [ev_x])

    (raw_count,) = connection.execute("SELECT COUNT(*) FROM cal_corp_raw").fetchone()
    (event_count,) = connection.execute("SELECT COUNT(*) FROM cal_corp_event").fetchone()
    assert raw_count == 2
    assert event_count == 1
    row = connection.execute(
        "SELECT currency, content_hash FROM cal_corp_event"
    ).fetchone()
    assert row[0] == "USD"
    assert row[1] == ev_x.content_hash


@respx.mock
def test_fetch_dividend_details_persists_and_counts(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/div/AAPL.US").mock(
        return_value=httpx.Response(
            200, json=[_dividend_detail_row(date="2026-02-09"),
                       _dividend_detail_row(date="2026-05-15", value=0.26)],
        )
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        fixed_now = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)
        summary = fetch_dividend_details(
            connection=connection,
            client=client,
            symbols=["AAPL.US"],
            dry_run=False,
            now_utc=lambda: fixed_now,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.subtype == "dividend_detail"
    assert summary.requests_spent == 1
    assert summary.rows_parsed == 2
    assert summary.rows_raw_inserted == 2
    assert summary.events_upserted == 2
    assert summary.stopped_reason == "completed"


@respx.mock
def test_fetch_dividend_details_caps_at_max_requests(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/div/").mock(
        return_value=httpx.Response(200, json=[])
    )
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        summary = fetch_dividend_details(
            connection=connection,
            client=client,
            symbols=["A.US", "B.US", "C.US", "D.US"],
            max_requests=2,
            dry_run=False,
        )
        connection.commit()
    finally:
        connection.close()
    assert summary.requests_spent == 2
    assert summary.stopped_reason == "max_requests_reached"


def test_fetch_dividend_details_dry_run_emits_no_http(
    store: SQLiteEngineStore,
) -> None:
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        with respx.mock(assert_all_called=False) as router:
            router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
            summary = fetch_dividend_details(
                connection=connection,
                client=client,
                symbols=["AAPL.US", "MSFT.US"],
                dry_run=True,
            )
            assert router.calls.call_count == 0
    finally:
        connection.close()
    assert summary.dry_run is True
    assert summary.windows_planned == 2
    assert summary.stopped_reason == "dry_run"


def test_fetcher_trend_requires_symbols(store: SQLiteEngineStore) -> None:
    connection = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        fetcher = CorpCalendarFetcher(connection=connection, client=client)
        with respx.mock(assert_all_called=False) as router:
            router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
            summary = fetcher.fetch(subtype="earnings_trend", dry_run=False)
            assert router.calls.call_count == 0
    finally:
        connection.close()
    assert summary.stopped_reason == "symbols_required_for_earnings_trend"
