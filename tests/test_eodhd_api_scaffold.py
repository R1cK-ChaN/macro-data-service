"""Scaffold tests for the EODHD Calendar API fetcher (issue #8 slice 3).

Everything is mocked — these tests never call EODHD. Coverage:

- Parser: subtype round-trip for all five endpoints, content-hash
  revision detection, identity stability vs. mutable-field changes.
- Projector: idempotent raw inserts, upsert honors observed_at.
- Client: 429 retry exhaustion, fmt=json auto-inject.
- Fetcher: dry-run emits no HTTP, window slicing, trend symbols guard.
- Service op: dry_run=True returns plan without HTTP.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from ingestion.calendar.eodhd_api import (
    CorpCalendarFetcher,
    EODHDAPIClient,
    EODHDAuthMissing,
    SUBTYPES,
    fetch_dividend_details,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
    project_corp_events,
    store_corp_raw,
    synthesize_provider_event_id,
)
from ingestion.calendar.eodhd_api.client import EODHDThrottled
from storage.sqlite import SQLiteEngineStore


# ──────────────────────────────────────────────────────────────────────────
# Fixtures — sample rows modelled on EODHD response shapes.
# ──────────────────────────────────────────────────────────────────────────


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


def _ipo_row(**overrides):
    base = {
        "code": "NEWCO.US",
        "name": "New Company Inc",
        "exchange": "NASDAQ",
        "currency": "USD",
        "start_date": "2026-06-15",
        "filing_date": "2026-04-01",
        "amended_date": None,
        "price_from": 18.0,
        "price_to": 22.0,
        "offer_price": 20.0,
        "shares": 10_000_000,
        "deal_type": "Expected",
    }
    base.update(overrides)
    return base


def _split_row(**overrides):
    base = {
        "code": "NVDA.US",
        "split_date": "2026-06-10",
        "optionable": "Y",
        "old_shares": 1,
        "new_shares": 10,
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


# ──────────────────────────────────────────────────────────────────────────
# Subtype registry
# ──────────────────────────────────────────────────────────────────────────


def test_subtype_registry_matches_schema_check() -> None:
    assert SUBTYPES == {"earnings", "earnings_trend", "ipo", "split", "dividend"}


# ──────────────────────────────────────────────────────────────────────────
# Parser — earnings
# ──────────────────────────────────────────────────────────────────────────


def test_parse_earnings_round_trip() -> None:
    row = _earnings_row()
    raw, event = parse_earnings_row(row, snapshot_epoch_ms=1_700_000_000_000)
    assert event.event_subtype == "earnings"
    assert event.ticker == "AAPL"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-05-01"
    assert event.event_time_precision == "date"
    assert event.currency == "USD"
    assert event.reference_date == "2026-04-30"
    # payload_json preserves every input field.
    for k, v in row.items():
        assert json.loads(raw.payload_json)[k] == v


def test_earnings_id_stable_across_value_revision() -> None:
    baseline = _earnings_row()
    revision = _earnings_row(actual=1.60, difference=0.10, percent=6.0)
    _, ev_a = parse_earnings_row(baseline, snapshot_epoch_ms=1)
    _, ev_b = parse_earnings_row(revision, snapshot_epoch_ms=2)
    # Same code + report_date + baf → same provider_event_id
    assert ev_a.provider_event_id == ev_b.provider_event_id
    # But the content hash moved (actual changed)
    assert ev_a.content_hash != ev_b.content_hash


def test_earnings_id_flips_when_before_after_market_changes() -> None:
    morning = _earnings_row(before_after_market="BeforeMarket")
    evening = _earnings_row(before_after_market="AfterMarket")
    _, ev_m = parse_earnings_row(morning, snapshot_epoch_ms=1)
    _, ev_e = parse_earnings_row(evening, snapshot_epoch_ms=1)
    assert ev_m.provider_event_id != ev_e.provider_event_id


def test_earnings_rejects_missing_dates() -> None:
    with pytest.raises(ValueError):
        parse_earnings_row({"code": "AAPL.US"}, snapshot_epoch_ms=0)


# ──────────────────────────────────────────────────────────────────────────
# Parser — trends / IPOs / splits / dividends
# ──────────────────────────────────────────────────────────────────────────


def test_trend_id_forks_by_period() -> None:
    q0 = _trend_row(period="0q")
    q1 = _trend_row(period="+1q")
    _, ev_q0 = parse_trend_row(q0, snapshot_epoch_ms=1)
    _, ev_q1 = parse_trend_row(q1, snapshot_epoch_ms=1)
    assert ev_q0.provider_event_id != ev_q1.provider_event_id
    assert ev_q0.event_subtype == "earnings_trend"
    assert ev_q0.reference_date == "0q"


def test_ipo_primary_date_falls_back_to_filing_date() -> None:
    row = _ipo_row(start_date=None)
    _, event = parse_ipo_row(row, snapshot_epoch_ms=1)
    assert event.event_time_utc == "2026-04-01"
    assert event.event_time_precision == "approximate"  # deal_type=Expected


def test_ipo_precision_date_when_priced() -> None:
    row = _ipo_row(deal_type="Priced")
    _, event = parse_ipo_row(row, snapshot_epoch_ms=1)
    assert event.event_time_precision == "date"


def test_ipo_id_stable_when_start_date_appears_later() -> None:
    """At filing time EODHD ships filing_date only; start_date arrives
    later. The event id must anchor on filing_date so the same listing
    keeps a single cal_corp_event row instead of forking into two."""
    early = _ipo_row(start_date=None, amended_date=None)  # filing stage
    later = _ipo_row(start_date="2026-06-15")             # start_date populated
    _, ev_early = parse_ipo_row(early, snapshot_epoch_ms=1)
    _, ev_later = parse_ipo_row(later, snapshot_epoch_ms=2)
    assert ev_early.provider_event_id == ev_later.provider_event_id
    # The projected event_time_utc still advances to the best-known date.
    assert ev_early.event_time_utc == "2026-04-01"
    assert ev_later.event_time_utc == "2026-06-15"


def test_ipo_id_independent_of_deal_type() -> None:
    """IPO id must stay stable across the Filed → Priced lifecycle so the
    two raw snapshots land on the same event row rather than forking
    into two different events."""
    filed = _ipo_row(deal_type="Filed")
    priced = _ipo_row(deal_type="Priced")
    _, ev_f = parse_ipo_row(filed, snapshot_epoch_ms=1)
    _, ev_p = parse_ipo_row(priced, snapshot_epoch_ms=2)
    assert ev_f.provider_event_id == ev_p.provider_event_id
    assert ev_f.content_hash != ev_p.content_hash  # lifecycle flipped


def test_split_ratio_encoded_in_id() -> None:
    one_ten = _split_row(old_shares=1, new_shares=10)
    two_ten = _split_row(old_shares=2, new_shares=10)
    _, ev_a = parse_split_row(one_ten, snapshot_epoch_ms=1)
    _, ev_b = parse_split_row(two_ten, snapshot_epoch_ms=1)
    assert ev_a.provider_event_id != ev_b.provider_event_id


def test_dividend_parses_discovery_only_shape() -> None:
    """Live EODHD /calendar/dividends returns just {symbol, date} — no
    value/period/currency/declaration/record/payment dates. The parser
    must accept that minimal shape without raising, and the resulting
    event row must leave currency/reference_date empty (the richer
    fields are sourced from /api/div/{TICKER} in a follow-up slice)."""
    row = _dividend_row()
    _raw, event = parse_dividend_row(row, snapshot_epoch_ms=1)
    assert event.ticker == "MSFT"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-05-15"
    assert event.event_subtype == "dividend"
    assert event.currency == ""
    assert event.reference_date is None


def test_dividend_id_stable_for_symbol_date_pair() -> None:
    """(symbol, date) is the natural key for a dividend calendar entry.
    Two identical rows produce the same provider_event_id; changing
    either component forks it."""
    a = _dividend_row(symbol="AAPL.US", date="2026-02-09")
    b = _dividend_row(symbol="AAPL.US", date="2026-02-09")
    c = _dividend_row(symbol="AAPL.US", date="2026-05-15")  # different date
    _, ev_a = parse_dividend_row(a, snapshot_epoch_ms=1)
    _, ev_b = parse_dividend_row(b, snapshot_epoch_ms=1)
    _, ev_c = parse_dividend_row(c, snapshot_epoch_ms=1)
    assert ev_a.provider_event_id == ev_b.provider_event_id
    assert ev_a.provider_event_id != ev_c.provider_event_id


def test_synthesize_provider_event_id_is_deterministic() -> None:
    a = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="AfterMarket"
    )
    b = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="AfterMarket"
    )
    assert a == b
    c = synthesize_provider_event_id(
        subtype="earnings", code="AAPL.US", primary_date="2026-05-01", subtype_key="BeforeMarket"
    )
    assert a != c


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_corp_raw_is_idempotent(connection: sqlite3.Connection) -> None:
    raw, _event = parse_earnings_row(_earnings_row(), snapshot_epoch_ms=1)
    assert store_corp_raw(connection, [raw]) == 1
    assert store_corp_raw(connection, [raw]) == 0  # same hash ignored
    (count,) = connection.execute("SELECT COUNT(*) FROM cal_corp_raw").fetchone()
    assert count == 1


def test_project_corp_events_upsert_on_revision(connection: sqlite3.Connection) -> None:
    raw1, ev1 = parse_earnings_row(_earnings_row(), snapshot_epoch_ms=1_000_000_000_000)
    raw2, ev2 = parse_earnings_row(
        _earnings_row(actual=1.60, percent=6.0),
        snapshot_epoch_ms=2_000_000_000_000,
    )
    store_corp_raw(connection, [raw1])
    project_corp_events(connection, [ev1])
    store_corp_raw(connection, [raw2])
    project_corp_events(connection, [ev2])
    (raw_count,) = connection.execute("SELECT COUNT(*) FROM cal_corp_raw").fetchone()
    (event_count,) = connection.execute("SELECT COUNT(*) FROM cal_corp_event").fetchone()
    assert raw_count == 2
    assert event_count == 1
    (hash_after,) = connection.execute(
        "SELECT content_hash FROM cal_corp_event"
    ).fetchone()
    assert hash_after == ev2.content_hash


def test_project_rejects_older_snapshot(connection: sqlite3.Connection) -> None:
    _raw_new, ev_new = parse_earnings_row(
        _earnings_row(actual=1.55), snapshot_epoch_ms=2_000_000_000_000
    )
    _raw_old, ev_old = parse_earnings_row(
        _earnings_row(actual=1.40), snapshot_epoch_ms=1_000_000_000_000
    )
    project_corp_events(connection, [ev_new])
    project_corp_events(connection, [ev_old])
    row = connection.execute(
        "SELECT payload_json FROM cal_corp_event"
    ).fetchone()
    payload = json.loads(row[0])
    assert payload["actual"] == 1.55  # newer snapshot wins


def test_v_calendar_item_projects_corporate_rows(connection: sqlite3.Connection) -> None:
    """Sanity-check the unified view surfaces eodhd rows on the corporate lane."""
    _raw, event = parse_earnings_row(_earnings_row(), snapshot_epoch_ms=1)
    project_corp_events(connection, [event])
    row = connection.execute(
        "SELECT domain, subtype, provider, ticker FROM v_calendar_item WHERE domain='corporate'"
    ).fetchone()
    assert row is not None
    assert row[0] == "corporate"
    assert row[1] == "earnings"
    assert row[2] == "eodhd"
    assert row[3] == "AAPL"


# ──────────────────────────────────────────────────────────────────────────
# Client — auth + retry
# ──────────────────────────────────────────────────────────────────────────


def test_client_auth_missing_raised_only_on_call(monkeypatch) -> None:
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    from env import DEFAULT_ENV_FILES, clear_env_cache
    monkeypatch.setattr("env.DEFAULT_ENV_FILES", ())
    clear_env_cache()
    client = EODHDAPIClient()
    with pytest.raises(EODHDAuthMissing):
        client.get("/api/calendar/earnings")


@respx.mock
def test_client_retries_on_429(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    route = respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        side_effect=[
            httpx.Response(429, text=""),
            httpx.Response(200, json={"earnings": [_earnings_row()]}),
        ]
    )
    client = EODHDAPIClient(sleeper=lambda _s: None)
    result = client.get_rows(
        "/api/calendar/earnings", params={"from": "2026-05-01", "to": "2026-05-02"},
        rows_key="earnings",
    )
    assert route.call_count == 2
    assert len(result.rows) == 1


@respx.mock
def test_client_exhausts_429_and_raises(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit-test")
    respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        return_value=httpx.Response(429, text="")
    )
    client = EODHDAPIClient(sleeper=lambda _s: None, max_retries=1)
    with pytest.raises(EODHDThrottled):
        client.get("/api/calendar/earnings")


@respx.mock
def test_client_injects_fmt_json_and_api_token(monkeypatch) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "secret-42")
    captured = {}

    def _record(request):
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={"earnings": []})

    respx.get(url__startswith="https://eodhd.com/api/calendar/earnings").mock(
        side_effect=_record
    )
    client = EODHDAPIClient(sleeper=lambda _s: None)
    client.get("/api/calendar/earnings", params={"from": "2026-05-01"})
    assert captured["params"]["api_token"] == "secret-42"
    assert captured["params"]["fmt"] == "json"
    assert captured["params"]["from"] == "2026-05-01"


# ──────────────────────────────────────────────────────────────────────────
# Fetcher — dry-run, execution, subtype guard
# ──────────────────────────────────────────────────────────────────────────


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


@respx.mock
def test_dividend_detail_parser_populates_extended_fields() -> None:
    row = _dividend_detail_row()
    raw, event = parse_dividend_detail_row(
        row, code="AAPL.US", snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.event_subtype == "dividend"
    assert event.ticker == "AAPL"
    assert event.exchange == "US"
    assert event.event_time_utc == "2026-02-09"
    assert event.currency == "USD"
    assert event.reference_date == "Quarterly"
    # Raw payload preserves every detail field for downstream extraction.
    payload = json.loads(raw.payload_json)
    for k in ("value", "unadjustedValue", "declarationDate", "recordDate", "paymentDate"):
        assert payload[k] == row[k]


def test_dividend_detail_shares_identity_with_discovery_row() -> None:
    """Detail parser must produce the same provider_event_id the
    discovery parser produced for the same (symbol, ex_date) — that is
    how the enrichment snapshot lands on the existing cal_corp_event
    row instead of forking into a new event."""
    code = "AAPL.US"
    ex_date = "2026-02-09"
    discovery = _dividend_row(symbol=code, date=ex_date)
    detail = _dividend_detail_row(date=ex_date)
    _, ev_d = parse_dividend_row(discovery, snapshot_epoch_ms=1)
    _, ev_x = parse_dividend_detail_row(detail, code=code, snapshot_epoch_ms=2)
    assert ev_d.provider_event_id == ev_x.provider_event_id
    # Content hashes differ — discovery hashes {date}, detail hashes 8
    # extended fields — so both land as separate cal_corp_raw snapshots.
    assert ev_d.content_hash != ev_x.content_hash


def test_dividend_detail_handles_minimal_shape() -> None:
    """Smaller tickers only return {date, value}. Parser must not raise
    on the missing extended fields; currency/reference_date stay empty."""
    row = {"date": "2026-02-09", "value": 0.05}
    _raw, event = parse_dividend_detail_row(
        row, code="SMALL.US", snapshot_epoch_ms=1,
    )
    assert event.currency == ""
    assert event.reference_date is None


def test_dividend_detail_requires_code_and_date() -> None:
    with pytest.raises(ValueError):
        parse_dividend_detail_row({"date": "2026-02-09"}, code="", snapshot_epoch_ms=1)
    with pytest.raises(ValueError):
        parse_dividend_detail_row({}, code="AAPL.US", snapshot_epoch_ms=1)


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


def test_calendar_corp_fetch_dividend_details_rejects_missing_symbols(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke("calendar_corp_fetch_dividend_details", {})
    assert "error" in result


def test_calendar_corp_fetch_dividend_details_rejects_malformed_dates(
    store: SQLiteEngineStore,
) -> None:
    """A typo in from/to must fail loudly — silently dropping the bound
    would turn /api/div/{symbol} into a full-history pull for every
    requested symbol."""
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_fetch_dividend_details",
        {"symbols": ["AAPL.US"], "from": "2026-02-30", "dry_run": True},
    )
    assert "error" in result
    assert "from" in result["error"]


def test_calendar_corp_fetch_dividend_details_dry_run(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_corp_fetch_dividend_details",
            {"symbols": ["AAPL.US", "MSFT.US"], "dry_run": True},
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["subtype"] == "dividend_detail"
    assert result["symbols_planned"] == 2
    assert result["stopped_reason"] == "dry_run"


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


# ──────────────────────────────────────────────────────────────────────────
# Service op — dry run
# ──────────────────────────────────────────────────────────────────────────


def test_calendar_corp_fetch_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_corp_fetch",
            {
                "subtype": "earnings",
                "from": "2026-05-01",
                "to": "2026-05-14",
                "dry_run": True,
            },
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["subtype"] == "earnings"
    assert result["windows_planned"] >= 1
    assert result["stopped_reason"] == "dry_run"


def test_calendar_corp_fetch_rejects_missing_subtype(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke("calendar_corp_fetch", {})
    assert "error" in result
