"""Tests for ``GET /v1/calendar`` + ``list_calendar_items`` (issue #9 P7).

Three layers:

- **Storage** — direct ``SQLiteEngineStore.list_calendar_items`` calls.
- **Service op** — ``LocalMacroDataService.invoke("list_calendar_items")``.
- **HTTP route** — real ``ThreadingHTTPServer`` on a free port,
  driven by ``http.client`` so the request path exercises the query-
  parser, dispatcher, and JSON writer.

Fixture data is seeded by projecting synthetic NBS + ECB scaffolds
through the shared ``project_events``, so the test doubles as a
cross-connector smoke check on the consolidated projector.
"""

from __future__ import annotations

import json
import threading
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from ingestion.calendar.ecb_api import fetch_ecb_calendar
from ingestion.calendar.ecb_api.parser import parse_observation as parse_ecb
from ingestion.calendar.nbs_api import release_entry_to_records
from ingestion.calendar.nbs_api.parser import NBSReleaseEntry
from ingestion.calendar._official_shared.projector import (
    project_events,
    store_raw,
)
from ingestion.timeseries.sdmx._types import SDMXObservation
from macro_data.server import MacroDataRequestHandler
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_nbs_cpi(store: SQLiteEngineStore, *, months: list[int]) -> None:
    """Project N monthly NBS CPI events into the store."""
    records = []
    for m in months:
        entry = NBSReleaseEntry(
            year=2026, month=m, day=9,
            release_time_local="9:30",
            indicator="CPI", weekday_label="Fri",
            date_cell="9/Fri",
        )
        raw, event = release_entry_to_records(
            entry, snapshot_epoch_ms=1_700_000_000,
        )
        records.append((raw, event))
    with store._connection(commit=True) as conn:
        store_raw(conn, [r for r, _ in records])
        project_events(conn, [e for _, e in records])


def _seed_corporate_dividend(
    store: SQLiteEngineStore, *, ticker: str = "AAPL",
    amount: float = 0.24,
) -> None:
    """Project one EODHD-shaped dividend row into ``cal_corp_event``.

    Hand-written insert — we don't need the full EODHD parser here;
    the test only needs a corporate row with a realistic
    ``payload_json`` so the route can flatten it into ``values``.
    """
    import json as _json
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc).isoformat()
    payload = {
        "code":          f"{ticker}.US",
        "ex_date":       "2026-05-10",
        "amount":        amount,
        "currency":      "USD",
        "payment_date":  "2026-05-25",
    }
    with store._connection(commit=True) as conn:
        conn.execute(
            """
            INSERT INTO cal_corp_event (
                provider, provider_event_id, event_subtype,
                event_time_utc, event_time_precision, ticker, exchange,
                currency, currency_reporting, title, reference_date,
                source_url, content_hash, payload_json,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "eodhd",
                f"eodhd-div-{ticker}-20260510",
                "dividend",
                "2026-05-10T00:00:00+00:00",
                "date",
                ticker,
                "US",
                "USD",
                "USD",
                f"{ticker} Dividend",
                "2026-05-10",
                "https://example.test/div",
                "0" * 64,
                _json.dumps(payload, sort_keys=True),
                1_700_000_000_000,
                now,
                now,
            ),
        )


def _seed_ecb_dfr(store: SQLiteEngineStore, *, value: float = 4.0) -> None:
    obs = SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date="2024-06-12",
        value=value,
        dataflow="FM",
    )
    raw, event = parse_ecb(obs, snapshot_epoch_ms=1_700_000_000_000)
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [event])


# ──────────────────────────────────────────────────────────────────────────
# Storage: list_calendar_items
# ──────────────────────────────────────────────────────────────────────────


def test_list_calendar_items_empty_store_returns_empty(
    store: SQLiteEngineStore,
) -> None:
    items, total = store.list_calendar_items()
    assert items == []
    assert total == 0


def test_list_calendar_items_returns_all_without_filters(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2, 3])
    _seed_ecb_dfr(store)
    items, total = store.list_calendar_items()
    assert total == 4
    assert len(items) == 4
    providers = sorted({item["provider"] for item in items})
    assert providers == ["ecb", "nbs"]


def test_list_calendar_items_filters_by_provider(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    _seed_ecb_dfr(store)
    items, total = store.list_calendar_items(provider="nbs")
    assert total == 2
    assert all(item["provider"] == "nbs" for item in items)


def test_list_calendar_items_filters_by_country(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    _seed_ecb_dfr(store)
    items, total = store.list_calendar_items(country="CN")
    assert total == 2
    assert all(item["country"] == "CN" for item in items)


def test_list_calendar_items_filters_by_domain(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1])
    _seed_ecb_dfr(store)
    items, total = store.list_calendar_items(domain="economic")
    assert total == 2
    assert all(item["domain"] == "economic" for item in items)


def test_list_calendar_items_pagination_round_trips(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2, 3, 4, 5])
    first, total = store.list_calendar_items(limit=2, offset=0)
    second, _ = store.list_calendar_items(limit=2, offset=2)
    third, _ = store.list_calendar_items(limit=2, offset=4)
    assert total == 5
    assert len(first) == 2 and len(second) == 2 and len(third) == 1
    ids = [i["event_id"] for i in (*first, *second, *third)]
    # Stable ordering — all five monthly releases show up exactly once.
    assert len(set(ids)) == 5


def test_corporate_row_exposes_payload_values(
    store: SQLiteEngineStore,
) -> None:
    """Corporate rows keep their subtype-specific fields in
    ``cal_corp_event.payload_json``; the list method must flatten
    scalar keys into ``values`` so HTTP clients see the full
    CalendarItem contract (Codex P7 round-1 finding)."""
    _seed_corporate_dividend(store, ticker="AAPL", amount=0.24)
    items, total = store.list_calendar_items(
        domain="corporate", ticker="AAPL",
    )
    assert total == 1
    values = items[0]["values"]
    assert values["amount"] == "0.24"
    assert values["ex_date"] == "2026-05-10"
    assert values["currency"] == "USD"
    assert values["payment_date"] == "2026-05-25"
    assert items[0]["ticker"] == "AAPL"
    assert items[0]["subtype"] == "dividend"


def test_list_calendar_items_limit_is_clamped(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1])
    items, _ = store.list_calendar_items(limit=0)
    # Limit 0 clamped to 1; one row matches so one is returned.
    assert len(items) == 1
    items, _ = store.list_calendar_items(limit=10_000)
    assert len(items) == 1


# ──────────────────────────────────────────────────────────────────────────
# Service op: list_calendar_items
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_envelope_shape(store: SQLiteEngineStore) -> None:
    _seed_nbs_cpi(store, months=[1, 2, 3])
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("list_calendar_items", {"page_limit": 2})
    assert set(result.keys()) == {"data", "meta", "links"}
    assert result["meta"] == {"count": 3, "offset": 0, "limit": 2}
    assert len(result["data"]) == 2
    assert result["links"]["next"] == {"page_offset": 2, "page_limit": 2}


def test_service_op_cursor_round_trip_advances_pages(
    store: SQLiteEngineStore,
) -> None:
    """A client that spreads ``links.next`` back into the op's args
    must land on the second page — cursor keys therefore must be the
    same names the op reads (``page_offset`` / ``page_limit``)."""
    _seed_nbs_cpi(store, months=[1, 2, 3, 4, 5])
    svc = LocalMacroDataService(store=store)
    first = svc.invoke("list_calendar_items", {"page_limit": 2})
    cursor = first["links"]["next"]
    assert cursor is not None
    second = svc.invoke("list_calendar_items", cursor)
    assert second["meta"]["offset"] == 2
    # No event id overlap — the second page must be a different slice.
    first_ids = {item["event_id"] for item in first["data"]}
    second_ids = {item["event_id"] for item in second["data"]}
    assert first_ids.isdisjoint(second_ids)


def test_service_op_last_page_has_null_next(store: SQLiteEngineStore) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("list_calendar_items", {"page_limit": 100})
    assert result["links"]["next"] is None


def test_service_op_applies_filters(store: SQLiteEngineStore) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    _seed_ecb_dfr(store)
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_calendar_items",
        {"provider": "ecb", "page_limit": 100},
    )
    assert result["meta"]["count"] == 1
    assert result["data"][0]["provider"] == "ecb"


def test_service_op_handles_blank_filters(store: SQLiteEngineStore) -> None:
    _seed_nbs_cpi(store, months=[1])
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_calendar_items",
        {"domain": "", "country": "", "ticker": ""},
    )
    assert result["meta"]["count"] == 1


def test_service_op_handles_non_int_pagination(
    store: SQLiteEngineStore,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_calendar_items",
        {"page_offset": "junk", "page_limit": "junk"},
    )
    # Fallbacks kick in — defaults are (0, 100).
    assert result["meta"] == {"count": 2, "offset": 0, "limit": 100}


# ──────────────────────────────────────────────────────────────────────────
# HTTP route: GET /v1/calendar
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def live_server(store: SQLiteEngineStore):
    """Start a real ThreadingHTTPServer on a free port, yield its
    (host, port) tuple, then shut down cleanly."""
    svc = LocalMacroDataService(store=store)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MacroDataRequestHandler)
    httpd.service = svc  # type: ignore[attr-defined]
    httpd.api_token = ""  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    try:
        yield host, port
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _http_get(host: str, port: int, path: str) -> tuple[int, dict]:
    conn = HTTPConnection(host, port, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8")
    finally:
        conn.close()
    parsed = json.loads(body) if body else {}
    return resp.status, parsed


def test_http_route_returns_full_envelope(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2, 3])
    host, port = live_server
    status, payload = _http_get(host, port, "/v1/calendar")
    assert status == 200
    assert payload["meta"]["count"] == 3
    assert len(payload["data"]) == 3
    assert payload["links"]["next"] is None


def test_http_route_parses_jsonapi_pagination(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2, 3, 4, 5])
    host, port = live_server
    status, payload = _http_get(
        host, port, "/v1/calendar?page%5Boffset%5D=2&page%5Blimit%5D=2",
    )
    assert status == 200
    assert payload["meta"] == {"count": 5, "offset": 2, "limit": 2}
    assert len(payload["data"]) == 2
    assert payload["links"]["next"] == {"page_offset": 4, "page_limit": 2}


def test_http_route_applies_query_filters(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_nbs_cpi(store, months=[1, 2])
    _seed_ecb_dfr(store)
    host, port = live_server
    status, payload = _http_get(host, port, "/v1/calendar?provider=ecb")
    assert status == 200
    assert payload["meta"]["count"] == 1
    assert payload["data"][0]["provider"] == "ecb"


def test_http_route_empty_result_still_has_envelope(
    store: SQLiteEngineStore, live_server,
) -> None:
    host, port = live_server
    status, payload = _http_get(host, port, "/v1/calendar?provider=unknown")
    assert status == 200
    assert payload["meta"]["count"] == 0
    assert payload["data"] == []
    assert payload["links"]["next"] is None


def test_http_route_unknown_path_returns_404(
    store: SQLiteEngineStore, live_server,
) -> None:
    host, port = live_server
    status, payload = _http_get(host, port, "/v1/does-not-exist")
    assert status == 404
    assert "error" in payload


def test_healthz_still_responds(
    store: SQLiteEngineStore, live_server,
) -> None:
    # Safety rail — the new route must not shadow existing handlers.
    host, port = live_server
    status, payload = _http_get(host, port, "/healthz")
    assert status == 200
    assert payload == {"status": "ok"}
