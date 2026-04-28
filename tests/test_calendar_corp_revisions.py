"""Coverage for ``cal_corp_raw`` revision surfacing (issue #66).

Three layers, mirroring ``test_calendar_http_route``:

- **Storage** — direct ``SQLiteEngineStore.list_corp_revisions`` calls
  and ``list_corp_revision_versions`` for the per-event chain.
- **Service op** — ``LocalMacroDataService.invoke("list_corp_revisions")``.
- **HTTP route** — ``GET /v1/calendar/revisions`` driven by a real
  ``ThreadingHTTPServer``.

Plus a projector-level test that ``store_corp_raw`` emits a structured
log line when a new ``content_hash`` lands for an already-seen
``provider_event_id``.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from ingestion.calendar.eodhd_api.parser import CalendarCorpRawRecord
from ingestion.calendar.eodhd_api.projector import store_corp_raw
from macro_data.server import MacroDataRequestHandler
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_event_with_revisions(
    store: SQLiteEngineStore,
    *,
    provider: str = "eodhd",
    provider_event_id: str = "eodhd-earn-AAPL-20260201",
    ticker: str = "AAPL",
    subtype: str = "earnings",
    snapshot_epochs_ms: tuple[int, ...] = (
        1_768_000_000_000,  # 2026-01-09
        1_770_000_000_000,  # 2026-02-01
        1_772_000_000_000,  # 2026-02-24
    ),
) -> None:
    """Insert one ``cal_corp_event`` projection plus N raw snapshots
    each with a distinct ``content_hash``. The latest snapshot's
    payload mirrors the projection so PIT consumers stay coherent.
    """
    now = datetime.now(timezone.utc).isoformat()
    payloads = [
        json.dumps({"code": f"{ticker}.US", "eps_actual": 1.0 + i * 0.1},
                   sort_keys=True)
        for i, _ in enumerate(snapshot_epochs_ms)
    ]
    raw_rows = [
        (provider, provider_event_id, snap_ms,
         f"hash{i:02d}" + "0" * (64 - 6), payloads[i], now)
        for i, snap_ms in enumerate(snapshot_epochs_ms)
    ]
    with store._connection(commit=True) as conn:
        conn.executemany(
            """
            INSERT INTO cal_corp_raw (
                provider, provider_event_id, snapshot_epoch_ms,
                content_hash, payload_json, fetched_at
            ) VALUES (?,?,?,?,?,?)
            """,
            raw_rows,
        )
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
                provider, provider_event_id, subtype,
                "2026-02-01T12:30:00+00:00", "datetime",
                ticker, "US", "USD", "USD",
                f"{ticker} Earnings", "2026-02-01",
                "", raw_rows[-1][3], payloads[-1],
                snapshot_epochs_ms[-1], now, now,
            ),
        )


def _seed_single_snapshot_event(
    store: SQLiteEngineStore,
    *,
    provider_event_id: str = "eodhd-div-MSFT-20260315",
) -> None:
    """One event, one raw snapshot — never revised. Default
    ``min_versions=2`` must filter it out."""
    now = datetime.now(timezone.utc).isoformat()
    with store._connection(commit=True) as conn:
        conn.execute(
            """
            INSERT INTO cal_corp_raw (
                provider, provider_event_id, snapshot_epoch_ms,
                content_hash, payload_json, fetched_at
            ) VALUES (?,?,?,?,?,?)
            """,
            ("eodhd", provider_event_id, 1_770_000_000_000,
             "x" * 64, "{}", now),
        )
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
            ("eodhd", provider_event_id, "dividend",
             "2026-03-15T00:00:00+00:00", "date",
             "MSFT", "US", "USD", "USD",
             "MSFT Dividend", "2026-03-15", "",
             "x" * 64, "{}", 1_770_000_000_000, now, now),
        )


# ──────────────────────────────────────────────────────────────────────────
# Storage layer
# ──────────────────────────────────────────────────────────────────────────


def test_list_corp_revisions_empty_store(store: SQLiteEngineStore) -> None:
    items, total = store.list_corp_revisions()
    assert items == []
    assert total == 0


def test_default_min_versions_filters_unrevised(
    store: SQLiteEngineStore,
) -> None:
    _seed_event_with_revisions(store)
    _seed_single_snapshot_event(store)
    items, total = store.list_corp_revisions()
    assert total == 1
    assert items[0]["provider_event_id"] == "eodhd-earn-AAPL-20260201"
    assert items[0]["versions"] == 3
    assert items[0]["ticker"] == "AAPL"
    assert items[0]["subtype"] == "earnings"
    assert items[0]["first_snapshot_epoch_ms"] == 1_768_000_000_000
    assert items[0]["last_snapshot_epoch_ms"] == 1_772_000_000_000


def test_min_versions_one_returns_unrevised_too(
    store: SQLiteEngineStore,
) -> None:
    _seed_event_with_revisions(store)
    _seed_single_snapshot_event(store)
    items, total = store.list_corp_revisions(min_versions=1)
    assert total == 2


def test_window_filter_uses_snapshot_epoch(
    store: SQLiteEngineStore,
) -> None:
    """``from_ts`` / ``to_ts`` bound ``snapshot_epoch_ms`` *before*
    the GROUP BY — the version count must reflect revisions seen
    inside the window only."""
    _seed_event_with_revisions(store)
    # Window covers only the last two snapshots → 2 versions.
    items, total = store.list_corp_revisions(
        from_ts="2026-01-15T00:00:00+00:00",
        to_ts="2026-03-01T00:00:00+00:00",
    )
    assert total == 1
    assert items[0]["versions"] == 2
    # Window covers only the earliest → 1 version → filtered out
    # by default ``min_versions=2``.
    items, total = store.list_corp_revisions(
        from_ts="2026-01-01T00:00:00+00:00",
        to_ts="2026-01-15T00:00:00+00:00",
    )
    assert total == 0


def test_ticker_and_subtype_filters(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
    )
    items, total = store.list_corp_revisions(ticker="AAPL")
    assert total == 1
    assert items[0]["ticker"] == "AAPL"
    items, total = store.list_corp_revisions(subtype="dividend")
    assert total == 0
    items, total = store.list_corp_revisions(subtype="earnings")
    assert total == 2


def test_pagination_round_trips(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
    )
    page1, total = store.list_corp_revisions(limit=1, offset=0)
    page2, _ = store.list_corp_revisions(limit=1, offset=1)
    assert total == 2
    assert len(page1) == 1 and len(page2) == 1
    ids = {page1[0]["provider_event_id"], page2[0]["provider_event_id"]}
    assert ids == {"eodhd-earn-AAPL", "eodhd-earn-MSFT"}


def test_ordering_is_last_snapshot_desc(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
        snapshot_epochs_ms=(1_700_000_000_000, 1_701_000_000_000),
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
        snapshot_epochs_ms=(1_780_000_000_000, 1_782_000_000_000),
    )
    items, _ = store.list_corp_revisions()
    # MSFT's last snapshot is newer → sorts first.
    assert items[0]["ticker"] == "MSFT"
    assert items[1]["ticker"] == "AAPL"


def test_explain_query_plan_uses_index(store: SQLiteEngineStore) -> None:
    """Acceptance: query runs in <500ms on a populated db. Verifying
    the plan touches ``idx_cal_corp_raw_latest`` (rather than full
    SCAN) is the static check; runtime budget falls out of that."""
    _seed_event_with_revisions(store)
    with store._connection(commit=False) as conn:
        plan = conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT raw.provider, raw.provider_event_id,
                   COUNT(DISTINCT raw.content_hash) AS v
            FROM cal_corp_raw raw
            LEFT JOIN cal_corp_event evt
              ON evt.provider = raw.provider
             AND evt.provider_event_id = raw.provider_event_id
            GROUP BY raw.provider, raw.provider_event_id
            HAVING v >= 2
            """
        ).fetchall()
    detail_blob = " ".join(row["detail"] for row in plan)
    assert "idx_cal_corp_raw_latest" in detail_blob


def test_revision_versions_returns_full_chain(
    store: SQLiteEngineStore,
) -> None:
    _seed_event_with_revisions(store)
    versions, total = store.list_corp_revision_versions(
        provider="eodhd",
        provider_event_id="eodhd-earn-AAPL-20260201",
    )
    assert total == 3
    epochs = [v["snapshot_epoch_ms"] for v in versions]
    assert epochs == sorted(epochs)
    assert versions[0]["content_hash"] != versions[1]["content_hash"]
    assert json.loads(versions[0]["payload_json"])["eps_actual"] == 1.0


def test_revision_versions_unknown_event_returns_empty(
    store: SQLiteEngineStore,
) -> None:
    versions, total = store.list_corp_revision_versions(
        provider="eodhd", provider_event_id="missing",
    )
    assert versions == []
    assert total == 0


# ──────────────────────────────────────────────────────────────────────────
# Service op layer
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_envelope_shape(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(store)
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("list_corp_revisions", {})
    assert set(result.keys()) == {"data", "meta", "links"}
    assert result["meta"]["count"] == 1
    assert result["data"][0]["versions"] == 3
    assert result["links"]["next"] is None


def test_service_op_cursor_preserves_filters(
    store: SQLiteEngineStore,
) -> None:
    """Codex round-1 P2 finding: when the first page is filtered by
    ticker / subtype / from_ts / to_ts / min_versions, the cursor must
    carry those filters forward — otherwise spreading ``links.next``
    back into the op runs page 2 with default filters and clients
    skip matching rows or pull mixed ones."""
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
        snapshot_epochs_ms=(1_770_000_000_000, 1_771_000_000_000),
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL-2", ticker="AAPL",
        snapshot_epochs_ms=(1_780_000_000_000, 1_781_000_000_000),
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
    )
    svc = LocalMacroDataService(store=store)
    first = svc.invoke(
        "list_corp_revisions", {"ticker": "AAPL", "page_limit": 1},
    )
    assert first["meta"]["count"] == 2
    cursor = first["links"]["next"]
    assert cursor is not None
    assert cursor.get("ticker") == "AAPL"
    second = svc.invoke("list_corp_revisions", cursor)
    # Cursor round-trip must preserve filter — second page only sees
    # the second AAPL event, never MSFT.
    assert second["meta"]["count"] == 2
    assert all(item["ticker"] == "AAPL" for item in second["data"])


def test_service_op_pagination_cursor(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
        snapshot_epochs_ms=(1_700_000_000_000, 1_701_000_000_000),
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
        snapshot_epochs_ms=(1_780_000_000_000, 1_782_000_000_000),
    )
    svc = LocalMacroDataService(store=store)
    first = svc.invoke("list_corp_revisions", {"page_limit": 1})
    cursor = first["links"]["next"]
    assert cursor == {"page_offset": 1, "page_limit": 1}
    second = svc.invoke("list_corp_revisions", cursor)
    first_ids = {item["provider_event_id"] for item in first["data"]}
    second_ids = {item["provider_event_id"] for item in second["data"]}
    assert first_ids.isdisjoint(second_ids)


def test_service_op_rejects_invalid_from_ts(
    store: SQLiteEngineStore,
) -> None:
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("list_corp_revisions", {"from_ts": "not-a-date"})
    assert "error" in result


def test_service_op_include_versions_returns_chain(
    store: SQLiteEngineStore,
) -> None:
    _seed_event_with_revisions(store)
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_corp_revisions",
        {
            "event_id": "eodhd:eodhd-earn-AAPL-20260201",
            "include_versions": True,
        },
    )
    assert result["meta"]["count"] == 3
    assert len(result["data"]) == 3
    keys = set(result["data"][0].keys())
    assert keys == {
        "snapshot_epoch_ms", "content_hash",
        "payload_json", "fetched_at",
    }


def test_service_op_include_versions_requires_event_id(
    store: SQLiteEngineStore,
) -> None:
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_corp_revisions", {"include_versions": True},
    )
    assert "error" in result


def test_service_op_handles_blank_filters(store: SQLiteEngineStore) -> None:
    _seed_event_with_revisions(store)
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_corp_revisions",
        {"ticker": "", "subtype": "", "from_ts": "", "to_ts": ""},
    )
    assert result["meta"]["count"] == 1


# ──────────────────────────────────────────────────────────────────────────
# HTTP route
# ──────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def live_server(store: SQLiteEngineStore):
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
    return resp.status, json.loads(body) if body else {}


def test_http_route_returns_revisions_envelope(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_event_with_revisions(store)
    host, port = live_server
    status, payload = _http_get(host, port, "/v1/calendar/revisions")
    assert status == 200
    assert payload["meta"]["count"] == 1
    assert payload["data"][0]["versions"] == 3


def test_http_route_filter_by_ticker(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-AAPL", ticker="AAPL",
    )
    _seed_event_with_revisions(
        store, provider_event_id="eodhd-earn-MSFT", ticker="MSFT",
    )
    host, port = live_server
    status, payload = _http_get(
        host, port, "/v1/calendar/revisions?ticker=AAPL",
    )
    assert status == 200
    assert payload["meta"]["count"] == 1
    assert payload["data"][0]["ticker"] == "AAPL"


def test_http_route_include_versions_for_one_event(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_event_with_revisions(store)
    host, port = live_server
    status, payload = _http_get(
        host, port,
        "/v1/calendar/revisions?event_id=eodhd:eodhd-earn-AAPL-20260201"
        "&include_versions=true",
    )
    assert status == 200
    assert payload["meta"]["count"] == 3


def test_http_route_invalid_from_ts_returns_400(
    store: SQLiteEngineStore, live_server,
) -> None:
    host, port = live_server
    status, payload = _http_get(
        host, port, "/v1/calendar/revisions?from_ts=not-a-date",
    )
    assert status == 400
    assert "error" in payload


# ──────────────────────────────────────────────────────────────────────────
# Projector log-on-revision
# ──────────────────────────────────────────────────────────────────────────


def _raw_record(
    *, provider_event_id: str, snapshot_epoch_ms: int, content_hash: str,
    payload: dict | None = None, fetched_at: str | None = None,
) -> CalendarCorpRawRecord:
    return CalendarCorpRawRecord(
        provider="eodhd",
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=json.dumps(payload or {}, sort_keys=True),
        fetched_at=fetched_at or "2026-04-01T00:00:00+00:00",
    )


def test_store_corp_raw_logs_revision_when_hash_changes(
    store: SQLiteEngineStore, caplog: pytest.LogCaptureFixture,
) -> None:
    with store._connection(commit=True) as conn:
        store_corp_raw(
            conn,
            [_raw_record(
                provider_event_id="ev-x", snapshot_epoch_ms=1,
                content_hash="h1",
            )],
        )
    caplog.clear()
    with caplog.at_level(
        logging.INFO,
        logger="ingestion.calendar.eodhd_api.projector",
    ):
        with store._connection(commit=True) as conn:
            store_corp_raw(
                conn,
                [_raw_record(
                    provider_event_id="ev-x", snapshot_epoch_ms=2,
                    content_hash="h2",
                )],
            )
    revision_logs = [
        rec for rec in caplog.records
        if "corp-event revised" in rec.getMessage()
    ]
    assert len(revision_logs) == 1
    msg = revision_logs[0].getMessage()
    assert "ev-x" in msg
    assert "versions=2" in msg


def test_store_corp_raw_logs_revision_when_input_is_iterator(
    store: SQLiteEngineStore, caplog: pytest.LogCaptureFixture,
) -> None:
    """Codex round-1 P3 finding: ``store_corp_raw`` accepts
    ``Iterable``; if the function consumed the iterator twice (once
    to materialize ``rows``, once to build ``by_provider``) the
    second pass would see nothing and the revision log would never
    fire. Pass a generator to lock in the single-pass invariant."""
    with store._connection(commit=True) as conn:
        store_corp_raw(
            conn,
            iter([_raw_record(
                provider_event_id="ev-it", snapshot_epoch_ms=1,
                content_hash="h1",
            )]),
        )
    caplog.clear()
    with caplog.at_level(
        logging.INFO,
        logger="ingestion.calendar.eodhd_api.projector",
    ):
        with store._connection(commit=True) as conn:
            store_corp_raw(
                conn,
                iter([_raw_record(
                    provider_event_id="ev-it", snapshot_epoch_ms=2,
                    content_hash="h2",
                )]),
            )
    revision_logs = [
        rec for rec in caplog.records
        if "corp-event revised" in rec.getMessage()
    ]
    assert len(revision_logs) == 1


def test_store_corp_raw_no_log_for_first_snapshot(
    store: SQLiteEngineStore, caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(
        logging.INFO,
        logger="ingestion.calendar.eodhd_api.projector",
    ):
        with store._connection(commit=True) as conn:
            store_corp_raw(
                conn,
                [_raw_record(
                    provider_event_id="ev-y", snapshot_epoch_ms=1,
                    content_hash="h1",
                )],
            )
    revision_logs = [
        rec for rec in caplog.records
        if "corp-event revised" in rec.getMessage()
    ]
    assert revision_logs == []


def test_store_corp_raw_no_log_when_hash_unchanged(
    store: SQLiteEngineStore, caplog: pytest.LogCaptureFixture,
) -> None:
    """An identical snapshot is INSERT OR IGNORE'd — no new row, no
    revision log either."""
    with store._connection(commit=True) as conn:
        store_corp_raw(
            conn,
            [_raw_record(
                provider_event_id="ev-z", snapshot_epoch_ms=1,
                content_hash="h1",
            )],
        )
    caplog.clear()
    with caplog.at_level(
        logging.INFO,
        logger="ingestion.calendar.eodhd_api.projector",
    ):
        with store._connection(commit=True) as conn:
            store_corp_raw(
                conn,
                [_raw_record(
                    provider_event_id="ev-z", snapshot_epoch_ms=2,
                    content_hash="h1",
                )],
            )
    revision_logs = [
        rec for rec in caplog.records
        if "corp-event revised" in rec.getMessage()
    ]
    assert revision_logs == []
