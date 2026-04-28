"""Point-in-time (``as_of``) coverage on ``GET /v1/calendar`` (issue #65).

Three layers, mirroring ``test_calendar_http_route``:

- **Storage** — direct ``SQLiteEngineStore.list_calendar_items(as_of=…)``;
  econ rows must reflect the vintage active at the cutoff.
- **Service op** — future ``as_of`` rejection (HTTP 400 path), corp
  fallback meta flag.
- **HTTP route** — ``as_of`` query-string flows through.

The fixtures stage a single NBS CPI release and append two vintages
spaced a day apart so the PIT branch has a non-trivial restatement to
resolve. A separate corp dividend fixture exercises the C1 fallback.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from ingestion.calendar.nbs_api import release_entry_to_records
from ingestion.calendar.nbs_api.parser import NBSReleaseEntry
from ingestion.calendar._official_shared.projector import (
    project_events,
    store_raw,
)
from macro_data.server import MacroDataRequestHandler
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_revised_econ_event(
    store: SQLiteEngineStore,
) -> tuple[str, str]:
    """Seed one NBS CPI event with two vintages — original and revision.

    Returns ``(provider_event_id, provider)`` so callers can correlate
    the seeded row to the vintage rows.

    Vintage 1 (``2026-01-15T09:30:00+00:00``) — actual=2.0, the wire
    value the day of release.
    Vintage 2 (``2026-02-20T09:30:00+00:00``) — actual=1.8, the
    revision a month later.
    """
    entry = NBSReleaseEntry(
        year=2026, month=1, day=15,
        release_time_local="9:30",
        indicator="CPI", weekday_label="Thu",
        date_cell="15/Thu",
    )
    raw, event = release_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [event])
        # Two vintages — original then a revision a month later.
        conn.execute(
            "INSERT INTO calendar_event_vintages ("
            "event_id, provider, vintage_date, observed_at, "
            "actual, forecast, previous, metadata_json, scraped_at, "
            "source_url) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                event.provider_event_id, event.provider,
                "2026-01-15T09:30:00+00:00",
                "2026-01-15T09:30:00+00:00",
                "2.0", "1.9", "1.7", "{}",
                "2026-01-15T09:30:00+00:00",
                "https://example.test/v1",
            ),
        )
        conn.execute(
            "INSERT INTO calendar_event_vintages ("
            "event_id, provider, vintage_date, observed_at, "
            "actual, forecast, previous, metadata_json, scraped_at, "
            "source_url) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                event.provider_event_id, event.provider,
                "2026-02-20T09:30:00+00:00",
                "2026-02-20T09:30:00+00:00",
                "1.8", "1.9", "1.7", "{}",
                "2026-02-20T09:30:00+00:00",
                "https://example.test/v2",
            ),
        )
    return event.provider_event_id, event.provider


def _seed_corp_dividend(store: SQLiteEngineStore) -> None:
    """Project one EODHD-shaped dividend row plus a single matching
    raw snapshot. Anchor for the C2-flag-removal regression.
    """
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc).isoformat()
    payload = {
        "code": "AAPL.US", "ex_date": "2026-05-10",
        "amount": 0.24, "currency": "USD",
        "payment_date": "2026-05-25",
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
                "eodhd", "eodhd-div-AAPL-20260510", "dividend",
                "2026-05-10T00:00:00+00:00", "date",
                "AAPL", "US", "USD", "USD",
                "AAPL Dividend", "2026-05-10",
                "https://example.test/div",
                "0" * 64, json.dumps(payload, sort_keys=True),
                1_700_000_000_000, now, now,
            ),
        )
        conn.execute(
            """
            INSERT INTO cal_corp_raw (
                provider, provider_event_id, snapshot_epoch_ms,
                content_hash, payload_json, fetched_at
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                "eodhd", "eodhd-div-AAPL-20260510",
                1_700_000_000_000, "0" * 64,
                json.dumps(payload, sort_keys=True), now,
            ),
        )


def _seed_corp_dividend_with_revision(
    store: SQLiteEngineStore,
) -> None:
    """Two raw snapshots for one dividend — original 0.24 then a
    later restatement to 0.30. Drives the C2 corp-PIT specs.

    Snapshot epochs:
    - ``2026-04-01T00:00:00+00:00`` (1838332800000) → amount=0.24
    - ``2026-05-15T00:00:00+00:00`` (1842220800000) → amount=0.30

    Projection mirrors the latest snapshot (amount=0.30).
    """
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc).isoformat()
    payload_v1 = {
        "code": "AAPL.US", "ex_date": "2026-05-10",
        "amount": 0.24, "currency": "USD",
        "payment_date": "2026-05-25",
    }
    payload_v2 = {**payload_v1, "amount": 0.30}
    snap_v1_ms = int(
        _dt(2026, 4, 1, tzinfo=_tz.utc).timestamp() * 1000,
    )
    snap_v2_ms = int(
        _dt(2026, 5, 15, tzinfo=_tz.utc).timestamp() * 1000,
    )
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
                "eodhd", "eodhd-div-AAPL-20260510", "dividend",
                "2026-05-10T00:00:00+00:00", "date",
                "AAPL", "US", "USD", "USD",
                "AAPL Dividend", "2026-05-10",
                "https://example.test/div",
                "h2" * 32, json.dumps(payload_v2, sort_keys=True),
                snap_v2_ms, now, now,
            ),
        )
        conn.executemany(
            """
            INSERT INTO cal_corp_raw (
                provider, provider_event_id, snapshot_epoch_ms,
                content_hash, payload_json, fetched_at
            ) VALUES (?,?,?,?,?,?)
            """,
            [
                (
                    "eodhd", "eodhd-div-AAPL-20260510",
                    snap_v1_ms, "h1" * 32,
                    json.dumps(payload_v1, sort_keys=True), now,
                ),
                (
                    "eodhd", "eodhd-div-AAPL-20260510",
                    snap_v2_ms, "h2" * 32,
                    json.dumps(payload_v2, sort_keys=True), now,
                ),
            ],
        )


# ──────────────────────────────────────────────────────────────────────────
# Storage layer
# ──────────────────────────────────────────────────────────────────────────


def test_econ_as_of_returns_vintage_at_cutoff(
    store: SQLiteEngineStore,
) -> None:
    _seed_revised_econ_event(store)
    items, total = store.list_calendar_items(
        as_of="2026-02-01T00:00:00+00:00",
    )
    assert total == 1
    item = items[0]
    # Cutoff is between v1 and v2 — must see v1's actual=2.0, not the
    # revised 1.8. ``last_update_epoch_ms`` aligns with v1's
    # ``observed_at``.
    assert item["values"]["actual"] == "2.0"
    assert item["values"]["forecast"] == "1.9"
    assert item["values"]["previous"] == "1.7"
    v1_epoch = int(
        datetime(2026, 1, 15, 9, 30, tzinfo=timezone.utc).timestamp()
        * 1000
    )
    assert item["last_update_epoch_ms"] == v1_epoch
    assert item["source_url"] == "https://example.test/v1"


def test_econ_as_of_after_revision_returns_revised_values(
    store: SQLiteEngineStore,
) -> None:
    _seed_revised_econ_event(store)
    items, _ = store.list_calendar_items(
        as_of="2026-03-01T00:00:00+00:00",
    )
    assert items[0]["values"]["actual"] == "1.8"


def test_econ_as_of_before_release_drops_econ_values(
    store: SQLiteEngineStore,
) -> None:
    """Cutoff before the first vintage — the row didn't exist on the
    wire yet, so econ value-bearing fields must be absent.

    The shared projector writes a discovery vintage at the snapshot
    epoch (``1_700_000_000_000`` ms ≈ 2023-11-14) on first call, so the
    cutoff has to predate that for the no-vintage path to fire.
    """
    _seed_revised_econ_event(store)
    items, total = store.list_calendar_items(
        as_of="2020-01-01T00:00:00+00:00",
    )
    assert total == 1
    item = items[0]
    assert "actual" not in item["values"]
    assert "forecast" not in item["values"]
    assert "previous" not in item["values"]
    assert item["last_update_epoch_ms"] is None


def test_corp_as_of_returns_pre_revision_snapshot(
    store: SQLiteEngineStore,
) -> None:
    """C2: cutoff between v1 (amount=0.24) and v2 (amount=0.30) must
    surface the original snapshot, not the latest projection."""
    _seed_corp_dividend_with_revision(store)
    items, total = store.list_calendar_items(
        as_of="2026-04-15T00:00:00+00:00",
    )
    assert total == 1
    item = items[0]
    assert item["values"]["amount"] == "0.24"
    assert item["last_update_epoch_ms"] == int(
        datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp() * 1000
    )


def test_corp_as_of_after_revision_returns_revised(
    store: SQLiteEngineStore,
) -> None:
    """Cutoff after the restatement returns the revised snapshot —
    matches the latest projection."""
    _seed_corp_dividend_with_revision(store)
    items, _ = store.list_calendar_items(
        as_of="2026-06-01T00:00:00+00:00",
    )
    assert items[0]["values"]["amount"] == "0.3"


def test_corp_as_of_before_first_snapshot_drops_payload(
    store: SQLiteEngineStore,
) -> None:
    """Cutoff before any snapshot — payload-derived ``values`` must be
    absent. Structural columns (ticker, event_time_utc) stay so the
    row still identifies the event."""
    _seed_corp_dividend_with_revision(store)
    items, _ = store.list_calendar_items(
        as_of="2025-01-01T00:00:00+00:00",
    )
    item = items[0]
    assert item["ticker"] == "AAPL"
    assert "amount" not in item["values"]
    assert "ex_date" not in item["values"]
    assert item["last_update_epoch_ms"] is None


def test_corp_as_of_overrides_currency_from_snapshot(
    store: SQLiteEngineStore,
) -> None:
    """Top-level ``currency`` must follow the snapshot's payload —
    EODHD marks the field mutable across snapshots, so without the
    override a corp PIT response would leak the latest projection's
    corrected currency while values come from the older snapshot
    (Codex P2 finding on the C2 review).
    """
    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc).isoformat()
    snap_v1_ms = int(
        _dt(2026, 4, 1, tzinfo=_tz.utc).timestamp() * 1000,
    )
    snap_v2_ms = int(
        _dt(2026, 5, 15, tzinfo=_tz.utc).timestamp() * 1000,
    )
    payload_v1 = {
        "code": "AAPL.US", "ex_date": "2026-05-10",
        "amount": 0.24, "currency": "GBP",
        "payment_date": "2026-05-25",
    }
    payload_v2 = {**payload_v1, "currency": "USD"}
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
                "eodhd", "eodhd-div-AAPL-20260510", "dividend",
                "2026-05-10T00:00:00+00:00", "date",
                "AAPL", "US", "USD", "USD",
                "AAPL Dividend", "2026-05-10",
                "https://example.test/div",
                "h2" * 32, json.dumps(payload_v2, sort_keys=True),
                snap_v2_ms, now, now,
            ),
        )
        conn.executemany(
            """
            INSERT INTO cal_corp_raw (
                provider, provider_event_id, snapshot_epoch_ms,
                content_hash, payload_json, fetched_at
            ) VALUES (?,?,?,?,?,?)
            """,
            [
                ("eodhd", "eodhd-div-AAPL-20260510",
                 snap_v1_ms, "h1" * 32,
                 json.dumps(payload_v1, sort_keys=True), now),
                ("eodhd", "eodhd-div-AAPL-20260510",
                 snap_v2_ms, "h2" * 32,
                 json.dumps(payload_v2, sort_keys=True), now),
            ],
        )
    items, _ = store.list_calendar_items(
        as_of="2026-04-15T00:00:00+00:00",
    )
    assert items[0]["currency"] == "GBP"
    assert items[0]["values"]["currency"] == "GBP"


def test_calendar_corp_as_of_storage_helper(
    store: SQLiteEngineStore,
) -> None:
    """Direct cover of the new ``calendar_corp_as_of`` storage method.
    Mirrors ``calendar_actual_as_of`` for the corp lane."""
    _seed_corp_dividend_with_revision(store)
    snap = store.calendar_corp_as_of(
        provider="eodhd",
        provider_event_id="eodhd-div-AAPL-20260510",
        as_of="2026-04-15T00:00:00+00:00",
    )
    assert snap is not None
    assert json.loads(snap["payload_json"])["amount"] == 0.24
    # Cutoff before any snapshot — None.
    assert store.calendar_corp_as_of(
        provider="eodhd",
        provider_event_id="eodhd-div-AAPL-20260510",
        as_of="2025-01-01T00:00:00+00:00",
    ) is None


def test_econ_as_of_drops_consensus_forecast(
    store: SQLiteEngineStore,
) -> None:
    """Vintages don't snapshot ``consensus_forecast`` — the field must
    not leak the latest projection's value through ``as_of``."""
    pid, prov = _seed_revised_econ_event(store)
    # Backfill a consensus_forecast on the projection so latest mode
    # would expose it.
    with store._connection(commit=True) as conn:
        conn.execute(
            "UPDATE cal_econ_event SET consensus_forecast = '2.1' "
            "WHERE provider = ? AND provider_event_id = ?",
            (prov, pid),
        )
    latest, _ = store.list_calendar_items()
    assert latest[0]["values"].get("consensus_forecast") == "2.1"
    pit, _ = store.list_calendar_items(
        as_of="2026-02-01T00:00:00+00:00",
    )
    assert "consensus_forecast" not in pit[0]["values"]


# ──────────────────────────────────────────────────────────────────────────
# Service layer
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_rejects_future_as_of(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    future = (datetime.now(timezone.utc) + timedelta(days=365)).isoformat()
    result = svc.invoke("list_calendar_items", {"as_of": future})
    assert "error" in result
    assert "future" in result["error"].lower()


def test_service_op_rejects_unparseable_as_of(
    store: SQLiteEngineStore,
) -> None:
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("list_calendar_items", {"as_of": "not-a-date"})
    assert "error" in result
    assert "as_of" in result["error"]


def test_service_op_no_corp_unsupported_flag(
    store: SQLiteEngineStore,
) -> None:
    """C2: the C1-era ``as_of_corp_unsupported`` fallback flag is
    retired — corp rows are now snapshot-resolved, so the meta flag
    must not appear on any PIT response (econ-only or mixed)."""
    _seed_revised_econ_event(store)
    _seed_corp_dividend(store)
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "list_calendar_items",
        {"as_of": "2026-04-01T00:00:00+00:00"},
    )
    assert "as_of_corp_unsupported" not in result["meta"]


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


def test_http_route_as_of_resolves_vintage(
    store: SQLiteEngineStore, live_server,
) -> None:
    _seed_revised_econ_event(store)
    host, port = live_server
    status, payload = _http_get(
        host, port,
        "/v1/calendar?as_of=2026-02-01T00:00:00%2B00:00",
    )
    assert status == 200
    assert payload["data"][0]["values"]["actual"] == "2.0"


def test_http_route_future_as_of_returns_400(
    store: SQLiteEngineStore, live_server,
) -> None:
    host, port = live_server
    future = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    status, payload = _http_get(
        host, port,
        f"/v1/calendar?as_of={future}",
    )
    assert status == 400
    assert "error" in payload
