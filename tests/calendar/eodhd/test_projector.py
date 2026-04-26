"""EODHD scaffold tests: store_corp_raw / project_corp_events / v_calendar_item projection.

Split out of the original tests/test_eodhd_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.eodhd_api import (
    parse_earnings_row,
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
