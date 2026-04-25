"""Tests for TE daily-pull tripwire (issue #22 P1).

Mocks all HTTP. Verifies:

- Single-day path is `country/All/{date}/{date}`.
- Returned rows land in `cal_econ_raw` and `cal_econ_event`.
- Re-running the same day with unchanged content is a no-op (idempotency).
- A revision (changed mutable field) appends a new raw row + new vintage.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import httpx
import pytest
import respx

from ingestion.calendar.te_api import TEAPIClient, pull_daily
from storage.sqlite import SQLiteEngineStore


def _te_row(**overrides):
    base = {
        "CalendarId": "9001",
        "Date": "2026-04-24T12:30:00",
        "Country": "United States",
        "Category": "Inflation",
        "Event": "Core Inflation Rate YoY",
        "Reference": "Mar",
        "ReferenceDate": "2026-03-31T00:00:00",
        "Source": "BLS",
        "SourceURL": "https://bls.gov/cpi",
        "Actual": "3.1%",
        "Previous": "3.2%",
        "Forecast": "3.0%",
        "TEForecast": "3.1%",
        "Revised": "",
        "URL": "/united-states/core-inflation-rate",
        "DateSpan": "0",
        "Importance": 3,
        "Currency": "USD",
        "Unit": "%",
        "Ticker": "USCORECPI",
        "Symbol": "USCORECPI",
        "LastUpdate": "2026-04-24T12:35:00",
    }
    base.update(overrides)
    return base


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


@respx.mock
def test_daily_pull_writes_rows(monkeypatch, store: SQLiteEngineStore) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    route = respx.get(
        url__startswith=(
            "https://api.tradingeconomics.com/calendar/country/All/"
            "2026-04-24/2026-04-24"
        ),
    ).mock(return_value=httpx.Response(200, json=[_te_row()]))

    client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)
    with store.get_connection() as conn:
        summary = pull_daily(
            connection=conn, client=client, target_date=date(2026, 4, 24),
        )

    assert route.call_count == 1
    assert summary.target_date == "2026-04-24"
    assert summary.rows_returned == 1
    assert summary.rows_raw_inserted == 1
    assert summary.events_upserted == 1
    assert summary.requests_spent == 1
    assert summary.truncated is False

    with store.get_connection() as conn:
        rows = conn.execute(
            "SELECT provider_event_id, actual FROM cal_econ_event "
            "WHERE provider = 'tradingeconomics'",
        ).fetchall()
    assert len(rows) == 1
    assert rows[0]["provider_event_id"] == "9001"
    assert rows[0]["actual"] == "3.1%"


@respx.mock
def test_daily_pull_is_idempotent_when_unchanged(
    monkeypatch, store: SQLiteEngineStore,
) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar").mock(
        return_value=httpx.Response(200, json=[_te_row()]),
    )
    client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)

    with store.get_connection() as conn:
        first = pull_daily(
            connection=conn, client=client, target_date=date(2026, 4, 24),
        )
    with store.get_connection() as conn:
        second = pull_daily(
            connection=conn, client=client, target_date=date(2026, 4, 24),
        )

    assert first.rows_raw_inserted == 1
    # Same content_hash → INSERT OR IGNORE swallows the second insert.
    assert second.rows_raw_inserted == 0


@respx.mock
def test_daily_pull_records_revision_as_new_raw(
    monkeypatch, store: SQLiteEngineStore,
) -> None:
    monkeypatch.setenv("TE_API_KEY", "unit:test")
    first_row = _te_row(Actual="3.1%")
    revised_row = _te_row(Actual="3.2%", LastUpdate="2026-04-25T01:00:00")
    respx.get(url__startswith="https://api.tradingeconomics.com/calendar").mock(
        side_effect=[
            httpx.Response(200, json=[first_row]),
            httpx.Response(200, json=[revised_row]),
        ],
    )
    client = TEAPIClient(rate_limit_seconds=0, sleeper=lambda _s: None)

    with store.get_connection() as conn:
        pull_daily(
            connection=conn, client=client, target_date=date(2026, 4, 24),
        )
    with store.get_connection() as conn:
        second = pull_daily(
            connection=conn, client=client, target_date=date(2026, 4, 24),
        )

    assert second.rows_raw_inserted == 1
    with store.get_connection() as conn:
        raws = conn.execute(
            "SELECT content_hash FROM cal_econ_raw "
            "WHERE provider = 'tradingeconomics' "
            "AND provider_event_id = '9001'",
        ).fetchall()
        vintages = conn.execute(
            "SELECT actual FROM calendar_event_vintages "
            "WHERE provider = 'tradingeconomics' AND event_id = '9001' "
            "ORDER BY id ASC",
        ).fetchall()
    assert len({r["content_hash"] for r in raws}) == 2
    assert [v["actual"] for v in vintages] == ["3.1%", "3.2%"]
