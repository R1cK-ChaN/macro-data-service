"""Projector tests for the EODHD fundamentals package (issue #68 S1)."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from ingestion.market.fundamentals.eodhd_fundamentals import (
    build_raw_record,
    parse_payload_records,
    project_fundamentals_company,
    project_fundamentals_financials,
    project_fundamentals_highlights,
    store_fundamentals_raw,
)
from storage.sqlite import SQLiteEngineStore


def _general() -> dict:
    return {
        "Name":          "Apple Inc",
        "Type":          "Common Stock",
        "Sector":        "Technology",
        "Industry":      "Consumer Electronics",
        "FiscalYearEnd": "September",
        "Exchange":      "NASDAQ",
        "CurrencyCode":  "USD",
        "CountryISO":    "US",
        "ISIN":          "US0378331005",
    }


def _financials_block() -> dict:
    return {
        "Income_Statement": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":         "2024-09-30",
                    "totalRevenue": 391035000000.0,
                    "netIncome":    93736000000.0,
                },
            },
            "quarterly": {
                "2024-09-30": {
                    "date":         "2024-09-30",
                    "totalRevenue": 94930000000.0,
                    "netIncome":    14736000000.0,
                },
            },
        },
        "Balance_Sheet": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":                   "2024-09-30",
                    "totalAssets":            364980000000.0,
                    "totalStockholderEquity": 56950000000.0,
                    "totalLiab":              308030000000.0,
                },
            },
            "quarterly": {},
        },
        "Cash_Flow": {
            "currency_symbol": "USD",
            "yearly": {
                "2024-09-30": {
                    "date":                              "2024-09-30",
                    "totalCashFromOperatingActivities":  118254000000.0,
                    "capitalExpenditures":               -9447000000.0,
                },
            },
            "quarterly": {},
        },
    }


def _payload() -> dict:
    return {
        "General":     _general(),
        "Highlights":  {
            "MarketCapitalization": 3.0e12,
            "PERatio":              28.5,
            "EarningsShare":        6.42,
        },
        "Valuation":   {"PERatio": 28.5},
        "SharesStats": {"SharesOutstanding": 15400000000.0},
        "Financials":  _financials_block(),
    }


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


def test_store_raw_is_idempotent(connection: sqlite3.Connection) -> None:
    raw = build_raw_record(
        ticker="AAPL.US", payload_text="{}", snapshot_epoch_ms=1
    )
    assert store_fundamentals_raw(connection, [raw]) == 1
    assert store_fundamentals_raw(connection, [raw]) == 0
    (count,) = connection.execute(
        "SELECT COUNT(*) FROM fundamentals_raw"
    ).fetchone()
    assert count == 1


def test_store_raw_logs_revision_on_second_distinct_hash(
    connection: sqlite3.Connection, caplog
) -> None:
    raw1 = build_raw_record(
        ticker="AAPL.US", payload_text="{}", snapshot_epoch_ms=1
    )
    raw2 = build_raw_record(
        ticker="AAPL.US", payload_text="{\"x\":1}", snapshot_epoch_ms=2
    )
    store_fundamentals_raw(connection, [raw1])
    with caplog.at_level(
        logging.INFO,
        logger="ingestion.market.fundamentals.eodhd_fundamentals.projector",
    ):
        store_fundamentals_raw(connection, [raw2])
    msgs = [r.getMessage() for r in caplog.records]
    assert any(
        "fundamentals revised" in m and "ticker=AAPL.US" in m and "versions=2" in m
        for m in msgs
    ), msgs


def test_project_company_upsert_pit_guard(connection: sqlite3.Connection) -> None:
    company1, _hl, _fins = parse_payload_records(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert company1 is not None
    assert project_fundamentals_company(connection, [company1]) == 1

    # Newer snapshot with sector change → applies.
    payload2 = _payload()
    payload2["General"]["Sector"] = "Technology Hardware"
    company2, _, _ = parse_payload_records(
        payload2, ticker="AAPL.US", snapshot_epoch_ms=1_700_000_010_000
    )
    assert company2 is not None
    assert project_fundamentals_company(connection, [company2]) == 1
    (sector,) = connection.execute(
        "SELECT sector FROM fundamentals_company WHERE ticker = 'AAPL.US'"
    ).fetchone()
    assert sector == "Technology Hardware"

    # Older snapshot tries to overwrite — PIT guard rejects.
    payload3 = _payload()
    payload3["General"]["Sector"] = "STALE"
    company3, _, _ = parse_payload_records(
        payload3, ticker="AAPL.US", snapshot_epoch_ms=1_600_000_000_000
    )
    assert company3 is not None
    assert project_fundamentals_company(connection, [company3]) == 0
    (sector_after,) = connection.execute(
        "SELECT sector FROM fundamentals_company WHERE ticker = 'AAPL.US'"
    ).fetchone()
    assert sector_after == "Technology Hardware"


def test_project_financials_upsert_period_grain(
    connection: sqlite3.Connection,
) -> None:
    _co, _hl, financials = parse_payload_records(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert project_fundamentals_financials(connection, financials) == len(financials)
    rows = connection.execute(
        """
        SELECT period_end, period_type, statement, revenue, total_assets,
               cash_from_ops, currency
        FROM fundamentals_financials ORDER BY period_end, period_type, statement
        """
    ).fetchall()
    keys = [(r["period_end"], r["period_type"], r["statement"]) for r in rows]
    assert keys == [
        ("2024-09-30", "A", "BS"),
        ("2024-09-30", "A", "CF"),
        ("2024-09-30", "A", "IS"),
        ("2024-09-30", "Q", "IS"),
    ]
    is_row = next(
        r for r in rows
        if r["period_type"] == "A" and r["statement"] == "IS"
    )
    assert is_row["revenue"] == 391035000000.0
    assert is_row["currency"] == "USD"
    bs_row = next(r for r in rows if r["statement"] == "BS")
    assert bs_row["total_assets"] == 364980000000.0
    cf_row = next(r for r in rows if r["statement"] == "CF")
    assert cf_row["cash_from_ops"] == 118254000000.0


def test_project_financials_restatement_replaces_latest(
    connection: sqlite3.Connection,
) -> None:
    _co, _hl, fins1 = parse_payload_records(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    project_fundamentals_financials(connection, fins1)

    payload2 = _payload()
    payload2["Financials"]["Income_Statement"]["yearly"]["2024-09-30"][
        "totalRevenue"
    ] = 391100000000.0
    _co, _hl, fins2 = parse_payload_records(
        payload2, ticker="AAPL.US", snapshot_epoch_ms=1_700_000_999_000
    )
    project_fundamentals_financials(connection, fins2)
    (revenue,) = connection.execute(
        """
        SELECT revenue FROM fundamentals_financials
        WHERE ticker='AAPL.US' AND period_end='2024-09-30'
              AND period_type='A' AND statement='IS'
        """
    ).fetchone()
    assert revenue == 391100000000.0


def test_project_highlights_pit_grain(connection: sqlite3.Connection) -> None:
    _co, hl1, _fins = parse_payload_records(
        _payload(), ticker="AAPL.US", snapshot_epoch_ms=1_700_000_000_000
    )
    assert hl1 is not None
    project_fundamentals_highlights(connection, [hl1])
    rows = connection.execute(
        "SELECT as_of_date, market_cap, pe_ratio FROM fundamentals_highlights"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["market_cap"] == 3.0e12
    assert rows[0]["pe_ratio"] == 28.5

    # Same as_of_date, newer snapshot — overwrites by PIT guard.
    payload2 = _payload()
    payload2["Highlights"]["PERatio"] = 30.0
    _co, hl2, _ = parse_payload_records(
        payload2, ticker="AAPL.US", snapshot_epoch_ms=1_700_000_500_000
    )
    assert hl2 is not None
    assert hl2.as_of_date == hl1.as_of_date
    project_fundamentals_highlights(connection, [hl2])
    (pe,) = connection.execute(
        "SELECT pe_ratio FROM fundamentals_highlights"
    ).fetchone()
    assert pe == 30.0
