"""Smoke tests for the two-lane calendar schema (issue #8)."""

from __future__ import annotations

from pathlib import Path

import pytest

from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _tables(store: SQLiteEngineStore) -> set[str]:
    with store._connection(commit=False) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


def _views(store: SQLiteEngineStore) -> set[str]:
    with store._connection(commit=False) as c:
        return {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='view'"
            ).fetchall()
        }


def test_schema_creates_six_calendar_tables(store: SQLiteEngineStore) -> None:
    expected = {
        "cal_provider",
        "cal_econ_raw",
        "cal_econ_event",
        "cal_econ_drops",
        "cal_corp_raw",
        "cal_corp_event",
    }
    assert expected <= _tables(store)


def test_schema_creates_unified_view(store: SQLiteEngineStore) -> None:
    assert "v_calendar_item" in _views(store)
    with store._connection(commit=False) as c:
        rows = c.execute("SELECT * FROM v_calendar_item").fetchall()
    assert rows == []


def test_view_unions_both_lanes(store: SQLiteEngineStore) -> None:
    with store._connection(commit=True) as c:
        c.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc, event_time_precision,
                country_code, title, content_hash, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (
                'tradingeconomics', '12345', '2026-05-01T12:30:00+00:00', 'datetime',
                'US', 'CPI YoY', 'h1', 0, '2026-05-01', '2026-05-01'
            )
            """
        )
        c.execute(
            """
            INSERT INTO cal_corp_event (
                provider, provider_event_id, event_subtype, event_time_utc,
                event_time_precision, ticker, title, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (
                'eodhd', 'sha256hex', 'earnings', '2026-05-02', 'date',
                'AAPL.US', 'AAPL Q2 Earnings', 'h2', 0, '2026-05-01', '2026-05-01'
            )
            """
        )
    with store._connection(commit=False) as c:
        rows = [
            tuple(r)
            for r in c.execute(
                "SELECT domain, subtype, country, ticker, title "
                "FROM v_calendar_item ORDER BY provider"
            ).fetchall()
        ]
    assert rows == [
        ("corporate", "earnings",  None, "AAPL.US", "AAPL Q2 Earnings"),
        ("economic",  "release",   "US", None,       "CPI YoY"),
    ]


def test_cal_provider_seeded(store: SQLiteEngineStore) -> None:
    """TE + EODHD aggregators (precedence=10) plus the five official-source
    providers seeded by issue #9 P0 (precedence=100)."""
    with store._connection(commit=False) as c:
        rows = [
            tuple(r)
            for r in c.execute(
                "SELECT provider_id, provider_type, domain, precedence "
                "FROM cal_provider ORDER BY provider_id"
            ).fetchall()
        ]
    assert rows == [
        ("bea",              "government_agency", "economic",  100),
        ("bls",              "government_agency", "economic",  100),
        ("ecb",              "central_bank",      "economic",  100),
        ("eodhd",            "data_aggregator",   "corporate", 10),
        ("federal-reserve",  "central_bank",      "economic",  100),
        ("nbs",              "government_agency", "economic",  100),
        ("tradingeconomics", "data_aggregator",   "economic",  10),
    ]


def test_cal_provider_seed_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "engine.db"
    SQLiteEngineStore(db_path=db)
    SQLiteEngineStore(db_path=db)  # re-init; INSERT OR IGNORE must not duplicate
    second = SQLiteEngineStore(db_path=db)
    with second._connection(commit=False) as c:
        n = c.execute("SELECT COUNT(*) FROM cal_provider").fetchone()[0]
    assert n == 7


def test_importance_flows_through_view_as_enum_string(store: SQLiteEngineStore) -> None:
    """Economic rows store 'low'/'medium'/'high' TEXT — matches CalendarItem.Importance.

    Regression guard against the INTEGER-vs-enum mismatch caught in codex review.
    """
    with store._connection(commit=True) as c:
        c.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc, event_time_precision,
                country_code, title, importance, content_hash, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (
                'tradingeconomics', '99', '2026-05-01T12:30:00+00:00', 'datetime',
                'US', 'CPI YoY', 'high', 'h', 0, '2026-05-01', '2026-05-01'
            )
            """
        )
    with store._connection(commit=False) as c:
        (importance,) = c.execute(
            "SELECT importance FROM v_calendar_item WHERE provider_event_id='99'"
        ).fetchone()
    assert importance == "high"


def test_importance_check_constraint_rejects_integer(store: SQLiteEngineStore) -> None:
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        with store._connection(commit=True) as c:
            c.execute(
                """
                INSERT INTO cal_econ_event (
                    provider, provider_event_id, event_time_utc, event_time_precision,
                    country_code, title, importance, content_hash, observed_at_epoch_ms,
                    created_at, updated_at
                ) VALUES (
                    'tradingeconomics', 'bad', '2026-05-01', 'date',
                    'US', 'CPI', '3', 'h', 0, 'x', 'x'
                )
                """
            )


def test_event_subtype_check_constraint(store: SQLiteEngineStore) -> None:
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        with store._connection(commit=True) as c:
            c.execute(
                """
                INSERT INTO cal_corp_event (
                    provider, provider_event_id, event_subtype, event_time_utc,
                    event_time_precision, ticker, content_hash,
                    observed_at_epoch_ms, created_at, updated_at
                ) VALUES (
                    'eodhd', 'abc', 'bogus_subtype', '2026-05-02', 'date',
                    'AAPL', 'h', 0, 'x', 'x'
                )
                """
            )


def test_calendar_item_dto_extension() -> None:
    """Legacy construction still works; new fields have defaults."""
    from datetime import datetime, timezone

    from contracts import CalendarItem, Importance

    legacy = CalendarItem(
        event_id="x",
        release_time=datetime.now(timezone.utc),
        indicator="CPI",
        country="US",
        importance=Importance.HIGH,
    )
    assert legacy.domain == "economic"
    assert legacy.subtype == "release"
    assert legacy.ticker is None
    assert legacy.values == {}

    corp = CalendarItem(
        event_id="eodhd:abc",
        release_time=datetime.now(timezone.utc),
        indicator="",
        country="",
        importance=Importance.MEDIUM,
        domain="corporate",
        subtype="earnings",
        provider="eodhd",
        ticker="AAPL.US",
        values={"actual_eps": "1.52", "estimate_eps": "1.50"},
    )
    assert corp.domain == "corporate"
    assert corp.values["actual_eps"] == "1.52"
