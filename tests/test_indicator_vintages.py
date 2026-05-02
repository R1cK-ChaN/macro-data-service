"""Tests for the canonical-vintage write contract (issue #114).

P0: ``indicator_vintages`` carries a ``vintage_quality`` column; every
write goes through the vintage record path and round-trips the tag.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from storage.sqlite import IndicatorVintageRecord, SQLiteEngineStore


@pytest.fixture()
def store() -> SQLiteEngineStore:
    with tempfile.TemporaryDirectory() as td:
        s = SQLiteEngineStore(Path(td) / "test.db")
        yield s


class TestVintageQualityColumn:
    def test_column_exists_and_check_constraint(self, store):
        with store._connection(commit=False) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(indicator_vintages)").fetchall()}
            assert "vintage_quality" in cols

    def test_default_is_single_observation(self, store):
        with store._connection(commit=True) as conn:
            conn.execute(
                "INSERT INTO indicator_vintages "
                "(series_id, source, observation_date, vintage_date, value, "
                " metadata_json, scraped_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("X", "fred", "2024-01-01", "2024-01-02", 1.0, "{}", "2024-01-02T00:00:00"),
            )
        with store._connection(commit=False) as conn:
            row = conn.execute(
                "SELECT vintage_quality FROM indicator_vintages WHERE series_id='X'"
            ).fetchone()
        assert row[0] == "single_observation"

    def test_check_constraint_rejects_bad_value(self, store):
        import sqlite3

        with pytest.raises(sqlite3.IntegrityError):
            with store._connection(commit=True) as conn:
                conn.execute(
                    "INSERT INTO indicator_vintages "
                    "(series_id, source, observation_date, vintage_date, value, "
                    " metadata_json, scraped_at, vintage_quality) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("X", "fred", "2024-01-01", "2024-01-02", 1.0, "{}", "now", "garbage"),
                )

    def test_writer_rejects_bad_value(self, store):
        # Defensive guard: legacy DBs ALTERed in P0 cannot carry the CHECK
        # constraint, so the writer must reject unknown tags too.
        with pytest.raises(ValueError, match="vintage_quality must be"):
            store.upsert_indicator_vintage(
                IndicatorVintageRecord(
                    series_id="X",
                    source="fred",
                    observation_date="2024-01-01",
                    vintage_date="2024-01-02",
                    value=1.0,
                    vintage_quality="garbage",
                )
            )


class TestUpsertVintageRoundtrip:
    def test_native_pit_round_trips(self, store):
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="GDP",
                source="fred",
                observation_date="2024-01-01",
                vintage_date="2024-04-25",
                value=27000.0,
                vintage_quality="native_pit",
            )
        )
        rows = store.get_vintages_for_series("GDP", limit=5)
        assert len(rows) == 1
        assert rows[0].vintage_quality == "native_pit"

    def test_synthetic_snapshot_round_trips(self, store):
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="X",
                source="bls",
                observation_date="2024-01-01",
                vintage_date="2024-02-01",
                value=2.5,
                vintage_quality="synthetic_snapshot",
            )
        )
        rows = store.get_vintage_history("X", "2024-01-01")
        assert len(rows) == 1
        assert rows[0].vintage_quality == "synthetic_snapshot"

    def test_default_dataclass_quality_is_single_observation(self, store):
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="Y",
                source="bls",
                observation_date="2024-01-01",
                vintage_date="2024-02-01",
                value=3.0,
            )
        )
        rows = store.get_vintage_history("Y", "2024-01-01")
        assert rows[0].vintage_quality == "single_observation"
