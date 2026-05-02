"""Tests for the canonical-vintage write contract (issue #114).

P0: ``indicator_vintages`` carries a ``vintage_quality`` column; every
write goes through the vintage record path and round-trips the tag.

P1: ``indicators`` is a SQL view over the latest vintage;
``upsert_indicator_observation`` redirects to the vintage path with a
value-change-triggered filter; legacy ``indicators`` tables migrate
into vintages on schema init.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from storage.sqlite import (
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    SQLiteEngineStore,
)


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


class TestIndicatorsView:
    def test_indicators_is_a_view(self, store):
        with store._connection(commit=False) as conn:
            row = conn.execute(
                "SELECT type FROM sqlite_master WHERE name='indicators'"
            ).fetchone()
        assert row is not None
        assert row[0] == "view"

    def test_view_returns_latest_vintage(self, store):
        # Two vintages for the same observation; view picks the later
        # vintage_date.
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="GDP", source="fred",
                observation_date="2024-01-01", vintage_date="2024-04-25",
                value=27000.0, vintage_quality="native_pit",
            )
        )
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="GDP", source="fred",
                observation_date="2024-01-01", vintage_date="2024-05-30",
                value=27250.0, vintage_quality="native_pit",
            )
        )
        with store._connection(commit=False) as conn:
            row = conn.execute(
                "SELECT date, value FROM indicators "
                "WHERE source = 'fred' AND series_id = 'GDP'"
            ).fetchone()
        assert row["date"] == "2024-01-01"
        assert row["value"] == 27250.0

    def test_view_one_row_per_obs_date(self, store):
        # Three observation_dates, two vintages each → view shows three
        # rows, one per observation_date.
        for obs_d, v_old, v_new in [
            ("2024-01-01", 100.0, 101.0),
            ("2024-02-01", 200.0, 202.0),
            ("2024-03-01", 300.0, 303.0),
        ]:
            store.upsert_indicator_vintage(
                IndicatorVintageRecord(
                    series_id="X", source="bls",
                    observation_date=obs_d, vintage_date="2024-04-01",
                    value=v_old, vintage_quality="native_pit",
                )
            )
            store.upsert_indicator_vintage(
                IndicatorVintageRecord(
                    series_id="X", source="bls",
                    observation_date=obs_d, vintage_date="2024-05-01",
                    value=v_new, vintage_quality="native_pit",
                )
            )
        with store._connection(commit=False) as conn:
            rows = conn.execute(
                "SELECT date, value FROM indicators "
                "WHERE source = 'bls' AND series_id = 'X' "
                "ORDER BY date"
            ).fetchall()
        assert len(rows) == 3
        assert [r["value"] for r in rows] == [101.0, 202.0, 303.0]


class TestUpsertObservationRedirect:
    def test_observation_writes_synthetic_snapshot(self, store):
        store.upsert_indicator_observation(
            IndicatorObservationRecord(
                series_id="CUUR0000SA0", source="bls",
                date="2024-01-01", value=312.0,
            )
        )
        rows = store.get_vintage_history("CUUR0000SA0", "2024-01-01")
        assert len(rows) == 1
        assert rows[0].vintage_quality == "synthetic_snapshot"
        assert rows[0].value == 312.0

    def test_unchanged_value_does_not_create_new_vintage(self, store):
        rec = IndicatorObservationRecord(
            series_id="X", source="bls", date="2024-01-01", value=100.0,
        )
        store.upsert_indicator_observation(rec)
        store.upsert_indicator_observation(rec)
        store.upsert_indicator_observation(rec)
        rows = store.get_vintage_history("X", "2024-01-01")
        assert len(rows) == 1

    def test_value_change_creates_new_vintage(self, store):
        store.upsert_indicator_observation(
            IndicatorObservationRecord(
                series_id="X", source="bls", date="2024-01-01", value=100.0,
            )
        )
        store.upsert_indicator_observation(
            IndicatorObservationRecord(
                series_id="X", source="bls", date="2024-01-01", value=101.0,
            )
        )
        rows = store.get_vintage_history("X", "2024-01-01")
        assert len(rows) == 2
        # latest-by-vintage_date wins in the view
        with store._connection(commit=False) as conn:
            row = conn.execute(
                "SELECT value FROM indicators "
                "WHERE source = 'bls' AND series_id = 'X'"
            ).fetchone()
        assert row["value"] == 101.0

    def test_synthetic_snapshot_filter_applies_to_imf_path(self, store):
        # IMF SDMX `asOf` callsite goes through `upsert_indicator_vintage`
        # directly with synthetic_snapshot. Same filter must apply.
        for vd in ("2025-01-01", "2025-02-01", "2025-03-01"):
            store.upsert_indicator_vintage(
                IndicatorVintageRecord(
                    series_id="IMF_X", source="imf",
                    observation_date="2024-12-01", vintage_date=vd,
                    value=42.0, vintage_quality="synthetic_snapshot",
                )
            )
        rows = store.get_vintage_history("IMF_X", "2024-12-01")
        assert len(rows) == 1

    def test_out_of_order_arrival_preserves_intermediate_row(self, store):
        # IMF backfill iterates as_of dates from newest to oldest.
        # Stream: Mar=100, Feb=100, Jan=90.
        # Feb must NOT be deduped against Mar: PIT(Feb-15) needs the
        # Feb row, otherwise it collapses to Jan=90 (wrong).
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="X", source="imf",
                observation_date="2024-01-01", vintage_date="2024-03-01",
                value=100.0, vintage_quality="synthetic_snapshot",
            )
        )
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="X", source="imf",
                observation_date="2024-01-01", vintage_date="2024-02-01",
                value=100.0, vintage_quality="synthetic_snapshot",
            )
        )
        store.upsert_indicator_vintage(
            IndicatorVintageRecord(
                series_id="X", source="imf",
                observation_date="2024-01-01", vintage_date="2024-01-15",
                value=90.0, vintage_quality="synthetic_snapshot",
            )
        )
        rows = store.get_vintage_history("X", "2024-01-01")
        # All three rows preserved — Feb=100 not deduped against Mar.
        assert len(rows) == 3
        # PIT(2024-02-15) → Feb=100 (not Jan=90)
        with store._connection(commit=False) as conn:
            row = conn.execute(
                """
                SELECT value FROM indicator_vintages
                WHERE source='imf' AND series_id='X'
                  AND observation_date='2024-01-01'
                  AND vintage_date <= '2024-02-15'
                ORDER BY vintage_date DESC LIMIT 1
                """
            ).fetchone()
        assert row[0] == 100.0


class TestLegacyIndicatorsMigration:
    def test_legacy_table_migrates_to_vintages(self, tmp_path):
        # Build a legacy DB shaped like pre-#114 (indicators is a TABLE).
        db = tmp_path / "legacy.db"
        conn = sqlite3.connect(db)
        conn.execute(
            """
            CREATE TABLE indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                series_id TEXT NOT NULL,
                source TEXT NOT NULL,
                date TEXT NOT NULL,
                value REAL NOT NULL,
                metadata_json TEXT NOT NULL,
                scraped_at TEXT NOT NULL,
                obs_family_id TEXT DEFAULT NULL,
                UNIQUE(series_id, source, date)
            )
            """
        )
        conn.execute(
            "INSERT INTO indicators "
            "(series_id, source, date, value, metadata_json, scraped_at) "
            "VALUES ('LEG', 'fred', '2023-01-01', 99.0, '{}', '2023-02-01T00:00:00')"
        )
        conn.commit()
        conn.close()
        # Now apply schema → migration should run.
        store = SQLiteEngineStore(db)
        with store._connection(commit=False) as c:
            row = c.execute(
                "SELECT type FROM sqlite_master WHERE name='indicators'"
            ).fetchone()
        assert row[0] == "view"
        rows = store.get_vintage_history("LEG", "2023-01-01")
        assert len(rows) == 1
        assert rows[0].source == "fred"
        assert rows[0].value == 99.0
        assert rows[0].vintage_quality == "single_observation"
        # Re-apply schema is idempotent.
        store2 = SQLiteEngineStore(db)
        rows = store2.get_vintage_history("LEG", "2023-01-01")
        assert len(rows) == 1
