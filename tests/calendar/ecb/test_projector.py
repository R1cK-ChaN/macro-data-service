"""ECB calendar scaffold tests: store_raw / project_events / v_calendar_item projection.

Split out of the original tests/test_ecb_calendar_api_scaffold.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from pathlib import Path
import pytest
from ingestion.calendar.ecb_api import (
    INDICATOR_REGISTRY,
    ECBCalendarEventRecord,
    ECBCalendarRawRecord,
    fetch_ecb_calendar,
    parse_observation,
    project_events,
    store_raw,
)
from ingestion.calendar.ecb_api.parser import PROVIDER, _content_hash
from ingestion.timeseries.sdmx._types import SDMXObservation
from storage.sqlite import SQLiteEngineStore


def _dfr_obs(
    value: float = 4.0, date_str: str = "2024-06-12",
) -> SDMXObservation:
    return SDMXObservation(
        series_id="FM.B.U2.EUR.4F.KR.DFR.LEV",
        date=date_str,
        value=value,
        dataflow="FM",
    )


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_store_raw_inserts_then_deduplicates_by_content_hash(
    store: SQLiteEngineStore,
) -> None:
    raw, _ = parse_observation(
        _dfr_obs(), snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])  # same hash → no-op
    assert first == 1
    assert second == 0


def test_project_events_upserts_and_honors_observed_at_ordering(
    store: SQLiteEngineStore,
) -> None:
    _, first = parse_observation(
        _dfr_obs(value=4.0),
        snapshot_epoch_ms=1_700_000_000_000,
        observed_at_epoch_ms=1_700_000_000_000,
    )
    _, revised = parse_observation(
        _dfr_obs(value=4.25),
        snapshot_epoch_ms=1_700_000_100_000,
        observed_at_epoch_ms=1_700_000_100_000,
    )
    _, stale = parse_observation(
        _dfr_obs(value=99.0),
        snapshot_epoch_ms=1_699_999_900_000,
        observed_at_epoch_ms=1_699_999_900_000,
    )

    with store._connection(commit=True) as conn:
        project_events(conn, [first])
        project_events(conn, [revised])
        project_events(conn, [stale])  # older → ignored

    with store._connection(commit=False) as conn:
        (actual,) = conn.execute(
            "SELECT actual FROM cal_econ_event WHERE provider='ecb'"
        ).fetchone()
    assert actual == "4.25"


def test_projected_rows_surface_in_v_calendar_item(
    store: SQLiteEngineStore,
) -> None:
    raw, event = parse_observation(
        _dfr_obs(value=4.0), snapshot_epoch_ms=1_700_000_000_000,
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [event])

    with store._connection(commit=False) as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                "SELECT provider, country, indicator_id, actual, importance "
                "FROM v_calendar_item WHERE provider='ecb'"
            ).fetchall()
        ]
    assert rows == [("ecb", "EU", None, "4.0", "high")]
