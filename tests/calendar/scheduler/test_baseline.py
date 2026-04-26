"""Scheduler tests: ConnectorState mark_failure / mark_success primitives.

Split out of the original tests/test_calendar_refresh_scheduler.py
as part of issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.scheduler_state import (
    COOLDOWN_SECONDS,
    ConnectorState,
    FAILURE_THRESHOLD,
    get_connector_state,
    is_cooling,
    mark_connector_failure,
    mark_connector_success,
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_state_helpers_default_to_empty(store: SQLiteEngineStore) -> None:
    """A connector with no prior state reads as a fresh default —
    zero failures, no cooling. Avoids a special-case at every read
    site."""
    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state == ConnectorState(connector="bls")
    assert is_cooling(state, now_ms=1_800_000_000_000) is False


def test_state_mark_failure_increments_and_trips(
    store: SQLiteEngineStore,
) -> None:
    """N consecutive failures trip the breaker; the ``cooling_until_ms``
    anchor is set to ``now_ms + COOLDOWN_SECONDS * 1000``."""
    now_ms = 1_800_000_000_000
    with store.get_connection() as conn:
        for i in range(1, FAILURE_THRESHOLD):
            new_state = mark_connector_failure(
                conn, "bls", error=f"attempt {i}", now_ms=now_ms + i,
            )
            assert new_state.consecutive_failures == i
            assert new_state.cooling_until_ms is None  # not yet tripped
        tripped = mark_connector_failure(
            conn, "bls", error="final",
            now_ms=now_ms + FAILURE_THRESHOLD,
        )
        conn.commit()
    assert tripped.consecutive_failures == FAILURE_THRESHOLD
    assert tripped.cooling_until_ms == (
        now_ms + FAILURE_THRESHOLD + COOLDOWN_SECONDS * 1000
    )
    assert is_cooling(tripped, now_ms=now_ms + FAILURE_THRESHOLD) is True
    assert is_cooling(
        tripped,
        now_ms=now_ms + FAILURE_THRESHOLD + COOLDOWN_SECONDS * 1000 + 1,
    ) is False


def test_state_mark_success_resets_after_trip(
    store: SQLiteEngineStore,
) -> None:
    """A successful run clears the counter + cool-down even when the
    breaker was previously tripped."""
    now_ms = 1_800_000_000_000
    with store.get_connection() as conn:
        for i in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error=f"flaky {i}", now_ms=now_ms,
            )
        conn.commit()
        tripped = get_connector_state(conn, "bls")
        assert tripped.cooling_until_ms is not None

        mark_connector_success(conn, "bls")
        conn.commit()
        after = get_connector_state(conn, "bls")
    assert after.consecutive_failures == 0
    assert after.cooling_until_ms is None
    assert after.last_error is None
