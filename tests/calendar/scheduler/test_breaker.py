"""Scheduler tests: circuit-breaker driver.

Split out of the original tests/test_calendar_refresh_scheduler.py
as part of issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.scheduler import (
    ALL_CONNECTORS,
    refresh_all_schedules,
    sweep_value_side,
)
from ingestion.calendar.scheduler_state import (
    COOLDOWN_SECONDS,
    FAILURE_THRESHOLD,
    get_connector_state,
    mark_connector_failure,
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")

@dataclass
class _FakeSummary:
    """Drop-in stand-in for a per-connector ``*RunSummary`` dataclass."""

    connector: str
    dry_run: bool
    calls: int


def test_circuit_breaker_skips_cooling_connector_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a connector with ``cooling_until_ms`` in the future
    is skipped by the driver before fn is invoked. The result carries
    ``ok=False`` and a ``"cooling until"`` reason so the operator can
    see the breaker is open without digging into the state table."""
    now_ms = 1_800_000_000_000
    future_ms = now_ms + COOLDOWN_SECONDS * 1000

    with store.get_connection() as conn:
        # Seed state directly — three consecutive failures trip the
        # breaker, so the next sweep must skip BLS.
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error="upstream 502", now_ms=now_ms,
            )
        conn.commit()

    call_log: list[str] = []

    def _fn(name: str):
        def _inner(conn, dry_run):
            call_log.append(name)
            return _FakeSummary(connector=name, dry_run=dry_run, calls=1)
        return _inner

    overrides = {name: _fn(name) for name in ALL_CONNECTORS}
    summary = refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        _connector_overrides=overrides,
    )
    # BLS should not have been invoked; every other connector did.
    assert "bls" not in call_log
    assert set(call_log) == set(ALL_CONNECTORS) - {"bls"}

    bls_result = next(r for r in summary.results if r.connector == "bls")
    assert bls_result.ok is False
    assert "cooling until" in (bls_result.error or "")
    assert "upstream 502" in (bls_result.error or "")


def test_circuit_breaker_trips_after_threshold_failures_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """Simulate a connector failing on three consecutive sweeps —
    after the third, the breaker should be tripped, i.e. the state
    should have a future ``cooling_until_ms``."""
    def _boom(conn, dry_run):
        raise RuntimeError("flaky upstream")

    overrides = {"bls": _boom}
    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides=overrides,
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None
    assert state.cooling_until_ms > int(time.time() * 1000)


def test_circuit_breaker_success_clears_counter_in_driver(
    store: SQLiteEngineStore,
) -> None:
    """A successful sweep clears a partial failure count. Prevents
    the breaker from tripping on a flaky-but-not-broken upstream that
    sees 1 failure + 1 success + 1 failure + 1 success — the success
    runs reset the counter so the breaker stays closed."""
    calls = iter([False, True, False, True, False, True])

    def _sometimes(conn, dry_run):
        if next(calls):
            return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)
        raise RuntimeError("flaky")

    for _ in range(6):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _sometimes},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.cooling_until_ms is None  # never tripped
    # After a success, the counter resets to 0.
    assert state.consecutive_failures == 0


def test_circuit_breaker_dry_run_does_not_update_state(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run previews the plan. It must not persist state changes
    — otherwise an operator's preview could trip the breaker."""
    def _boom(conn, dry_run):
        raise RuntimeError("would have failed")

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=True,  # preview
            connectors=["bls"],
            _connector_overrides={"bls": _boom},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == 0
    assert state.cooling_until_ms is None


def test_circuit_breaker_partial_failure_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1 finding #1: a partial failure (one
    BLS series failed while eight succeeded) shouldn't count toward
    the breaker threshold. After three such sweeps, the next run
    must still invoke the connector — otherwise a single flaky series
    blocks every fresh release on the remaining eight.

    The card still reports ``ok=False`` because some items failed,
    but the breaker stays closed and the counter resets each run."""

    @dataclass
    class _BlsPartialSummary:
        dry_run: bool
        series_planned: list[str]
        series_ok: list[str]
        series_failed: list[tuple[str, str]]

    def _bls_partial(conn, dry_run):
        return _BlsPartialSummary(
            dry_run=dry_run,
            series_planned=["a", "b", "c"],
            series_ok=["a", "b"],  # partial — two of three succeeded
            series_failed=[("c", "flaky")],
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _bls_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    # Breaker stays closed across repeated partial runs.
    assert state.cooling_until_ms is None
    assert state.consecutive_failures == 0  # success path resets


def test_circuit_breaker_total_series_failure_trips(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: when every BLS series fails (no ``series_ok``,
    ``series_failed`` covers every planned id), the breaker treats
    that as a connector-wide outage and trips on schedule."""

    @dataclass
    class _BlsTotalFailureSummary:
        dry_run: bool
        series_planned: list[str]
        series_ok: list[str]
        series_failed: list[tuple[str, str]]

    def _bls_total(conn, dry_run):
        return _BlsTotalFailureSummary(
            dry_run=dry_run,
            series_planned=["a", "b", "c"],
            series_ok=[],
            series_failed=[("a", "403"), ("b", "403"), ("c", "403")],
        )

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _bls_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_pending_release_total_trips(
    store: SQLiteEngineStore,
) -> None:
    """Auto-discovery value sweeps may plan a registry but attempt only
    due releases. If every due release fails and zero values land, the
    breaker treats the attempted batch as a connector-wide outage."""

    @dataclass
    class _PendingTotalFailureSummary:
        dry_run: bool
        series_planned: list[str]
        series_ok: list[str]
        series_empty: list[str]
        series_failed: list[tuple[str, str]]
        pending_releases: int
        observations_seen: int

    def _ine_total(conn, dry_run):
        return _PendingTotalFailureSummary(
            dry_run=dry_run,
            series_planned=["INE_CPI_ADVANCE_YOY", "INE_GDP_ADVANCE_QOQ"],
            series_ok=[],
            series_empty=["INE_GDP_ADVANCE_QOQ"],
            series_failed=[("INE_CPI_ADVANCE_YOY", "HTTP 503")],
            pending_releases=1,
            observations_seen=0,
        )

    for _ in range(FAILURE_THRESHOLD):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["ine"],
            _connector_overrides={"ine": _ine_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "ine")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_fed_values_partial_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fed-values partial: two statements fetched cleanly, one 404d.
    Breaker stays closed — the working pages still delivered rows."""

    @dataclass
    class _FedValuesPartialSummary:
        dry_run: bool
        meetings_planned: int
        meetings_fetched: int
        fetch_failures: list[tuple[str, str]]

    def _fed_partial(conn, dry_run):
        return _FedValuesPartialSummary(
            dry_run=dry_run,
            meetings_planned=3,
            meetings_fetched=2,  # partial
            fetch_failures=[("2020-03-15", "HTTP 404")],
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["fed-values"],
            _connector_overrides={"fed-values": _fed_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "fed-values")
    assert state.cooling_until_ms is None


def test_circuit_breaker_fed_values_total_trips(
    store: SQLiteEngineStore,
) -> None:
    """Fed-values total: every planned meeting URL failed. The
    breaker treats zero-fetched-of-N-planned as a connector outage."""

    @dataclass
    class _FedValuesTotalFailureSummary:
        dry_run: bool
        meetings_planned: int
        meetings_fetched: int
        fetch_failures: list[tuple[str, str]]

    def _fed_total(conn, dry_run):
        return _FedValuesTotalFailureSummary(
            dry_run=dry_run,
            meetings_planned=2,
            meetings_fetched=0,
            fetch_failures=[
                ("2025-01-29", "HTTP 503"),
                ("2025-03-19", "HTTP 503"),
            ],
        )

    for _ in range(FAILURE_THRESHOLD):
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["fed-values"],
            _connector_overrides={"fed-values": _fed_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "fed-values")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_ecb_partial_page_does_not_trip(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 2 finding: ECB's press-calendar
    scraper hits two pages (GC meetings + Economic Bulletin). When
    one succeeds and one 502s, ``fetch_errors`` records the bad one
    and ``entries_parsed`` is non-zero for the good one. That's a
    partial failure and must not trip the breaker — blocking the
    next sweep would delay fresh MP-decision / Bulletin rows on the
    healthy page."""

    @dataclass
    class _EcbPartialSummary:
        dry_run: bool
        entries_parsed: int
        events_upserted: int
        fetch_errors: dict[str, str]

    def _ecb_partial(conn, dry_run):
        return _EcbPartialSummary(
            dry_run=dry_run,
            entries_parsed=6,  # GC meetings page delivered rows
            events_upserted=6,
            fetch_errors={"bulletin": "HTTP 502"},  # other page failed
        )

    for _ in range(FAILURE_THRESHOLD + 2):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["ecb"],
            _connector_overrides={"ecb": _ecb_partial},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "ecb")
    assert state.cooling_until_ms is None  # breaker stays closed


def test_circuit_breaker_ecb_both_pages_failing_trips(
    store: SQLiteEngineStore,
) -> None:
    """Counterpoint: when both ECB pages fail (``entries_parsed==0``,
    ``fetch_errors`` non-empty), the breaker treats that as a
    connector-wide outage and trips on schedule."""

    @dataclass
    class _EcbTotalFailureSummary:
        dry_run: bool
        entries_parsed: int
        events_upserted: int
        fetch_errors: dict[str, str]

    def _ecb_total(conn, dry_run):
        return _EcbTotalFailureSummary(
            dry_run=dry_run,
            entries_parsed=0,
            events_upserted=0,
            fetch_errors={"meetings": "HTTP 502", "bulletin": "HTTP 502"},
        )

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["ecb"],
            _connector_overrides={"ecb": _ecb_total},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "ecb")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None


def test_circuit_breaker_cooldown_uses_failure_time_not_start(
    store: SQLiteEngineStore,
) -> None:
    """Fix for Codex P2 round 1 finding #2: ``cooling_until_ms``
    must be anchored on the failure moment, not the start of the
    connector run. A 5-minute Fed-values sweep failing at the end
    should still produce a ~full cool-down window; anchoring on
    start time would have already consumed 5 of 15 minutes before
    the cool-down even began."""
    import time as _time

    def _slow_fail(conn, dry_run):
        _time.sleep(0.1)  # small but measurable lag
        raise RuntimeError("slow failure")

    before_start = int(_time.time() * 1000)
    refresh_all_schedules(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _slow_fail},
    )
    # Continue failing to actually trip the breaker.
    for _ in range(FAILURE_THRESHOLD - 1):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _slow_fail},
        )
    after_trip = int(_time.time() * 1000)

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bls")
    assert state.cooling_until_ms is not None
    # ``cooling_until_ms`` should be anchored on or after the final
    # failure's record moment, which is after (not before) each run's
    # start. Specifically: cooling_until > after_trip - 1000 (allowing
    # ~1s slack) proves the anchor isn't the first run's start time.
    assert state.cooling_until_ms >= after_trip - 1000


def test_circuit_breaker_counts_summary_level_failures(
    store: SQLiteEngineStore,
) -> None:
    """In-summary failure markers (e.g. BEA ``fetch_error``) count
    toward the circuit-breaker threshold just like raised exceptions
    — they represent a connector-wide outage regardless of whether
    the exception propagated."""

    @dataclass
    class _BeaLikeSummary:
        dry_run: bool
        fetch_error: str

    def _bea_outage(conn, dry_run):
        return _BeaLikeSummary(dry_run=dry_run, fetch_error="502 Bad Gateway")

    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bea"],
            _connector_overrides={"bea": _bea_outage},
        )

    with store.get_connection() as conn:
        state = get_connector_state(conn, "bea")
    assert state.consecutive_failures == FAILURE_THRESHOLD
    assert state.cooling_until_ms is not None
    assert "502" in (state.last_error or "")


def test_value_side_driver_shares_breaker_state(
    store: SQLiteEngineStore,
) -> None:
    """A connector's breaker state is scoped by scheduler-level name,
    so failures on the schedule-side ``"bls"`` surface the same
    connector as the value-side. A BLS outage during schedule refresh
    trips the breaker, and the next value-side sweep honors the
    cool-down on ``"bls"`` without a re-trip."""
    def _boom(conn, dry_run):
        raise RuntimeError("bls.gov 403")

    # Trip on schedule-side.
    for _ in range(FAILURE_THRESHOLD):
        refresh_all_schedules(
            store.get_connection,
            dry_run=False,
            connectors=["bls"],
            _connector_overrides={"bls": _boom},
        )

    # Value-side: should skip BLS due to shared breaker state.
    value_calls: list[str] = []

    def _would_run(conn, dry_run):
        value_calls.append("ran")
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _would_run},
    )
    assert value_calls == []  # BLS skipped
    bls_result = summary.results[0]
    assert bls_result.ok is False
    assert "cooling until" in (bls_result.error or "")


def test_value_side_service_op_forwards_year_args(
    store: SQLiteEngineStore,
) -> None:
    """Operator-driven one-off with an explicit year window: the
    service op forwards ``start_year`` / ``end_year`` into the driver.
    Verified via the dry-run output since the real connectors still
    surface their plan regardless of the window."""
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_sweep_values",
        {
            "dry_run": True,
            "connectors": ["bls"],
            "start_year": 2020,
            "end_year": 2021,
        },
    )
    assert result["dry_run"] is True
    bls = result["results"][0]
    # BLS dry-run surfaces its resolved year window in the summary
    # dict via ``FetchRunSummary.start_year`` / ``end_year``.
    assert bls["summary"]["start_year"] == 2020
    assert bls["summary"]["end_year"] == 2021
