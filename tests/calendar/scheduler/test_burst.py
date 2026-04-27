"""Scheduler tests: schedule-aware burst (issue #37).

Split out of the original tests/test_calendar_refresh_scheduler.py
as part of issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pytest
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.scheduler import (
    ALL_VALUE_SIDE_CONNECTORS,
    sweep_value_side,
)
from ingestion.calendar.scheduler_state import (
    FAILURE_THRESHOLD,
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


def _insert_pending_event(
    store: SQLiteEngineStore,
    *,
    provider: str,
    provider_event_id: str,
    title: str,
    event_time_utc: str,
    actual: str | None = None,
) -> None:
    """Seed a ``cal_econ_event`` row for burst-window predicate tests."""
    with store.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                event_time_precision, reference_date, reference_label,
                country_code, indicator_id, category, title,
                importance, currency, unit, actual, previous, revised,
                forecast, consensus_forecast, ticker, source, source_url,
                content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, 'datetime', NULL, '',
                'US', NULL, '', ?,
                NULL, '', '', ?, NULL, NULL,
                NULL, NULL, '', '', '',
                'h', NULL, 0,
                '2026-04-26T00:00:00+00:00', '2026-04-26T00:00:00+00:00'
            )
            """,
            (provider, provider_event_id, event_time_utc, title, actual),
        )
        conn.commit()


def test_burst_filters_cover_every_burst_eligible_connector() -> None:
    """Every value-side connector except the documented exclusions has
    a burst predicate. ECB / EIA / DOL / ONS / BoE are intentionally
    absent — their value fetchers write rows only after publication
    (the API / press-release / Bank Rate page carries period + value
    together), so a pre-release schedule row never exists for the
    burst's "until ``actual`` lands" completion check. They fall
    through to the hourly baseline."""
    from ingestion.calendar.scheduler import _VALUE_SIDE_DUE_ROW_FILTERS

    expected = set(ALL_VALUE_SIDE_CONNECTORS) - {"ecb", "eia", "dol", "ons", "boe"}
    assert set(_VALUE_SIDE_DUE_ROW_FILTERS.keys()) == expected


def test_burst_zero_due_rows_runs_connector_once(
    store: SQLiteEngineStore,
) -> None:
    """Baseline behavior preserved: a connector with no rows in the
    burst window runs exactly once and the breaker handles it as
    before. ``burst_*`` keys are absent from the summary so the
    operator card stays clean for the no-burst case."""
    call_count = {"n": 0}

    def _fn(conn, dry_run):
        call_count["n"] += 1
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
    )
    assert call_count["n"] == 1
    bls = next(r for r in summary.results if r.connector == "bls")
    assert bls.ok is True
    assert "burst_attempts" not in bls.summary


def test_burst_loops_until_actual_filled(
    store: SQLiteEngineStore,
) -> None:
    """With a single due row in the window, the driver bursts until
    ``actual`` lands. The fake fills the row on attempt 3 and the
    burst exits on the next due-row recount with
    ``burst_stopped_reason='actual_filled'``."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi",
        title="CPI",
        event_time_utc=sweep_start.isoformat(),
    )

    sleep_log: list[float] = []
    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        if attempts["n"] == 3:
            conn.execute(
                "UPDATE cal_econ_event SET actual = '0.4' "
                "WHERE provider = 'bls' AND provider_event_id = 'bls-cpi'"
            )
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=sleep_log.append,
    )
    assert attempts["n"] == 3
    # Two sleeps separate three attempts.
    assert sleep_log == [60.0, 60.0]
    bls = next(r for r in summary.results if r.connector == "bls")
    assert bls.summary["burst_attempts"] == 3
    assert bls.summary["burst_initial_due"] == 1
    assert bls.summary["burst_stopped_reason"] == "actual_filled"


def test_burst_caps_at_max_attempts(
    store: SQLiteEngineStore,
) -> None:
    """A connector that never fills the row exits after the configured
    max-attempts ceiling. Reason is ``max_attempts``; budget consumed
    is bounded (5 attempts here)."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi",
        title="CPI",
        event_time_utc=sweep_start.isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1  # never writes ``actual``
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_max_attempts=5,
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 5
    bls = next(r for r in summary.results if r.connector == "bls")
    assert bls.summary["burst_attempts"] == 5
    assert bls.summary["burst_stopped_reason"] == "max_attempts"


def test_burst_stops_when_window_closes(
    store: SQLiteEngineStore,
) -> None:
    """If the wall clock advances past ``sweep_start + 30min`` mid-
    burst, the driver exits with ``window_closed`` rather than
    spinning until max-attempts. The burst is bounded by the upper
    edge of the window even when attempts are cheap."""
    from datetime import datetime, timedelta, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi",
        title="CPI",
        event_time_utc=sweep_start.isoformat(),
    )

    clock_calls = {"n": 0}

    def _clock():
        clock_calls["n"] += 1
        # First call: sweep start. Subsequent calls: advance enough
        # to push past the window after a couple of iterations.
        if clock_calls["n"] == 1:
            return sweep_start
        return sweep_start + timedelta(minutes=31)

    def _fn(conn, dry_run):
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=_clock,
        _burst_sleep=lambda _: None,
    )
    bls = next(r for r in summary.results if r.connector == "bls")
    assert bls.summary["burst_stopped_reason"] == "window_closed"


def test_burst_stops_on_breaker_cooling(
    store: SQLiteEngineStore,
) -> None:
    """A connector tripped into cooling at sweep start is caught on
    the very first attempt — the breaker returns the cooling reason
    without invoking the connector. The burst loop sees the skip and
    exits with ``breaker_or_budget`` rather than spinning."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi",
        title="CPI",
        event_time_utc=sweep_start.isoformat(),
    )

    # Pre-trip the BLS breaker so the first attempt skips.
    with store.get_connection() as conn:
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(
                conn, "bls", error="x",
                now_ms=int(sweep_start.timestamp() * 1000),
            )
        conn.commit()

    attempts = {"n": 0}

    def _fn(conn, dry_run):  # pragma: no cover — never called
        attempts["n"] += 1
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 0
    bls = next(r for r in summary.results if r.connector == "bls")
    assert bls.ok is False
    assert "circuit breaker cooling" in (bls.error or "")
    assert bls.summary["burst_stopped_reason"] == "breaker_or_budget"


def test_burst_skips_when_event_outside_window(
    store: SQLiteEngineStore,
) -> None:
    """An event outside the [now-1h, now+30min] window doesn't
    qualify as a due row — connector runs exactly once. Validates
    the window membership filter, not just the ``actual IS NULL``
    half of the predicate."""
    from datetime import datetime, timedelta, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-old",
        title="CPI",
        # Two hours before sweep start — outside the lookback window.
        event_time_utc=(sweep_start - timedelta(hours=2)).isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1
    bls = next(r for r in summary.results if r.connector == "bls")
    assert "burst_attempts" not in bls.summary


def test_burst_disambiguates_boj_mpm_from_tankan(
    store: SQLiteEngineStore,
) -> None:
    """Both BoJ MPM (``boj-values``) and Tankan (``boj-tankan-values``)
    write under provider ``boj``. The filter map disambiguates by
    title so each connector only bursts on its own pending rows.
    Tankan row inside the buffered MPM window must not trigger the
    MPM burst — the title predicate weeds it out before the count."""
    from datetime import datetime, timedelta, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    # 90 min ago — inside boj-values' 1h-buffered window
    # ``[sweep_start - 2h, sweep_start - 30min]``. Without title
    # disambiguation the Tankan row would count as due.
    _insert_pending_event(
        store,
        provider="boj",
        provider_event_id="tankan-q1",
        title="Tankan Large Manufacturers",
        event_time_utc=(sweep_start - timedelta(minutes=90)).isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(
            connector="boj-values", dry_run=dry_run, calls=1,
        )

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["boj-values"],
        _connector_overrides={"boj-values": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1  # baseline single pass — Tankan row is not MPM
    bv = next(r for r in summary.results if r.connector == "boj-values")
    assert "burst_attempts" not in bv.summary


def test_burst_skips_ecb_falls_through_to_baseline(
    store: SQLiteEngineStore,
) -> None:
    """Codex round 2 P2 #1: ECB's schedule rows (``MP Decision`` /
    ``Bulletin``) and value-row outputs (``ECB % Rate``) are
    decoupled — value fetcher writes new rate rows rather than
    filling ``actual`` on schedule rows. Neither side fits the
    burst's "until actual lands" completion check, so ECB is absent
    from the predicate map and any ECB row in the window leaves
    the connector on the hourly baseline."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="ecb",
        provider_event_id="ecb-mp-decision",
        title="ECB Monetary Policy Decision",
        event_time_utc=sweep_start.isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="ecb", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["ecb"],
        _connector_overrides={"ecb": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1  # baseline only — ECB excluded from burst
    ecb = next(r for r in summary.results if r.connector == "ecb")
    assert "burst_attempts" not in ecb.summary


def test_burst_excludes_event_at_window_upper_boundary(
    store: SQLiteEngineStore,
) -> None:
    """Codex round 2 P2 #3: a row exactly at ``sweep_start + 30min``
    would be counted as due under an inclusive boundary, but the
    burst's last attempt fires at sweep_start + ~29min — the row
    never gets an attempt at its eligibility time. Exclusive upper
    bound (``<`` not ``<=``) keeps the row on the next hourly
    sweep instead of triggering a wasted burst."""
    from datetime import datetime, timedelta, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi-edge",
        title="CPI",
        event_time_utc=(sweep_start + timedelta(minutes=30)).isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1  # boundary row excluded — no burst
    bls = next(r for r in summary.results if r.connector == "bls")
    assert "burst_attempts" not in bls.summary


def test_burst_excludes_fed_release_date_rows(
    store: SQLiteEngineStore,
) -> None:
    """Codex round 1 P2 #2: ``fed-values`` only fills FOMC Rate
    Decision rows. Beige Book / H.4.1 / H.8 release-date rows under
    ``provider='federal-reserve'`` carry ``actual=NULL`` permanently
    and would otherwise spin the burst until max-attempts."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="federal-reserve",
        provider_event_id="fed-beige-book",
        title="Beige Book",
        event_time_utc=sweep_start.isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="fed-values", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["fed-values"],
        _connector_overrides={"fed-values": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1  # baseline — release-date row excluded
    fv = next(r for r in summary.results if r.connector == "fed-values")
    assert "burst_attempts" not in fv.summary


def test_burst_skips_buffered_connector_for_future_event(
    store: SQLiteEngineStore,
) -> None:
    """Codex round 1 P2 #3: a JP-buffered connector (BoJ MPM here)
    cannot fetch a row until 1h after its scheduled time. A row at
    sweep_start would otherwise be counted as due even though no
    attempt could fill it for the next hour. Burst window must
    shift back by the connector's fetch buffer."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="boj",
        provider_event_id="boj-rate-202604",
        title="BoJ Interest Rate Decision",
        event_time_utc=sweep_start.isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="boj-values", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["boj-values"],
        _connector_overrides={"boj-values": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1  # outside the buffered window — no burst
    bv = next(r for r in summary.results if r.connector == "boj-values")
    assert "burst_attempts" not in bv.summary


def test_burst_buffered_connector_inside_shifted_window(
    store: SQLiteEngineStore,
) -> None:
    """Mirror of the future-event test: a row 90 minutes ago (inside
    the BoJ-shifted ``[sweep_start − 2h, sweep_start − 30min]``
    window) is correctly counted as due and triggers a burst."""
    from datetime import datetime, timedelta, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="boj",
        provider_event_id="boj-rate-past",
        title="BoJ Interest Rate Decision",
        event_time_utc=(sweep_start - timedelta(minutes=90)).isoformat(),
    )

    def _fn(conn, dry_run):
        return _FakeSummary(connector="boj-values", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=False,
        connectors=["boj-values"],
        _connector_overrides={"boj-values": _fn},
        _burst_max_attempts=3,
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    bv = next(r for r in summary.results if r.connector == "boj-values")
    assert bv.summary["burst_initial_due"] == 1
    assert bv.summary["burst_attempts"] == 3


def test_burst_dry_run_does_not_burst(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run never bursts — even with a due row in the window —
    because no fetches mean nothing could ever fill ``actual``. A
    dry-run burst would just spin until max_attempts. Single pass
    keeps the dry-run plan envelope identical to the pre-#37 shape."""
    from datetime import datetime, timezone

    sweep_start = datetime(2026, 4, 26, 12, 30, tzinfo=timezone.utc)
    _insert_pending_event(
        store,
        provider="bls",
        provider_event_id="bls-cpi",
        title="CPI",
        event_time_utc=sweep_start.isoformat(),
    )

    attempts = {"n": 0}

    def _fn(conn, dry_run):
        attempts["n"] += 1
        return _FakeSummary(connector="bls", dry_run=dry_run, calls=1)

    summary = sweep_value_side(
        store.get_connection,
        dry_run=True,
        connectors=["bls"],
        _connector_overrides={"bls": _fn},
        _burst_clock=lambda: sweep_start,
        _burst_sleep=lambda _: None,
    )
    assert attempts["n"] == 1
    bls = next(r for r in summary.results if r.connector == "bls")
    assert "burst_attempts" not in bls.summary


def test_bls_fetcher_dry_run_reports_zero_requests_made() -> None:
    """Dry-run issues no HTTP so ``requests_made`` stays zero — even
    if the caller supplied a client whose counter is already non-zero
    from prior same-day activity."""
    from ingestion.calendar.bls_api.fetcher import fetch_bls_calendar
    from storage.sqlite import SQLiteEngineStore

    class _FakeBLS:
        api_key = "dummy"
        daily_query_count = 47  # prior same-day activity

        def get_series(self, *args, **kwargs):  # pragma: no cover — not called
            raise AssertionError("get_series must not fire during dry_run")

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        store = SQLiteEngineStore(db_path=Path(tmp) / "engine.db")
        with store.get_connection() as conn:
            summary = fetch_bls_calendar(
                conn, _FakeBLS(),
                start_year=2025,
                end_year=2026,
                series_ids=["CUUR0000SA0"],
                dry_run=True,
            )
    assert summary.requests_made == 0
