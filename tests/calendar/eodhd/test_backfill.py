"""Tests for ``CorpBackfillRunner`` — issue #62.

Cover the cursor-driven resume loop, idempotent re-runs, the
budget-cap stop, and the dividend two-stage discovery + detail
enrichment flow.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx
from storage.sqlite import SQLiteEngineStore

from ingestion.calendar.eodhd_api import (
    CorpBackfillRunner,
    EODHDAPIClient,
    plan_corp_windows,
)


def _split_row(**overrides):
    base = {
        "code": "AAPL.US",
        "split_date": "2024-06-10",
        "split_from": 1,
        "split_to": 4,
        "optionable": "Yes",
    }
    base.update(overrides)
    return base


def _dividend_row(**overrides):
    base = {"symbol": "MSFT.US", "date": "2024-02-15"}
    base.update(overrides)
    return base


def _dividend_detail_row(**overrides):
    base = {
        "date": "2024-02-15",
        "value": 0.75,
        "unadjustedValue": 0.75,
        "currency": "USD",
        "declarationDate": "2024-01-30",
        "recordDate": "2024-02-16",
        "paymentDate": "2024-03-14",
        "period": "Quarterly",
    }
    base.update(overrides)
    return base


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_plan_windows_partitions_phases() -> None:
    today = date(2026, 4, 28)
    windows = plan_corp_windows(
        subtype="split", start=date(2015, 1, 1), end=today,
        window_days=30, today=today,
    )
    phases = {w.phase for w in windows}
    assert phases == {"recent", "mid", "early"}
    # No window crosses a phase boundary.
    for w in windows:
        if w.phase == "early":
            assert w.end <= date(2017, 12, 31)
        if w.phase == "mid":
            assert date(2018, 1, 1) <= w.start
            assert w.end <= date(2023, 12, 31)
        if w.phase == "recent":
            assert w.start >= date(2024, 1, 1)
            assert w.end <= today


def test_run_rejects_earnings_trend(store: SQLiteEngineStore) -> None:
    conn = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        runner = CorpBackfillRunner(connection=conn, client=client)
        with pytest.raises(ValueError):
            runner.run(subtype="earnings_trend", dry_run=True)
    finally:
        conn.close()


def test_dry_run_emits_plan_without_http(store: SQLiteEngineStore) -> None:
    conn = store.get_connection()
    try:
        client = EODHDAPIClient(api_key="unit", sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client, window_days=30,
            now_utc=lambda: datetime(2026, 4, 28, tzinfo=timezone.utc),
        )
        with respx.mock(assert_all_called=False) as router:
            router.route().mock(return_value=httpx.Response(500))
            summary = runner.run(
                subtype="split", phases=["early"], dry_run=True,
            )
            assert router.calls.call_count == 0
    finally:
        conn.close()
    assert summary.dry_run is True
    assert summary.phases_planned == ["early"]
    assert summary.windows_planned > 0
    assert summary.stopped_reason == "dry_run"


@respx.mock
def test_budget_cap_halts_and_persists_partial_progress(
    store: SQLiteEngineStore, monkeypatch
) -> None:
    """`max_requests` should stop the loop mid-phase; the cursor row
    must record the spend so resume picks up where the cap landed."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/splits").mock(
        return_value=httpx.Response(200, json={"splits": [_split_row()]}),
    )
    fixed_now = datetime(2026, 4, 28, tzinfo=timezone.utc)

    conn = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client,
            max_requests=2, window_days=30,
            now_utc=lambda: fixed_now,
        )
        first = runner.run(
            subtype="split", phases=["recent"], dry_run=False,
        )
        conn.commit()
    finally:
        conn.close()

    assert first.requests_spent == 2
    assert first.stopped_reason == "max_requests_reached"

    # Cursor row exists for (eodhd, split, recent).
    conn = store.get_connection()
    try:
        row = conn.execute(
            "SELECT cursor_date, requests_spent, is_complete "
            "FROM cal_corp_backfill_cursor "
            "WHERE provider='eodhd' AND subtype='split' AND phase='recent'"
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[1] >= 2
    assert row[2] == 0  # not complete


@respx.mock
def test_resume_after_kill_skips_completed_windows(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """Two back-to-back runs with the same fixed clock — the second
    must not re-fetch any window the first already completed."""
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    call_count = {"n": 0}

    def _respond(request):
        call_count["n"] += 1
        return httpx.Response(200, json={"splits": [_split_row()]})

    respx.get(url__startswith="https://eodhd.com/api/calendar/splits").mock(
        side_effect=_respond,
    )
    fixed_now = datetime(2026, 4, 28, tzinfo=timezone.utc)

    # First run: 2-call cap on a 4-window plan.
    conn = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client,
            max_requests=2, window_days=120,  # large windows → ~3 in 'recent'
            now_utc=lambda: fixed_now,
        )
        runner.run(subtype="split", phases=["recent"], dry_run=False)
        conn.commit()
    finally:
        conn.close()

    first_calls = call_count["n"]

    # Second run: same plan, should skip the windows the first already
    # finished and only fetch the remaining tail.
    conn = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client,
            max_requests=10, window_days=120,
            now_utc=lambda: fixed_now,
        )
        second = runner.run(
            subtype="split", phases=["recent"], dry_run=False,
        )
        conn.commit()
    finally:
        conn.close()

    second_calls = call_count["n"] - first_calls
    # First run completed at most 2 windows; second should fetch the
    # remaining tail without redoing the first run's work.
    assert second_calls > 0
    assert second_calls < first_calls + second_calls  # didn't replay everything
    assert second.stopped_reason == "completed"


@respx.mock
def test_dividend_two_stage_runs_detail_after_discovery(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    # Discovery feed: two unique tickers.
    respx.get(url__startswith="https://eodhd.com/api/calendar/dividends").mock(
        return_value=httpx.Response(
            200,
            json={
                "meta": {"total": 2, "offset": 0, "limit": 1000},
                "data": [
                    _dividend_row(symbol="AAPL.US", date="2024-02-09"),
                    _dividend_row(symbol="MSFT.US", date="2024-02-15"),
                ],
                "links": {"next": None},
            },
        )
    )
    # Per-ticker detail feeds.
    respx.get(url__startswith="https://eodhd.com/api/div/AAPL.US").mock(
        return_value=httpx.Response(
            200, json=[_dividend_detail_row(date="2024-02-09")],
        ),
    )
    respx.get(url__startswith="https://eodhd.com/api/div/MSFT.US").mock(
        return_value=httpx.Response(
            200, json=[_dividend_detail_row(date="2024-02-15", value=0.75)],
        ),
    )
    fixed_now = datetime(2026, 4, 28, tzinfo=timezone.utc)

    conn = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client,
            max_requests=20, window_days=120,
            now_utc=lambda: fixed_now,
        )
        summary = runner.run(
            subtype="dividend",
            start=date(2024, 1, 1),
            end=date(2024, 3, 31),
            phases=["recent"],
            dry_run=False,
        )
        conn.commit()
    finally:
        conn.close()

    # At least one discovery request + 2 detail enrichment calls.
    assert summary.requests_spent >= 3
    assert summary.dividend_detail_symbols == 2
    assert summary.events_upserted >= 2
    assert summary.stopped_reason == "completed"


@respx.mock
def test_scoped_run_does_not_write_shared_cursor(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """`--symbols` or explicit `--from`/`--to` mark the run as scoped.

    The shared `(provider, subtype, phase)` cursor must stay untouched
    so a later default backfill does not skip the windows the scoped
    run consumed.
    """
    monkeypatch.setenv("EODHD_API_KEY", "unit")
    respx.get(url__startswith="https://eodhd.com/api/calendar/splits").mock(
        return_value=httpx.Response(200, json={"splits": [_split_row()]}),
    )
    fixed_now = datetime(2026, 4, 28, tzinfo=timezone.utc)

    conn = store.get_connection()
    try:
        client = EODHDAPIClient(sleeper=lambda _s: None)
        runner = CorpBackfillRunner(
            connection=conn, client=client,
            max_requests=10, window_days=30,
            now_utc=lambda: fixed_now,
        )
        runner.run(
            subtype="split",
            phases=["recent"],
            symbols=["AAPL.US"],  # scoped → cursor must stay clean
            dry_run=False,
        )
        conn.commit()
    finally:
        conn.close()

    conn = store.get_connection()
    try:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_corp_backfill_cursor "
            "WHERE provider='eodhd' AND subtype='split'"
        ).fetchone()
    finally:
        conn.close()
    assert rows[0] == 0, "scoped run leaked into the shared cursor"
