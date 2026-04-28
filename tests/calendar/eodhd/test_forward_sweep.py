"""Forward-sweep service op + orchestrator wiring — issue #63."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
import respx

from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def test_forward_sweep_dry_run_emits_no_http(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    with respx.mock(assert_all_called=False) as router:
        router.route().mock(return_value=httpx.Response(500, text="must_not_call"))
        result = service.invoke(
            "calendar_corp_forward_sweep",
            {"dry_run": True},
        )
        assert router.calls.call_count == 0
    assert result["dry_run"] is True
    assert result["failed_count"] == 0
    assert result["ok_count"] == 5
    # dividend_details must run last so it sees this run's discovery rows.
    assert [r["subtype"] for r in result["results"]][-1] == "dividend_details"


def test_forward_sweep_window_is_lookback_to_lookforward(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_forward_sweep",
        {"dry_run": True, "lookback_days": 7, "lookforward_days": 90},
    )
    # 7-back + 90-forward = 98 inclusive days; the planner emits ceil(98/7)=14
    # weekly windows for the date-scoped subtypes.
    earnings = next(r for r in result["results"] if r["subtype"] == "earnings")
    assert earnings["windows_planned"] == 14


def test_forward_sweep_rejects_unknown_subtype(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_forward_sweep",
        {"dry_run": True, "subtypes": ["earnings", "totally_made_up"]},
    )
    assert "error" in result
    assert "totally_made_up" in result["error"]


def test_forward_sweep_subset_filters_subtypes(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_forward_sweep",
        {"dry_run": True, "subtypes": ["ipo", "split"]},
    )
    assert sorted(r["subtype"] for r in result["results"]) == ["ipo", "split"]


def test_forward_sweep_isolates_per_subtype_failure(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """A raising subtype must not abort the rest — AC4 of issue #63."""
    from macro_data.service import LocalMacroDataService
    from ingestion.calendar.eodhd_api import fetcher as fetcher_mod

    real_fetch = fetcher_mod.CorpCalendarFetcher.fetch
    calls: list[str] = []

    def flaky_fetch(self, *, subtype, start, end, symbols=None, dry_run=True):
        calls.append(subtype)
        if subtype == "ipo":
            raise RuntimeError("upstream blew up")
        return real_fetch(
            self, subtype=subtype, start=start, end=end,
            symbols=symbols, dry_run=dry_run,
        )

    monkeypatch.setattr(fetcher_mod.CorpCalendarFetcher, "fetch", flaky_fetch)

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_forward_sweep",
        {"dry_run": True, "subtypes": ["earnings", "ipo", "split"]},
    )
    assert calls == ["earnings", "ipo", "split"]
    by_subtype = {r["subtype"]: r for r in result["results"]}
    assert by_subtype["earnings"]["ok"] is True
    assert by_subtype["ipo"]["ok"] is False
    assert "upstream blew up" in by_subtype["ipo"]["error"]
    assert by_subtype["split"]["ok"] is True
    assert result["failed_count"] == 1


def test_forward_sweep_dividend_details_runs_last_when_listed_first(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    service = LocalMacroDataService(store=store)
    result = service.invoke(
        "calendar_corp_forward_sweep",
        {"dry_run": True, "subtypes": ["dividend_details", "dividend"]},
    )
    assert [r["subtype"] for r in result["results"]] == ["dividend", "dividend_details"]


def test_orchestrator_registers_corp_calendar_sources() -> None:
    """Orchestrator entries surface the corp lane in list_sources()."""
    from ingestion import IngestionOrchestrator
    from ingestion.sources import SOURCE_FAMILIES

    orch = IngestionOrchestrator(store=Mock())
    names = {s["name"] for s in orch.list_sources()}
    expected = {
        "corp_calendar_earnings",
        "corp_calendar_ipo",
        "corp_calendar_split",
        "corp_calendar_dividend",
        "corp_calendar_dividend_details",
    }
    assert expected.issubset(names)
    for name in expected:
        assert SOURCE_FAMILIES[name] == "calendar"
        # Not on the auto-refresh roster — timer-driven only so refresh_all
        # does not silently consume EODHD quota on every operator sweep.
        assert name not in orch._default_refresh_order


def test_corp_calendar_storage_stats_target_cal_corp_event(
    store: SQLiteEngineStore,
) -> None:
    stats = store.get_source_storage_stats("corp_calendar")
    assert stats["table"] == "cal_corp_event"
    assert stats["count"] == 0


def test_corp_calendar_capability_appears_in_health_dashboard(
    store: SQLiteEngineStore,
) -> None:
    from ingestion import IngestionOrchestrator

    orch = IngestionOrchestrator(store=store)
    health = orch.get_source_health_dashboard(include_internal=True)
    sources = {item["source_id"]: item for item in health["sources"]}
    assert "corp_calendar" in sources
    assert sources["corp_calendar"]["storage_table"] == "cal_corp_event"
    assert sources["corp_calendar"]["is_default_scheduled"] is True


def test_corp_calendar_sync_latest_honours_limit(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """``catalog-sync-latest --limit 1`` must run one job, not five."""
    from ingestion import IngestionOrchestrator
    from ingestion.source_capabilities import SourceCapabilityManager

    calls: list[str] = []

    class _StubReport:
        error = ""
        stored = 0

    orch = IngestionOrchestrator(store=store)
    monkeypatch.setattr(
        orch, "run_source",
        lambda name: (calls.append(name) or _StubReport()),
    )
    mgr = SourceCapabilityManager(store=store, orchestrator=orch)
    adapter = mgr._adapters["corp_calendar"]
    result = adapter.sync_latest(None, 1)
    assert calls == ["corp_calendar_earnings"]
    assert result.entities_total == 1


def test_corp_calendar_sync_latest_isolates_failures_then_raises(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """A failing subtype must not abort the others; partial failure
    must surface as a RuntimeError so SourceCapabilityManager records
    the run as failed in catalog_sync_runs (silent success would mask
    a half-broken sweep from /health automation)."""
    from ingestion import IngestionOrchestrator
    from ingestion.source_capabilities import SourceCapabilityManager

    calls: list[str] = []

    class _Report:
        def __init__(self, *, error: str = "", stored: int = 0) -> None:
            self.error = error
            self.stored = stored

    def _fake_run(name: str) -> _Report:
        calls.append(name)
        if name == "corp_calendar_ipo":
            return _Report(error="upstream blew up")
        return _Report(stored=3)

    orch = IngestionOrchestrator(store=store)
    monkeypatch.setattr(orch, "run_source", _fake_run)
    mgr = SourceCapabilityManager(store=store, orchestrator=orch)
    adapter = mgr._adapters["corp_calendar"]
    with pytest.raises(RuntimeError, match="upstream blew up"):
        adapter.sync_latest(None, None)
    # All five jobs were attempted before the raise.
    assert len(calls) == 5


def test_corp_calendar_sync_latest_force_orders_dividend_before_details(
    store: SQLiteEngineStore, monkeypatch,
) -> None:
    """``dividend_details`` reads from cal_corp_event, so the discovery
    job must always run first even if the caller asked for the reverse."""
    from ingestion import IngestionOrchestrator
    from ingestion.source_capabilities import SourceCapabilityManager

    calls: list[str] = []

    class _Report:
        error = ""
        stored = 0

    orch = IngestionOrchestrator(store=store)
    monkeypatch.setattr(
        orch, "run_source",
        lambda name: (calls.append(name) or _Report()),
    )
    mgr = SourceCapabilityManager(store=store, orchestrator=orch)
    adapter = mgr._adapters["corp_calendar"]
    adapter.sync_latest(["dividend_details", "dividend"], None)
    assert calls == ["corp_calendar_dividend", "corp_calendar_dividend_details"]


def test_collect_dividend_tickers_orders_unenriched_first(tmp_path) -> None:
    """Daily budget must rotate — already-enriched tickers go to the
    back of the queue so a 30-request cap doesn't loop on the same
    alphabetic prefix."""
    import sqlite3
    from datetime import date as _date
    from macro_data.service._calendar import _collect_dividend_tickers

    db_path = tmp_path / "engine.db"
    store = SQLiteEngineStore(db_path=db_path)
    conn = store.get_connection()
    # AAPL.US — enriched (reference_date set); ZZZ.US — unenriched.
    conn.execute(
        """INSERT INTO cal_corp_event(
              provider, provider_event_id, event_subtype, event_time_utc,
              ticker, exchange, content_hash, observed_at_epoch_ms,
              reference_date, created_at, updated_at
            ) VALUES
            ('eodhd','aapl-1','dividend','2026-05-01',
             'AAPL','US','h1', 100, '2026-04-15', '2026-04-01', '2026-04-01'),
            ('eodhd','zzz-1','dividend','2026-05-02',
             'ZZZ','US','h2', 200, NULL, '2026-04-01', '2026-04-01')
        """
    )
    conn.commit()
    conn.close()
    conn = store.get_connection()
    try:
        codes = _collect_dividend_tickers(
            conn, start=_date(2026, 4, 1), end=_date(2026, 6, 30),
        )
    finally:
        conn.close()
    assert codes == ["ZZZ.US", "AAPL.US"]


def test_forward_sweep_script_treats_top_level_error_as_failure(tmp_path) -> None:
    """An invalid --subtypes that returns a top-level ``error`` key must
    yield exit code 1 — not 0 with status=ok."""
    import scripts.calendar_corp_forward as runner

    log_path = tmp_path / "out.log"
    rc = runner.main([
        "--db-path", str(tmp_path / "engine.db"),
        "--log-path", str(log_path),
        "--dry-run",
        "--subtypes", "totally_made_up",
    ])
    assert rc == 1
    payload = log_path.read_text().strip().splitlines()[-1]
    import json as _json
    assert _json.loads(payload)["status"] == "error"
