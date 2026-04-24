from __future__ import annotations

import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.source_capabilities import (
    CapabilitySyncResult,
    SourceCapabilityAdapter,
    SourceCapabilityManager,
)
from storage.sqlite import SQLiteEngineStore


def _make_store() -> SQLiteEngineStore:
    temp_dir = tempfile.TemporaryDirectory()
    store = SQLiteEngineStore(Path(temp_dir.name) / "engine.db")
    store._temp_dir = temp_dir  # keep alive for test lifetime
    return store


def test_capability_registry_is_seeded_in_store() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
                supports_structure=True,
            )
        },
    )

    rows = manager.list_capabilities()

    assert len(rows) == 1
    assert rows[0]["source_id"] == "alpha"
    assert rows[0]["supports_structure"] is True
    assert store.get_source_capability("alpha") is not None


def test_sync_discovery_persists_entities_and_checkpoint() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
                discover=lambda query, limit: [
                    {
                        "source_id": "alpha",
                        "entity_id": "ds1",
                        "entity_type": "dataset",
                        "display_name": "Dataset 1",
                        "description": "first dataset",
                        "metadata": {"kind": "demo"},
                    },
                    {
                        "source_id": "alpha",
                        "entity_id": "ds2",
                        "entity_type": "dataset",
                        "display_name": "Dataset 2",
                        "description": "second dataset",
                        "metadata": {"kind": "demo"},
                    },
                ],
            )
        },
    )

    result = manager.sync_discovery("alpha")

    assert result["status"] == "success"
    assert result["entities_synced"] == 2
    assert store.count_catalog_entities("alpha") == 2
    checkpoint = store.get_catalog_sync_checkpoint("alpha", "discovery")
    assert checkpoint is not None
    assert checkpoint["entities_synced"] == 2


def test_list_entities_auto_refreshes_when_store_is_empty() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
                discover=lambda query, limit: [
                    {
                        "source_id": "alpha",
                        "entity_id": "ds1",
                        "entity_type": "dataset",
                        "display_name": "Dataset 1",
                        "description": "first dataset",
                        "metadata": {},
                    }
                ],
            )
        },
    )

    payload = manager.list_entities("alpha", limit=10)

    assert payload["total"] == 1
    assert payload["entities"][0]["entity_id"] == "ds1"


def test_sync_latest_records_checkpoint_and_run() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
                sync_latest=lambda entity_ids, limit: CapabilitySyncResult(
                    entities_total=3,
                    entities_synced=2,
                    observations_synced=42,
                    metadata={"limit": limit},
                ),
            )
        },
    )

    result = manager.sync_latest("alpha", limit=5)

    assert result["status"] == "success"
    assert result["observations_synced"] == 42
    checkpoint = store.get_catalog_sync_checkpoint("alpha", "latest_sync")
    assert checkpoint is not None
    assert checkpoint["observations_synced"] == 42
    runs = store.list_catalog_sync_runs(source_id="alpha", job_type="latest_sync", limit=5)
    assert len(runs) == 1
    assert runs[0]["status"] == "success"


def test_default_calendar_capability_is_discovery_only() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(store)

    capability = store.get_source_capability("calendar")
    assert capability is not None
    assert capability["supports_latest_sync"] is False
    assert capability["is_default_scheduled"] is False

    entities = manager.list_entities("calendar", limit=50)["entities"]
    ids = {entity["entity_id"] for entity in entities}
    assert {"tradingeconomics", "eodhd", "bls", "nar", "zew", "insee", "hcob"} <= ids
    assert manager.sync_latest("calendar") == {
        "error": "latest sync unavailable for calendar"
    }


def _make_manager_with_fake_adapter() -> SourceCapabilityManager:
    """Minimal manager for health-dashboard tests — one trivial adapter
    keeps the sources array non-empty so we exercise the calendar
    block alongside the existing source logic."""
    store = _make_store()
    return SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
            ),
        },
    )


def test_health_dashboard_exposes_empty_calendar_block_by_default() -> None:
    """No scheduler runs yet → the calendar block is present with
    empty counters. Operators can tell "no cooling / no budget
    exhaustion" at a glance without parsing a missing field."""
    manager = _make_manager_with_fake_adapter()

    dashboard = manager.get_customer_health()

    assert dashboard["calendar_connectors"] == []
    summary = dashboard["summary"]
    assert summary["calendar_cooling"] == 0
    assert summary["calendar_budget_exhausted"] == 0
    assert summary["calendar_failing"] == 0


def test_health_dashboard_surfaces_cooling_connector() -> None:
    """Three consecutive BLS failures trip the breaker; the dashboard
    flips overall status to ``degraded`` so /health consumers notice
    the stale forward calendar without parsing the nested block."""
    import time
    from ingestion.calendar.scheduler_state import (
        FAILURE_THRESHOLD,
        mark_connector_failure,
    )

    manager = _make_manager_with_fake_adapter()
    # Anchor on ``time.time()`` so the cooling window always extends
    # past the dashboard's wall-clock read — a fixed fixture timestamp
    # would expire once wall-clock time passed ``now_ms +
    # COOLDOWN_SECONDS * 1000`` and the assertion would flip to
    # ``failing`` (no longer cooling).
    now_ms = int(time.time() * 1000)
    with manager._store.get_connection() as conn:
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(conn, "bls", error="bls.gov 403", now_ms=now_ms)
        conn.commit()

    dashboard = manager.get_customer_health()

    assert dashboard["status"] == "degraded"
    assert dashboard["summary"]["calendar_cooling"] == 1
    bls = next(
        c for c in dashboard["calendar_connectors"] if c["connector"] == "bls"
    )
    assert bls["status"] == "cooling"
    assert bls["consecutive_failures"] == FAILURE_THRESHOLD
    assert bls["cooling_until"] is not None
    assert bls["last_error"] == "bls.gov 403"


def test_health_dashboard_surfaces_budget_exhausted_connector() -> None:
    """A BLS state row at cap for today is reported as
    ``budget_exhausted`` and flips overall status to ``degraded``."""
    from ingestion.calendar.scheduler_state import (
        DAILY_BUDGET_CAPS,
        record_connector_requests,
        today_utc_iso,
    )

    manager = _make_manager_with_fake_adapter()
    with manager._store.get_connection() as conn:
        record_connector_requests(
            conn, "bls", DAILY_BUDGET_CAPS["bls"], today_iso=today_utc_iso(),
        )
        conn.commit()

    dashboard = manager.get_customer_health()

    assert dashboard["status"] == "degraded"
    assert dashboard["summary"]["calendar_budget_exhausted"] == 1
    bls = next(
        c for c in dashboard["calendar_connectors"] if c["connector"] == "bls"
    )
    assert bls["status"] == "budget_exhausted"
    assert bls["budget_cap"] == DAILY_BUDGET_CAPS["bls"]
    assert bls["requests_today"] == DAILY_BUDGET_CAPS["bls"]


def test_health_dashboard_surfaces_failing_but_not_yet_tripped() -> None:
    """A partial failure count short of the threshold should register
    as ``failing`` without tripping the breaker or flipping overall
    status — operators see the early warning but consumers don't
    read ``degraded`` until the breaker or budget trips."""
    from ingestion.calendar.scheduler_state import mark_connector_failure

    manager = _make_manager_with_fake_adapter()
    with manager._store.get_connection() as conn:
        # One failure short of the cool-down threshold.
        mark_connector_failure(
            conn, "bea", error="BEA 502", now_ms=1_800_000_000_000,
        )
        conn.commit()

    dashboard = manager.get_customer_health()

    # ``alpha`` adapter is empty (no entity_count / no latest_sync
    # success), so the fake source lands as ``empty`` and overall as
    # ``healthy`` (no degraded, no other healthy to flip the branch).
    # The calendar failing count shows up without moving the main
    # status — important: the main rollup is source-capability health,
    # not calendar telemetry, and a single in-flight failure is not
    # actionable until it crosses the threshold.
    assert dashboard["summary"]["calendar_failing"] == 1
    assert dashboard["summary"]["calendar_cooling"] == 0
    assert dashboard["summary"]["calendar_budget_exhausted"] == 0
    bea = next(
        c for c in dashboard["calendar_connectors"] if c["connector"] == "bea"
    )
    assert bea["status"] == "failing"
    assert bea["consecutive_failures"] == 1


def test_calendar_telemetry_does_not_demote_unhealthy_rollup() -> None:
    """Fix for Codex R1 finding: calendar telemetry is additive. If
    every visible source is already failing (overall=``unhealthy``), a
    cooling calendar connector must NOT downgrade the rollup to
    ``degraded`` — that would mask the source outage.

    Simulated here with zero visible sources after the filter (forces
    ``healthy == 0 and degraded == 0`` → ``unhealthy``-like), then
    layered with a cooling BLS entry. The rollup must remain at or
    above the pre-calendar severity.
    """
    import time
    from ingestion.calendar.scheduler_state import (
        FAILURE_THRESHOLD,
        mark_connector_failure,
    )

    # Manually drive a store into the ``unhealthy`` rollup: one
    # adapter that claims ``supports_latest_sync=True`` but has no
    # storage, no recent run, and no checkpoint → ``empty`` status →
    # ``healthy == 0``, ``degraded == 0``. We then backfill an error
    # checkpoint so the adapter classifies as ``degraded`` instead,
    # with no healthy peer — the rollup goes to ``unhealthy``.
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
            ),
        },
    )
    # Synthesize a degraded-but-visible source: drop a catalog_entity
    # row so ``get_source_storage_stats("alpha")`` reports count > 0
    # (its default mapping reads the ``catalog_entity`` table filtered
    # by source_id), then record a latest-sync run with an error. The
    # capability manager classifies this as ``degraded`` because
    # ``storage.count > 0`` and ``last_error`` is non-empty.
    store.upsert_catalog_entity({
        "source_id": "alpha",
        "entity_id": "alpha-entity-1",
        "entity_type": "dataset",
        "display_name": "entity",
        "description": "",
        "metadata": {},
        "is_active": True,
    })
    store.insert_catalog_sync_run({
        "source_id": "alpha",
        "job_type": "latest_sync",
        "status": "error",
        "started_at": "2026-04-22T00:00:00+00:00",
        "finished_at": "2026-04-22T00:00:01+00:00",
        "error": "synthetic upstream 502",
    })

    now_ms = int(time.time() * 1000)
    with store.get_connection() as conn:
        for _ in range(FAILURE_THRESHOLD):
            mark_connector_failure(conn, "bls", error="bls.gov 403", now_ms=now_ms)
        conn.commit()

    dashboard = manager.get_customer_health()

    # Source rollup is ``unhealthy`` (1 degraded, 0 healthy). Calendar
    # telemetry is additive and must not demote it to ``degraded``.
    assert dashboard["summary"]["degraded"] == 1
    assert dashboard["summary"]["healthy"] == 0
    assert dashboard["status"] == "unhealthy"
    assert dashboard["summary"]["calendar_cooling"] == 1


def test_health_dashboard_ok_connector_with_row() -> None:
    """A connector with a clean prior run (row exists, zero failures)
    reads as ``ok`` — not absent, but explicitly healthy."""
    from ingestion.calendar.scheduler_state import mark_connector_success

    manager = _make_manager_with_fake_adapter()
    with manager._store.get_connection() as conn:
        mark_connector_success(conn, "ecb")
        conn.commit()

    dashboard = manager.get_customer_health()

    ecb = next(
        c for c in dashboard["calendar_connectors"] if c["connector"] == "ecb"
    )
    assert ecb["status"] == "ok"
    assert ecb["consecutive_failures"] == 0
    assert ecb["cooling_until"] is None


def test_customer_visible_capabilities_filter_hides_internal_sources() -> None:
    store = _make_store()
    manager = SourceCapabilityManager(
        store,
        adapters={
            "alpha": SourceCapabilityAdapter(
                source_id="alpha",
                display_name="Alpha Source",
                source_type="catalog-crawlable",
                entity_type="dataset",
                description="alpha description",
            ),
            "ilo": SourceCapabilityAdapter(
                source_id="ilo",
                display_name="ILO",
                source_type="catalog-crawlable",
                entity_type="dataflow",
                description="ilo description",
            ),
        },
    )

    visible = manager.list_capabilities(include_internal=False)
    internal = manager.list_capabilities(include_internal=True)

    assert [item["source_id"] for item in visible] == ["alpha"]
    assert {item["source_id"] for item in internal} == {"alpha", "ilo"}
