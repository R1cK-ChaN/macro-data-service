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
