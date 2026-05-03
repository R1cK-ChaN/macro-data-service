from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from macro_data import factory
from macro_data.factory import (
    build_local_macro_data_service,
    build_read_only_macro_data_service,
)
from macro_data import server
from storage.sqlite import (
    IndicatorObservationRecord,
    IndicatorVintageRecord,
    SQLiteEngineStore,
)
from storage.subjects import sync_from_yaml


def _seed_reader_fixture(db_path: Path) -> None:
    store = SQLiteEngineStore(db_path=db_path)
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)
    store.upsert_indicator_observation(
        IndicatorObservationRecord(
            series_id="CPIAUCSL",
            source="fred",
            date="2026-01-01",
            value=321.4,
        )
    )
    store.upsert_indicator_vintage(
        IndicatorVintageRecord(
            series_id="CPIAUCSL",
            source="fred",
            observation_date="2026-01-01",
            vintage_date="2026-02-12",
            value=321.4,
            vintage_quality="native_pit",
        )
    )
    store.upsert_source_capability({
        "source_id": "fred",
        "display_name": "FRED",
        "source_type": "data_aggregator",
        "entity_type": "series",
        "supports_discovery": True,
        "supports_latest_sync": True,
        "is_default_scheduled": True,
    })


def test_read_only_store_blocks_physical_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "engine.db"
    _seed_reader_fixture(db_path)

    store = SQLiteEngineStore(db_path=db_path, read_only=True)
    assert store.read_only is True
    assert store.list_subjects()

    with pytest.raises(sqlite3.OperationalError):
        with store._connection(commit=True) as connection:
            connection.execute(
                "INSERT INTO subjects (subject_id, display_name) VALUES (?, ?)",
                ("test.subject", "Test Subject"),
            )


def test_read_only_factory_starts_from_wal_snapshot_in_read_only_directory(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "engine.db"
    _seed_reader_fixture(db_path)
    for suffix in ("-wal", "-shm"):
        db_path.with_name(f"{db_path.name}{suffix}").unlink(missing_ok=True)

    original_mode = tmp_path.stat().st_mode
    os.chmod(tmp_path, 0o555)
    try:
        service = build_read_only_macro_data_service(db_path=db_path)
        resolved = service.invoke(
            "resolve_indicator",
            {"concept_id": "CPI_US", "date": "2026-01-01"},
        )
    finally:
        os.chmod(tmp_path, original_mode)

    assert resolved["resolved"]["value"] == 321.4
    assert not db_path.with_name(f"{db_path.name}-wal").exists()
    assert not db_path.with_name(f"{db_path.name}-shm").exists()


def test_read_only_store_reads_committed_wal_frames(tmp_path: Path) -> None:
    db_path = tmp_path / "engine.db"
    store = SQLiteEngineStore(db_path=db_path)

    writer = store.get_connection()
    try:
        writer.execute("CREATE TABLE wal_probe (value INTEGER NOT NULL)")
        writer.execute("INSERT INTO wal_probe (value) VALUES (7)")
        writer.commit()
        assert db_path.with_name(f"{db_path.name}-wal").exists()

        reader = SQLiteEngineStore(db_path=db_path, read_only=True)
        with reader._connection(commit=False) as connection:
            row = connection.execute("SELECT value FROM wal_probe").fetchone()
    finally:
        writer.close()

    assert row["value"] == 7


def test_read_only_store_detects_wal_next_to_resolved_db_path(
    tmp_path: Path,
) -> None:
    real_dir = tmp_path / "real"
    link_dir = tmp_path / "link"
    real_dir.mkdir()
    link_dir.mkdir()
    real_db_path = real_dir / "engine.db"
    link_db_path = link_dir / "engine.db"
    link_db_path.symlink_to(real_db_path)
    store = SQLiteEngineStore(db_path=link_db_path)

    writer = store.get_connection()
    try:
        writer.execute("CREATE TABLE symlink_wal_probe (value INTEGER NOT NULL)")
        writer.execute("INSERT INTO symlink_wal_probe (value) VALUES (11)")
        writer.commit()
        assert real_db_path.with_name(f"{real_db_path.name}-wal").exists()

        reader = SQLiteEngineStore(db_path=link_db_path, read_only=True)
        with reader._connection(commit=False) as connection:
            row = connection.execute(
                "SELECT value FROM symlink_wal_probe",
            ).fetchone()
    finally:
        writer.close()

    assert row["value"] == 11


def test_read_only_service_uses_preseeded_data_without_bootstrap_writes(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "engine.db"
    _seed_reader_fixture(db_path)

    service = build_read_only_macro_data_service(db_path=db_path)
    assert service._ingestion is None
    assert service._health is not None
    assert service._store.read_only is True

    health = service.invoke("get_source_health_dashboard", {})
    assert health["sources"][0]["source_id"] == "fred"
    assert health["sources"][0]["status"] == "healthy"

    resolved = service.invoke(
        "resolve_indicator",
        {"concept_id": "CPI_US", "date": "2026-01-01"},
    )
    assert resolved["resolved"]["value"] == 321.4

    schedule = service.invoke("get_release_schedule", {"concept_id": "CPI_US"})
    assert schedule["schedules"][0]["concept_id"] == "CPI_US"

    items = service.invoke(
        "list_items",
        {"subject": "econ.cpi", "family": "economic_data"},
    )
    assert any(item["series_id"] == "CPIAUCSL" for item in items["items"])


def test_writable_factory_keeps_orchestrator(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(factory, "_build_clickhouse_market_store", lambda: None)

    service = build_local_macro_data_service(db_path=tmp_path / "engine.db")

    assert service._store.read_only is False
    assert service._ingestion is not None
    assert service._store.get_source_capability("fred") is not None


def test_read_only_factory_wires_market_store_for_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "engine.db"
    _seed_reader_fixture(db_path)
    sentinel_market = object()
    calls: list[bool] = []

    def fake_build_clickhouse_market_store(
        *,
        initialize_schema: bool = True,
    ) -> object:
        calls.append(initialize_schema)
        return sentinel_market

    monkeypatch.setattr(
        factory,
        "_build_clickhouse_market_store",
        fake_build_clickhouse_market_store,
    )

    service = build_read_only_macro_data_service(db_path=db_path)

    assert calls == [False]
    assert service._market_store is sentinel_market


def test_read_only_factory_requires_bootstrap_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "engine.db"
    SQLiteEngineStore(db_path=db_path)

    with pytest.raises(RuntimeError, match="reader bootstrap rows"):
        build_read_only_macro_data_service(db_path=db_path)


def test_server_main_uses_read_only_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    sentinel_service = object()

    def fake_build_read_only_macro_data_service(
        db_path: Path | None = None,
    ) -> object:
        calls["db_path"] = db_path
        return sentinel_service

    def fake_serve(**kwargs: Any) -> None:
        calls["serve"] = kwargs

    monkeypatch.setattr(
        factory,
        "build_read_only_macro_data_service",
        fake_build_read_only_macro_data_service,
    )
    monkeypatch.setattr(server, "serve", fake_serve)

    db_path = tmp_path / "engine.db"
    result = server.main([
        "--host", "127.0.0.1",
        "--port", "0",
        "--db-path", str(db_path),
    ])

    assert result == 0
    assert calls["db_path"] == db_path
    assert calls["serve"]["service"] is sentinel_service
