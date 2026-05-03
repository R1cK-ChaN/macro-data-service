from __future__ import annotations

import sqlite3
from pathlib import Path

from storage.sqlite import SQLiteEngineStore


def test_retired_social_trend_table_is_dropped_on_migration(tmp_path: Path) -> None:
    db_path = tmp_path / "engine.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE trend_topics (
                trend_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                observed_at INTEGER NOT NULL,
                category TEXT NOT NULL,
                region TEXT NOT NULL,
                popularity_score REAL NOT NULL,
                normalized_topic_hash TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX idx_trend_topics_active "
            "ON trend_topics(expires_at, observed_at)"
        )
        connection.execute(
            "CREATE INDEX idx_trend_topics_scope "
            "ON trend_topics(category, region)"
        )
        connection.execute(
            "CREATE INDEX idx_trend_topics_popularity "
            "ON trend_topics(popularity_score DESC, observed_at DESC)"
        )
        connection.execute(
            "CREATE INDEX idx_trend_topics_normalized "
            "ON trend_topics(normalized_topic_hash)"
        )

    store = SQLiteEngineStore(db_path=db_path)
    with store._connection(commit=False) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        indexes = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }

    assert {"trend_topics"}.isdisjoint(tables)
    assert {
        "idx_trend_topics_active",
        "idx_trend_topics_scope",
        "idx_trend_topics_popularity",
        "idx_trend_topics_normalized",
    }.isdisjoint(indexes)


def test_retired_social_source_metadata_is_dropped_on_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "engine.db"
    store = SQLiteEngineStore(db_path=db_path)
    for source_id in ("reddit_trends", "weibo_trends", "alpha"):
        store.upsert_source_capability(
            {
                "source_id": source_id,
                "display_name": source_id,
                "source_type": "fixed-scope-complete",
                "entity_type": "provider",
                "supports_discovery": True,
                "supports_latest_sync": True,
            }
        )
        store.upsert_catalog_entity(
            {
                "source_id": source_id,
                "entity_id": "entity-1",
                "entity_type": "provider",
                "display_name": "Entity 1",
                "metadata": {},
            }
        )
        store.upsert_catalog_sync_checkpoint(
            {
                "source_id": source_id,
                "job_type": "latest_sync",
                "entities_total": 1,
                "entities_synced": 1,
            }
        )
        store.insert_catalog_sync_run(
            {
                "source_id": source_id,
                "job_type": "latest_sync",
                "status": "success",
                "entities_total": 1,
                "entities_synced": 1,
            }
        )

    migrated = SQLiteEngineStore(db_path=db_path)

    assert migrated.get_source_capability("alpha") is not None
    assert migrated.count_catalog_entities("alpha") == 1
    assert migrated.get_catalog_sync_checkpoint("alpha", "latest_sync") is not None
    assert len(migrated.list_catalog_sync_runs(source_id="alpha")) == 1
    for retired_source in ("reddit_trends", "weibo_trends"):
        assert migrated.get_source_capability(retired_source) is None
        assert migrated.count_catalog_entities(retired_source) == 0
        assert migrated.get_catalog_sync_checkpoint(
            retired_source,
            "latest_sync",
        ) is None
        assert migrated.list_catalog_sync_runs(source_id=retired_source) == []
