"""Packaged shadow digest helpers for data-quality gates."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any


def _table_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    exists = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if exists is None:
        return 0
    return int(cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def compute_digest(db_path: str) -> dict[str, Any]:
    """Compute coverage, freshness, and per-source/tier stats."""
    from ingestion.series_config import get_concept_tier

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT source, COUNT(DISTINCT series_id), COUNT(*) "
            "FROM indicators GROUP BY source ORDER BY source"
        )
        sources: dict[str, dict[str, int]] = {}
        total_obs = 0
        for source, series, obs in cursor.fetchall():
            sources[source] = {"series": int(series), "obs": int(obs)}
            total_obs += int(obs)

        cursor.execute("SELECT COUNT(DISTINCT concept_id) FROM concept_map")
        total_concepts = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT COUNT(DISTINCT cm.concept_id) FROM concept_map cm "
            "JOIN indicators i ON i.source = cm.source_id "
            "AND i.series_id = cm.provider_series_id"
        )
        covered_concepts = int(cursor.fetchone()[0])

        cursor.execute(
            "SELECT source, MAX(scraped_at) FROM indicators GROUP BY source"
        )
        freshness = {
            source: scraped_at or ""
            for source, scraped_at in cursor.fetchall()
        }
        now_iso = datetime.now(UTC).isoformat()

        latency: dict[str, float] = {}
        for source, ts in freshness.items():
            if not ts:
                continue
            try:
                scraped = datetime.fromisoformat(ts)
                delta = (datetime.now(UTC) - scraped).total_seconds()
                latency[source] = round(delta, 1)
            except (ValueError, TypeError):
                pass

        cursor.execute(
            "SELECT COUNT(DISTINCT cm.concept_id) FROM concept_map cm "
            "JOIN indicators i ON i.source = cm.source_id "
            "AND i.series_id = cm.provider_series_id "
            "AND i.scraped_at > datetime('now', '-1 day')"
        )
        confirmed_24h = int(cursor.fetchone()[0])

        cursor.execute(
            "SELECT DISTINCT cm.concept_id FROM concept_map cm "
            "JOIN indicators i ON i.source = cm.source_id "
            "AND i.series_id = cm.provider_series_id"
        )
        covered_ids = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT DISTINCT concept_id FROM concept_map")
        all_ids = {row[0] for row in cursor.fetchall()}

        tier_stats: dict[str, dict[str, Any]] = {}
        for tier_n in (1, 2, 3):
            tier_all = {cid for cid in all_ids if get_concept_tier(cid) == tier_n}
            tier_covered = tier_all & covered_ids
            tier_stats[f"T{tier_n}"] = {
                "total": len(tier_all),
                "covered": len(tier_covered),
                "missing": sorted(tier_all - tier_covered),
            }

        document_count = _table_count(cursor, "document")
        news_article_count = _table_count(cursor, "news_articles")
        release_schedule_count = _table_count(cursor, "release_schedule")
        release_status_count = _table_count(cursor, "release_status")
    finally:
        conn.close()

    return {
        "timestamp": now_iso,
        "concepts_covered": covered_concepts,
        "concepts_total": total_concepts,
        "coverage_pct": (
            round(covered_concepts / total_concepts * 100, 1)
            if total_concepts
            else 0
        ),
        "total_obs": total_obs,
        "sources": sources,
        "freshness": freshness,
        "latency_seconds": latency,
        "confirmed_24h": confirmed_24h,
        "tier_coverage": tier_stats,
        "document_count": document_count,
        "news_article_count": news_article_count,
        "release_schedule_count": release_schedule_count,
        "release_status_count": release_status_count,
    }


__all__ = ["compute_digest"]
