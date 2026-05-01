#!/usr/bin/env python3
"""Shadow mode runner — runs full ingestion on a schedule with structured logging.

Logs to both console and file for quality monitoring.
Produces a daily digest of coverage, freshness, and errors.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from ingestion._shared.redaction import SecretRedactingFilter, redact_secrets  # noqa: E402

# ── Logging setup ──────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / ".macro-data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "shadow.log"
DIGEST_FILE = LOG_DIR / "daily_digest.jsonl"

logger = logging.getLogger("shadow")


def _table_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    exists = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if exists is None:
        return 0
    return int(cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File — rotates daily via name, append mode. The redacting filter
    # scrubs api_key=… style secrets before they hit disk.
    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    fh.addFilter(SecretRedactingFilter())
    root.addHandler(fh)


# ── Digest ─────────────────────────────────────────────────────────────────

def compute_digest(db_path: str) -> dict:
    """Compute coverage, freshness, and per-source/tier stats."""
    from ingestion.series_config import get_concept_tier

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Per-source counts
    c.execute(
        "SELECT source, COUNT(DISTINCT series_id), COUNT(*) "
        "FROM indicators GROUP BY source ORDER BY source"
    )
    sources = {}
    total_obs = 0
    for row in c.fetchall():
        sources[row[0]] = {"series": row[1], "obs": row[2]}
        total_obs += row[2]

    # Concept coverage
    c.execute("SELECT COUNT(DISTINCT concept_id) FROM concept_map")
    total_concepts = c.fetchone()[0]
    c.execute(
        "SELECT COUNT(DISTINCT cm.concept_id) FROM concept_map cm "
        "JOIN indicators i ON i.source = cm.source_id "
        "AND i.series_id = cm.provider_series_id"
    )
    covered_concepts = c.fetchone()[0]

    # Freshness: latest scraped_at per source
    c.execute(
        "SELECT source, MAX(scraped_at) FROM indicators GROUP BY source"
    )
    freshness = {}
    now_iso = datetime.now(UTC).isoformat()
    for row in c.fetchall():
        freshness[row[0]] = row[1] or ""

    # Ingestion latency: seconds between scraped_at and now for most recent obs per source
    latency = {}
    for src, ts in freshness.items():
        if ts:
            try:
                scraped = datetime.fromisoformat(ts)
                delta = (datetime.now(UTC) - scraped).total_seconds()
                latency[src] = round(delta, 1)
            except (ValueError, TypeError):
                pass

    # Confirmed concepts in last 24h
    c.execute(
        "SELECT COUNT(DISTINCT cm.concept_id) FROM concept_map cm "
        "JOIN indicators i ON i.source = cm.source_id "
        "AND i.series_id = cm.provider_series_id "
        "AND i.scraped_at > datetime('now', '-1 day')"
    )
    confirmed_24h = c.fetchone()[0]

    # Per-tier coverage
    c.execute(
        "SELECT DISTINCT cm.concept_id FROM concept_map cm "
        "JOIN indicators i ON i.source = cm.source_id "
        "AND i.series_id = cm.provider_series_id"
    )
    covered_ids = {row[0] for row in c.fetchall()}
    c.execute("SELECT DISTINCT concept_id FROM concept_map")
    all_ids = {row[0] for row in c.fetchall()}

    tier_stats = {}
    for tier_n in (1, 2, 3):
        tier_all = {cid for cid in all_ids if get_concept_tier(cid) == tier_n}
        tier_covered = tier_all & covered_ids
        tier_stats[f"T{tier_n}"] = {
            "total": len(tier_all),
            "covered": len(tier_covered),
            "missing": sorted(tier_all - tier_covered),
        }

    document_count = _table_count(c, "document")
    news_article_count = _table_count(c, "news_articles")
    release_schedule_count = _table_count(c, "release_schedule")
    release_status_count = _table_count(c, "release_status")

    conn.close()

    return {
        "timestamp": now_iso,
        "concepts_covered": covered_concepts,
        "concepts_total": total_concepts,
        "coverage_pct": round(covered_concepts / total_concepts * 100, 1) if total_concepts else 0,
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


def _redact_digest_payload(digest: dict) -> dict:
    """Redact provider secrets from any error strings inside the digest.

    We only walk the well-known string fields that carry upstream error
    text (per-source ``error`` and the digest-level ``error`` from the
    runner). Any future free-text field added here should be added to
    this list rather than dumped raw.
    """
    redacted = dict(digest)
    sources = redacted.get("source_results")
    if isinstance(sources, dict):
        redacted["source_results"] = {
            name: {**entry, "error": redact_secrets(entry.get("error", ""))}
            if isinstance(entry, dict)
            else entry
            for name, entry in sources.items()
        }
    if isinstance(redacted.get("error"), str):
        redacted["error"] = redact_secrets(redacted["error"])
    return redacted


def write_digest(digest: dict) -> None:
    """Append digest to JSONL file for time-series tracking."""
    safe_digest = _redact_digest_payload(digest)
    with open(DIGEST_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(safe_digest, ensure_ascii=False) + "\n")
    logger.info(
        "DIGEST: coverage=%d/%d (%.0f%%), confirmed_24h=%d, total_obs=%d",
        safe_digest["concepts_covered"],
        safe_digest["concepts_total"],
        safe_digest["coverage_pct"],
        safe_digest.get("confirmed_24h", 0),
        safe_digest["total_obs"],
    )


# ── Main loop ──────────────────────────────────────────────────────────────

def run_shadow(
    *,
    interval_hours: float = 6,
    db_path: str | None = None,
) -> None:
    """Run ingestion every N hours, log everything, write daily digest."""
    # Lazy import so logging is set up first
    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from macro_data.factory import build_local_macro_data_service

    db = Path(db_path) if db_path else None
    svc = build_local_macro_data_service(db_path=db)
    orch = svc._ingestion
    resolved_db = str(db or Path(".macro-data/engine.db"))

    # Ensure concept_map is seeded for coverage tracking
    svc._store.seed_concept_map()
    svc._store.seed_release_schedules()
    logger.info("Concept map seeded")
    logger.info("Release schedules seeded")

    cycle = 0
    while True:
        cycle += 1
        logger.info("═══ Shadow cycle %d starting ═══", cycle)
        t0 = time.perf_counter()

        # Run full refresh
        source_results = {}
        for source_name in orch._default_refresh_order:
            try:
                report = orch.run_source(source_name)
                source_results[source_name] = {
                    "stored": report.stored,
                    "error": report.error or "",
                    "ms": int(report.duration_ms),
                }
                if report.error:
                    logger.warning("  %s: stored=%d ERROR=%s", source_name, report.stored, report.error)
                elif report.stored > 0:
                    logger.info("  %s: stored=%d (%dms)", source_name, report.stored, report.duration_ms)
                else:
                    logger.debug("  %s: stored=0 (%dms)", source_name, report.duration_ms)
            except Exception as exc:
                logger.error("  %s: EXCEPTION %s", source_name, exc)
                source_results[source_name] = {"stored": 0, "error": str(exc), "ms": 0}

        elapsed = time.perf_counter() - t0
        logger.info("═══ Shadow cycle %d done in %.1fs ═══", cycle, elapsed)

        # Compute and write digest
        try:
            digest = compute_digest(resolved_db)
            digest["cycle"] = cycle
            digest["cycle_duration_s"] = round(elapsed, 1)
            digest["source_results"] = source_results

            # Count errors
            errors = [s for s, r in source_results.items() if r.get("error")]
            digest["error_sources"] = errors
            digest["error_count"] = len(errors)

            write_digest(digest)

            # Alert on coverage drop
            if digest["coverage_pct"] < 100:
                logger.warning(
                    "COVERAGE DROP: %d/%d concepts (%.0f%%)",
                    digest["concepts_covered"],
                    digest["concepts_total"],
                    digest["coverage_pct"],
                )
        except Exception as exc:
            logger.error("Failed to compute digest: %s", exc)

        # Sleep until next cycle
        logger.info("Sleeping %.1fh until next cycle...", interval_hours)
        time.sleep(interval_hours * 3600)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Shadow mode ingestion runner")
    parser.add_argument("--interval", type=float, default=6, help="Hours between cycles (default 6)")
    parser.add_argument("--db-path", default=None, help="Database path")
    args = parser.parse_args()

    setup_logging()
    logger.info("Starting shadow mode (interval=%.1fh)", args.interval)
    try:
        run_shadow(interval_hours=args.interval, db_path=args.db_path)
    except KeyboardInterrupt:
        logger.info("Shadow mode stopped by user")
