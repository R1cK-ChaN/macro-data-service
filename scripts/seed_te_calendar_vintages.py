#!/usr/bin/env python3
"""Seed ``calendar_event_vintages`` from the offline TE backup snapshot.

Issue #21 P1.5. Zero external API calls — reads
``.macro-data/backups/te_calendar_2026-04-23/te_calendar.db`` and projects
each ``cal_econ_event`` row into up to two vintages on the engine DB:

* current state (always when ``actual`` is non-null) at the
  ``max(last_update_epoch_ms, event_time_utc)`` floor;
* prior state (when the *next* same-ticker event's ``previous`` field
  differs from this event's ``actual``) at ``event_time_utc`` — this is
  the first-print value before TE later revised it. TE's schema stores
  the previous-period revised value in ``revised``, so the only correct
  source for *this event's* original print is the next event's
  ``previous`` field.

Idempotent via ``UNIQUE(event_id, provider, vintage_date)``.

Usage::

    PYTHONPATH=src python3 scripts/seed_te_calendar_vintages.py \
        --backup-path .macro-data/backups/te_calendar_2026-04-23/te_calendar.db \
        --db-path .macro-data/engine.db
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

PROVIDER = "tradingeconomics"
BATCH_SIZE = 5000
SOURCE_TAG = "te_backup_2026-04-23"


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat().replace("+00:00", "Z")


def normalize_release_iso(text: str | None) -> str | None:
    """Backup stores event_time_utc as e.g. '2013-04-05T02:00:00' (no tz)."""
    if not text:
        return None
    if text.endswith("Z") or "+" in text[10:]:
        return text
    return text + "Z"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backup-path", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=None,
                        help="engine.db (default: .macro-data/engine.db)")
    parser.add_argument("--limit", type=int, default=None,
                        help="cap rows scanned from cal_econ_event (debug)")
    parser.add_argument("--dry-run", action="store_true",
                        help="parse + project without writing or touching engine.db")
    args = parser.parse_args()

    if not args.backup_path.is_file():
        print(f"backup not found: {args.backup_path}", file=sys.stderr)
        return 2

    engine_db = args.db_path or default_engine_db_path()
    engine: sqlite3.Connection | None = None
    if not args.dry_run:
        engine_db.parent.mkdir(parents=True, exist_ok=True)
        SQLiteEngineStore(db_path=engine_db).init_schema()
        engine = sqlite3.connect(engine_db)
        engine.execute("PRAGMA journal_mode=WAL")
        engine.execute("PRAGMA synchronous=NORMAL")

    backup = sqlite3.connect(args.backup_path)
    backup.row_factory = sqlite3.Row

    sql = (
        "SELECT provider_event_id, event_time_utc, country_code, ticker, "
        "title, actual, previous, forecast, last_update_epoch_ms, "
        "LEAD(previous) OVER (PARTITION BY ticker ORDER BY event_time_utc, "
        "                     provider_event_id) AS next_previous "
        "FROM cal_econ_event"
    )
    if args.limit:
        sql = (
            "SELECT * FROM (" + sql + ") LIMIT " + str(int(args.limit))
        )

    rows_scanned = 0
    rows_with_actual = 0
    rows_with_prior = 0
    vintages_inserted = 0
    vintages_skipped = 0
    per_ticker_counts: Counter[str] = Counter()

    insert_sql = (
        "INSERT OR IGNORE INTO calendar_event_vintages ("
        "event_id, provider, vintage_date, observed_at, "
        "actual, forecast, previous, metadata_json, scraped_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    scraped_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    pending: list[tuple] = []

    def flush() -> None:
        nonlocal vintages_inserted, vintages_skipped
        if not pending:
            return
        attempted = len(pending)
        if args.dry_run or engine is None:
            vintages_inserted += attempted  # projected, not written
            pending.clear()
            return
        before = engine.execute(
            "SELECT COUNT(*) FROM calendar_event_vintages"
        ).fetchone()[0]
        engine.executemany(insert_sql, pending)
        engine.commit()
        after = engine.execute(
            "SELECT COUNT(*) FROM calendar_event_vintages"
        ).fetchone()[0]
        inserted = after - before
        vintages_inserted += inserted
        vintages_skipped += attempted - inserted
        pending.clear()

    print(f"reading backup={args.backup_path}")
    if args.dry_run:
        print(f"dry_run=True (engine.db not opened or modified)")
    else:
        print(f"writing engine={engine_db}")

    for row in backup.execute(sql):
        rows_scanned += 1
        provider_event_id = row["provider_event_id"]
        actual = row["actual"]
        forecast = row["forecast"]
        previous = row["previous"]
        next_previous = row["next_previous"]
        last_update = row["last_update_epoch_ms"]
        release_iso = normalize_release_iso(row["event_time_utc"])
        ticker = row["ticker"] or ""

        last_update_iso = ms_to_iso(last_update)
        # Clamp current observed_at to the release time floor — rare TE rows
        # have last_update_epoch_ms slightly before event_time_utc which would
        # leak future state into pre-release PIT reads.
        if last_update_iso and release_iso and last_update_iso < release_iso:
            current_observed = release_iso
        else:
            current_observed = last_update_iso or release_iso

        if current_observed is None:
            continue

        metadata = {
            "source_backup": SOURCE_TAG,
            "ticker": ticker,
            "country": row["country_code"],
            "title": row["title"],
        }
        meta_json = json.dumps(metadata, ensure_ascii=True, sort_keys=True)

        # Prior vintage — the first-print value, sourced from the *next*
        # same-ticker event's `previous` field. Only emit when (a) the next
        # event exists, (b) its `previous` is non-empty, (c) it differs from
        # this event's current `actual` (otherwise no revision occurred), and
        # (d) we have a release timestamp distinct from current_observed.
        if (
            next_previous
            and actual
            and next_previous != actual
            and release_iso
            and release_iso < current_observed
        ):
            pending.append((
                provider_event_id, PROVIDER, release_iso, release_iso,
                next_previous, forecast, previous, meta_json, scraped_at,
            ))
            rows_with_prior += 1
            per_ticker_counts[ticker] += 1

        # Current vintage at clamped observed_at.
        if actual:
            pending.append((
                provider_event_id, PROVIDER, current_observed, current_observed,
                actual, forecast, previous, meta_json, scraped_at,
            ))
            rows_with_actual += 1
            per_ticker_counts[ticker] += 1

        if len(pending) >= BATCH_SIZE * 2:
            flush()
            if rows_scanned % 50000 == 0:
                print(f"  scanned={rows_scanned:,} inserted={vintages_inserted:,} skipped={vintages_skipped:,}")

    flush()
    if engine is not None:
        engine.close()
    backup.close()

    print()
    print("--- import summary ---")
    print(f"rows scanned         : {rows_scanned:,}")
    print(f"rows with current    : {rows_with_actual:,}")
    print(f"rows with first-print: {rows_with_prior:,}")
    print(f"vintages {'projected' if args.dry_run else 'inserted'}    : {vintages_inserted:,}")
    print(f"vintages skipped     : {vintages_skipped:,}  (UNIQUE collisions on rerun)")
    print()
    print("--- top 10 tickers by vintage count ---")
    for ticker, n in per_ticker_counts.most_common(10):
        print(f"  {ticker:30s} {n:,}")

    print()
    print("--- high-revision sample coverage (PIT fidelity ceiling) ---")
    print("  ticker                  events  with-2vint  pct")
    sample_tickers = (
        "NFP TCH", "RSTAMOM", "NAPMPMI", "USAPPIAC", "USURTOT",
        "CPI YOY", "USACORECPIRATE",
        "JNCPIYOY", "CNCPIYOY", "CNGDPYOY",
    )
    backup_for_report = sqlite3.connect(args.backup_path)
    backup_for_report.row_factory = sqlite3.Row
    for tkr in sample_tickers:
        r = backup_for_report.execute(
            "SELECT COUNT(*) AS n, "
            "SUM(CASE WHEN next_previous IS NOT NULL AND next_previous != '' "
            "         AND actual IS NOT NULL AND next_previous != actual THEN 1 ELSE 0 END) AS r "
            "FROM (SELECT actual, "
            "             LEAD(previous) OVER (PARTITION BY ticker ORDER BY event_time_utc, provider_event_id) AS next_previous "
            "      FROM cal_econ_event WHERE ticker = ?)",
            (tkr,),
        ).fetchone()
        n = r["n"] or 0
        revs = r["r"] or 0
        pct = (100.0 * revs / n) if n else 0.0
        print(f"  {tkr:22s} {n:6d}  {revs:9d}  {pct:5.1f}%")
    backup_for_report.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
