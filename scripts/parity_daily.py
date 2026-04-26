#!/usr/bin/env python3
"""Daily TE-vs-official parity job — issue #22 P5.

Runs the full pipeline once per invocation:

1. Pull TE actuals for ``--date`` (default: yesterday UTC) into the
   shared engine DB via the existing TE projector.
2. Snapshot the engine DB into ``.macro-data/backups/te_calendar_<date>/``.
3. Run :func:`parity_daily.compare_daily` for the same date.
4. File / comment / close GitHub issues per agency via :mod:`parity_filer`.
5. Append a structured log line to ``.macro-data/logs/parity_daily.log``.

Exit codes:

- 0 on success (clean run, anomaly run, or dry-run).
- 1 on TE pull failure or comparator failure (the systemd unit's
  self-monitor counts these against the 2-strike threshold).
- 2 on argument / environment errors.

The systemd timer wraps this with ``flock`` so two instances cannot
race the engine DB.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import shutil
import sqlite3
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from ingestion.calendar.parity_daily import compare_daily  # noqa: E402
from ingestion.calendar.parity_filer import (  # noqa: E402
    GhRunner,
    default_state_path,
    file_infra_failure,
    file_reports,
)
from ingestion.calendar.te_api import TEAPIClient, pull_daily  # noqa: E402
from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

LOG_FILENAME = "parity_daily.log"
INFRA_STREAK_FILE = "parity_infra_streak.json"
INFRA_STRIKE_LIMIT = 2

logger = logging.getLogger("parity_daily")


def _yesterday_utc(now: dt.datetime | None = None) -> dt.date:
    base = now or dt.datetime.now(dt.timezone.utc)
    return (base - dt.timedelta(days=1)).date()


def _refresh_backup_snapshot(*, engine_db: Path, target: dt.date) -> Path:
    """Copy the live engine DB into a per-date backup directory.

    Uses SQLite's online backup API so a writer holding the WAL doesn't
    corrupt the snapshot — :func:`shutil.copy2` of a live WAL DB can
    drop in-flight pages.
    """
    backup_dir = engine_db.parent / "backups" / f"te_calendar_{target.isoformat()}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    target_db = backup_dir / "engine.db"

    src = sqlite3.connect(str(engine_db))
    try:
        dst = sqlite3.connect(str(target_db))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    return target_db


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _load_infra_streak(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        return int(json.loads(path.read_text()).get("consecutive_failures", 0))
    except (json.JSONDecodeError, OSError, ValueError):
        return 0


def _save_infra_streak(path: Path, value: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"consecutive_failures": value}))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: dt.date.fromisoformat(s),
        default=None,
        help="Comparison date (default: yesterday UTC).",
    )
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="engine.db override (default: .macro-data/engine.db).",
    )
    parser.add_argument(
        "--state-path", type=Path, default=None,
        help="Filer state path override.",
    )
    parser.add_argument(
        "--log-path", type=Path, default=None,
        help="Log file path override.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run TE pull + comparator but stub out gh side effects.",
    )
    parser.add_argument(
        "--skip-fetch", action="store_true",
        help="Skip the TE fetch (useful for re-running comparator + filer "
             "off an existing engine state).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Log to stderr at DEBUG.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    target = args.date or _yesterday_utc()
    engine_db = args.db_path or default_engine_db_path()
    log_path = args.log_path or (engine_db.parent / "logs" / LOG_FILENAME)
    state_path = args.state_path or default_state_path(engine_db.parent.parent)
    infra_streak_path = engine_db.parent / "logs" / INFRA_STREAK_FILE

    parity_run_time = dt.datetime.now(dt.timezone.utc)
    summary: dict = {
        "target_date": target.isoformat(),
        "started_at": parity_run_time.isoformat(),
        "parity_run_time": parity_run_time.isoformat(),
        "dry_run": args.dry_run,
        "skip_fetch": args.skip_fetch,
    }
    runner = GhRunner(dry_run=args.dry_run)

    try:
        store = SQLiteEngineStore(db_path=engine_db)

        if not args.skip_fetch:
            client = TEAPIClient()
            try:
                with store.get_connection() as conn:
                    pull = pull_daily(
                        connection=conn, client=client, target_date=target,
                    )
            finally:
                client.close()
            summary["te_pull"] = {
                "rows_returned": pull.rows_returned,
                "rows_raw_inserted": pull.rows_raw_inserted,
                "events_upserted": pull.events_upserted,
                "requests_spent": pull.requests_spent,
                "truncated": pull.truncated,
            }
            backup_path = _refresh_backup_snapshot(
                engine_db=engine_db, target=target,
            )
            summary["backup_path"] = str(backup_path)
        else:
            summary["te_pull"] = "skipped"

        with store.get_connection() as conn:
            reports = compare_daily(
                conn, target_date=target, now_utc=parity_run_time,
            )
        summary["agencies_with_anomalies"] = [
            r.agency_id for r in reports if not r.clean
        ]
        summary["total_anomalies"] = sum(r.total_anomalies for r in reports)
        summary["lag_after_parity_count"] = sum(
            r.total_lag_after_parity for r in reports
        )
        summary["lag_after_parity_by_agency"] = {
            r.agency_id: r.total_lag_after_parity
            for r in reports if r.lag_after_parity
        }

        action_log = file_reports(
            reports=reports,
            target_date=target,
            runner=runner,
            state_path=state_path,
        )
        summary["created_issues"] = [a for a, _ in action_log.created]
        summary["commented_issues"] = [a for a, _ in action_log.commented]
        summary["closed_issues"] = [a for a, _ in action_log.closed]
        summary["lag_only_agencies"] = [a for a, _ in action_log.lag_only]

        # Reset infra strike counter on success.
        _save_infra_streak(infra_streak_path, 0)
        summary["status"] = "ok"
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        return 0

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)

        streak = _load_infra_streak(infra_streak_path) + 1
        _save_infra_streak(infra_streak_path, streak)
        if streak >= INFRA_STRIKE_LIMIT and not args.dry_run:
            try:
                file_infra_failure(
                    summary=f"{exc!r} (consecutive failures: {streak})",
                    target_date=target,
                    runner=runner,
                    extra_body=(
                        "```\n" + summary["traceback"] + "```\n"
                    ),
                )
            except Exception:  # pragma: no cover — best-effort
                logger.exception("infra self-report failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
