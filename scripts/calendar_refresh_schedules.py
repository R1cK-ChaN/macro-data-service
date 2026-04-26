#!/usr/bin/env python3
"""Daily schedule-side refresh entry-point — issue #31 P1.

Invokes the ``calendar_econ_refresh_schedules`` service op so every
official-source connector pulls forward-looking schedule rows once
per day. Designed to be wrapped by ``scripts/calendar_refresh_schedules_wrapper.sh``
and driven by the ``calendar-schedule-refresh.timer`` systemd unit.

Per-connector exceptions are isolated by the underlying
:func:`refresh_all_schedules` driver (one failed connector does not
abort the rest). Connector-level failure surfaces in the structured
log line and in ``calendar_connector_state``; the daily TE parity
tripwire (#22) catches downstream drift if any agency goes silent.

Exit codes:

- 0 on successful run (any per-connector failures are isolated).
- 1 on unhandled exception (the wrapper / systemd unit see this).
- 2 on argument / environment errors.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from macro_data.service import LocalMacroDataService  # noqa: E402
from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

LOG_FILENAME = "calendar_refresh_schedules.log"
OPERATION = "calendar_econ_refresh_schedules"

logger = logging.getLogger("calendar_refresh_schedules")


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _build_service(db_path: Path) -> LocalMacroDataService:
    return LocalMacroDataService(store=SQLiteEngineStore(db_path=db_path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="engine.db override (default: .macro-data/engine.db).",
    )
    parser.add_argument(
        "--log-path", type=Path, default=None,
        help="Log file path override.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run the planner only — no HTTP, no DB writes.",
    )
    parser.add_argument(
        "--connectors", nargs="+", default=None,
        help="Optional subset of connector names; default is the full roster.",
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

    engine_db = args.db_path or default_engine_db_path()
    log_path = args.log_path or (engine_db.parent / "logs" / LOG_FILENAME)

    summary: dict = {
        "operation": OPERATION,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run": args.dry_run,
    }

    try:
        svc = _build_service(engine_db)
        op_args: dict = {"dry_run": args.dry_run}
        if args.connectors is not None:
            op_args["connectors"] = list(args.connectors)
        result = svc.invoke(OPERATION, op_args)

        summary["status"] = "ok"
        summary["ok_count"] = result.get("ok_count")
        summary["failed_count"] = result.get("failed_count")
        summary["unknown_connectors"] = result.get("unknown_connectors", [])
        summary["wall_seconds"] = result.get("wall_seconds")
        summary["failed_connectors"] = [
            {"connector": r["connector"], "error": r["error"]}
            for r in result.get("results", [])
            if not r.get("ok")
        ]
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        return 0

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("schedule refresh entry-point crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
