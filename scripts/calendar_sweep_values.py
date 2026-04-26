#!/usr/bin/env python3
"""Hourly value-side sweep entry-point — issue #31 P1.

Invokes the ``calendar_econ_sweep_values`` service op so a freshly
published headline release crosses into ``cal_econ_event`` with
``actual`` populated within minutes of publication. Designed to be
wrapped by ``scripts/calendar_sweep_values_wrapper.sh`` and driven by
the ``calendar-value-sweep.timer`` systemd unit (hourly at :15).

Per-connector exceptions are isolated by the underlying
:func:`sweep_value_side` driver. The schedule-side window
(``start_year``, ``end_year``, ``start_period``, ``end_period``) is
left to the driver's defaults so the sweep stays inside its routine
recent-window without operator tuning.

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

LOG_FILENAME = "calendar_sweep_values.log"
OPERATION = "calendar_econ_sweep_values"

logger = logging.getLogger("calendar_sweep_values")


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
        "--start-year", type=int, default=None,
        help="Override the start_year window (default: current_year − 1).",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Override the end_year window (default: current_year).",
    )
    parser.add_argument(
        "--start-period", default=None,
        help="Optional SDMX period (ECB / Eurostat).",
    )
    parser.add_argument(
        "--end-period", default=None,
        help="Optional SDMX period (ECB / Eurostat).",
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
        if args.start_year is not None:
            op_args["start_year"] = args.start_year
        if args.end_year is not None:
            op_args["end_year"] = args.end_year
        if args.start_period is not None:
            op_args["start_period"] = args.start_period
        if args.end_period is not None:
            op_args["end_period"] = args.end_period
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
        logger.exception("value sweep entry-point crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
