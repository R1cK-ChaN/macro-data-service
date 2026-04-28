#!/usr/bin/env python3
"""Resumable historical backfill for the EODHD corp calendar — issue #62.

Walks one subtype at a time across the configured phases (recent / mid /
early), persisting progress in ``cal_corp_backfill_cursor`` so a budget
breach or 429 storm during a multi-month run does not start over from
scratch on the next invocation.

Operator usage::

    PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \\
        --subtype split --phase early --max-requests 100

    # Resume the same phase next day; the cursor advances only past
    # windows that completed.
    PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \\
        --subtype split --phase early --max-requests 100

    # Discovery + dividend-detail two-stage pass:
    PYTHONPATH=src python3 scripts/backfill_corp_calendar.py \\
        --subtype dividend --phase recent --max-requests 200

Drives the runner via the service op ``calendar_corp_backfill`` so the
HTTP entry point and the script share a single code path.

Exit codes:

- 0 on successful run (any per-window throttle is reflected in the log
  but does not fail the script).
- 1 on unhandled exception.
- 2 on argument errors.
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

LOG_FILENAME = "backfill_corp_calendar.log"
OPERATION = "calendar_corp_backfill"

logger = logging.getLogger("backfill_corp_calendar")


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--subtype", required=True,
        choices=["earnings", "ipo", "split", "dividend"],
        help="Corp calendar subtype to backfill (earnings_trend is forward-only).",
    )
    parser.add_argument(
        "--phase", action="append", default=None,
        choices=["recent", "mid", "early"],
        help="Phase(s) to run; repeat to combine. Default: all three.",
    )
    parser.add_argument(
        "--from", dest="from_date", default=None,
        help="Optional ISO floor; clipped to the phase span.",
    )
    parser.add_argument(
        "--to", dest="to_date", default=None,
        help="Optional ISO ceiling; clipped to the phase span.",
    )
    parser.add_argument(
        "--max-requests", type=int, default=100,
        help="Hard cap on EODHD calls per invocation.",
    )
    parser.add_argument(
        "--window-days", type=int, default=7,
        help="Window slice width in days (default 7).",
    )
    parser.add_argument(
        "--symbols", nargs="+", default=None,
        help="Optional ticker filter (e.g. AAPL.US MSFT.US). "
             "Earnings/splits/dividends only.",
    )
    parser.add_argument(
        "--no-dividend-details", action="store_true",
        help="Skip the per-ticker dividend detail enrichment pass.",
    )
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
        help="Plan windows + show cursor state; no HTTP, no writes.",
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
        "subtype": args.subtype,
        "phase": args.phase,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run": args.dry_run,
    }

    try:
        svc = LocalMacroDataService(store=SQLiteEngineStore(db_path=engine_db))
        op_args: dict = {
            "subtype": args.subtype,
            "dry_run": args.dry_run,
            "max_requests": args.max_requests,
            "window_days": args.window_days,
            "enrich_dividend_details": not args.no_dividend_details,
        }
        if args.phase:
            op_args["phases"] = list(args.phase)
        if args.from_date:
            op_args["from"] = args.from_date
        if args.to_date:
            op_args["to"] = args.to_date
        if args.symbols:
            op_args["symbols"] = list(args.symbols)

        result = svc.invoke(OPERATION, op_args)

        summary["status"] = "ok" if "error" not in result else "error"
        summary.update({
            "phases_planned":   result.get("phases_planned"),
            "windows_planned":  result.get("windows_planned"),
            "requests_spent":   result.get("requests_spent"),
            "rows_parsed":      result.get("rows_parsed"),
            "rows_raw_inserted": result.get("rows_raw_inserted"),
            "events_upserted":  result.get("events_upserted"),
            "parse_errors":     result.get("parse_errors"),
            "stopped_reason":   result.get("stopped_reason"),
            "cursor_state":     result.get("cursor_state"),
            "dividend_detail_symbols": result.get("dividend_detail_symbols"),
        })
        if "error" in result:
            summary["error"] = result["error"]
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)

        # Stdout one-liner for systemd journal / ad-hoc operator runs.
        print(json.dumps({
            k: summary[k] for k in (
                "subtype", "phase", "windows_planned", "requests_spent",
                "rows_raw_inserted", "events_upserted", "stopped_reason",
            )
        }, sort_keys=True))
        return 0 if summary["status"] == "ok" else 1

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("corp calendar backfill crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
