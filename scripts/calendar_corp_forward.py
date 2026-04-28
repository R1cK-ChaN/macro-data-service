#!/usr/bin/env python3
"""Daily forward-window sweep of the EODHD corp calendar — issue #63.

Drives the ``calendar_corp_forward_sweep`` service op so a freshly
announced earnings / IPO / split / dividend event lands in
``cal_corp_event`` within a day of upstream publication. Designed to be
wrapped by ``scripts/calendar_corp_forward_wrapper.sh`` and driven by
the ``calendar-corp-forward.timer`` systemd unit (daily, 22:00 ET).

Per-subtype exceptions are isolated by the underlying op — a transient
EODHD 5xx, parse failure, or budget halt on one subtype does not abort
the others. ``earnings_trend`` is symbol-scoped and depends on a
watchlist source-of-truth, deferred.

Exit codes:

- 0 on a fully successful run.
- 1 on unhandled exception or any failed subtype.
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

LOG_FILENAME = "calendar_corp_forward.log"
OPERATION = "calendar_corp_forward_sweep"

logger = logging.getLogger("calendar_corp_forward")


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days", type=int, default=7,
        help="Backward window in days (default 7).",
    )
    parser.add_argument(
        "--lookforward-days", type=int, default=90,
        help="Forward window in days (default 90).",
    )
    parser.add_argument(
        "--max-requests", type=int, default=30,
        help="Per-subtype request cap (default 30).",
    )
    parser.add_argument(
        "--window-days", type=int, default=7,
        help="Window slice width in days (default 7).",
    )
    parser.add_argument(
        "--subtypes", nargs="+", default=None,
        help="Optional subset of subtypes (default: all).",
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
        help="Plan windows only — no HTTP, no DB writes.",
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
        "operation":   OPERATION,
        "started_at":  dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run":     args.dry_run,
    }

    try:
        svc = LocalMacroDataService(store=SQLiteEngineStore(db_path=engine_db))
        op_args: dict = {
            "dry_run":          args.dry_run,
            "lookback_days":    args.lookback_days,
            "lookforward_days": args.lookforward_days,
            "max_requests":     args.max_requests,
            "window_days":      args.window_days,
        }
        if args.subtypes:
            op_args["subtypes"] = list(args.subtypes)

        result = svc.invoke(OPERATION, op_args)

        # An ``error`` key on the top-level envelope (e.g. unknown subtype,
        # store missing get_connection) means the sweep never started — no
        # ``failed_count`` is reported, so the failure must be derived from
        # the error key itself rather than from a missing default.
        had_error = bool(result.get("error")) or int(result.get("failed_count") or 0) > 0
        summary["status"] = "error" if had_error else "ok"
        summary.update({
            "from":             result.get("from"),
            "to":               result.get("to"),
            "ok_count":         result.get("ok_count"),
            "failed_count":     result.get("failed_count"),
            "events_upserted":  result.get("events_upserted"),
            "requests_spent":   result.get("requests_spent"),
            "results":          result.get("results", []),
        })
        if "error" in result:
            summary["error"] = result["error"]
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)

        # Per-subtype journald-friendly one-liners (one log line per subtype
        # per run, per acceptance criterion 3 of issue #63).
        for sub in summary.get("results", []):
            print(json.dumps({
                "operation":       OPERATION,
                "subtype":         sub.get("subtype"),
                "ok":              sub.get("ok"),
                "requests_spent":  sub.get("requests_spent"),
                "events_upserted": sub.get("events_upserted"),
                "stopped_reason":  sub.get("stopped_reason") or sub.get("error"),
            }, sort_keys=True))
        return 0 if summary["status"] == "ok" else 1

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("corp calendar forward sweep crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
