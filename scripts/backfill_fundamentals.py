#!/usr/bin/env python3
"""Universe-bounded fundamentals backfill — issue #68 slice 3.

Pulls one ``/api/fundamentals/{TICKER}.{EX}`` snapshot per ticker
across the seeded universes (``_tiingo_universe.py`` +
``_eodhd_universe.py``) and writes raw + projection rows via the
``fundamentals_fetch`` service op. Default: all 17 names that map to
EODHD fundamentals (11 Tiingo ETFs flagged by the suffix
inference + 6 EODHD non-FX/crypto/commodity entries). FX / crypto /
spot-metal / index entries are skipped — they don't ship financial
statements; an ETF entry returns an empty ``Financials`` block but
still lands a ``General`` row plus the raw audit trail, so leaving
them in the default keeps the audit lane representative.

Operator usage::

    # Dry run (default) — print the plan, no HTTP.
    PYTHONPATH=src python3 scripts/backfill_fundamentals.py

    # Live run — bounded budget.
    PYTHONPATH=src python3 scripts/backfill_fundamentals.py \\
        --execute --max-requests 25

    # Ad-hoc tickers (override universe walk).
    PYTHONPATH=src python3 scripts/backfill_fundamentals.py \\
        --execute --tickers AAPL.US MSFT.US

Exit codes:

- 0 on success.
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

from ingestion.market._eodhd_universe import EODHD_GLOBAL_UNIVERSE  # noqa: E402
from ingestion.market._tiingo_universe import (  # noqa: E402
    TIINGO_MACRO_ETF_UNIVERSE,
)
from macro_data.service import LocalMacroDataService  # noqa: E402
from storage import SQLiteEngineStore, default_engine_db_path  # noqa: E402

LOG_FILENAME = "backfill_fundamentals.log"
OPERATION = "fundamentals_fetch"

logger = logging.getLogger("backfill_fundamentals")

# EODHD fundamentals returns a ``Financials`` block only for issuers
# (equities, ETFs to a lesser degree). FX / crypto / spot metals /
# pure indices ship a different shape (or empty); those entries are
# filtered out at plan time so we don't burn quota on guaranteed-
# empty payloads. ``equity_etf`` and ``index`` stay in the default —
# their snapshots are still useful for the raw audit lane and the
# General projection (issuer name, sector, listing exchange) even
# when Financials is absent.
DEFAULT_ASSET_CLASSES: frozenset[str] = frozenset({
    "equity",
    "equity_etf",
    "bond_etf",
    "commodity_etf",
    "index",
})


def _append_log(log_path: Path, payload: dict) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as fh:
        fh.write(json.dumps(payload, sort_keys=True) + "\n")


def _resolve_universe_tickers(
    *, asset_classes: frozenset[str],
) -> list[str]:
    """Walk both seeded universes; return EODHD-shaped tickers.

    Tiingo entries are US listings, so the EODHD ticker is
    ``{ticker}.US``. EODHD entries already carry their ``eodhd_ticker``
    suffix verbatim.
    """
    tickers: list[str] = []
    for entry in TIINGO_MACRO_ETF_UNIVERSE:
        if entry.asset_class in asset_classes:
            tickers.append(f"{entry.ticker}.US")
    for entry in EODHD_GLOBAL_UNIVERSE:
        if entry.asset_class in asset_classes:
            tickers.append(entry.eodhd_ticker)
    # de-duplicate while preserving first-seen order.
    seen: set[str] = set()
    deduped: list[str] = []
    for t in tickers:
        if t not in seen:
            deduped.append(t)
            seen.add(t)
    return deduped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tickers", nargs="+", default=None,
        help="Override the universe-derived ticker list (e.g. AAPL.US MSFT.US).",
    )
    parser.add_argument(
        "--asset-classes", nargs="+", default=None,
        help=(
            "Universe asset-class filter; defaults to "
            f"{sorted(DEFAULT_ASSET_CLASSES)}. Pass space-separated to override."
        ),
    )
    parser.add_argument(
        "--max-requests", type=int, default=25,
        help="Hard cap on EODHD calls per invocation (default 25).",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually call EODHD; default is a dry-run plan-only pass.",
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
        "--verbose", "-v", action="store_true",
        help="Log to stderr at DEBUG.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.tickers:
        tickers = list(args.tickers)
    else:
        if args.asset_classes:
            asset_classes = frozenset(c.strip().lower() for c in args.asset_classes)
        else:
            asset_classes = DEFAULT_ASSET_CLASSES
        tickers = _resolve_universe_tickers(asset_classes=asset_classes)
    if not tickers:
        print("error: no tickers to fetch", file=sys.stderr)
        return 2

    engine_db = args.db_path or default_engine_db_path()
    log_path = args.log_path or (engine_db.parent / "logs" / LOG_FILENAME)

    summary: dict = {
        "operation":  OPERATION,
        "tickers":    tickers,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dry_run":    not args.execute,
    }

    try:
        svc = LocalMacroDataService(store=SQLiteEngineStore(db_path=engine_db))
        op_args: dict = {
            "tickers":      tickers,
            "dry_run":      not args.execute,
            "max_requests": args.max_requests,
        }
        result = svc.invoke(OPERATION, op_args)
        # ``error`` covers argument-validation failures; everything past
        # that is per-ticker. Success requires at least one ticker to
        # have fetched AND parsed cleanly — ``tickers_fetched`` alone
        # is incremented before the parse step, so an all-parse-fail
        # batch (HTTP 200 + crashing parser) would otherwise pass an
        # ``ok`` to the systemd timer with zero projection writes
        # (Codex review #68 S3 R2 P2). Idempotent re-runs are still
        # ok — same content_hash skips the raw insert but parse
        # succeeds, so ``parse_errors < tickers_fetched`` holds.
        if "error" in result:
            summary["status"] = "error"
        elif not args.execute:
            summary["status"] = "ok"
        else:
            fetched = int(result.get("tickers_fetched") or 0)
            parse_errors = int(result.get("parse_errors") or 0)
            if fetched > 0 and parse_errors < fetched:
                summary["status"] = "ok"
            else:
                summary["status"] = "error"
                summary["error"] = (
                    f"execute-mode run wrote zero tickers; "
                    f"fetched={fetched} parse_errors={parse_errors} "
                    f"errors={result.get('errors')}"
                )
        summary.update({
            "tickers_planned":      result.get("tickers_planned"),
            "tickers_fetched":      result.get("tickers_fetched"),
            "tickers_skipped_error": result.get("tickers_skipped_error"),
            "requests_spent":       result.get("requests_spent"),
            "raw_inserted":         result.get("raw_inserted"),
            "company_upserted":     result.get("company_upserted"),
            "financials_upserted":  result.get("financials_upserted"),
            "highlights_upserted":  result.get("highlights_upserted"),
            "parse_errors":         result.get("parse_errors"),
            "stopped_reason":       result.get("stopped_reason"),
            "errors":               result.get("errors"),
        })
        if "error" in result:
            summary["error"] = result["error"]
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)

        print(json.dumps({
            k: summary[k] for k in (
                "tickers_fetched", "requests_spent",
                "raw_inserted", "company_upserted",
                "financials_upserted", "highlights_upserted",
                "stopped_reason",
            )
            if summary.get(k) is not None
        }, sort_keys=True))
        return 0 if summary["status"] == "ok" else 1

    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = repr(exc)
        summary["traceback"] = traceback.format_exc()
        summary["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _append_log(log_path, summary)
        logger.exception("fundamentals backfill crashed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
